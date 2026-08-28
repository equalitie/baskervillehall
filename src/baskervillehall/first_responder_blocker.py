# -*- coding: utf-8 -*-
import json
import logging
import time

import psycopg2
from cachetools import TTLCache
from kafka import KafkaProducer


class FirstResponderBlocker:
    """
    Session-pipeline-level blocker driven by first_responder_actions.

    Polls Postgres every `check_interval` seconds for active LLM-issued actions
    (block_asn / block_country / block_ua).  For each incoming web request, if the
    request's ASN name, country, or User-Agent matches an active action for that host,
    emits a block_ip command to Kafka immediately — without waiting for a full session
    to be built.

    This covers IPs that only hit static files and never produce a session, so the
    predictor never sees them.
    """

    def __init__(
            self,
            postgres_connection=None,
            kafka_connection=None,
            kafka_connection_output=None,
            topic_commands='banjax_command_topic',
            dnet_partition_map=None,
            check_interval=60,
            block_ttl=300,
            logger=None,
    ):
        self.postgres_connection = postgres_connection or {}
        self.topic_commands = topic_commands
        self.dnet_partition_map = dnet_partition_map or {}
        self.check_interval = check_interval
        self.logger = logger or logging.getLogger(__name__)

        # host → {'action': str, 'target': set[str], 'survey_country': str}
        self._actions: dict = {}
        # host → set of blocked SHA256 fingerprint hashes
        self._fingerprints: dict = {}
        self._last_refresh: float = 0.0

        # Per-IP dedup cache: avoids re-sending block_ip on every request from same IP
        self._blocked_ips = TTLCache(maxsize=100_000, ttl=block_ttl)

        self._producer_output = KafkaProducer(**kafka_connection_output) if kafka_connection_output else None
        self._producer = KafkaProducer(**kafka_connection) if kafka_connection else None

    # ------------------------------------------------------------------
    # Refresh
    # ------------------------------------------------------------------

    def refresh(self):
        """Call once per poll loop (TTL-gated)."""
        now = time.monotonic()
        if now - self._last_refresh < self.check_interval:
            return
        self._last_refresh = now
        self._refresh_from_postgres()

    def _refresh_from_postgres(self):
        try:
            conn = psycopg2.connect(**self.postgres_connection)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT host, action, target
                    FROM first_responder_actions
                    WHERE expires_at > NOW()
                    ORDER BY created_at DESC
                """)
                rows = cur.fetchall()
            conn.close()

            new_actions: dict = {}
            seen_hosts: set = set()
            for host, action, target_str in rows:
                if host in seen_hosts:
                    continue  # take the most recent action per host
                seen_hosts.add(host)  # mark as seen regardless of action type
                # monitor_only / raise_threshold cancel any previous block action
                if action not in ('block_asn', 'block_country', 'block_ua'):
                    continue
                targets = {t.strip() for t in (target_str or '').split('|') if t.strip()}
                if targets:
                    new_actions[host] = {'action': action, 'target': targets}

            if new_actions != self._actions:
                if new_actions:
                    for h, ra in new_actions.items():
                        self.logger.warning(
                            f"[FirstResponderBlocker] active: host={h} action={ra['action']} "
                            f"target={ra['target']}"
                        )
                else:
                    self.logger.info("[FirstResponderBlocker] no active actions")
            self._actions = new_actions

            # Load blocked TLS fingerprints (set by first_responder when uniformity >= 70%)
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT host, fingerprint FROM blocked_fingerprints
                    WHERE expires_at > NOW()
                """)
                fp_rows = cur.fetchall()

            new_fingerprints: dict = {}
            for host, fp in fp_rows:
                new_fingerprints.setdefault(host, set()).add(fp)

            added_fps = sum(len(v) for v in new_fingerprints.values()) - sum(len(v) for v in self._fingerprints.values())
            if added_fps > 0:
                self.logger.warning(
                    f"[FirstResponderBlocker] blocked fingerprints: "
                    f"{sum(len(v) for v in new_fingerprints.values())} active across "
                    f"{len(new_fingerprints)} hosts"
                )
            self._fingerprints = new_fingerprints

        except Exception as e:
            self.logger.error(f"[FirstResponderBlocker] Postgres refresh failed: {e!r}")

    # ------------------------------------------------------------------
    # Per-request check
    # ------------------------------------------------------------------

    def process(self, host, ip, country, asn_name, survey_country, dnet='-', ua='', fingerprint=''):
        """
        Returns True if the request matches an active block action and a block_ip
        command has been (or was already) issued.  The session pipeline should then
        skip further processing of this request.
        """
        # Fingerprint check first — most precise signal, no collateral damage.
        # Catches 1-req-per-IP bots that never build a full session (invisible to predictor).
        if fingerprint and fingerprint in self._fingerprints.get(host, set()):
            if ip not in self._blocked_ips:
                self._blocked_ips[ip] = True
                self._send_block(host, ip, country, asn_name, 'block_fingerprint', dnet, ua)
            return True

        ra = self._actions.get(host)
        if ra is None:
            return False

        action = ra['action']
        target = ra['target']
        hit = False

        if action == 'block_ua':
            # UA matching: exact string match against target list
            if ua and ua in target:
                hit = True
        else:
            # Survey-country protection: never block the site's primary audience
            if survey_country and country and country == survey_country:
                return False

            if action == 'block_asn' and asn_name and asn_name in target:
                hit = True
            elif action == 'block_country' and country and country in target:
                hit = True

        if not hit:
            return False

        # Dedup: if IP already blocked, just return True (suppress session building)
        if ip in self._blocked_ips:
            return True

        self._blocked_ips[ip] = True
        self._send_block(host, ip, country, asn_name, action, dnet, ua)
        return True

    # ------------------------------------------------------------------
    # Kafka
    # ------------------------------------------------------------------

    def _send_block(self, host, ip, country, asn_name, action, dnet, ua=''):
        meta = f'first_responder [{action}] [block_ip] [session_pipeline]'
        command = {
            'Name': 'block_ip',
            'Value': ip,
            'host': host,
            'country': country,
            'dnet': dnet,
            'source': meta,
            'meta': meta,
            'ttl': 300,
        }
        value = json.dumps(command).encode('utf-8')
        key = bytearray(host, encoding='utf-8')
        partition = self.dnet_partition_map.get(dnet, -1)

        if self._producer is not None:
            self._producer.send(topic=self.topic_commands, value=value, key=key)
        if partition < 0:
            if self._producer_output is not None:
                self._producer_output.send(topic=self.topic_commands, value=value, key=key)
        else:
            if self._producer_output is not None:
                self._producer_output.send(topic=self.topic_commands, value=value, partition=partition)

        self.logger.warning(
            f'[FirstResponderBlocker] block_ip ip={ip} host={host} '
            f'country={country} asn={asn_name} ua={ua!r} action={action}'
        )
