# -*- coding: utf-8 -*-
import json
import logging
import os
import time

import psycopg2
from cachetools import TTLCache
from kafka import KafkaProducer

from baskervillehall.block_guard import guard_command_name
from baskervillehall.incident_first_responder import (
    FINGERPRINT_TARGET_PREFIX, is_browser_ua, is_verifiable_crawler_ua,
)


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

        # Manual stopgap for a long attack by browser UAs that pass the challenge (tapol.org,
        # Nov 2026): {"host": ["exact UA", ...]} -> block_ip on the first request.
        # Note: banjax block_ip applies to every site on the edge.
        self._static_ua_blocks = {
            h: set(uas) for h, uas in
            json.loads(os.environ.get('SESSION_STATIC_UA_BLOCKS', '') or '{}').items()
        }
        self._static_ua_block_ttl = int(os.environ.get('SESSION_STATIC_UA_BLOCK_TTL', '3600'))
        self._static_blocked_ips = TTLCache(maxsize=200_000, ttl=self._static_ua_block_ttl)
        if self._static_ua_blocks:
            self.logger.warning(
                f"[FirstResponderBlocker] static UA blocks: {self._static_ua_blocks} "
                f"ttl={self._static_ua_block_ttl}s"
            )

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

            new_actions: dict = {}
            seen_hosts: set = set()
            for host, action, target_str in rows:
                if host in seen_hosts:
                    continue  # take the most recent action per host
                seen_hosts.add(host)  # mark as seen regardless of action type
                # monitor_only / raise_threshold cancel any previous block action.
                # Tool/bot UAs of block_ua are enforced natively by banjax. Here:
                # - verifiable crawler UAs (Googlebot, Bingbot...): only from unverified IPs;
                # - block_ua_fingerprint: browser UAs only together with the attack's fingerprints.
                if action not in ('block_asn', 'block_country', 'block_ua', 'block_ua_fingerprint',
                                  'challenge_ua', 'challenge_ua_fingerprint'):
                    continue
                targets = {t.strip() for t in (target_str or '').split('|') if t.strip()}
                fingerprints = {t[len(FINGERPRINT_TARGET_PREFIX):] for t in targets
                                if t.startswith(FINGERPRINT_TARGET_PREFIX)}
                targets -= {FINGERPRINT_TARGET_PREFIX + fp for fp in fingerprints}
                crawlers = set()
                if action in ('block_ua', 'block_ua_fingerprint', 'challenge_ua', 'challenge_ua_fingerprint'):
                    crawlers = {t for t in targets if is_verifiable_crawler_ua(t)}
                    targets = {t for t in targets if is_browser_ua(t)} if fingerprints else set()
                if targets or crawlers:
                    new_actions[host] = {'action': action, 'target': targets,
                                         'fingerprints': fingerprints, 'crawlers': crawlers}

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
            conn.close()

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

    def process(self, host, ip, country, asn_name, survey_country, dnet='-', ua='', fingerprint='',
                verified_bot=False):
        """
        Returns True if the request matches an active block action and a block_ip
        command has been (or was already) issued.  The session pipeline should then
        skip further processing of this request.

        verified_bot: True for crawlers verified by IP and GlobalAccessGranted (never blocked),
        None when verification was skipped (lag) — then crawler UAs are not blocked either.
        """
        if verified_bot:
            return False

        if ua and ua in self._static_ua_blocks.get(host, ()):
            if ip not in self._static_blocked_ips:
                self._static_blocked_ips[ip] = True
                self._send_block(host, ip, country, asn_name, 'static_ua_block', dnet, ua,
                                 ttl=self._static_ua_block_ttl)
            return True

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
        command_name = 'block_ip'

        if ua and verified_bot is False and ua in ra.get('crawlers', ()):
            # Crawler UA from an IP that is not the crawler's: a fake
            hit = True
            action = 'block_ua_unverified_crawler'
        elif action in ('block_ua_fingerprint', 'challenge_ua_fingerprint'):
            # Browser UA shared by real users: only together with the attack's fingerprint,
            # and challenge rather than block — mobile carrier IPs are shared by many people.
            if ua and ua in target and fingerprint and fingerprint in ra['fingerprints']:
                hit = True
                command_name = 'challenge_ip'
        elif action in ('block_ua', 'challenge_ua'):
            pass
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
        self._send_block(host, ip, country, asn_name, action, dnet, ua, command_name)
        return True

    # ------------------------------------------------------------------
    # Kafka
    # ------------------------------------------------------------------

    def _send_block(self, host, ip, country, asn_name, action, dnet, ua='', command_name='block_ip',
                    ttl=300):
        command_name = guard_command_name(command_name)
        meta = f'first_responder [{action}] [{command_name}] [session_pipeline]'
        command = {
            'Name': command_name,
            'Value': ip,
            'host': host,
            'country': country,
            'dnet': dnet,
            'source': meta,
            'meta': meta,
            'ttl': ttl,
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
            f'[FirstResponderBlocker] {command_name} ip={ip} host={host} '
            f'country={country} asn={asn_name} ua={ua!r} action={action}'
        )
