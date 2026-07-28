import json
import logging
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import requests
from kafka import KafkaProducer

# Hard-coded blocklist: these ASNs must NEVER be blocked regardless of LLM output.
PROTECTED_ASNS = {
    "Google LLC",
    "Cloudflare, Inc.",
    "Cloudflare Inc.",
    "Amazon.com, Inc.",
    "Amazon Technologies Inc.",
    "Microsoft Corporation",
    "Akamai Technologies",
    "Akamai Connected Cloud",
    "Fastly, Inc.",
    "Fastly Inc.",
    "Meta Platforms, Inc.",
    "Apple Inc.",
    "Twitter Inc.",
    "Oracle Corporation",
    "Wikimedia Foundation",
    "AT&T Enterprises, LLC",
    "Charter Communications Inc",
    "Charter Communications LLC",
    "Comcast Cable Communications, LLC",
    "Verizon Business",
    "Deutsche Telekom AG",
    "Space Exploration Technologies Corporation",
    "Space Exploration Technologies Corp.",
    "Space Exploration Technologies Corporati",
}

CREATE_FIRST_RESPONDER_TABLES_SQL = """
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS first_responder_processed BOOLEAN DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS first_responder_actions (
    id           BIGSERIAL PRIMARY KEY,
    incident_id  BIGINT NOT NULL REFERENCES incidents(id),
    host         TEXT NOT NULL,
    action       TEXT NOT NULL,
    target       TEXT NOT NULL DEFAULT '',
    confidence   TEXT NOT NULL,
    reasoning    TEXT,
    ttl_minutes  INTEGER NOT NULL DEFAULT 30,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at   TIMESTAMPTZ NOT NULL,
    applied      BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS first_responder_actions_host_expires
    ON first_responder_actions (host, expires_at DESC);
"""

NARRATIVE_SYSTEM_PROMPT = """You are a security analyst writing a brief incident summary for a report.
Write exactly 2-3 sentences in plain English. Be specific: include the site name, start time (UTC),
spike magnitude, duration, attack origin (top countries/ASNs), AI response action, and whether
traffic returned to normal. Do not use bullet points or headers — just flowing prose."""

SYSTEM_PROMPT = """You are a DDoS mitigation expert. You receive attack statistics and normal traffic
baseline for a website and must recommend a mitigation action.

Respond with a JSON object only — no explanation outside the JSON.

Output format:
{{
  "action": "block_country" | "block_asn" | "block_ua" | "raise_threshold" | "monitor_only",
  "target": ["CN", "RU"] | ["Hetzner Online GmbH", "OVH SAS"] | ["python-requests/2.31.0", "curl/7.81.0"] | [],
  "confidence": "high" | "medium" | "low",
  "ttl_minutes": 30,
  "reasoning": "brief explanation"
}}

Decision rules (apply in order):
1. Prefer block_asn over block_country — it causes less collateral damage.
2. Use block_asn if datacenter ASNs account for the majority of the attack.
3. Use block_country if a country accounts for >{min_attack_pct}% of the attack AND
   represents <{max_normal_pct}% of the site's normal traffic.
   Never include more than 3 countries in a single block_country action.
   If more than 3 countries would need to be blocked to cover the attack, use raise_threshold instead —
   blocking many countries causes excessive collateral damage and is a sign of a diffuse attack.
4. *** ABSOLUTE HARD RULE — NO EXCEPTIONS WHATSOEVER ***
   NEVER include survey_country in "target", under ANY circumstances.
   survey_country is the site's PRIMARY AUDIENCE — the real users you are protecting.
   If the attack appears to come from survey_country, it means attackers are SPOOFING or
   ROUTING THROUGH that country, not that the country should be blocked.
   Blocking survey_country would take the site offline for its intended users — this is
   WORSE than the attack itself. If survey_country dominates the attack, use raise_threshold
   or monitor_only instead. There are NO exceptions to this rule.
5. Use monitor_only if blocking would affect >{max_normal_pct}% of normal traffic
   (collateral damage too high).
6. Use raise_threshold as a fallback when the attack is diffuse across many sources.
7. Set confidence=high only when a single pattern dominates (>70% of attack).
8. ttl_minutes should be 30 unless there is a strong reason to differ.
9. *** ABSOLUTE HARD RULE — NO EXCEPTIONS WHATSOEVER ***
   NEVER put any of the following ASNs in "target", under any circumstances,
   regardless of their attack share percentage:
     "Google LLC", "Cloudflare, Inc.", "Cloudflare Inc.", "Amazon.com, Inc.",
     "Amazon Technologies Inc.", "Microsoft Corporation", "Akamai Technologies",
     "Akamai Connected Cloud", "Fastly, Inc.", "Fastly Inc.",
     "Meta Platforms, Inc.", "Apple Inc.", "Twitter Inc.", "Oracle Corporation",
     "Wikimedia Foundation", "AT&T Enterprises, LLC", "Charter Communications Inc",
     "Charter Communications LLC", "Comcast Cable Communications, LLC",
     "Verizon Business", "Deutsche Telekom AG",
     "Space Exploration Technologies Corporation", "Space Exploration Technologies Corp.".
   These are major internet infrastructure providers. Their IPs appear in attack
   traffic because attackers ROUTE THROUGH them, not because they are attackers.
   Blocking them would break the internet for millions of legitimate users.
   If these ASNs dominate the attack and no other clear target exists,
   use raise_threshold or monitor_only — NEVER block them.
10. If the attack is spread across more than 10 distinct ASNs with no single ASN exceeding
    {min_attack_pct}% of the attack, use raise_threshold — targeted blocking is ineffective
    and causes excessive collateral damage. Do NOT switch to block_country as a workaround
    when ASN blocking fails — a diffuse ASN attack is also a diffuse country attack.
11. For block_asn: only include ASNs that appear in the attack AND have <{max_normal_pct}%
    in the normal traffic baseline. If an ASN has significant normal traffic, skip it.
    Never include more than 3 ASNs in a single block_asn action.
12. Use block_ua when one or more User Agents account for a large share of the attack AND
    have <1% in normal traffic. Put the exact UA strings in "target". This works for any UA —
    scripted (python-requests, curl) or disguised (fake browser strings unique to the attack).
    Set confidence=high if the top UA alone exceeds {min_attack_pct}% of attack traffic.
13. If the attack concentrates on URLs that are NOT in the site's normal top URLs (0% normal
    traffic) — especially /wp-admin/, /xmlrpc.php, /api/ endpoints — this strengthens
    confidence in the block recommendation.
14. URL and UA data is from sessions only (static-file-only bots are not included).
    Do not use absence of URL/UA data as a reason to downgrade confidence if ASN/country
    data is already conclusive.
"""


class IncidentFirstResponder:
    def __init__(
            self,
            postgres_connection=None,
            ollama_url='http://localhost:11434',
            llm_model='qwen2.5:7b',
            check_interval=30,
            min_attack_pct=50.0,
            max_normal_pct=20.0,
            ttl_minutes=30,
            min_spike_ratio=3.0,
            min_challenge_count=50,
            kafka_connection=None,
            kafka_connection_output=None,
            topic_commands='banjax_command_topic',
            dnet_partition_map=None,
            fingerprint_min_pct=40.0,
            fingerprint_max_ips=500,
            host_whitelist=None,
            logger=None,
    ):
        self.postgres_connection = postgres_connection or {}
        self.ollama_url = ollama_url.rstrip('/')
        self.llm_model = llm_model
        self.check_interval = check_interval
        self.min_attack_pct = min_attack_pct
        self.max_normal_pct = max_normal_pct
        self.ttl_minutes = ttl_minutes
        self.min_spike_ratio = min_spike_ratio
        self.min_challenge_count = min_challenge_count
        self.topic_commands = topic_commands
        self.dnet_partition_map = dnet_partition_map or {}
        self.host_whitelist = set(h.strip() for h in (host_whitelist or []) if h.strip())
        self.fingerprint_min_pct = fingerprint_min_pct
        self.fingerprint_max_ips = fingerprint_max_ips
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self._producer = KafkaProducer(**kafka_connection) if kafka_connection else None
        self._producer_output = KafkaProducer(**kafka_connection_output) if kafka_connection_output else None

    # ------------------------------------------------------------------
    # Postgres helpers
    # ------------------------------------------------------------------

    def _pg_connect(self):
        conn = psycopg2.connect(**self.postgres_connection)
        conn.autocommit = True
        return conn

    def _ensure_tables(self, conn):
        with conn.cursor() as cur:
            cur.execute(CREATE_FIRST_RESPONDER_TABLES_SQL)
        self.logger.info("first_responder_actions table ready")

    def _get_unprocessed_incidents(self, conn):
        """Return incidents to process:
        - Active (ended_at IS NULL): always re-analyze every cycle to pick up fresh stats.
        - Recently closed (ended_at within 60 min): process once if not yet processed.
        """
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, host, challenge_count, baseline_avg, spike_ratio,
                       started_at, survey_country, botnet_info, ended_at,
                       avg_api_ratio, avg_path_diversity, behavior_samples,
                       traffic_peak_ratio, traffic_close_ratio, traffic_recent_ratio,
                       traffic_peak_count, traffic_close_count, traffic_recent_count
                FROM incidents
                WHERE started_at > NOW() - INTERVAL '24 hours'
                  AND (
                      ended_at IS NULL
                      OR (first_responder_processed = FALSE AND ended_at > NOW() - INTERVAL '60 minutes')
                  )
                ORDER BY started_at DESC
            """)
            rows = cur.fetchall()
        return [
            {
                'id': r[0],
                'host': r[1],
                'challenge_count': r[2],
                'baseline_avg': float(r[3]),
                'spike_ratio': float(r[4]),
                'started_at': r[5],
                'survey_country': r[6] or '',
                'botnet_info': r[7] or '',
                'ended_at': r[8],
                'avg_api_ratio': float(r[9] or 0.0),
                'avg_path_diversity': float(r[10] or 1.0),
                'behavior_samples': int(r[11] or 0),
                'traffic_peak_ratio': float(r[12] or 0.0),
                'traffic_close_ratio': float(r[13] or 0.0),
                'traffic_recent_ratio': float(r[14] or 0.0),
                'traffic_peak_count': int(r[15] or 0),
                'traffic_close_count': int(r[16] or 0),
                'traffic_recent_count': int(r[17] or 0),
            }
            for r in rows
        ]

    def _get_country_stats(self, conn, incident_id):
        """Attack country distribution from incident_country_stats."""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT country, cmd_count,
                       ROUND(cmd_count * 100.0 / NULLIF(SUM(cmd_count) OVER (), 0), 1) AS pct
                FROM incident_country_stats
                WHERE incident_id = %s
                ORDER BY cmd_count DESC
                LIMIT 15
            """, (incident_id,))
            rows = cur.fetchall()
        return [{'country': r[0], 'count': r[1], 'pct': float(r[2] or 0)} for r in rows]

    def _get_asn_stats(self, conn, incident_id):
        """Attack ASN distribution from incident_asn_stats."""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT asn_name, datacenter, cmd_count,
                       ROUND(cmd_count * 100.0 / NULLIF(SUM(cmd_count) OVER (), 0), 1) AS pct
                FROM incident_asn_stats
                WHERE incident_id = %s
                ORDER BY cmd_count DESC
                LIMIT 15
            """, (incident_id,))
            rows = cur.fetchall()
        return [
            {'asn_name': r[0], 'datacenter': bool(r[1]), 'count': r[2], 'pct': float(r[3] or 0)}
            for r in rows
        ]

    def _get_normal_traffic(self, conn, host):
        """Pre-aggregated 3-day country baseline maintained by StorageSessions."""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT country, pct
                FROM host_country_stats
                WHERE host = %s
                ORDER BY pct DESC
            """, (host,))
            rows = cur.fetchall()
        return {r[0]: float(r[1]) for r in rows}

    def _get_normal_asn_traffic(self, conn, host):
        """Pre-aggregated 3-day ASN baseline maintained by StorageSessions."""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT asn_name, pct
                FROM host_asn_stats
                WHERE host = %s
                ORDER BY pct DESC
            """, (host,))
            rows = cur.fetchall()
        return {r[0]: float(r[1]) for r in rows}

    def _get_url_stats(self, conn, incident_id):
        """Attack URL distribution from incident_url_stats."""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT url,
                       ROUND(cmd_count * 100.0 / NULLIF(SUM(cmd_count) OVER (), 0), 1) AS pct
                FROM incident_url_stats
                WHERE incident_id = %s
                ORDER BY cmd_count DESC
                LIMIT 15
            """, (incident_id,))
            rows = cur.fetchall()
        return [{'url': r[0], 'pct': float(r[1] or 0)} for r in rows]

    def _get_ua_stats(self, conn, incident_id):
        """Attack UA distribution from incident_ua_stats."""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ua,
                       ROUND(cmd_count * 100.0 / NULLIF(SUM(cmd_count) OVER (), 0), 1) AS pct
                FROM incident_ua_stats
                WHERE incident_id = %s
                ORDER BY cmd_count DESC
                LIMIT 10
            """, (incident_id,))
            rows = cur.fetchall()
        return [{'ua': r[0], 'pct': float(r[1] or 0)} for r in rows]

    def _get_normal_url_traffic(self, conn, host):
        """Pre-aggregated URL baseline maintained by StorageSessions."""
        with conn.cursor() as cur:
            cur.execute(
                "SELECT url, pct FROM host_url_stats WHERE host = %s ORDER BY pct DESC",
                (host,)
            )
            rows = cur.fetchall()
        return {r[0]: float(r[1]) for r in rows}

    def _get_normal_ua_traffic(self, conn, host):
        """Pre-aggregated UA baseline maintained by StorageSessions."""
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ua, pct FROM host_ua_stats WHERE host = %s ORDER BY pct DESC",
                (host,)
            )
            rows = cur.fetchall()
        return {r[0]: float(r[1]) for r in rows}

    def _get_fingerprint_stats(self, conn, incident):
        """Top fingerprints accumulated by IncidentDetector during the incident."""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT fingerprint,
                       cmd_count,
                       ROUND(cmd_count * 100.0 / NULLIF(SUM(cmd_count) OVER (), 0), 1) AS pct
                FROM incident_fingerprint_stats
                WHERE incident_id = %s
                ORDER BY cmd_count DESC
                LIMIT 10
            """, (incident['id'],))
            rows = cur.fetchall()
        return [{'fingerprint': r[0], 'count': r[1], 'pct': float(r[2] or 0)} for r in rows]

    def _block_fingerprint_ips(self, conn, incident, fingerprint_hash):
        """
        Find all IPs seen with this fingerprint during the incident and send
        block_ip Kafka commands for each. Bypasses the LLM — fingerprint
        concentration is a structural signal that doesn't need LLM judgment.
        """
        if self._producer is None and self._producer_output is None:
            self.logger.warning("[FIRST_RESPONDER] No Kafka producer — fingerprint IP blocking skipped")
            return 0

        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ip
                FROM sessions
                WHERE host_name = %s
                  AND session_start >= %s
                  AND (%s IS NULL OR session_start <= %s)
                  AND fingerprints = %s
                LIMIT %s
            """, (
                incident['host'],
                incident['started_at'],
                incident['ended_at'],
                incident['ended_at'],
                fingerprint_hash,
                self.fingerprint_max_ips,
            ))
            ips = [r[0] for r in cur.fetchall()]

        host = incident['host']
        dnet = '-'
        meta = f'first_responder [block_fingerprint] [block_ip]'
        blocked = 0
        for ip in ips:
            command = {
                'Name': 'block_ip',
                'Value': ip,
                'host': host,
                'dnet': dnet,
                'meta': meta,
                'ttl': self.ttl_minutes * 60,
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
            blocked += 1

        self.logger.warning(
            f"[FIRST_RESPONDER] block_fingerprint host={host} fingerprint={fingerprint_hash} "
            f"ips_blocked={blocked}"
        )
        return blocked

    def _mark_processed(self, conn, incident_id):
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE incidents SET first_responder_processed = TRUE WHERE id = %s",
                (incident_id,)
            )

    def _save_action(self, conn, incident_id, host, rec):
        ttl = rec.get('ttl_minutes', self.ttl_minutes)
        target_list = rec.get('target', [])
        target_str = '|'.join(str(t) for t in target_list)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO first_responder_actions
                    (incident_id, host, action, target, confidence, reasoning,
                     ttl_minutes, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s,
                        NOW() + INTERVAL '1 minute' * %s)
                RETURNING id
            """, (
                incident_id, host,
                rec['action'], target_str,
                rec['confidence'], f"{rec.get('reasoning', '')} | Blocked: {target_str}",
                ttl, ttl,
            ))
            action_id = cur.fetchone()[0]
        self.logger.warning(
            f"[FIRST_RESPONDER] incident_id={incident_id} host={host} "
            f"action={rec['action']} target={target_str} "
            f"confidence={rec['confidence']} ttl={ttl}min "
            f"action_id={action_id}"
        )
        self.logger.info(f"[FIRST_RESPONDER] reasoning: {rec.get('reasoning', '')}")

    # ------------------------------------------------------------------
    # LLM
    # ------------------------------------------------------------------

    @staticmethod
    def _classify_ua(ua: str) -> str:
        """Return a tag string for known scripted/suspicious UA patterns."""
        ua_lower = ua.lower()
        if any(p in ua_lower for p in ('python-requests', 'python-urllib', 'python/')):
            return '[scripted]'
        if any(p in ua_lower for p in ('curl/', 'wget/', 'httpie/')):
            return '[scripted]'
        if any(p in ua_lower for p in ('go-http-client', 'java/', 'okhttp', 'apache-httpclient')):
            return '[scripted]'
        if any(p in ua_lower for p in ('headlesschrome', 'phantomjs', 'puppeteer')):
            return '[headless browser]'
        if any(p in ua_lower for p in ('msie', 'trident/')):
            return '[suspicious: legacy IE]'
        if not ua.strip():
            return '[empty UA]'
        return ''

    def _build_prompt(self, incident, country_stats, asn_stats, normal_traffic,
                      normal_asn_traffic=None, url_stats=None, ua_stats=None,
                      normal_url_traffic=None, normal_ua_traffic=None,
                      fingerprint_stats=None):
        host = incident['host']
        survey_country = incident['survey_country']
        normal_asn_traffic = normal_asn_traffic or {}
        url_stats = url_stats or []
        ua_stats = ua_stats or []
        normal_url_traffic = normal_url_traffic or {}
        normal_ua_traffic = normal_ua_traffic or {}
        fingerprint_stats = fingerprint_stats or []

        traffic_peak = incident.get('traffic_peak_ratio', 0.0)
        traffic_close = incident.get('traffic_close_ratio', 0.0)
        traffic_recent = incident.get('traffic_recent_ratio', 0.0)
        traffic_peak_count = incident.get('traffic_peak_count', 0)
        traffic_close_count = incident.get('traffic_close_count', 0)
        traffic_recent_count = incident.get('traffic_recent_count', 0)

        lines = [
            f"=== ATTACK on {host} ===",
            f"Started: {incident['started_at']}",
            f"Challenge count: {incident['challenge_count']}  "
            f"Baseline avg: {incident['baseline_avg']:.1f}  "
            f"Spike ratio: {incident['spike_ratio']:.1f}x",
            f"Site primary audience (survey_country): {survey_country or 'unknown'}",
        ]

        if traffic_peak > 0.0:
            peak_str = f"peak={traffic_peak:.1f}x"
            if traffic_peak_count > 0:
                peak_str += f" ({traffic_peak_count} req/min)"
            if incident.get('ended_at') is not None and traffic_close > 0.0:
                close_str = f"at close={traffic_close:.1f}x"
                if traffic_close_count > 0:
                    close_str += f" ({traffic_close_count} req/min)"
                if traffic_close < 1.5:
                    effectiveness = "✅ traffic returned to normal — mitigation effective"
                elif traffic_close < traffic_peak * 0.5:
                    effectiveness = "⚠ traffic reduced but still elevated"
                else:
                    effectiveness = "❌ traffic remained high — attack may have continued despite blocking"
                lines.append(
                    f"Raw traffic spike: {peak_str} baseline  {close_str}  {effectiveness}"
                )
            else:
                if traffic_recent > 0.0:
                    recent_str = f"  current={traffic_recent:.1f}x"
                    if traffic_recent_count > 0:
                        recent_str += f" ({traffic_recent_count} req/min)"
                else:
                    recent_str = ""
                lines.append(f"Raw traffic spike: {peak_str} baseline{recent_str} (ongoing)")
        else:
            lines.append("Raw traffic spike: not detected (challenge spike only, or traffic monitor warming up)")

        if incident['botnet_info']:
            lines.append(f"Botnet overlap with previous incidents:\n{incident['botnet_info']}")

        behavior_samples = incident.get('behavior_samples', 0)
        if behavior_samples > 0:
            avg_api = incident.get('avg_api_ratio', 0.0)
            avg_path = incident.get('avg_path_diversity', 1.0)
            api_note = " ← HIGH (API endpoint hammering)" if avg_api > 0.7 else ""
            path_note = " ← LOW (few distinct endpoints, parameter iteration)" if avg_path < 0.2 else ""
            lines.append(
                f"\n--- API behavior ({behavior_samples} sessions sampled) ---"
            )
            lines.append(f"  avg_api_ratio:      {avg_api:.2f}{api_note}")
            lines.append(f"  avg_path_diversity: {avg_path:.2f}{path_note}")
            if avg_api > 0.7 and avg_path < 0.2:
                lines.append(
                    "  ⚠ Strong API hammering pattern: bots are repeatedly hitting a small set of "
                    "API endpoints with varying parameters. Consider block_ua if UA is uniform."
                )

        lines.append("\n--- Attack country distribution ---")
        if country_stats:
            for s in country_stats:
                normal_pct = normal_traffic.get(s['country'], 0.0)
                lines.append(
                    f"  {s['country']:4s}  attack: {s['pct']:5.1f}%  "
                    f"normal traffic: {normal_pct:.1f}%"
                )
        else:
            lines.append("  (no country data yet)")

        lines.append("\n--- Attack ASN distribution ---")
        if asn_stats:
            total_asns = len(asn_stats)
            lines.append(f"  Total distinct ASNs in attack: {total_asns}")
            for s in asn_stats:
                dc = " [datacenter]" if s['datacenter'] else ""
                normal_pct = normal_asn_traffic.get(s['asn_name'], 0.0)
                normal_str = f"  normal traffic: {normal_pct:.1f}%" if normal_pct > 0 else "  normal traffic: 0%"
                lines.append(
                    f"  {s['asn_name'][:40]:40s}{dc}  "
                    f"attack: {s['pct']:5.1f}%{normal_str}"
                )
        else:
            lines.append("  (no ASN data yet)")

        lines.append("\n--- Normal traffic baseline (last 3 days) ---")
        if normal_traffic:
            top_normal = sorted(normal_traffic.items(), key=lambda x: -x[1])[:10]
            lines.append("  Countries: " + ", ".join(f"{c}: {p:.1f}%" for c, p in top_normal))
        else:
            lines.append("  (no baseline data — site may be new)")

        if normal_asn_traffic:
            top_asn = sorted(normal_asn_traffic.items(), key=lambda x: -x[1])[:10]
            lines.append("  ASNs: " + ", ".join(f"{a}: {p:.1f}%" for a, p in top_asn))
        else:
            lines.append("  ASNs: (no ASN baseline yet — site may be new)")

        lines.append("\n--- Attack URL distribution ---")
        if url_stats:
            lines.append(f"  Total distinct URLs: {len(url_stats)}")
            for s in url_stats:
                normal_pct = normal_url_traffic.get(s['url'], 0.0)
                note = "  ← NOT in normal top URLs" if normal_pct == 0.0 and normal_url_traffic else ""
                lines.append(
                    f"  {s['url'][:50]:50s}  attack: {s['pct']:5.1f}%  "
                    f"normal traffic: {normal_pct:.1f}%{note}"
                )
        else:
            lines.append("  (no URL data yet — static-file-only sessions not counted)")

        lines.append("\n--- Attack User Agent distribution ---")
        if ua_stats:
            for s in ua_stats:
                normal_pct = normal_ua_traffic.get(s['ua'], 0.0)
                tag = self._classify_ua(s['ua'])
                tag_str = f"  {tag}" if tag else ""
                lines.append(
                    f"  {s['ua'][:60]:60s}  attack: {s['pct']:5.1f}%  "
                    f"normal traffic: {normal_pct:.1f}%{tag_str}"
                )
        else:
            lines.append("  (no UA data yet)")

        if fingerprint_stats:
            lines.append("\n--- TLS/Browser fingerprint distribution (attack sessions) ---")
            lines.append("  (fingerprint = hash of UA + TLS cipher suite order + accept-language)")
            for s in fingerprint_stats:
                dominant = " ← DOMINANT" if s['pct'] >= self.fingerprint_min_pct else ""
                lines.append(f"  {s['fingerprint']}  {s['pct']:5.1f}%  ({s['count']} sessions){dominant}")
            top_pct = fingerprint_stats[0]['pct'] if fingerprint_stats else 0
            if top_pct >= self.fingerprint_min_pct:
                lines.append(
                    f"  ⚠ Single fingerprint covers {top_pct:.1f}% of attack sessions — "
                    f"same tool/library across all IPs. IPs with this fingerprint will be "
                    f"blocked directly (separate from this LLM decision)."
                )

        lines.append(
            f"\nParameters: min_attack_pct={self.min_attack_pct}%, "
            f"max_normal_pct={self.max_normal_pct}%"
        )

        return "\n".join(lines)

    def _call_llm(self, user_prompt):
        system = SYSTEM_PROMPT.format(
            min_attack_pct=self.min_attack_pct,
            max_normal_pct=self.max_normal_pct,
        )
        url = f"{self.ollama_url}/v1/chat/completions"
        payload = {
            "model": self.llm_model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.1,
            "stream": False,
        }
        try:
            resp = requests.post(url, json=payload, timeout=600)
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"].strip()
            # Strip markdown code fences if present
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
            return json.loads(content)
        except Exception as e:
            self.logger.error(f"LLM call failed: {e!r}")
            return None

    def _build_narrative_prompt(self, incident, country_stats, asn_stats, action_rec):
        host = incident['host']
        started_at = incident['started_at'].strftime('%Y-%m-%d %H:%M UTC')
        spike = incident['spike_ratio']
        challenges = incident['challenge_count']
        traffic_peak = incident.get('traffic_peak_ratio', 0.0)
        traffic_close = incident.get('traffic_close_ratio', 0.0)

        # Duration
        if incident.get('ended_at') and incident['started_at']:
            duration_sec = (incident['ended_at'] - incident['started_at']).total_seconds()
            duration_min = int(duration_sec / 60)
            if duration_min >= 60:
                duration_str = f"{duration_min // 60}h {duration_min % 60}min"
            else:
                duration_str = f"{duration_min} min"
        else:
            duration_str = "unknown duration"

        # Top attack sources
        top_countries = ", ".join(
            f"{s['country']} ({s['pct']:.0f}%)" for s in (country_stats or [])[:3]
        ) or "unknown"
        top_asns = ", ".join(
            f"{s['asn_name']} ({s['pct']:.0f}%)" for s in (asn_stats or [])[:3]
        ) or "unknown"

        # Action taken
        if action_rec:
            action = action_rec.get('action', 'monitor_only')
            target = action_rec.get('target', [])
            if action == 'block_country' and target:
                action_str = f"AI blocked countries: {', '.join(target)}"
            elif action == 'block_asn' and target:
                action_str = f"AI blocked ASNs: {', '.join(target)}"
            elif action == 'block_ua' and target:
                action_str = f"AI blocked user agents: {', '.join(target)}"
            elif action == 'raise_threshold':
                action_str = "AI raised the challenge threshold (diffuse attack)"
            else:
                action_str = "AI monitored only (no blocking)"
        else:
            action_str = "no automated response"

        # Traffic outcome
        if traffic_peak > 0 and traffic_close > 0:
            if traffic_close < 1.5:
                outcome = f"traffic returned to normal at close ({traffic_close:.1f}x baseline)"
            elif traffic_close < traffic_peak * 0.5:
                outcome = f"traffic reduced but still elevated at close ({traffic_close:.1f}x baseline)"
            else:
                outcome = f"traffic remained high at close ({traffic_close:.1f}x baseline)"
        elif traffic_peak > 0:
            outcome = f"traffic peak was {traffic_peak:.1f}x baseline"
        else:
            outcome = "traffic monitor data unavailable"

        return (
            f"Write a 2-3 sentence incident summary:\n"
            f"Site: {host}\n"
            f"Start: {started_at}\n"
            f"Spike: ×{spike:.1f}, duration: {duration_str}, challenges issued: {challenges}\n"
            f"Top attack countries: {top_countries}\n"
            f"Top attack ASNs: {top_asns}\n"
            f"Response: {action_str}\n"
            f"Outcome: {outcome}\n"
        )

    def _generate_narrative(self, prompt):
        url = f"{self.ollama_url}/v1/chat/completions"
        payload = {
            "model": self.llm_model,
            "messages": [
                {"role": "system", "content": NARRATIVE_SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.3,
            "stream": False,
        }
        try:
            resp = requests.post(url, json=payload, timeout=600)
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"].strip()
        except Exception as e:
            self.logger.error(f"Narrative LLM call failed: {e!r}")
            return None

    def _save_narrative(self, conn, incident_id, narrative):
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE incidents SET narrative = %s WHERE id = %s",
                (narrative, incident_id)
            )

    def _validate_response(self, rec):
        """Basic validation of LLM response before saving."""
        valid_actions = {'block_country', 'block_asn', 'block_ua', 'raise_threshold', 'monitor_only'}
        valid_confidence = {'high', 'medium', 'low'}
        if not isinstance(rec, dict):
            return False
        if rec.get('action') not in valid_actions:
            return False
        if rec.get('confidence') not in valid_confidence:
            return False
        if not isinstance(rec.get('target', []), list):
            return False
        return True

    def _sanitize_response(self, rec, survey_country=None):
        """
        - Remove protected ASNs from block_asn targets.
        - Never block survey_country (site's primary audience).
        - Enforce max 3 targets for block_asn and block_country.
        - Downgrade to raise_threshold if block_country targets >3 countries.
        """
        action = rec.get('action')

        if action == 'block_country' and survey_country:
            targets = rec.get('target', [])
            if survey_country in targets:
                self.logger.error(
                    f"[FIRST_RESPONDER] LLM tried to block survey_country={survey_country!r} — REMOVED"
                )
                targets = [t for t in targets if t != survey_country]
                rec['target'] = targets
                rec['reasoning'] = (
                    rec.get('reasoning', '') +
                    f' [auto-removed: {survey_country} is the site primary audience and must never be blocked]'
                )
                if not targets:
                    self.logger.warning(
                        f"[FIRST_RESPONDER] block_country had only survey_country as target — "
                        f"downgrading to monitor_only"
                    )
                    rec['action'] = 'monitor_only'
                    rec['confidence'] = 'low'

        if action == 'block_country':
            targets = rec.get('target', [])
            if len(targets) > 3:
                self.logger.warning(
                    f"[FIRST_RESPONDER] block_country has {len(targets)} targets — "
                    f"downgrading to raise_threshold (too many countries = diffuse attack)"
                )
                rec['action'] = 'raise_threshold'
                rec['target'] = []
                rec['reasoning'] = (
                    rec.get('reasoning', '') +
                    f' [auto-downgraded: {len(targets)} countries is a diffuse attack, use raise_threshold]'
                )
            return rec

        if action != 'block_asn':
            return rec

        original_targets = rec.get('target', [])
        filtered = [t for t in original_targets if t not in PROTECTED_ASNS]
        removed = [t for t in original_targets if t in PROTECTED_ASNS]

        if removed:
            self.logger.error(
                f"[FIRST_RESPONDER] LLM tried to block protected ASNs — REMOVED: {removed}"
            )

        if not filtered:
            self.logger.warning(
                "[FIRST_RESPONDER] All block_asn targets were protected ASNs — "
                "downgrading to raise_threshold"
            )
            rec['action'] = 'raise_threshold'
            rec['target'] = []
            rec['reasoning'] = (
                rec.get('reasoning', '') +
                ' [auto-downgraded: all targets were protected infrastructure ASNs]'
            )
        else:
            rec['target'] = filtered

        return rec

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def _process_incident(self, conn, incident):
        incident_id = incident['id']
        host = incident['host']

        # For active incidents: re-analyze only every 10 minutes
        if incident['ended_at'] is None:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT MAX(created_at) FROM first_responder_actions WHERE incident_id = %s",
                    (incident_id,)
                )
                last_action_at = cur.fetchone()[0]
            if last_action_at is not None:
                age_minutes = (datetime.now(timezone.utc) - last_action_at).total_seconds() / 60
                if age_minutes < 10:
                    return

        if incident['spike_ratio'] < self.min_spike_ratio:
            self.logger.info(
                f"[FIRST_RESPONDER] Skipping incident_id={incident_id} host={host} "
                f"spike_ratio={incident['spike_ratio']:.1f}x < min={self.min_spike_ratio}"
            )
            self._save_action(conn, incident_id, host, {
                'action': 'monitor_only',
                'target': [],
                'confidence': 'low',
                'reasoning': f"Skipped: spike_ratio={incident['spike_ratio']:.1f}x below minimum {self.min_spike_ratio}x",
                'ttl_minutes': self.ttl_minutes,
            })
            self._mark_processed(conn, incident_id)
            return

        if incident['challenge_count'] < self.min_challenge_count:
            self.logger.info(
                f"[FIRST_RESPONDER] Skipping incident_id={incident_id} host={host} "
                f"challenge_count={incident['challenge_count']} < min={self.min_challenge_count}"
            )
            self._save_action(conn, incident_id, host, {
                'action': 'monitor_only',
                'target': [],
                'confidence': 'low',
                'reasoning': f"Skipped: challenge_count={incident['challenge_count']} below minimum {self.min_challenge_count}",
                'ttl_minutes': self.ttl_minutes,
            })
            self._mark_processed(conn, incident_id)
            return

        if host in self.host_whitelist:
            self.logger.info(
                f"[FIRST_RESPONDER] host={host} is whitelisted — monitor only, no blocking"
            )
            self._save_action(conn, incident_id, host, {
                'action': 'monitor_only',
                'target': [],
                'confidence': 'low',
                'reasoning': f"Host {host} is in the First Responder whitelist — blocking disabled to protect legitimate users.",
                'ttl_minutes': self.ttl_minutes,
            })
            self._mark_processed(conn, incident_id)
            return

        self.logger.info(
            f"[FIRST_RESPONDER] Analyzing incident_id={incident_id} host={host} "
            f"spike_ratio={incident['spike_ratio']:.1f}x"
        )

        country_stats = self._get_country_stats(conn, incident_id)
        asn_stats = self._get_asn_stats(conn, incident_id)
        normal_traffic = self._get_normal_traffic(conn, host)
        normal_asn_traffic = self._get_normal_asn_traffic(conn, host)
        url_stats = self._get_url_stats(conn, incident_id)
        ua_stats = self._get_ua_stats(conn, incident_id)
        normal_url_traffic = self._get_normal_url_traffic(conn, host)
        normal_ua_traffic = self._get_normal_ua_traffic(conn, host)
        fingerprint_stats = self._get_fingerprint_stats(conn, incident)

        if not country_stats and not asn_stats:
            age_minutes = (datetime.now(timezone.utc) - incident['started_at']).total_seconds() / 60
            if age_minutes > 30:
                self.logger.warning(
                    f"[FIRST_RESPONDER] incident_id={incident_id}: no stats after {age_minutes:.0f}min, giving up"
                )
                self._save_action(conn, incident_id, host, {
                    'action': 'monitor_only',
                    'target': [],
                    'confidence': 'low',
                    'reasoning': f"Skipped: no country/ASN stats available after {age_minutes:.0f} minutes",
                    'ttl_minutes': self.ttl_minutes,
                })
                self._mark_processed(conn, incident_id)
            else:
                self.logger.info(
                    f"[FIRST_RESPONDER] incident_id={incident_id}: no stats yet, skipping"
                )
            return

        # Fingerprint blocking: if one fingerprint dominates, block known IPs directly
        if fingerprint_stats and fingerprint_stats[0]['pct'] >= self.fingerprint_min_pct:
            dominant_fp = fingerprint_stats[0]['fingerprint']
            self.logger.warning(
                f"[FIRST_RESPONDER] incident_id={incident_id} host={host} "
                f"dominant fingerprint={dominant_fp} pct={fingerprint_stats[0]['pct']:.1f}% "
                f"— blocking IPs directly"
            )
            self._block_fingerprint_ips(conn, incident, dominant_fp)

        prompt = self._build_prompt(
            incident, country_stats, asn_stats, normal_traffic, normal_asn_traffic,
            url_stats, ua_stats, normal_url_traffic, normal_ua_traffic,
            fingerprint_stats=fingerprint_stats,
        )
        self.logger.info(f"[FIRST_RESPONDER] Calling LLM for incident_id={incident_id}")

        rec = self._call_llm(prompt)
        if rec is None:
            self.logger.error(
                f"[FIRST_RESPONDER] LLM returned no response for incident_id={incident_id}"
            )
            self._mark_processed(conn, incident_id)
            return

        if not self._validate_response(rec):
            self.logger.error(
                f"[FIRST_RESPONDER] Invalid LLM response for incident_id={incident_id}: {rec}"
            )
            self._mark_processed(conn, incident_id)
            return

        rec = self._sanitize_response(rec, survey_country=incident.get('survey_country'))
        self._save_action(conn, incident_id, host, rec)
        # Mark processed only when incident is closed — active incidents are re-analyzed each cycle
        if incident.get('ended_at') is not None:
            self._mark_processed(conn, incident_id)
            # Generate a short narrative summary at incident close
            narrative_prompt = self._build_narrative_prompt(
                incident, country_stats, asn_stats, rec
            )
            narrative = self._generate_narrative(narrative_prompt)
            if narrative:
                self._save_narrative(conn, incident_id, narrative)
                self.logger.info(
                    f"[FIRST_RESPONDER] narrative saved for incident_id={incident_id}: {narrative[:120]}…"
                )

    def _get_incidents_missing_narrative(self, conn):
        """Return closed incidents from the last 7 days that have an action but no narrative."""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT i.id, i.host, i.challenge_count, i.baseline_avg, i.spike_ratio,
                       i.started_at, i.survey_country, i.botnet_info, i.ended_at,
                       i.avg_api_ratio, i.avg_path_diversity, i.behavior_samples,
                       i.traffic_peak_ratio, i.traffic_close_ratio, i.traffic_recent_ratio,
                       i.traffic_peak_count, i.traffic_close_count, i.traffic_recent_count,
                       a.action, a.target, a.confidence
                FROM incidents i
                JOIN first_responder_actions a ON a.incident_id = i.id
                WHERE i.started_at > NOW() - INTERVAL '7 days'
                  AND i.ended_at IS NOT NULL
                  AND i.narrative IS NULL
                  AND a.action != 'monitor_only'
                ORDER BY a.created_at DESC
                LIMIT 1
            """)
            rows = cur.fetchall()
        result = []
        for r in rows:
            action_rec = {'action': r[18], 'target': r[19].split('|') if r[19] else [], 'confidence': r[20]}
            result.append(({
                'id': r[0], 'host': r[1], 'challenge_count': r[2],
                'baseline_avg': float(r[3]), 'spike_ratio': float(r[4]),
                'started_at': r[5], 'survey_country': r[6] or '',
                'botnet_info': r[7] or '', 'ended_at': r[8],
                'avg_api_ratio': float(r[9] or 0.0), 'avg_path_diversity': float(r[10] or 1.0),
                'behavior_samples': int(r[11] or 0),
                'traffic_peak_ratio': float(r[12] or 0.0), 'traffic_close_ratio': float(r[13] or 0.0),
                'traffic_recent_ratio': float(r[14] or 0.0),
                'traffic_peak_count': int(r[15] or 0), 'traffic_close_count': int(r[16] or 0),
                'traffic_recent_count': int(r[17] or 0),
            }, action_rec))
        return result

    def _backfill_narratives(self, conn):
        """Generate narratives for recently closed incidents that don't have one yet."""
        pending = self._get_incidents_missing_narrative(conn)
        if not pending:
            return
        incident, action_rec = pending[0]
        incident_id = incident['id']
        host = incident['host']
        self.logger.info(f"[FIRST_RESPONDER] Backfilling narrative for incident_id={incident_id} host={host}")
        country_stats = self._get_country_stats(conn, incident_id)
        asn_stats = self._get_asn_stats(conn, incident_id)
        narrative_prompt = self._build_narrative_prompt(incident, country_stats, asn_stats, action_rec)
        narrative = self._generate_narrative(narrative_prompt)
        if narrative:
            self._save_narrative(conn, incident_id, narrative)
            self.logger.info(
                f"[FIRST_RESPONDER] narrative backfilled for incident_id={incident_id}: {narrative[:120]}…"
            )

    def run(self):
        self.logger.info(
            f"IncidentFirstResponder starting | ollama={self.ollama_url} "
            f"model={self.llm_model} check_interval={self.check_interval}s "
            f"min_attack_pct={self.min_attack_pct}% max_normal_pct={self.max_normal_pct}%"
        )

        conn = None
        while True:
            try:
                if conn is None:
                    conn = self._pg_connect()
                    self._ensure_tables(conn)

                incidents = self._get_unprocessed_incidents(conn)
                if incidents:
                    self.logger.info(
                        f"[FIRST_RESPONDER] Found {len(incidents)} unprocessed incident(s)"
                    )
                    for incident in incidents:
                        try:
                            self._process_incident(conn, incident)
                        except Exception:
                            self.logger.exception(
                                f"[FIRST_RESPONDER] Failed to process incident_id={incident['id']}"
                            )
                else:
                    # No active work — use idle time to backfill missing narratives
                    try:
                        self._backfill_narratives(conn)
                    except Exception:
                        self.logger.exception("[FIRST_RESPONDER] Backfill error")

            except Exception:
                self.logger.exception("[FIRST_RESPONDER] Postgres error, reconnecting")
                try:
                    conn.close()
                except Exception:
                    pass
                conn = None

            time.sleep(self.check_interval)
