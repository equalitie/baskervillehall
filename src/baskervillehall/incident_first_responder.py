import json
import logging
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import requests

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

SYSTEM_PROMPT = """You are a DDoS mitigation expert. You receive attack statistics and normal traffic
baseline for a website and must recommend a mitigation action.

Respond with a JSON object only — no explanation outside the JSON.

Output format:
{{
  "action": "block_country" | "block_asn" | "raise_threshold" | "monitor_only",
  "target": ["CN", "RU"] | ["Hetzner Online GmbH", "OVH SAS"] | [],
  "confidence": "high" | "medium" | "low",
  "ttl_minutes": 30,
  "reasoning": "brief explanation"
}}

Decision rules (apply in order):
1. Prefer block_asn over block_country — it causes less collateral damage.
2. Use block_asn if datacenter ASNs account for the majority of the attack.
3. Use block_country if a country accounts for >{min_attack_pct}% of the attack AND
   represents <{max_normal_pct}% of the site's normal traffic.
4. Never include survey_country in the block target — it is the site's primary audience.
5. Use monitor_only if blocking would affect >{max_normal_pct}% of normal traffic
   (collateral damage too high).
6. Use raise_threshold as a fallback when the attack is diffuse across many sources.
7. Set confidence=high only when a single pattern dominates (>70% of attack).
8. ttl_minutes should be 30 unless there is a strong reason to differ.
9. NEVER block these ASNs regardless of attack share — they are critical internet infrastructure:
   Google LLC, Cloudflare Inc., Amazon.com Inc., Microsoft Corporation, Akamai Technologies,
   Fastly Inc., Meta Platforms Inc., Apple Inc., Twitter Inc., Wikimedia Foundation.
   If these dominate the attack, use raise_threshold or monitor_only instead.
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
            logger=None,
    ):
        self.postgres_connection = postgres_connection or {}
        self.ollama_url = ollama_url.rstrip('/')
        self.llm_model = llm_model
        self.check_interval = check_interval
        self.min_attack_pct = min_attack_pct
        self.max_normal_pct = max_normal_pct
        self.ttl_minutes = ttl_minutes
        self.logger = logger or logging.getLogger(self.__class__.__name__)

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
                       started_at, survey_country, botnet_info, ended_at
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

    def _build_prompt(self, incident, country_stats, asn_stats, normal_traffic):
        host = incident['host']
        survey_country = incident['survey_country']

        lines = [
            f"=== ATTACK on {host} ===",
            f"Started: {incident['started_at']}",
            f"Challenge count: {incident['challenge_count']}  "
            f"Baseline avg: {incident['baseline_avg']:.1f}  "
            f"Spike ratio: {incident['spike_ratio']:.1f}x",
            f"Site primary audience (survey_country): {survey_country or 'unknown'}",
        ]

        if incident['botnet_info']:
            lines.append(f"Botnet overlap with previous incidents:\n{incident['botnet_info']}")

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
            for s in asn_stats:
                dc = " [datacenter]" if s['datacenter'] else ""
                lines.append(
                    f"  {s['asn_name'][:40]:40s}{dc}  "
                    f"attack: {s['pct']:5.1f}%"
                )
        else:
            lines.append("  (no ASN data yet)")

        lines.append("\n--- Normal traffic baseline (last 3 days) ---")
        if normal_traffic:
            top_normal = sorted(normal_traffic.items(), key=lambda x: -x[1])[:10]
            lines.append("  Countries: " + ", ".join(f"{c}: {p:.1f}%" for c, p in top_normal))
        else:
            lines.append("  (no baseline data — site may be new)")

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

    def _validate_response(self, rec):
        """Basic validation of LLM response before saving."""
        valid_actions = {'block_country', 'block_asn', 'raise_threshold', 'monitor_only'}
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

        self.logger.info(
            f"[FIRST_RESPONDER] Analyzing incident_id={incident_id} host={host} "
            f"spike_ratio={incident['spike_ratio']:.1f}x"
        )

        country_stats = self._get_country_stats(conn, incident_id)
        asn_stats = self._get_asn_stats(conn, incident_id)
        normal_traffic = self._get_normal_traffic(conn, host)

        if not country_stats and not asn_stats:
            age_minutes = (datetime.now(timezone.utc) - incident['started_at']).total_seconds() / 60
            if age_minutes > 30:
                self.logger.warning(
                    f"[FIRST_RESPONDER] incident_id={incident_id}: no stats after {age_minutes:.0f}min, giving up"
                )
                self._mark_processed(conn, incident_id)
            else:
                self.logger.info(
                    f"[FIRST_RESPONDER] incident_id={incident_id}: no stats yet, skipping"
                )
            return

        prompt = self._build_prompt(incident, country_stats, asn_stats, normal_traffic)
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

        self._save_action(conn, incident_id, host, rec)
        # Mark processed only when incident is closed — active incidents are re-analyzed each cycle
        if incident.get('ended_at') is not None:
            self._mark_processed(conn, incident_id)

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

            except Exception:
                self.logger.exception("[FIRST_RESPONDER] Postgres error, reconnecting")
                try:
                    conn.close()
                except Exception:
                    pass
                conn = None

            time.sleep(self.check_interval)
