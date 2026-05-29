# Attack Response Mode

## Overview

Attack Response Mode is an automatic DDoS mitigation feature that escalates Baskerville's
defensive response when an active attack is detected on a specific host. It bridges two
systems: the **IncidentDetector** in `deflect_alerts` (which detects attack spikes) and the
**Predictor** in `baskervillehall` (which enforces decisions).

When an incident is active, the Predictor switches from its normal challenge-based workflow
to a more aggressive posture — proportional to the severity of the attack.

## How It Works

### Detection Source

Attack Response Mode does not perform its own spike detection. It reads from the `incidents`
table in Postgres, which is populated by `IncidentDetector` in the `deflect_alerts` project.
`IncidentDetector` monitors the banjax command topic and fires when challenge volume for a
host exceeds a configurable multiple of its rolling baseline.

### Polling

The Predictor polls the `incidents` table every 30 seconds:

```sql
SELECT host, MAX(spike_ratio) FROM incidents
WHERE ended_at IS NULL
AND started_at > NOW() - INTERVAL '30 minutes'
AND challenge_count >= {ATTACK_MIN_CHALLENGE_COUNT}
AND spike_ratio >= {ATTACK_MIN_SPIKE_RATIO}
GROUP BY host
```

The result is a dict `{host → spike_ratio}` stored in memory. Each host is assigned one of
three response levels based on its current spike_ratio.

### Response Levels

| Level | spike_ratio | Actions |
|---|---|---|
| **MODERATE** | ≥ 4.0 | Classifier challenge threshold raised from 30 → 50 |
| **AGGRESSIVE** | ≥ 6.0 | + `bad_bot` sessions → `block_ip` instead of `challenge_ip`; anomaly (bot) → `block_ip` |
| **EXTREME** | ≥ 15.0 | + datacenter ASN sessions → `block_ip` immediately, before model scoring |

Levels are cumulative: EXTREME includes all AGGRESSIVE and MODERATE actions.

### Normal Mode vs Attack Response Mode

| Decision point | Normal | MODERATE | AGGRESSIVE | EXTREME |
|---|---|---|---|---|
| Classifier score < threshold | challenge (threshold=30) | challenge (threshold=50) | challenge (threshold=50) | challenge (threshold=50) |
| `bad_bot` flag | `challenge_ip` | `challenge_ip` | `block_ip` | `block_ip` |
| Anomaly (IF/AE), bot session | `challenge_ip` | `challenge_ip` | `block_ip` | `block_ip` |
| `datacenter_asn` flag | normal flow | normal flow | normal flow | `block_ip` immediately |

### Survey Country Protection

Each site can declare a primary target audience country (`survey_country`, stored in the
`incidents` table and passed through sessions). During an attack, if a session's country
matches the site's `survey_country`, the Predictor **will not escalate to `block_ip`** —
it falls back to `challenge_ip` instead.

This prevents collateral blocking of the site's own audience. For example, if `sudanile.com`
has `survey_country = "SD"` and an attack comes partly from Sudan, Sudanese IPs are
challenged rather than blocked, even under AGGRESSIVE or EXTREME mode.

The protection applies at all escalation points:

| Escalation point | Normal attack response | With `survey_country` match |
|---|---|---|
| EXTREME: `datacenter_asn` | `block_ip` | `challenge_ip` |
| AGGRESSIVE: `bad_bot` | `block_ip` | `challenge_ip` |
| AGGRESSIVE: anomaly, primary bot session | `block_ip` | `challenge_ip` |
| AGGRESSIVE: anomaly, non-primary bot session | `block_ip` | `rate_limit` / `challenge_ip` |

When protection is active, the log will include `[survey_country protected]` for EXTREME
datacenter blocks.

### Command `meta` Field

All commands issued under Attack Response Mode include `[attack_response]` in the `meta`
field so they can be identified in logs and Grafana dashboards:

- `"classifier [attack_response]"`
- `"Bad bot rule [attack_response]"`
- `"anomaly [attack_response]"`
- `"datacenter_asn [attack_response]"`

### Verified Bots Are Always Skipped

`verified_bot` and `verified_ai_bot` sessions (Googlebot, GPTBot, etc.) are excluded from
all Attack Response Mode logic, as in normal mode.

## Configuration

All parameters are set via Kubernetes ConfigMap (`config_baskervillehall.yaml`):

| Parameter | Default | Description |
|---|---|---|
| `ATTACK_RESPONSE_MODE` | `True` | Enable/disable the feature entirely |
| `ATTACK_MIN_CHALLENGE_COUNT` | `50` | Minimum unique sessions in the spike window to qualify as an attack |
| `ATTACK_MIN_SPIKE_RATIO` | `4.0` | Minimum spike_ratio to enter MODERATE mode |
| `ATTACK_AGGRESSIVE_SPIKE_RATIO` | `6.0` | spike_ratio threshold to enter AGGRESSIVE mode |
| `ATTACK_EXTREME_SPIKE_RATIO` | `15.0` | spike_ratio threshold to enter EXTREME mode |

### Threshold Rationale

Thresholds were calibrated against 30 days of real incidents:

- Minimum observed spike_ratio in real attacks: **4.3** (palestinechronicle.com)
- Typical attack range: **4–13** (lile.cl, kavkaz-uzel.eu, sudanile.com, zmina.info)
- Severe attacks: **24–60** (tni.org, sea-watch.org, notav.info)
- `ATTACK_MIN_CHALLENGE_COUNT=50` filters out noise — all real attacks had ≥ 59 sessions
  in the window

## Datacenter ASN Detection

The `datacenter_asn` flag on sessions (used by EXTREME mode) is set by
`baskervillehall_session.py` and combines four signals:

1. **bad-asn-list.csv** — static list of known datacenter/hosting ASNs (744 entries)
2. **GeoLite2 `asn_name`** — organization name from MaxMind; matched against datacenter
   keywords (e.g. "Hetzner Online GmbH" → match on `hetzner`)
3. **VPS ASN list** — from `ASNDatabase2` (VPS Providers/ASN.txt)
4. **Malicious/VPN ASN lists** — from `ASNDatabase2`

`datacenter_asn = csv_match OR keyword_match(asn_name) OR vps_asn OR malicious_asn OR vpn_asn`

The GeoLite2 database is updated weekly via a Kubernetes CronJob (every Wednesday 06:00 UTC)
which triggers a rolling restart of Logstash pods, causing the initContainer to download
fresh databases from MaxMind.

## Postgres Dependency

The Predictor requires a live Postgres connection to use Attack Response Mode. If the
connection is unavailable, `_refresh_attack_response()` logs a warning and skips the
update — the Predictor continues with the last known state (or empty, on startup).

The `postgres_connection` is configured via the standard environment variables:
`POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DATABASE_NAME`.

## Deployment

Attack Response Mode is active by default. To disable it without redeploying:

```bash
# Disable
kubectl set env deployment/baskervillehall-predictor ATTACK_RESPONSE_MODE=False

# Re-enable
kubectl set env deployment/baskervillehall-predictor ATTACK_RESPONSE_MODE=True
```

Or update `config_baskervillehall.yaml` and apply:

```bash
kubectl apply -f config_baskervillehall.yaml
kubectl rollout restart statefulset/baskervillehall-predictor
```

## Relevant Files

| File | Role |
|---|---|
| `src/baskervillehall/baskervillehall_predictor.py` | `_refresh_attack_response()`, `_apply_decision_and_send()`, survey_country protection |
| `src/baskervillehall/asn_database.py` | `is_datacenter_asn()` with keyword matching |
| `src/baskervillehall/baskervillehall_session.py` | Sets `datacenter_asn` flag on sessions |
| `src/baskervillehall/storage_sessions.py` | Persists `survey_country` to sessions table |
| `src/baskervillehall/main.py` | Wires config env vars to predictor params |
| `config_baskervillehall.yaml` | All `ATTACK_*` config parameters |
| `deployment/postgres/create_schema.sql` | Sessions table schema including `survey_country` |
| `deployment/postgres/catch_up.sql` | Migration: adds `survey_country` to existing sessions table |
| `deflect_alerts/src/deflect_alerts/Incident_detector.py` | Writes to `incidents` table |
