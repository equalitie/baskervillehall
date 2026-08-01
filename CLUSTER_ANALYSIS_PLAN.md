# Cluster Analysis — Implementation Status

## Problem

The predictor analyzes **each session individually**. The ML model (Isolation Forest + AutoEncoder) cannot see coordination across IPs. Sophisticated botnets — multiple IPs, each session looking normal individually, but the crowd suspiciously uniform — bypass all per-session filters.

**Two detection layers are needed:**

1. **Session-level LLM** — analyze URL sequence semantics for individual sessions in the ML grey zone
2. **Cluster-level analysis** — detect uniformity across sessions per host every 5 minutes, catch coordinated attacks before the traffic spike

---

## Full Data Flow (as deployed)

```
Kafka TOPIC_SESSIONS
        ↓
predictor run() loop
        ↓
ML scoring (fast, milliseconds)
        ↓
┌──────────────────────────────────────────────────────────┐
│  score < 0.2  →  clear bot  →  block/challenge           │
│  score > 0.8  →  clear human  →  pass                   │
│  score 0.2–0.8  →  grey zone                             │
│        ↓                                                 │
│  Session-level LLM (async, via _session_llm_queue):      │
│  OpenAI gpt-4.1-nano analyzes URL sequence               │
│  verdict cached in session_llm_cache[ip] (TTL 5 min)     │
│  if label=bot + confidence>=0.85 → challenge_ip          │
│  verdict saved to session_llm_verdicts table             │
└──────────────────────────────────────────────────────────┘
        ↓  (in parallel, independently)
┌──────────────────────────────────────────────────────────┐
│  cluster_buffer[host].append(_extract_cluster_features)  │
│        ↓  every 5 minutes per host (_check_clusters)     │
│  Uniformity Scoring (synchronous, fast math):            │
│  5 metrics, weights [0.10, 0.50, 0.20, 0.15, 0.05]      │
│        ↓                                                 │
│  score >= 0.55  →  WARNING log + cluster_alerts[host]    │
│  score >= 0.70  →  enqueue to _cluster_llm_queue         │
│        ↓                                                 │
│  Cluster LLM worker thread (OpenAI):                     │
│  "coordinated attack or organic traffic?"                │
│  if attack + confidence high/medium:                     │
│    → build block_criteria from cluster data              │
│    → INSERT INTO incidents (source='cluster_analysis')   │
│        ↓                                                 │
│  First Responder picks up cluster incident:              │
│  applies block_criteria (fingerprint / ip_list / ua)     │
└──────────────────────────────────────────────────────────┘
```

---

## Layer A — Session-level LLM ✅ DEPLOYED

### What it does

Grey-zone sessions (ML score 0.2–0.8, ≥5 requests, not a verified bot) are sent to OpenAI **asynchronously**. The LLM sees the actual URL sequence and understands navigation semantics that the ML model cannot:

| Pattern | ML sees | LLM sees |
|---|---|---|
| Scraper with moderate rate | normal entropy → passes | `/sitemap.xml → /article/1 → /article/2` → bot |
| News reader | similar numbers | `/news/ → /kavkaz/story-123 → /ukraine/story-456` → human |
| Credential stuffer | varied paths → passes | `/login` repeated with varying params → bot |
| Google Search arrival | utm_source present | `?utm_source=google&utm_medium=organic` → human |

### Key implementation details

- **Provider**: OpenAI (gpt-4.1-nano by default), not Ollama
- **Queue**: `_session_llm_queue` (Queue, maxsize configurable), daemon thread `session-llm`
- **Cache**: `_session_llm_cache` TTLCache(5000, ttl=300) — ip → verdict, 5-minute window
- **Action**: if `label='bot'` and `confidence >= 0.85` → `challenge_ip` or `challenge_session` Kafka command
- **Storage**: every verdict saved to `session_llm_verdicts` PostgreSQL table
- **Cache feeds cluster analysis**: `_session_llm_cache` is read by `_compute_uniformity()` to blend individual verdicts into the cluster score (weight 0.30 when ≥3 verdicts available)

### Config keys

```yaml
SESSION_LLM_ENABLED: "true"
SESSION_LLM_PROVIDER: "openai"          # 'openai' or 'ollama'
SESSION_LLM_MODEL: "gpt-4.1-nano"
SESSION_LLM_SCORE_MIN: "20"             # grey zone lower bound (integer 0-100)
SESSION_LLM_SCORE_MAX: "80"             # grey zone upper bound
SESSION_LLM_MIN_REQUESTS: "5"
SESSION_LLM_QUEUE_SIZE: "200"
```

---

## Layer B — Uniformity Scoring ✅ DEPLOYED

### Method: `_extract_cluster_features(session) → dict`

Called for every session, stores lightweight data in `cluster_buffer[host]`:

```python
{
    'ip': '1.2.3.4',
    'ts': datetime,
    'url_paths': ['/news/', '/article/123'],  # first 10 URLs
    'ua': 'Mozilla/5.0 ...',
    'num_requests': 15,
    'interval_cv': 0.12,      # coefficient of variation of request intervals
    'fingerprint': 'a3f8b2c1...',  # 16-char SHA256 of UA+TLS cipher order+Accept-Language
}
```

### Method: `_compute_uniformity(sessions) → float`

Five metrics, **weights [0.10, 0.50, 0.20, 0.15, 0.05]**:

| # | Metric | Weight | What it detects |
|---|---|---|---|
| 1 | UA diversity | 0.10 | Few unique UAs across many IPs |
| 2 | **Fingerprint diversity** | **0.50** | Same TLS library/tool (cipher order hash) — primary signal, hard to spoof |
| 3 | URL path pattern | 0.20 | Most sessions hit same URL section |
| 4 | Interval CV | 0.15 | Low coefficient of variation = bot timer, no human jitter |
| 5 | Sequential numeric IDs | 0.05 | Cross-IP enumeration pattern |

**Fingerprint** gets 50% weight because it's the only signal an attacker cannot easily fake — TLS cipher suite order reflects the actual HTTP library, not the spoofable UA string.

**LLM blend**: when `session_llm_cache` has ≥3 verdicts for IPs in the cluster, blend in `bot_ratio` with weight 0.30:
```python
score = base_score * 0.70 + bot_ratio * 0.30
```

### Observed real-world results

| Host | unique_fps | avg_interval_cv | score | Interpretation |
|---|---|---|---|---|
| 3ayin.com | 1/12 | 0.00 | 0.89 | Single script, perfect timer — scraper |
| sup.coop | 1 | 0.00 | 0.87 | Same |
| hodnews.com | 4-7 | variable | 0.70-0.78 | Botnet with slight tool variation |
| kavkaz-uzel.eu | varied | varied | 0.61 | WebView botnet + GDELT scrapers (real attack) |

### Constants

```python
CLUSTER_CHECK_INTERVAL = 300    # seconds between checks per host
CLUSTER_MIN_SESSIONS = 10       # minimum sessions to analyze
CLUSTER_ALERT_THRESHOLD = 0.55  # score → WARNING log + cluster_alerts TTLCache
CLUSTER_LLM_THRESHOLD = 0.70    # score → enqueue for cluster LLM
CLUSTER_BUFFER_MAXLEN = 500     # max sessions per host in deque
```

---

## Layer C — Cluster LLM → Incident Creation ✅ DEPLOYED

### What it does

When `uniformity_score >= 0.70`, the cluster is sent to OpenAI via `_cluster_llm_queue` (Queue maxsize=20, daemon thread `cluster-llm`).

The LLM receives:
- Host, window, uniformity score
- Top UAs and fingerprints with counts
- Avg interval_cv, top URL pattern
- Individual session LLM verdicts if available (from `_session_llm_cache`)
- Full IP list

**Output**: `{"verdict": "attack"|"benign"|"uncertain", "confidence": "high"|"medium"|"low", "reasoning": "..."}`

If `verdict='attack'` and `confidence` is `high` or `medium` → create a PostgreSQL incident.

### Block criteria extraction

Block criteria are derived from cluster **data** (not from the LLM). The LLM only confirms "attack or not". Three criterion types:

| Type | Trigger | What it blocks |
|---|---|---|
| `fingerprint` | fp covers ≥30% of sessions | Forward-looking: all IPs with this TLS fingerprint (past + future) |
| `ip_list` | interval_cv < 0.10 (bot timer), or score ≥ 0.80 | Current cluster IPs only |
| `ua_exact` | UA covers ≥50% of sessions, count ≥5 | Forward-looking: all sessions with exact UA string |

```json
[
  {"type": "fingerprint", "value": "a3f8b2c1...", "count": 12, "pct": 100.0, "ips": ["1.2.3.4", ...], "confidence": "high"},
  {"type": "ip_list", "ips": ["1.2.3.4", "5.6.7.8", ...], "reason": "interval_cv<0.10 (bot-like request timer)", "confidence": "high"},
  {"type": "ua_exact", "value": "Mozilla/5.0 (Linux; Android 10; K)...", "count": 11, "pct": 91.7, "confidence": "high"}
]
```

### Incident table schema

New columns added to `incidents`:

```sql
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'spike_detector';
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS block_criteria JSONB;
```

Cluster incidents are inserted with `source='cluster_analysis'`, `challenge_count=0`, `spike_ratio=0`, `ended_at=NOW()` (point-in-time finding, not an ongoing tracked event).

---

## Layer D — Cluster Responder in First Responder ✅ DEPLOYED

`IncidentFirstResponder` automatically picks up `cluster_analysis` incidents (they have `ended_at` set and `first_responder_processed=FALSE`).

### Routing

```python
def _process_incident(self, conn, incident):
    if incident.get('source') == 'cluster_analysis':
        return self._process_cluster_incident(conn, incident)
    # ... existing spike_detector flow ...
```

### `_process_cluster_incident`

No LLM call — the LLM already ran in the predictor. Reads `block_criteria` JSON, applies each criterion via `_apply_block_criterion`:

- `fingerprint` → `_block_ip_list()` using stored IPs from criterion (or fallback to `_block_fingerprint_ips()` which queries sessions table)
- `ip_list` → `_block_ip_list()` — sends `block_ip` Kafka commands
- `ua_exact` → `_block_ua_exact()` — sends `block_ua` Kafka command
- Unknown types → log warning, skip gracefully

All actions recorded in `first_responder_actions` table with `reasoning` explaining which cluster signal triggered the block.

### Extensibility

To add a new blocking signal in the future:
1. Add extraction logic in `_build_block_criteria()` in predictor
2. Add `elif ctype == 'new_type':` in `_apply_block_criterion()` in first responder

No other changes needed.

---

## What is NOT yet done

### Phase 2: cluster_alerts feed into per-session decisions

`cluster_alerts` TTLCache is populated when `score >= 0.55`, but it's not yet connected to `_apply_decision_and_send`. The plan is to raise the effective baskerville_score threshold for hosts with an active cluster alert, so marginal sessions get challenged more aggressively during a cluster attack.

```python
# Proposed — not yet implemented:
if host in cluster_alerts:
    alert_score = cluster_alerts[host]
    effective_threshold = self.threshold * (1.0 - alert_score * 0.3)
```

### Phase 3: Cross-site signal (optional)

If the same IP appears in `cluster_alerts` on 2+ hosts within 10 minutes → escalate priority, include in First Responder incident narrative.

---

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| False positive (flash crowd, news spike) | Threshold 0.55 is conservative; fingerprint diversity is the dominant signal and doesn't spike with organic traffic |
| LLM timeout blocks worker | cluster-llm thread is daemon; timeout=30s; main loop unaffected |
| cluster_buffer memory growth | `deque(maxlen=500)` per host, ~1 KB per entry → ~500 KB max per host |
| Bot with diverse fingerprints | Other signals (interval_cv, URL pattern) compensate; total score still rises |
| Verified crawlers (Googlebot) | Already filtered before cluster_buffer by `bot_verificator.py` |
| LLM says "attack" incorrectly | Block criteria have their own thresholds (fp ≥30%, UA ≥50%) — weak clusters produce no criteria even if LLM says attack |
