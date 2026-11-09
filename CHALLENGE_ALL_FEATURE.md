# Feature: Automatic `challenge_all` Response

## What happened (Sep 18, 2026 — v2026.org attack)

Attack was so distributed (hundreds of IPs, botnet) that ML-based per-IP challenges were
insufficient — bots used too few requests per IP to be scored (`immature_ratio ~0.90`),
so `challenge_count` was only 10–50 per 6-min window despite 200x+ spike.

**Key insight:** bots without JavaScript can't pass JS challenge → they get `banjax_bot_score = -1`
and hit the rate limiter → **HTTP 429**. Real users with browsers solve challenge, get cookie, get 200.
So `challenge_all` + rate limiter = perfect filter against dumb bots.

---

## Trigger conditions (when to send `challenge_all`)

Should fire when **all** of:

1. `spike_ratio > X` (e.g., 50x) — extreme volumetric attack
2. `immature_ratio > 0.80` — most traffic is unscored (too short sessions)
3. `challenge_count < Y` (e.g., < 100) — ML isn't catching enough bots per window
4. Site is not on `challenge_all` cooldown (don't repeat if already active)

Optionally: `baseline_avg` must be high enough (attack is real, not just low-traffic noise).

---

## Command format

```python
message = {
    "Name": "challenge_all",
    "host": "v2026.org",
    "print_log": True,
    "ttl": 3600,         # seconds — start with 1 hour
    "source": "FirstResponder"
}
```

Send to `banjax_command_topic` on output Kafka cluster (kafkab).

---

## Monitoring after `challenge_all`

Watch `deflect.log-*` index for the host:

| Signal | Meaning |
|--------|---------|
| `http_response_code = 429`, `banjax_bot_score = -1` | Bots still attacking, JS-less, being rate-limited ✓ |
| `http_response_code = 200`, `deflect_session != -` | Real users passing challenge ✓ |
| `http_response_code = 200`, `banjax_bot_score > 0` | Bots solving JS (headless browsers) — need block_ip/block_ua instead |
| 429 rate drops significantly | Attack is over, bots gave up |

Check via postgres: count 429s vs 200s per minute for the host.

---

## TTL renewal logic

```
After challenge_all sent with TTL=T:
  Every check_interval minutes:
    if incident still active (spike_ratio still high OR 429 rate still high):
      if time_since_challenge_all > T * 0.8:  # renew before expiry
        new_ttl = min(T * 1.5, MAX_TTL)       # grow TTL up to a cap
        send challenge_all with new_ttl
    else:
      # attack over, don't renew — let TTL expire naturally
      log("challenge_all expired naturally, attack subsided")
```

**Suggested TTL schedule:**
- First send: `ttl = 3600` (1h)
- First renewal: `ttl = 5400` (1.5h)
- Second renewal: `ttl = 7200` (2h)
- Cap at `MAX_TTL = 14400` (4h) — elections/incidents rarely last longer

---

## Stop conditions

Stop renewing when **any** of:
- `spike_ratio < threshold` for 2+ consecutive checks
- `immature_ratio < 0.5` (ML starting to score traffic normally)
- Manual override via config/whitelist
- Absolute time limit (e.g., don't auto-renew after 12h — page someone)

---

## Implementation plan

### 1. State tracking in `IncidentFirstResponder`

```python
self._challenge_all_active: dict[str, dict] = {}
# { host: { "sent_at": timestamp, "ttl": int, "renewal_count": int } }
```

### 2. New method `_send_challenge_all(host, ttl)`

```python
def _send_challenge_all(self, host: str, ttl: int):
    msg = {
        "Name": "challenge_all",
        "host": host,
        "print_log": True,
        "ttl": ttl,
        "source": "FirstResponder"
    }
    self._send_kafka_command(host, msg)
    self._challenge_all_active[host] = {
        "sent_at": time.time(),
        "ttl": ttl,
        "renewal_count": self._challenge_all_active.get(host, {}).get("renewal_count", 0)
    }
    self.logger.info(f"[CHALLENGE_ALL] Sent challenge_all for {host}, ttl={ttl}")
```

### 3. Check in `_process_traffic_spike_incident`

After existing logic, before LLM call:

```python
if (spike_ratio > CHALLENGE_ALL_MIN_SPIKE and
        immature_ratio > CHALLENGE_ALL_MIN_IMMATURE and
        challenge_count < CHALLENGE_ALL_MAX_CHALLENGE_COUNT):
    
    state = self._challenge_all_active.get(host)
    if state is None:
        # First time — send
        self._send_challenge_all(host, ttl=3600)
        return  # don't also do LLM analysis, challenge_all is sufficient
    else:
        # Check if renewal needed
        elapsed = time.time() - state["sent_at"]
        if elapsed > state["ttl"] * 0.8:
            new_ttl = min(int(state["ttl"] * 1.5), MAX_CHALLENGE_ALL_TTL)
            state["renewal_count"] += 1
            self._send_challenge_all(host, ttl=new_ttl)
```

### 4. New config parameters

```yaml
CHALLENGE_ALL_ENABLED: "True"
CHALLENGE_ALL_MIN_SPIKE_RATIO: "50.0"
CHALLENGE_ALL_MIN_IMMATURE_RATIO: "0.80"
CHALLENGE_ALL_MAX_CHALLENGE_COUNT: "100"
CHALLENGE_ALL_INITIAL_TTL: "3600"
CHALLENGE_ALL_MAX_TTL: "14400"
```

---

## Open questions

1. **Should `challenge_all` replace or supplement per-IP blocks?**
   Currently first_responder also does `block_ua`/`block_ip`. Probably keep both —
   `challenge_all` stops new bots, per-IP blocks stop known bots faster.

2. **What if some bots DO have JS (headless Chrome)?**
   Monitor for `banjax_bot_score > 0.5` with 200 responses → if seen, switch to
   `block_fingerprint` or `block_ua` for those specific sessions.

3. **Postgres-based 429 monitoring vs incident-based?**
   Could add a `_check_challenge_all_effectiveness` method that queries
   `banjax_command_topic` index or uses a separate counter. Or just rely on
   spike_ratio still being high as proxy.

4. **Per-dnet rollout?**
   If attack only hits certain dnets, `challenge_all` could be sent per-dnet.
   Currently command goes to all dnets via Kafka topic key = host.

---

## Notes from v2026.org incident

- Attack ran from ~07:00 to 12:00+ UTC, elections day Sep 18 2026
- `challenge_all` was deployed manually by Jeremy after ~5 hours of attack
- Immediately: bots got 429 (no JS), site recovered
- Baskerville was blocking ~60 IPs/min but couldn't keep up with botnet rotation
- `baseline_override: v2026.org: 500` was set in deflect_alerts to detect spikes correctly
- Key gap: `challenge_all` command support was not yet in banjax when attack started
