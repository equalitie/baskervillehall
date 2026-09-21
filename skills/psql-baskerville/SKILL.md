---
description: Query Baskerville PostgreSQL database via kubectl exec
---

# Query Baskerville PostgreSQL

Run SQL queries against the Baskerville production database.

## Connection

The postgres host is Kubernetes-internal — only reachable via kubectl exec:

```bash
kubectl exec $(kubectl get pod -l app=postgres-baskervillehall -o jsonpath='{.items[0].metadata.name}') -- \
  psql -U postgres -d baskerville -c "<SQL>"
```

For multi-line queries:
```bash
kubectl exec $(kubectl get pod -l app=postgres-baskervillehall -o jsonpath='{.items[0].metadata.name}') -- \
  psql -U postgres -d baskerville -c "
SELECT ...;
"
```

## Key Tables

**`incidents`** — detected attacks
- id, host, source (traffic_spike | cluster_analysis | spike_detector), command
- started_at, ended_at, detected_at
- challenge_count, baseline_avg, spike_ratio
- traffic_peak_ratio, traffic_peak_count, traffic_close_ratio, traffic_close_count, traffic_recent_ratio, traffic_recent_count
- survey_country, botnet_info, narrative, block_criteria (jsonb)
- first_responder_processed (bool)

**`first_responder_actions`** — LLM decisions per incident
- incident_id, host, action, target, confidence, reasoning
- created_at, expires_at, ttl_minutes, applied

**`blocked_fingerprints`** — TLS fingerprints to block
- fingerprint (SHA256[:16] hash), host, incident_id, reason, expires_at

**`incident_country_stats`** — country breakdown per incident
- incident_id, country, cmd_count

**`incident_asn_stats`** — ASN breakdown per incident
- incident_id, asn_name, datacenter (bool), cmd_count

**`incident_ua_stats`** — User-Agent breakdown per incident
- incident_id, ua, cmd_count

**`incident_fingerprint_stats`** — TLS fingerprint breakdown per incident
- incident_id, fingerprint, cmd_count

**`incident_ips`** — IPs per incident (only cluster_analysis; empty for traffic_spike)
- incident_id, ip, hit_count

**`llm_usage_log`** — LLM token usage and cost tracking
- ts, source, provider, model, input_tokens, output_tokens, cost_usd, host, incident_id

## Common Queries

**Incidents last 24h with first_responder action:**
```sql
SELECT
    i.id, i.host, i.source,
    i.started_at, i.ended_at,
    ROUND(EXTRACT(EPOCH FROM (COALESCE(i.ended_at, NOW()) - i.started_at))/60) AS duration_min,
    i.challenge_count,
    i.spike_ratio::numeric(8,1) AS spike_ratio,
    i.traffic_peak_count AS peak_req_min,
    i.survey_country,
    fra.action, fra.confidence
FROM incidents i
LEFT JOIN LATERAL (
    SELECT action, confidence FROM first_responder_actions
    WHERE incident_id = i.id ORDER BY created_at DESC LIMIT 1
) fra ON true
WHERE i.started_at >= NOW() - INTERVAL '24 hours'
ORDER BY i.started_at DESC;
```

**Incident detail (country + ASN + UA + fingerprint):**
```sql
SELECT 'country' AS type, country AS key, cmd_count FROM incident_country_stats WHERE incident_id = <id>
UNION ALL
SELECT 'asn', asn_name, cmd_count FROM incident_asn_stats WHERE incident_id = <id>
UNION ALL
SELECT 'ua', ua, cmd_count FROM incident_ua_stats WHERE incident_id = <id>
UNION ALL
SELECT 'fingerprint', fingerprint, cmd_count FROM incident_fingerprint_stats WHERE incident_id = <id>
ORDER BY type, cmd_count DESC;
```

**Active incidents right now:**
```sql
SELECT id, host, source, started_at, spike_ratio::numeric(8,1), traffic_peak_count
FROM incidents
WHERE ended_at IS NULL
ORDER BY started_at DESC;
```

**LLM cost last 24h by source:**
```sql
SELECT source, COUNT(*) AS calls,
       SUM(input_tokens) AS input_tokens,
       SUM(output_tokens) AS output_tokens,
       ROUND(SUM(cost_usd)::numeric, 4) AS cost_usd
FROM llm_usage_log
WHERE ts >= NOW() - INTERVAL '24 hours'
GROUP BY source ORDER BY cost_usd DESC;
```

**Postgres table health:**
```sql
SELECT relname, n_live_tup, n_dead_tup,
       ROUND(n_dead_tup * 100.0 / NULLIF(n_live_tup + n_dead_tup, 0), 1) AS dead_pct,
       last_autovacuum
FROM pg_stat_user_tables
ORDER BY n_dead_tup DESC;
```

## Notes

- `traffic_spike` incidents: `incident_ips` is always empty (1-req-per-IP bots)
- `cluster_analysis` incidents: come from predictor, have IPs in `incident_ips`
- `first_responder_processed = false` means LLM hasn't acted yet (or was skipped)
- `blocked_fingerprints` entries expire automatically via `expires_at` TTL
