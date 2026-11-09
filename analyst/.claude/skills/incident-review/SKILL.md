---
name: incident-review
description: Review Baskerville incidents for a host (or one incident id) - attack profile, what the First Responder decided and why, and whether it worked. Use for "what is going on with site X", customer incident reports, or checking an AI decision.
---

# Review incidents for a host

Input: a host (and a period, default 24 h) or an incident id. All queries via `bin/q.sh`.

## 1. Incidents

```sql
SELECT id, source, started_at, ended_at,
       round(EXTRACT(EPOCH FROM (coalesce(ended_at, now()) - started_at))/60) AS dur_min,
       round(spike_ratio::numeric,1) AS spike, round(baseline_avg::numeric) AS baseline,
       traffic_peak_count AS peak_rpm, challenge_count, round(immature_ratio::numeric,2) AS immature
FROM incidents
WHERE host = '<HOST>' AND started_at > now() - interval '24 hours'
ORDER BY started_at DESC;
```

## 2. Attack profile of an incident

```sql
SELECT 'country' AS type, country AS key, cmd_count FROM incident_country_stats WHERE incident_id = <ID>
UNION ALL SELECT 'asn', asn_name, cmd_count FROM incident_asn_stats WHERE incident_id = <ID>
UNION ALL SELECT 'ua', ua, cmd_count FROM incident_ua_stats WHERE incident_id = <ID>
UNION ALL SELECT 'fingerprint', fingerprint, cmd_count FROM incident_fingerprint_stats WHERE incident_id = <ID>
UNION ALL SELECT 'url', url, cmd_count FROM incident_url_stats WHERE incident_id = <ID>
ORDER BY type, cmd_count DESC;
```

Compare with the host's normal profile (`host_country_stats`, `host_asn_stats`, `host_ua_stats`).

## 3. First Responder decisions

```sql
SELECT a.incident_id, a.created_at, a.action, a.target, a.confidence, a.ttl_minutes, a.reasoning
FROM first_responder_actions a
WHERE a.host = '<HOST>' AND a.created_at > now() - interval '24 hours'
ORDER BY a.created_at;
```

Check each decision against the stats: does the reasoning match the numbers? Is the target too
broad (a common Chrome UA, a whole country) or empty? Was `monitor_only` chosen during a big spike,
and why? Remember `block_*` is sent as `challenge_*` while blocks are disabled.

## 4. Did it work

What banjax did with the host's traffic per minute, around the action time:

```sql
SELECT minute, served, passed, challenged, blocked, rate_limited
FROM host_mitigation_minute
WHERE host = '<HOST>' AND minute > now() - interval '3 hours'
ORDER BY minute;
```

Per UA: same from `host_ua_mitigation_minute`. Origin up? `origin_uptime` / `origin_health` for the
host. In Kibana `deflect.log-*`: request rate, `banjax_decision` and `cache_result` before and after.
Bots that pass the challenge show up as `ShaChallengePassed` with an unchanged request rate.

## 5. Write-up

Summary → Attack profile → Timeline → AI response (quoted reasoning) → Result → Recommendation.
Save to `reports/<host>_<YYYY-MM-DD>.md` if a report was asked for.
