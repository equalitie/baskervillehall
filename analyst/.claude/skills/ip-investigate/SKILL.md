---
name: ip-investigate
description: Why did this IP get a challenge or block? Trace an IP (optionally with a host) through Baskerville commands, sessions, incidents and First Responder decisions. Use when support asks "why was X blocked/challenged" or "is this a false positive".
---

# Investigate an IP

Input: an IP, ideally the host and the approximate time (ask if missing; default last 3 days).
All queries go through `bin/q.sh`. Read-only.

## 1. Predictor commands for the IP (last 3 days)

```sql
SELECT created_at, host_name, command_type_name, source, hits, round(duration) AS dur_s,
       primary_session, human, passed_challenge, round(bot_score::numeric,2) AS bot_score,
       bot_score_top_factor, country, datacenter, scraper_name, baskerville_score,
       shapley_feature_if, user_agent
FROM challenge_command_history
WHERE ip_address = '<IP>' AND created_at > now() - interval '3 days'
ORDER BY created_at DESC LIMIT 20;
```

For the session behind a command, show the requests (JSON `[[seconds, url], ...]`):

```sql
SELECT created_at, host_name, source, left(requests, 2000) AS requests
FROM challenge_command_history
WHERE ip_address = '<IP>' AND created_at > now() - interval '3 days'
ORDER BY created_at DESC LIMIT 3;
```

Explain the `source` with the rule glossary in CLAUDE.md, then check the rule against the data
(e.g. for `Bad bot rule`: was the UA a real browser? one request without a cookie?). If unsure,
read the rule in `../src/baskervillehall/baskervillehall_predictor.py` / `baskerville_rules.py`.

## 2. Was the IP part of an incident?

```sql
SELECT i.id, i.host, i.source, i.started_at, i.ended_at,
       round(i.spike_ratio::numeric,1) AS spike, ip.hit_count
FROM incident_ips ip JOIN incidents i ON i.id = ip.incident_id
WHERE ip.ip = '<IP>'
ORDER BY i.started_at DESC LIMIT 10;
```

For each relevant incident, what did the First Responder do:

```sql
SELECT created_at, action, target, confidence, ttl_minutes, reasoning
FROM first_responder_actions WHERE incident_id = <ID> ORDER BY created_at;
```

An action can hit the IP without naming it: `challenge_all` (whole host), `challenge_ua` /
`block_ua` (matches the IP's UA exactly), `*_ua_fingerprint`, `challenge_subnet` (its /24).
Compare the target with the IP's UA, fingerprint and subnet.

## 3. Sessions on the host (needs host; keep the window to hours)

```sql
SELECT session_start, session_end, class, human, verified_bot, primary_session, passed_challenge,
       hits, asn_name, country, top_url, user_agent
FROM sessions
WHERE host_name = '<HOST>' AND ip = '<IP>'
  AND session_end BETWEEN '<T-3h>' AND '<T+1h>'
ORDER BY session_end DESC LIMIT 20;
```

## 4. Kibana (if the database has nothing, or older than 3 days)

- `baskerville_banjax_command-*`: `client_ip:"<IP>"`. Shows every command we sent, including First
  Responder ones (`source` starts with `first_responder`, `cluster_responder`, `[static_ua_block]`).
- `deflect.log-*`: `client_ip:"<IP>"`, look at `banjax_decision`, `http_response_code`,
  `client_url`, `client_ua`. Did it pass (`ShaChallengePassed`) or get stuck?
- `banjax-*`: `client_ip:"<IP>"`, `action` + `trigger` say which banjax rule or list fired.
  A block that isn't from Baskerville (e.g. `trigger: global_ua_list`, regex rules) is banjax config,
  not us.

## 5. Answer

- What happened to the IP (command, when, which host, TTL).
- Which component and rule did it, quoting `source` / `reasoning`.
- Whether that was justified, with evidence: UA, request pattern, ASN, passed challenge or not.
- If it looks like a false positive: say so plainly and name the rule to fix.
