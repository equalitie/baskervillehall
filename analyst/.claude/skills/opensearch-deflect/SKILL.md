---
name: opensearch-deflect
description: Query Deflect's logs in OpenSearch (Kibana at kibana.equalit.ie) from the command line. Use this whenever the user asks about traffic to a Deflect site, top IPs / user agents / countries / ASNs hitting a host, HTTP status or 5xx/403 breakdowns, cache hit rates, banjax bans or challenges, Baskerville commands sent to banjax, edge errors, or wants to check what Kibana / OpenSearch / the Deflect access log says for a time window, even if they do not name OpenSearch explicitly. Read-only.
---

# Query Deflect logs in OpenSearch

Copied from equalitie/baskerville-skills (5342378), without the 6-hour confirmation rule.

OpenSearch itself is not reachable. All queries go through the OpenSearch Dashboards
console proxy at `https://kibana.equalit.ie`, wrapped by `scripts/osq.sh`.

## Time-range limits (very important: they prevent cluster crashes)

The cluster is shared production infrastructure, and wide queries have taken it down.
These rules come from the skill owner and are not negotiable:

| Time range | Rule |
|---|---|
| Up to 1 day | Run it. |
| More than 1 day | Must also have at least one query condition besides the time range (for example `client_request_host`). |
| More than 7 days | **Never.** Not even if the user asks. Offer a narrower window or a daily breakdown over several queries. |

`osq.sh` enforces these limits (`scripts/check_query.py`) before it sends anything:
- A query with no `@timestamp` lower bound in a `must`/`filter` clause is refused (exit 5).
- More than 7 days, or more than 1 day with no other condition, is refused (exit 5).

Date rounding counts against you: `now-7d/d` can reach back 8 days and is refused. Use `now-7d`.

## Running queries

```bash
S=<skill base directory>/scripts/osq.sh
$S POST 'deflect.log-*/_search' '{"size":0,"timeout":"25s", ...}'
$S POST 'banjax-*/_count' '{"query":{...}}'
$S GET  'baskerville_banjax_command-*/_field_caps?fields=source,baskerville_command'
$S POST 'deflect.log-*/_search' @/path/to/query.json     # body from a file
```

The script:
- reads the username and password from the macOS Keychain item with service `kibana.equalit.ie`
- allows only GET/POST on `<index>/_search|_count|_msearch|_field_caps|_mapping`, and no scroll
- trims `_search` output to `took, timed_out, _shards.failed, hits.total, hits.hits._index,
  hits.hits._source, aggregations, count` (pass your own `filter_path=` or set `OSQ_RAW=1` to change that)
- prints a WARNING on stderr when results are partial (`timed_out` or failed shards). Say so in the answer.

## Field reference

`analyst/docs/deflect-log-columns.md` (three levels up from this skill directory) defines every column of the three main indexes, the banjax decision values, the
Baskerville `source` values, and the known gotchas. Read the section for the index you
query before writing anything non-trivial. Key points:

- **`host` is the edge server, not the website.** The website is `client_request_host`.
- All strings are `keyword` (exact match, aggregatable, no `.keyword` suffix). Numbers are floats.
  In the command log `human` and `datacenter_asn` are boolean.
- Empty strings are dropped at ingest: use `exists`, not `term: ""`.
- `http_response_code` 210 is Deflect's fail-open (banjax down, served anyway). A 200-300
  range bucket includes it; break it out when banjax health matters.
- `@timestamp` is UTC. State times in UTC in answers.

| Index pattern | One doc per | Use for |
|---|---|---|
| `deflect.log-*` | HTTP request at an edge (~2.3M/hour, ~1,700 sites/hour) | Traffic, status codes, cache, IPs, UAs, banjax decision per request |
| `banjax-*` | Ban, or request blocked by a list | Bans (`rule_type` `regex`/`failed_challenge`) and blocked requests (`decision_rendered`) |
| `baskerville_banjax_command-*` | Command Baskerville sent to banjax | Why an IP/session/UA was challenged or blocked (`source`, `baskerville_command`) |
| `deflect_error.log-*` | nginx error line (`dnet`, `host`, `level`, `error_message`) | Edge errors |
| `dnet_forwarder-*` | Forwarded connection | Forwarder throughput and upstreams |
| `ats-banjax-*`, `cstats.stats_*` | Legacy / pre-aggregated stats | Only when asked |

Ignore `delete-deflect.log-*` and `delete-banjax-*`: those are sites that opted out of logging.

Indexes are daily (`deflect.log-YYYY.MM.DD`, UTC). `deflect.log-*` fans out to about 680 shards;
naming the days you need (`deflect.log-2026.11.09,deflect.log-2026.11.10`) is lighter.

The account cannot run `_cat/*` or cluster APIs (403). Use `_field_caps` to discover fields.

## Other rules for querying

1. Start with the smallest time window that answers the question, then widen if needed (within the limits above).
2. **Aggregate, don't fetch.** Use `"size":0` plus aggregations. When raw documents are needed,
   set `"size"` ≤ 20 and a `"_source"` list of only the fields you need.
3. **Put `"timeout":"25s"` in every `_search` body** (not `_count`: it rejects `timeout`).
   The Dashboards proxy cuts requests off at about 30s.
4. Filter high-cardinality `terms` (`client_ip`, `client_url`, `client_ua`, `deflect_session`) by
   `client_request_host` when the window is over 1 hour. Keep `terms.size` ≤ 50.
5. Pick `fixed_interval` so a `date_histogram` has ≤ 500 buckets (1m for an hour, 5m for 6 hours, 1h for a week).
6. Use `cardinality` with `"precision_threshold": 1000` for unique counts. It is approximate; say so.
   `hits.total` stops at 10,000 (`"relation":"gte"`). Add `"track_total_hits":true` or use `_count` for an exact total.
7. No leading-wildcard or regexp queries. Hostnames are exact: check the `www.` variant too.
8. Run queries one at a time. Do not fan out parallel runners against this cluster.
9. Results contain visitor IPs, user agents and session cookies. Summarise them in the reply;
   do not write raw output into this repo.

## Ready-made queries

Replace `example.org` and the time range. All bodies go in the third argument.

**Traffic for a site over time, with status classes** (`deflect.log-*`):
```json
{"size":0,"timeout":"25s",
 "query":{"bool":{"filter":[{"term":{"client_request_host":"example.org"}},
   {"range":{"@timestamp":{"gte":"now-3h"}}}]}},
 "aggs":{"t":{"date_histogram":{"field":"@timestamp","fixed_interval":"5m"},
   "aggs":{"status":{"range":{"field":"http_response_code",
     "ranges":[{"key":"2xx","from":200,"to":300},{"key":"3xx","from":300,"to":400},
               {"key":"4xx","from":400,"to":500},{"key":"5xx","from":500,"to":600}]}}}}}}
```

**Top sources hitting a site (IP, country, network, UA)** (`deflect.log-*`):
```json
{"size":0,"timeout":"25s",
 "query":{"bool":{"filter":[{"term":{"client_request_host":"example.org"}},
   {"range":{"@timestamp":{"gte":"now-1h"}}}]}},
 "aggs":{"ip":{"terms":{"field":"client_ip","size":20}},
         "country":{"terms":{"field":"geoip.country_code2","size":15}},
         "asn":{"terms":{"field":"geoip.as_org","size":15}},
         "ua":{"terms":{"field":"client_ua","size":10}},
         "uniq_ip":{"cardinality":{"field":"client_ip","precision_threshold":1000}}}}
```

**Busiest sites right now** (`deflect.log-*`):
```json
{"size":0,"timeout":"25s","query":{"range":{"@timestamp":{"gte":"now-15m"}}},
 "aggs":{"site":{"terms":{"field":"client_request_host","size":20}}}}
```

**Banjax outcome mix for a site** (`deflect.log-*`: what banjax decided per request):
```json
{"size":0,"timeout":"25s",
 "query":{"bool":{"filter":[{"term":{"client_request_host":"example.org"}},
   {"range":{"@timestamp":{"gte":"now-1h"}}}]}},
 "aggs":{"decision":{"terms":{"field":"banjax_decision","size":20}},
         "loc_out":{"terms":{"field":"loc_out","size":10}},
         "cache":{"terms":{"field":"cache_result","size":10}}}}
```

**Bans and blocks for a site** (`banjax-*`):
```json
{"size":0,"timeout":"25s",
 "query":{"bool":{"filter":[{"term":{"client_request_host":"example.org"}},
   {"range":{"@timestamp":{"gte":"now-3h"}}}]}},
 "aggs":{"type":{"terms":{"field":"rule_type","size":5},
   "aggs":{"trigger":{"terms":{"field":"trigger","size":10}},
           "action":{"terms":{"field":"action","size":5}},
           "ips":{"cardinality":{"field":"client_ip","precision_threshold":1000}}}}}}
```

**Baskerville commands for a site** (`baskerville_banjax_command-*`):
```json
{"size":0,"timeout":"25s",
 "query":{"bool":{"filter":[{"term":{"client_request_host":"example.org"}},
   {"range":{"@timestamp":{"gte":"now-3h"}}}]}},
 "aggs":{"cmd":{"terms":{"field":"baskerville_command","size":10}},
         "source":{"terms":{"field":"source","size":15}},
         "t":{"date_histogram":{"field":"@timestamp","fixed_interval":"10m"}}}}
```
To find commands for one IP, match both fields, because Incident First Responder
commands carry lowercase `value` and no `client_ip`:
`{"bool":{"should":[{"term":{"client_ip":"203.0.113.10"}},{"term":{"value":"203.0.113.10"}}],"minimum_should_match":1}}`
Subnet commands store a CIDR in `value` (for example `203.0.113.0/24`), so an exact IP
term misses them. Also check `value` for the covering subnet when it matters.

**Edge error spikes** (`deflect_error.log-*`):
```json
{"size":0,"timeout":"25s","query":{"range":{"@timestamp":{"gte":"now-1h"}}},
 "aggs":{"dnet":{"terms":{"field":"dnet","size":10},
   "aggs":{"level":{"terms":{"field":"level","size":5}},
           "msg":{"terms":{"field":"error_message","size":5}}}}}}
```

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `REFUSED` (exit 5) | Query breaks a time-range limit | Narrow the window or add a filter. Do not try to get around the guard. |
| `HTTP 401 Authentication Exception` | Keychain password wrong or stale | User runs `security delete-generic-password -s kibana.equalit.ie` then `security add-generic-password -s kibana.equalit.ie -a <user> -w`. Do not retry in a loop: repeated failures can lock the account. |
| `no Keychain item` | Item missing | Same `add-generic-password` command |
| `HTTP 403 security_exception` | Role lacks that permission (for example `_cat`) | Use `_search` / `_field_caps` instead |
| `HTTP 400` on `_count` | `_count` accepts only `query` | Remove `timeout`, `size`, `aggs` |
| `HTTP 500` from the proxy | Malformed request or extra headers | Check the path and JSON |
| curl timeout, or `timed_out` warning | Query too heavy | Narrow the time range, add a host filter, lower `size` |
