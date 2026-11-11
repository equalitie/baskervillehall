---
updated: 2026-11-10
sources:
  - "deflect-kb@7420542: deflect-system/nginx-log.md"
  - "deflect-kb@7420542: deflect-system/banjax-log.md"
  - "deflect-kb@7420542: opsdash/opsdash-overview.md"
  - "deflect-kb@7420542: banjax/banjax-kafka-commands.md"
  - "deflect@346d021: config_generation/modules/nginx_http_block.py"
  - "banjax@45f7119: internal/iptables.go"
  - "banjax@45f7119: internal/kafka.go"
  - "DeflectAnalytics@244f943: logstash/pipelines/main_opsdash_json.conf"
  - "DeflectAnalytics@244f943: logstash/pipelines/main_template.json"
  - "DeflectAnalytics@244f943: logstash/pipelines/banjax_command.conf"
  - "DeflectAnalytics@244f943: logstash/pipelines/banjax_command_template.json"
  - "DeflectAnalytics@244f943: logstash/pipelines/torexits.json"
  - "baskervillehall@306a2ca: src/baskervillehall/baskervillehall_predictor.py"
  - "baskervillehall@306a2ca: src/baskervillehall/incident_first_responder.py"
  - "baskervillehall@306a2ca: src/baskervillehall/first_responder_blocker.py"
  - "baskervillehall@306a2ca: src/baskervillehall/country_blocker.py"
  - "baskervillehall@306a2ca: src/baskervillehall/block_guard.py"
---

# Deflect log columns

Column reference for the three Deflect log indexes in OpenSearch (Kibana). It is the base for a connector that reads them.

| Log | Index pattern | One document per | Written by |
|---|---|---|---|
| Deflect access log | `deflect.log-YYYY.MM.DD` | HTTP request served by an edge | nginx on each edge |
| Banjax log | `banjax-YYYY.MM.DD` | Enforcement event (ban, or request blocked by a list) | banjax on each edge |
| Baskerville command log | `baskerville_banjax_command-YYYY.MM.DD` | Command Baskerville sent to banjax over Kafka | Baskerville (Kubernetes), read off Kafka by Logstash |

Sites that turned off visitor logs go to `delete-deflect.log-*` and `delete-banjax-*` instead, which are deleted later. Do not report on them.

Detailed pages, written for support staff, are in the KB clone: `_contexts/deflect-kb/deflect-system/nginx-log.md`, `banjax-log.md`, `opsdash/opsdash-overview.md` and `banjax/banjax-kafka-commands.md`. This file summarises them and adds what a connector needs: stored field types, renames, and fields the KB does not cover (the whole command log).

## Rules that apply to all three indexes

1. **Types.** The index templates map every string to `keyword` and every JSON integer to `float`. Only a few fields have explicit types: `client_ip` is `ip`, `geoip.location` is `geo_point`, and in the command log `score` is `scaled_float` and `shapley` is `nested`. So a value the producer writes as a quoted string (for example `upstream_status`, `deflect_session_new`) is a `keyword`, not a number or boolean.
2. **Empty strings are dropped.** Logstash removes every top-level field whose value is `""`. Test with `_exists_:field`, not `field:""`.
3. **Document id** is a keyed SHA1 (HMAC, Logstash `fingerprint` with a key) of the raw line, so a connector cannot recompute it. A line sent twice overwrites itself, it is not counted twice.
4. **Time.** Use `@timestamp`. Logstash sets it from `time_local` (access log), `timestring` (banjax log) or `end` (command log), then removes the source text field.
5. **Enrichment.** Every document with `client_ip` gets `geoip.country_code2`, `geoip.country_name`, `geoip.city_name`, `geoip.continent_code`, `geoip.location` and `geoip.as_org` (network owner). It gets the tag `torified` in `tags` when the IP is on a Tor exit list. That list was last changed in 2023, so a missing tag does not prove the visitor did not use Tor.
6. **Edge fields.** In the access and banjax logs, filebeat on the edge adds `dnet` (edge group) and Logstash sets `host` to the name of the edge that wrote the line. `host` is the edge, not the website. The website is `client_request_host`.

## 1. Deflect access log (`deflect.log-*`)

One document per HTTP request, written when the response completes. Most requests go to banjax for an allow or deny decision first and then to the origin, so `upstream_*` fields usually hold two values: banjax first, origin second.

Source: `deflect: config_generation/modules/nginx_http_block.py` (log format `logstash_format_json`), `DeflectAnalytics: logstash/pipelines/main_opsdash_json.conf`.

### Request identity and timing

| Column | Type | Definition |
|---|---|---|
| `@timestamp` | date | Request time, from `time_local`. Second precision. |
| `msec` | date | The same instant with millisecond precision. Logstash converts the edge's epoch seconds to a date. Use it to order requests within one second. |
| `request_id` | keyword | 32 hex characters, unique per request. Also sent upstream, so it joins one request across nginx, banjax and origin logs. |
| `request_time` | float | Seconds from the first byte read from the client to the last byte sent. Includes slow-client transfer time. For origin latency use `upstream.response_time.right`. |

### What the client asked for

| Column | Type | Definition |
|---|---|---|
| `client_ip` | ip | TCP peer address. The reliable client field. If a proxy sits in front of the edge, this is the proxy. |
| `client_user` | keyword | HTTP Basic auth user name. Almost always absent. |
| `client_request_host` | keyword | Website hostname requested. The key for per-site reporting. |
| `client_request_method` | keyword | `GET`, `POST`, `HEAD`, and so on. |
| `client_url` | keyword | Path after nginx normalisation and rewrites, no query string. |
| `querystring` | keyword | Query string without the `?`. |
| `http_request_scheme` | keyword | `http` or `https`. |
| `http_request_version` | keyword | `HTTP/1.0`, `HTTP/1.1`, `HTTP/2.0` or `HTTP/3.0`. |
| `client_ua` | keyword | `User-Agent` header, verbatim. Client-controlled. |
| `user_agent.*` | object | Browser, OS and device parsed from `client_ua` by Logstash. |
| `referer` | keyword | `Referer` header, verbatim. Client-controlled. |
| `refererdomain` | keyword | Domain part of `referer`, added by Logstash. |
| `accept_language` | keyword | `Accept-Language` header, verbatim. |
| `accept_language_parsed` | object list | Logstash parse of `accept_language`: `language_primary`, `language_region`, `quality`. |
| `forwardedfor` | keyword | `X-Forwarded-For` as the client sent it. Not added or checked by Deflect. |

### What the edge answered

| Column | Type | Definition |
|---|---|---|
| `http_response_code` | float | Status returned to the client. `210` is Deflect-specific: banjax failed and the edge served the request anyway (`loc_out` = `fail_open`). |
| `reply_length_bytes` | float | Response body bytes, excluding headers. |
| `content_type` | keyword | `Content-Type` response header. |
| `cache_result` | keyword | `HIT`, `MISS`, `BYPASS`, `EXPIRED`, `STALE`, `UPDATING`, `REVALIDATED`, or absent when not cacheable. |

### Route that handled it

| Column | Type | Definition |
|---|---|---|
| `loc_in` | keyword | nginx route that matched the incoming request. Main values: `slash_block` (ordinary pages, the bulk), `static_file`, `pass_prot` (password-protected path), `restricted_path`, `blocked_path`, `websocket_path`, `cache_exc`, `tools_451`, `deflect_challenge`, `robots_txt`, redirects (`prefix_redirection`, `redir_to_ssl`, `redir_apex_www`, `redir_sitewide`), `maint_page`, `slash_banjax_bypass`, `edge_api_override`, `system_site` (Deflect's own hosts: exclude from client reporting). |
| `loc_out` | keyword | Route that produced the response. Any `loc_in` value, plus `access_granted` (normal), `access_denied` (403 from banjax), `fail_open` (banjax down, served anyway, status 210), `fail_closed` (not enabled), `options_no_banjax` (CORS preflight), `restricted_arg` (403 on a query parameter). |

### Origin and upstream

The edge writes these as text such as `200 : 502`. Logstash keeps the text field and adds numeric `upstream.<name>.left` (banjax) and `upstream.<name>.right` (origin). With one value, only `right` is set.

| Column | Type | Definition |
|---|---|---|
| `upstream_addr` | keyword | Address and port of each upstream contacted. |
| `upstream_status`, `upstream.status.left/right` | keyword, float | Status from each upstream. |
| `upstream_connect_time`, `upstream.connect_time.*` | keyword, float | Seconds to connect, including TLS. |
| `upstream_header_time`, `upstream.header_time.*` | keyword, float | Seconds until response headers arrived. Closest to origin think time. |
| `upstream_response_time`, `upstream.response_time.*` | keyword, float | Seconds until the full response arrived. |
| `upstream_bytes_sent`, `upstream.bytes_sent.*` | keyword, float | Bytes sent to each upstream. |
| `upstream_bytes_received`, `upstream.bytes_received.*` | keyword, float | Bytes received from each upstream. |
| `proxy_host`, `proxy_port` | keyword | Proxied server name and port as configured, not as resolved. |

### Banjax decision

Absent (or `-`, or `-1` for the score) on requests that never reached banjax, such as redirects and maintenance pages.

| Column | Type | Definition |
|---|---|---|
| `banjax_decision` | keyword | Why banjax allowed, challenged, rate-limited or blocked the request. Prefix is the rule source (`PerSite`, `Global`, `Expiring`, `SiteWide`, with `UA` for user-agent rules), suffix the outcome (`AccessGranted`, `Challenge`, `AltChaChallenge`, `Block`, and so on). `NoMention` means nothing matched and the request was allowed. `ExpiringChallenge` is a per-IP challenge, typically from Baskerville. `ShaChallengePassed` is the request after a solved challenge. Before banjax v0.13.0 (2026-11-06) challenge pages logged `-`. Full list: KB `banjax/banjax-decisions.md`. |
| `banjax_error` | keyword | Set only when banjax crashed on the request. Any value means the request went through `fail_open`. |
| `banjax_bot_score` | float | 0.0 to 1.0 automation likelihood from the browser fingerprint. `-1` means not computed, which is most requests. `-1` does not mean human. |
| `banjax_bot_score_top_factor` | keyword | Heaviest signal behind the score: `webdriver`, `gpu_renderer`, `no_plugins`, `zero_lang`, `low_cpu`, `low_memory`, `fullscreen`, `color_depth`, `small_screen`. |
| `banjax_bot_fingerprint` | keyword | SHA-256 of the reported browser characteristics. Groups one client across IPs; many ordinary devices share one value. |

### Session, site labels and TLS

| Column | Type | Definition |
|---|---|---|
| `deflect_session` | keyword | Signed `deflect_session` cookie issued by banjax. Best key for grouping requests into visits. Absent when the client sent no cookie. |
| `deflect_session_new` | keyword | `"true"` when banjax issued a new cookie on this request, `"false"` when an existing one validated. A string, not a boolean. |
| `survey_country` | keyword | Fixed ISO country label configured for the site, not the visitor's location. `-` when unset. |
| `survey_regional_focus` | keyword | Fixed regional label configured for the site. `-` when unset. |
| `inner_sni` | keyword | TLS server name requested (the real name when Encrypted Client Hello is used). |
| `outer_sni` | keyword | ECH public cover name. Absent when ECH was not used. |
| `ech_status` | keyword | `SUCCESS`, `NOT_TRIED` (normal), `GREASE`, `BACKEND`, `FAILED`, or `-` where ECH is off. |

**Added by Logstash to access and banjax documents:** `filter_duration` (milliseconds Logstash spent on the line). `tags` can contain `_grokparsefailure` when the referer or path parse fails.

**Written on the edge but removed by Logstash, so not in the index:** `time_local`, `disable_logging`, `ssl_cipher`, `ssl_ciphers`, `banjax_bot_fingerprint_full`.

## 2. Banjax log (`banjax-*`)

A log of enforcement events, not of traffic. Allowed, challenged and passed-through requests are not here. Three event types, split by `rule_type`:

- `regex`: a client tripped a rate-limit rule and was newly banned. One row per ban.
- `failed_challenge`: a client failed the challenge too often and was newly banned. One row per ban.
- `decision_rendered`: a request was blocked because the client was already on a list. One row per blocked request (may be rate-limited per client, site and trigger).

So `regex` and `failed_challenge` rows count bans, and `decision_rendered` rows count blocked requests.

Source: `banjax: internal/iptables.go` (`LogJson`), `DeflectAnalytics: logstash/pipelines/main_opsdash_json.conf`.

| Column | Type | Definition |
|---|---|---|
| `@timestamp` | date | Event time, from `timestring` (`2006-01-02T15:04:05`, edge local time, UTC on a standard edge). Second precision. For `regex` it is the time of the request that crossed the limit. |
| `client_ip` | ip | Client address. The banned address for `regex` and `failed_challenge`. |
| `client_ua` | keyword | `User-Agent` sent by the client. Often absent on challenge failures. |
| `client_request_host` | keyword | Website hostname requested. |
| `path` | keyword | Request path as banjax saw it. |
| `client_url`, `querystring` | keyword | Path and query string, split from `path` by Logstash. Unlike the access log, `querystring` here keeps the leading `?`. |
| `client_request_method` | keyword | HTTP method. |
| `http_request_scheme` | keyword | Always `https` (hardcoded). Ignore it. |
| `rule_type` | keyword | `regex`, `failed_challenge` or `decision_rendered`. |
| `trigger` | keyword | Detail under `rule_type`. `regex`: the operator-defined rule name (for example `instant block`). `failed_challenge`: `failed challenge sha_inv`, `failed challenge altcha`, `failed challenge altcha_click`, `failed challenge password`, `failed challenge too_many_403`. `decision_rendered`: the list matched: `per_site_ip_list`, `global_ip_list`, `per_site_ua_list`, `global_ua_list`, `expiring_ua_list`, or `baskerville` (a Kafka command, including `block_subnet`; `client_ip` is still the visitor). |
| `action` | keyword | `NginxBlock` (403 at nginx), `IptablesBlock` (dropped at the firewall: the hard ban). For `regex` rows also `Challenge` or `TooManyRequests` (429 page, banjax v0.11.2 and later). |
| `number_of_fails` | float | Configuration value, not a measurement: the threshold for `failed_challenge`, always `1` for `regex`, always `0` for `decision_rendered`. |
| `dnet`, `host` | keyword | Edge group and edge name (see rule 6). |

`disable_logging` is written on the edge and removed by Logstash.

## 3. Baskerville command log (`baskerville_banjax_command-*`)

One document per command Baskerville sent to banjax on the Kafka topic `banjax_command_topic`. Logstash reads the topic, renames a few fields and stores everything else as sent. The KB does not document this index yet; this section is from code.

Source: `DeflectAnalytics: logstash/pipelines/banjax_command.conf`, `banjax_command_template.json`; `baskervillehall: src/baskervillehall/*.py` as listed in the front matter.

### Who sends commands

Find the sender from `source` (or `meta`, which holds the same text for most senders).

| Sender | `source` looks like | Commands | Fields sent |
|---|---|---|---|
| Predictor (ML and rules) | `anomaly [challenge_ip]`, `Bad bot rule [block_ip]`, `classifier [challenge_session]`, `high_bot_score [block_ip]`, `weak_cipher [challenge_ip]`, `scraper [challenge_ip]`, `ai_spoofer`, `ai_bot_ua`, `commercial_crawler`, `brute_force_4xx`, `wp_brute_force`, `datacenter_asn [attack_response] [...]`, `Too many sessions. [challenge_ip]`, `jev [...] type=...`, and `first_responder [block_country\|block_asn\|block_ua\|block_fingerprint] [block_ip]` when it applies a First Responder decision. `[attack_response]` marks a site under attack; ` [novel_attack count=N]` can be appended. | `*_ip`, `*_session` | The large predictor set below. No `dnet`. |
| First responder blocker (session pipeline) | `first_responder [<action>] [<command>] [session_pipeline]` | `block_ip` (or `challenge_ip`) | `Value`, `host`, `country`, `dnet`, `ttl` |
| Incident First Responder / cluster responder | `cluster_responder [<command>] <reason>`, `first_responder [block_fingerprint] [block_ip]` | `*_ip`, `*_subnet` | lowercase `value`, `host`, `dnet`, `ttl` |
| Incident First Responder, non-IP | `source` = `first_responder`, `meta` = `first_responder [<command>] <reason>` | `block_ua`, `challenge_ua`, `challenge_all` | `ua` and `host`, or `host` only, `ttl` |
| Country blocker | `country_block` | `block_ip` or `challenge_ip` | `Value`, `host`, `country`, `dnet`, `ttl` |

While `BASKERVILLE_BLOCKS_DISABLED` is on (since November 2026), every `block_ip`, `block_subnet` and `block_session` is sent as the matching `challenge_*`, and a bracketed name inside `source`/`meta` (for example `[block_ip]`) is rewritten to match (`block_guard.py`). Sources without a bracketed name, such as `ai_spoofer`, keep their text, so they do not show whether a block or a challenge was intended. Sites in `BASKERVILLE_HOST_WHITELIST` never get commands.

### Columns renamed or set by Logstash

| Column | From | Type | Definition |
|---|---|---|---|
| `baskerville_command` | `Name` | keyword | Command name: `challenge_ip`, `block_ip`, `challenge_subnet`, `block_subnet`, `challenge_session`, `block_session`, `challenge_all`, `challenge_ua`, `block_ua`, `altcha_challenge_*`, `clear_rules`. See KB `banjax/banjax-kafka-commands.md` for effects and default lifetimes. |
| `client_ip` | `Value` | ip | Target IP. For `*_session` commands it is the visitor's IP, kept for reference. Only set when the sender used capital `Value` (see gotcha 1). |
| `client_request_host` | `host` | keyword | Website the command is about, lowercased. Note: banjax applies IP, subnet and session decisions on every site on the edge, not only this one. |
| `deflect_session` | `session_id` | keyword | Target session cookie for `*_session` commands. |
| `@timestamp` | `end` | date | End of the scored session (`yyyy-MM-dd HH:mm:ss`). Commands without `end` (all non-predictor senders) get the time Logstash processed them. |
| `start_time` | `start` | date | Start of the scored session. Predictor only. |
| `geoip.*`, `tags: torified` | `client_ip` | | See rule 5. Absent when `client_ip` is absent. |

### Columns from every sender

| Column | Type | Definition |
|---|---|---|
| `source` | keyword | Which component and rule sent the command (table above). Best field for grouping by reason. |
| `meta` | keyword | Same text as `source` for most senders; the longer form for Incident First Responder non-IP commands. |
| `ttl` | float | Requested lifetime in seconds. Absent on predictor commands, which use banjax's default (per the KB: 3600 for challenges, 600 for blocks, unless the site has a per-site block lifetime). |
| `dnet` | keyword | Edge group the command was routed to. Absent on predictor commands and on Incident First Responder non-IP commands. |
| `country` | keyword | Visitor country code. Predictor, first responder blocker and country blocker. |
| `value` | keyword | Lowercase target IP or CIDR from the Incident First Responder (see gotcha 1). |
| `ua` | keyword | Exact User-Agent for `*_ua` commands; also on full predictor payloads. |

### Predictor columns

Sent on both payloads unless marked. Shapley entries list features that pushed the session toward anomalous, sorted by impact.

| Column | Type | Definition |
|---|---|---|
| `score`, `score_if` | scaled_float, float | Isolation Forest anomaly score. `score` is a copy of `score_if`. |
| `score_ae` | float | AutoEncoder reconstruction score. |
| `threshold_ae` | float | AutoEncoder threshold in use. |
| `prediction_if`, `prediction_ae` | float | `1` when that model flagged the session, else `0`. |
| `shapley`, `shapley_if` | nested | List of `{name, values: {shapley, feature}}` for the Isolation Forest: feature name, its (negative) Shapley value and the feature's value. `shapley` is a copy of `shapley_if` and is the only one mapped as `nested`. |
| `shapley_ae` | object list | Same for the AutoEncoder. |
| `shapley_feature`, `shapley_feature_if`, `shapley_feature_ae` | keyword | Name of the single strongest feature. |
| `difficulty` | float | Always `0` in current code. |
| `duration` | float | Session length in seconds. |
| `num_requests` | float | Requests in the session. |
| `user_agent` | keyword | Session User-Agent. |
| `human` | keyword/boolean | Rule-based verdict: true when the session's rule score is above 30 and it is not a verified bot. |
| `bot_score`, `bot_score_top_factor` | float, keyword | Banjax browser bot score for the session (see access log). |
| `continent`, `datacenter_code` | keyword | Visitor continent and datacenter code. |
| `datacenter_asn` | boolean | IP belongs to a datacenter network. |
| `asn_name` | keyword | Network owner name. |
| `scraper_name` | keyword | Matched scraper name, if any. |
| `rate_limit_hits`, `rate_limit_interval`, `rate_limit_expiration` | float | Rate-limit parameters, usually `0`. |
| `survey_country` | keyword | Site's fixed country label. |
| `print_log` | boolean | **Short payload only.** Tells banjax to log the command. |
| `session`, `top_url`, `fingerprints`, `api_ratio`, `path_only_to_request_ratio`, `baskerville_score`, `cloudflare_score`, `novel_attack`, `novel_attack_count` | mixed | **Full payload only.** `session` is the whole session object, including its request list. |

### Gotchas for a connector

1. **Incident First Responder IP commands have no `client_ip`.** It sends lowercase `value`. Banjax still acts on them (Go JSON matching ignores case), but Logstash's rename is case-sensitive. Per code, these documents have `value`, no `client_ip`, no `geoip` and no `torified` tag. A search on `client_ip` misses them, including the one in the baskervillehall analyst skill `ip-investigate`. Query both fields: `client_ip:"203.0.113.10" OR value:"203.0.113.10"`. Confirm with `source:cluster_responder* AND _exists_:value`.
2. **Which predictor payload is stored is not known from code.** The predictor sends a full payload to Baskerville's internal Kafka and a short one to the output Kafka that feeds banjax. Both use the topic name `banjax_command_topic`. Logstash reads the Kafka named in its `BASKERVILLE_BOOTSTRAP_SERVERS` setting, which is runtime config. Check with predictor documents (`NOT source:*first_responder* AND NOT source:country_block AND NOT source:cluster_responder*`): `_exists_:print_log` means the short payload, `_exists_:session` means the full one. If both match, Logstash reads both clusters and every predictor command is stored twice (the payloads differ, so the SHA1 ids differ). Other senders send identical bytes to both, so they deduplicate.
3. **`@timestamp` is not always the send time.** For predictor commands it is the session end, which can be minutes before the command. The daily index follows `@timestamp`.
4. **Time zone.** `end`/`start` carry no zone and Logstash parses them in its own time zone. Assumed UTC, not verified.
5. **Empty-field removal is top-level only.** Empty strings inside `session` stay.
6. **`client_ip` is type `ip`.** A CIDR in capital `Value` would make the document fail to index. No current sender does this (subnet commands use lowercase `value`).

## Joining the three logs

| Question | Join |
|---|---|
| Did a Baskerville command take effect? | Command `client_ip`/`value` + time → banjax log `trigger:baskerville` (blocks) or access log `banjax_decision:ExpiringChallenge` (challenges), then `ShaChallengePassed` if the visitor solved it. |
| What did a banned IP do before the ban? | Banjax `client_ip` + `@timestamp` → access log `client_ip` in the minutes before. |
| One request across systems | Access log `request_id`. Not present in the other two logs. |
| One visitor across IPs | Access log `deflect_session` (command log `deflect_session` for `*_session` commands), or `banjax_bot_fingerprint`. |

Site names match across logs on `client_request_host`. The edge `host` field is not the site.

## Open questions

1. Which Kafka cluster or clusters Logstash's `banjax_command` input reads (gotcha 2).
2. Whether Logstash runs in UTC (gotcha 4).
3. The predictor's `security_scanner` rule calls `self._send_command`, which is not defined anywhere in baskervillehall at `306a2ca` (`baskervillehall_predictor.py:2543`). Per code, no `security_scanner:<name> [block_ip]` command is ever sent. Kibana `source:security_scanner*` should return nothing. Whether the error also disrupts the predictor loop is not traced.
4. How `human` is stored when the first document of a day's index sets the mapping (boolean or keyword); check the live mapping.
