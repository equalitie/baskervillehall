# Baskerville Daily Report: 2026-11-09

**Window:** last 24 h (to ~12:00 UTC). **Data:** `scripts/daily_report.sql`, read-only.
**Context:** all Baskerville blocks are sent as challenges since Nov 5. Action names below are as recorded; `block_*` went out as `challenge_*`.

## Summary

- **967 incidents on 297 hosts**, slightly below the 7-day average (1,067 a day). 8 open now.
- **1,157 First Responder actions** and **184k commands** (183k `challenge_ip`, 1.4k `challenge_session`) for 1,211 hosts.
- **The biggest issue is `challenge_all`: 412 in 24 h on 192 hosts.** That's the Global Risk pattern spread across the network. Every one of them locks out feeds, link previews, monitoring and client tools for 30 minutes.
- **tapol.org:** nothing works. 175 incidents, 42 `challenge_all`, 42 `monitor_only` at ≥ 6×, and the bots pass the JS challenge.
- LLM cost: **$2.90**.

## 1. New

| Host | Incidents | Max spike | Peak req/min | Action |
|---|---|---|---|---|
| ampl.iacrecetusbeneficiosbba.click | 9 | 273× | 390 | `challenge_all` ×9 |

The domain looks like a spam or phishing landing page. **Check who the client is**, and whether the site should be on Deflect at all.

## 2. Ongoing (incidents on 3+ of the last 7 days)

| Host | Days | Incidents (24 h) | Avg spike 24 h vs before | Note |
|---|---|---|---|---|
| **tapol.org** | 6 | 693 (175) | 30.7× vs 22.3×, **growing** | Linux Chrome botnet, passes JS challenge. See 3 |
| solidarityuganda.org | 6 | 134 (1) | 28× vs 35× | Quiet today |
| cmiguate.org | 8 | 133 (23) | 7.6× vs 6.0× | 13 `challenge_ua` + 10 `challenge_all` in 24 h |
| greencubator.info | 6 | 101 (17) | 1.3× vs 6.2× | **32 `block_ip`** on a fading attack. Review |
| kavkaz-uzel.eu | 5 | 92 (0) | – | Whitelisted since Nov 6. Working: subdomains now get only `monitor_only` |
| lile.cl, radiozamaneh.com | 8 | 83 / 82 (12 / 10) | ~7–9× | Long-running, low spikes |
| courtmonitoring.org | 6 | 38 (12) | 21.9× vs 14.3×, growing | **22 `challenge_all` in 24 h** |
| europeandatajournalism.eu | 8 | 51 (8) | ~21× | |
| fundamedios.org.ec | 7 | 44 (14) | 19.1× vs 10.9×, growing | Skipped 4× for "no country/ASN stats". See 3 |

## 3. First Responder review

**Repeated actions** (> 3 of one action on one host in 24 h):

| Host | Action | Count |
|---|---|---|
| tapol.org | `challenge_all` | **42** |
| greencubator.info | `block_ip` | 32 |
| zmina.info | `challenge_ip` / `block_ip` / `challenge_all` | 23 / 9 / 8 |
| courtmonitoring.org | `challenge_all` | **22** |
| comoabortar.org | `block_ip` / `challenge_ip` | 21 / 12 |
| cmiguate.org | `challenge_ua` / `challenge_all` | 13 / 10 |

**`challenge_all` repeated on one host achieves nothing new.** On tapol.org the bots pass it, and on courtmonitoring.org 22 times in a day is a site that is challenged most of the day.

**`monitor_only` at ≥ 6×:**

- **tapol.org, 42 times.** The reasoning is correct: two UAs make up 99.7% of the traffic and bots are single-request. But First Responder has no tool that works on them. It needs `altcha_challenge_ua` (BASKERVILLE_ALTCHA_PLAN.md, 4.3).
- **fundamedios.org.ec (104×) and tamarisk.org.ua (104×): "no country/ASN stats available after 31–32 minutes".** A strong attack got no response because the stats tables were empty. **Possible bug:** find out why `incident_*_stats` are empty for these incidents (stats writer lag, or a host name mismatch).
- **en.fergana.me, ecuadorchequea.com:** "`challenge_all` already active but ineffective — bots are solving the JS challenge". This is the same problem as tapol.org on two more sites.
- **fr.fergana.me (55×):** 100% of the spike is Facebook's ASN, so the action was correctly skipped (a Facebook crawler or link-preview spike).
- **antifainfoblatt.de, alainet.org, lile.cl:** "challenge_count below minimum". This is by design: these incidents were below 8×, and the bypass works.

**Recorded action ≠ sent command.** The cluster responder records `block_ua Go-http-client/2.0` with "6 current IPs block_ipd". What was actually sent is `challenge_ip` per IP (kill switch). The action table now overstates blocking, and anyone counting "blocks" from it (including Dmitri's question about over-blocking) gets the wrong number. **Proposal 3.**

**Empty targets:** `challenge_subnet` on fundamedios.org.ec, 2 times (dropped by the guardrail, harmless).

## 4. Collateral suspects

| Host | Action | Target | Verdict |
|---|---|---|---|
| nonviolence.ca | `challenge_ua` | `Googlebot/2.1 ...|...bingbot/2.0 ...` | **Search engines.** The target is also two UAs joined with `|` |
| aup.com.ua | `challenge_ua` | `WP-Optimize/4.7.0 WordPress/7.1.4 (Desktop; https://www.aup.com.ua/)` | **The site's own WordPress plugin.** The never-block rule for WordPress self-requests misses this format |
| kotsubynske.com.ua | `challenge_ua` | ElevenlabsCrawler, GPTBot | AI crawlers, by policy |
| thechasernews.co.uk | `challenge_ua` | `snapscout-benchmark/1.0; img2dataset` | Scraper, OK |

Without complaint data none of these are confirmed. Since native UA commands are off, they went out as per-IP `challenge_ip`, so the bots' IPs were challenged on every site of the edge.

## 5. Proposals

1. **Cap `challenge_all` per host:** at most 3 in 24 h and no new one within 2 h of the last. After that, `monitor_only` with "challenge_all ineffective" logged, and the host flagged in this report. This affects 192 hosts a day.
   *Where:* `incident_first_responder.py`, the rule-based `challenge_all` path and `_enforce_consistency`. *Risk:* low; a host that really needs it more often is visible in this report.
2. **Investigate the empty country/ASN stats** for fundamedios.org.ec and tamarisk.org.ua (104× spikes, no response). *Where:* the stats writer for `incident_country_stats` / `incident_asn_stats`, and the 30-minute wait in `incident_first_responder.py:2414/2664`. *Risk:* none, investigation only.
3. **Record what was sent, not what was decided:** `_save_action` stores the action after `guard_command_name`, plus the original in the reasoning (`[blocks disabled: block_ip -> challenge_ip]`, as for UA actions). The cluster responder doesn't do this today. *Risk:* low; it makes over-blocking numbers honest.
4. **Widen the never-block rule for WordPress self-requests** to any UA with `WordPress/x.y` and a URL on the request's own host (WP-Optimize, WP Rocket, Jetpack variants). Never put `|`-joined UAs into one target. *Risk:* low.
5. **tapol.org, en.fergana.me, ecuadorchequea.com:** "bots solve the JS challenge" is now a recurring class, not a single site. Prioritise `altcha_challenge_ua` in First Responder (BASKERVILLE_ALTCHA_PLAN.md, 4.2–4.3) once their dnets run banjax v0.13.0.

## 6. Cost

| Source | Calls | Input tokens | USD |
|---|---|---|---|
| first_responder_traffic_spike | 331 | 1.43M | 1.53 |
| predictor_cluster | 532 | 0.51M | 0.69 |
| first_responder_challenge | 180 | 0.62M | 0.68 |
| **Total** | 1,043 | 2.56M | **2.90** |

## Not covered yet

- Outcomes: whether each action changed traffic to the origin (step 3 of the design).
- Kibana: 403s and challenges for verified bots, health checks and client tools. This report only sees our own decisions.
- zhitomir.info got the most commands (32.7k `challenge_ip`) but has no incident in the ongoing list. Worth a look.
