# Baskerville: Using the New Banjax AltCha Commands

**Date:** 2026-11-09
**Based on:** deflect-kb `banjax/banjax-kafka-commands.md` and `banjax/altcha-challenge.md` (banjax v0.13.0, updated 2026-11-06)
**Status:** plan

---

## 1. What changed in banjax (relevant to us)

- Five new commands: `altcha_challenge_ip`, `_subnet`, `_session`, `_all`, `_ua`. They take the same fields as their `challenge_*` counterparts, plus three optional ones:
  - `altcha_max_number` (1000–20000, default 10000);
  - `altcha_click` (show an "I'm not a robot" checkbox before the PoW starts);
  - `altcha_force_new` (old `deflect_altcha` cookies stop being accepted).
- **Severity order:** challenge < AltCha < 429 < block. A weaker or equal command never replaces a stronger one, and **re-sending the same command does not extend its lifetime**. An AltCha command replaces an AltCha decision only when `altcha_max_number` or `altcha_click` differs, or `altcha_force_new` is set.
- **`*_ua` and `*_all` are per host. IP, subnet and session decisions are per edge** (every site on that banjax).
- **The `deflect_altcha` cookie** is bound to the IP (or the UA on some sites) and lives 4 h. It also passes the JS challenge.
- **AltCha failures from Kafka commands always use the `block` fail action:** 3 requests without a valid cookie in 10 s give an **iptables ban** for the IP on the edge. `clear_rules` does not lift it (only `/unban` on the edge).
- **The widget loads from cdn.jsdelivr.net.** Visitors whose network blocks it cannot pass.
- **Sites with `baskerville_whitelist_domain`** (banjax `sites_to_disable_baskerville`) drop all our commands, and banjax ignores our decisions for them.
- **Paths in `baskerville_whitelist_path`** are not challenged by our Kafka challenges, AltCha included.
- **Edges older than v0.13.0 silently ignore `altcha_*` commands.** Today only dnext6 runs v0.13.0; the other dnets get it at the next deploy of dev.
- **Reports** (`ip_passed_challenge`, `ip_failed_challenge`, `ip_banned`) do not say which challenge it was, and carry no session id.

## 2. What Baskerville sends today

| Source | Commands | TTL |
|---|---|---|
| Predictor | `challenge_ip`, `challenge_session` (all `block_*` sent as challenges since Nov 5) | none, so the banjax default: 3600 s |
| First Responder | `challenge_all`, `challenge_ip` (cluster responder, fingerprints) | 30–60 min |
| Session pipeline | `challenge_ip` (static tapol.org UA list, country rules) | 300–3600 s |
| UA commands | none since Nov 2 (`FIRST_RESPONDER_NATIVE_UA_COMMANDS_ENABLED=false`) | – |

Whitelisted hosts (`BASKERVILLE_HOST_WHITELIST`, kavkaz-uzel) get no commands at all.

## 3. Principles

1. **Host-scoped first.** Use AltCha through `altcha_challenge_ua` and `altcha_challenge_all` only: they affect one site. No `altcha_challenge_ip/subnet/session` from automatic rules until the fail action can be `no_block`: with an iptables ban after 3 failures they act as edge-wide blocks, which are banned.
2. **No new blocks.** The kill switch (`BASKERVILLE_BLOCKS_DISABLED`) also covers AltCha IP/subnet/session commands, so they are downgraded to `challenge_ip` / `challenge_session`.
3. **Always send `ttl` and `host`.** Don't rely on banjax defaults.
4. **Send AltCha only where it works:** dnets running v0.13.0, and not for sites whose audience can't reach jsDelivr until the widget is served from the edge.
5. **Measure every use:** passes, failures, bans and origin load, per site.

## 4. Work items

### 4.1 Command layer (`block_guard.py`)

- Add `ALTCHA_DNETS` (e.g. `dnext6`): send `altcha_*` only to partitions of these dnets. For other dnets fall back to the `challenge_*` counterpart.
- In the kill switch, map `altcha_challenge_ip` → `challenge_ip`, `altcha_challenge_subnet` → `challenge_subnet`, `altcha_challenge_session` → `challenge_session` while `BASKERVILLE_BLOCKS_DISABLED=true`.
- Add `ALTCHA_HOSTS_EXCLUDED` (sites with a Russian, Chinese or Iranian audience) until jsDelivr is replaced.
- Unit tests for the mapping and the dnet fallback.

### 4.2 First Responder

- New actions `altcha_challenge_ua` and `altcha_challenge_all`, both host-scoped, offered to the LLM and the rule-based path:
  - **`altcha_challenge_all`** replaces `challenge_all` for spikes ≥ 50× with immature ratio ≥ 0.80 (today's rule-based path). `altcha_max_number` scales with severity: 10000 by default, 20000 at ≥ 100×. `altcha_click` stays off.
  - **`altcha_challenge_ua`** applies to exact browser UAs from the attack that pass the JS challenge (`passed_challenge` high), i.e. the tapol.org case. The `_resolve_ua_targets` / never-block checks stay as they are.
- Update the prompt and `BLOCKS_DISABLED_NOTICE`: AltCha actions are allowed, IP-level AltCha is not.
- **Lifetime:** re-sending doesn't extend a decision. To keep one alive during a long attack, re-send after it expires (track `expires_at` in `first_responder_actions`), not before.
- **On incident end:** send `clear_rules` with `host` (+ `ua`) instead of waiting for the TTL.
- **Cooldown:** at most one `altcha_challenge_all` per host per hour. In October we sent up to 27 `challenge_all` a day on the Global Risk sites.

### 4.3 tapol.org

- Replace the static UA list in the session pipeline (`SESSION_STATIC_UA_BLOCKS`, now sent as per-IP `challenge_ip`) with three host-scoped commands: `altcha_challenge_ua`, `host=tapol.org`, the 3 attack UAs, `altcha_max_number` 10000, `ttl` 6 h, re-sent by First Responder after expiry.
- Before: measure requests per hour from these UAs reaching the origin (`/resource-centre`). After: the same, plus `AltChaChallengePassed`, `ExpiringUAAltChaChallenge` and `TooManyFailedAltChaChallenges` for tapol.org.
- **Stop condition:** `TooManyFailedAltChaChallenges` for IPs outside the botnet pattern (real users) → `clear_rules` and review.
- Needs tapol.org's dnet on v0.13.0.

### 4.4 Predictor

- No AltCha from automatic per-IP rules (principle 1).
- Later, once the fail action can be `no_block`: rule 10 ("high bot score after a passed challenge → `block_ip`", now sent as `challenge_ip`, which a passed client already has a cookie for) becomes `altcha_challenge_ip` with `altcha_force_new`. That is the natural escalation step instead of a block.

### 4.5 Reports and features

- `ip_storage.py` stores `ip_passed_challenge` by IP only. Store `value_site` too, and the challenge type once banjax reports it.
- Use passes as labels: a session that passes AltCha and still behaves like a bot is a strong bot signal (headless at scale); a pass followed by normal browsing is a probable false positive.

### 4.6 Whitelists

- Use the dashboard option `baskerville_whitelist_domain` as the source of truth: banjax already drops our commands for those sites. Check that `allowlist_domain` in `config_v3` is the same option, and set it for kavkaz-uzel in the dashboard.
- Keep `BASKERVILLE_HOST_WHITELIST` as an emergency override that works without a dashboard deploy.

### 4.7 Observability

- Kibana saved search per site: `banjax_decision` in (`Expiring*AltChaChallenge`, `AltChaChallengePassed`, `TooManyFailedAltChaChallenges`).
- Add the AltCha commands to the meta field (`... [altcha_challenge_ua]`) so the existing dashboards split them.
- Daily: number of AltCha decisions sent per host, and AltCha bans per host. Alert on bans for a host with no open incident.

## 5. What we need from banjax (Jeremy)

| # | Request | Why |
|---|---|---|
| 1 | `fail_action` field on Kafka commands (`no_block`) | Without it, `altcha_challenge_ip/session/subnet` are edge-wide blocks via iptables |
| 2 | Serve `altcha.min.js` from the edge | jsDelivr is blocked in Russia at times |
| 3 | Challenge type (`altcha` / `sha_inv`) and `session_id` in `ip_passed_challenge` / `ip_failed_challenge` | Labels and features for Baskerville |
| 4 | Solve times on a low-end Android for `altcha_max_number` 10000 and 20000, cost 5000 | To choose difficulty per severity |
| 5 | Which dnets run v0.13.0, and when the rest will | For `ALTCHA_DNETS` |
| 6 | Per-host scope for IP decisions (later) | Same collateral problem as `block_ip` |

## 6. Answers to the KB's open questions about Baskerville

- **Does Baskerville always send `host` with `challenge_ua` / `block_ua`?** Yes, since Nov 2. Native UA commands are currently off.
- **Which commands does it send in production, and with which TTLs?** See section 2.
- **Does it send `altcha_challenge_*`, `altcha_click` or `altcha_force_new`?** Not yet. Only the manual test on sub.farmal.in (Nov 9).

## 7. Order

1. 4.1 command layer + 4.6 whitelist check (small, no behaviour change while `ALTCHA_DNETS` is empty).
2. 4.3 tapol.org with measurement, as soon as its dnet runs v0.13.0.
3. 4.2 First Responder `altcha_challenge_all` / `_ua`.
4. 4.5 reports, once banjax adds the fields.
5. 4.4 predictor escalation, once `fail_action` exists.
