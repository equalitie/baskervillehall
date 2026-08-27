# Banjax Feature Request: Country, ASN, UA, TLS Fingerprint, and Host-level Commands

## Context

Baskerville's AI first responder analyzes DDoS incidents and produces blocking decisions.
Currently it can only act on decisions that target individual IPs (`block_ip`, `challenge_ip`).

For **traffic spike attacks** — where a botnet uses hundreds of IPs each making exactly 1 request
(cache-busting pattern) — there are no individual IPs to block. The attacker rotates through
thousands of IPs faster than per-IP blocking can keep up. We need edge-level commands.

## Requested Commands

All commands should accept a `ttl` field (seconds) after which the rule expires automatically.

---

### 1. `block_country`

Block all requests from a given country code.

```json
{
  "command": "block_country",
  "host": "example.org",
  "country": "PL",
  "ttl": 1800
}
```

---

### 2. `challenge_country`

Issue a JS/CAPTCHA challenge to all requests from a given country code (instead of hard block).
Useful when the country has some legitimate traffic but is dominated by attack traffic.

```json
{
  "command": "challenge_country",
  "host": "example.org",
  "country": "FR",
  "ttl": 1800
}
```

---

### 3. `challenge_all` (for host)

Issue a JS challenge to **all** incoming requests for a host, regardless of IP/country.
Used during extreme volumetric attacks where the attack source is too diffuse to target.

```json
{
  "command": "challenge_all",
  "host": "example.org",
  "ttl": 600
}
```

---

### 4. `block_ua`

Block all requests matching a specific User-Agent string (exact match or prefix).

```json
{
  "command": "block_ua",
  "host": "example.org",
  "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X)",
  "ttl": 3600
}
```

---

### 5. `challenge_ua`

Issue a JS challenge to requests matching a specific User-Agent string.

```json
{
  "command": "challenge_ua",
  "host": "example.org",
  "ua": "facebookexternalhit/1.1",
  "ttl": 900
}
```

---

### 6. `block_asn` / `challenge_asn`

Block or challenge all requests originating from a specific ASN. More precise than country
blocking — targets a specific datacenter, VPS provider, or residential botnet operator
(e.g. Tencent Building, F.n.s. Holdings Limited) without affecting the entire country.

```json
{
  "command": "block_asn",
  "host": "example.org",
  "asn": "AS45090",
  "ttl": 1800
}
```

```json
{
  "command": "challenge_asn",
  "host": "example.org",
  "asn": "AS45090",
  "ttl": 1800
}
```

---

### 7. `block_tls_fingerprint` / `challenge_tls_fingerprint`

Block or challenge requests matching a specific TLS fingerprint (JA3 or similar).

TLS fingerprint is the strongest bot signal we have — it cannot be spoofed and reflects the
exact HTTP library/tool being used. When 100% of attack traffic shares one fingerprint, this
command surgically blocks the attack tool without any collateral damage to legitimate users
who happen to be in the same country or ASN.

```json
{
  "command": "block_tls_fingerprint",
  "host": "example.org",
  "fingerprint": "TLS_AES_128_GCM_SHA256",
  "ttl": 3600
}
```

```json
{
  "command": "challenge_tls_fingerprint",
  "host": "example.org",
  "fingerprint": "TLS_AES_128_GCM_SHA256",
  "ttl": 3600
}
```

---

### 8. `rate_limit_country` / `rate_limit_asn`

Rate-limit requests from a country or ASN to N requests per minute. Softer alternative to
blocking — useful when the attack source overlaps with legitimate audience traffic, or when
the spike is borderline (e.g. survey country with 3× normal traffic).

```json
{
  "command": "rate_limit_country",
  "host": "example.org",
  "country": "MX",
  "requests_per_minute": 60,
  "ttl": 1800
}
```

```json
{
  "command": "rate_limit_asn",
  "host": "example.org",
  "asn": "AS8075",
  "requests_per_minute": 120,
  "ttl": 1800
}
```

---

### 9. `clear_rules`

Remove all active rules for a host immediately (emergency override / undo).

```json
{
  "command": "clear_rules",
  "host": "example.org"
}
```

Optionally scoped to a rule type:

```json
{
  "command": "clear_rules",
  "host": "example.org",
  "type": "block_country"
}
```

---

## TTL Behavior

- All rules must expire automatically after `ttl` seconds.
- Default TTL if not specified: 1800 seconds (30 minutes).
- Rules should be clearable via a `clear_rules` command if needed.

## Priority / Interaction

Suggested precedence (highest to lowest):
1. `block_ip` / `challenge_ip` (existing, per-IP)
2. `block_tls_fingerprint` / `challenge_tls_fingerprint`
3. `block_asn` / `challenge_asn`
4. `block_country` / `challenge_country`
5. `block_ua` / `challenge_ua`
6. `rate_limit_country` / `rate_limit_asn`
7. `challenge_all`

A more specific rule should override a less specific one for the same request.
`clear_rules` always takes immediate effect regardless of active TTLs.

## Why This Matters

In our current incident data, roughly 60–70% of DDoS incidents detected by Baskerville are
traffic-spike attacks with 1-request-per-IP patterns. The AI correctly identifies the attack
country/UA and writes a decision, but **zero commands are sent** because banjax has no way
to execute country or UA level rules. Adding these commands would allow Baskerville's AI
to automatically mitigate the majority of detected attacks without human intervention.

### Priority of impact

| Command | Precision | Collateral damage | Use case |
|---|---|---|---|
| `block_tls_fingerprint` | Highest | Near zero | Single-tool attacks (100% TLS uniformity) |
| `block_asn` / `challenge_asn` | High | Low | Datacenter/VPS botnet operators |
| `block_country` / `challenge_country` | Medium | Medium | Geographically concentrated attacks |
| `rate_limit_country` / `rate_limit_asn` | Medium | Low | Ambiguous spikes with legitimate overlap |
| `block_ua` / `challenge_ua` | Medium | Low | Known scripted UA strings |
| `challenge_all` | Low | High | Last resort — fully diffuse attacks |
