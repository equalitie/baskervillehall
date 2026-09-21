# Under Attack on Election Day: How We Kept an Independent Russian Election Monitor Online

**Platform:** v2026.org — "Nablyudaem Vmeste" (We Watch Together), independent election monitoring  
**Date:** September 18–20, 2026  
**Prepared by:** Deflect / eQualitie Security Team

---

## Summary

Over the three days of the Russian parliamentary elections, v2026.org — an independent platform for citizen election monitoring — sustained a continuous large-scale DDoS attack from a distributed global botnet. The attack ran for approximately 26 hours across Election Days 1 and 2, then stopped entirely. Election Day 3 was clean.

**Total downtime across the entire campaign: approximately 19 minutes** — all in the first hours of Election Day 1, before the decisive mitigation was in place. The site remained accessible for the rest of the elections.

| | |
|---|---|
| Attack duration | ~26 hours (Sep 18 morning – Sep 19 morning) |
| Total downtime | ~19 minutes |
| Peak attack volume | ~610,000 requests/minute |
| Botnet geography | 15+ countries — Turkey, Egypt, Mexico, China, Indonesia, Brazil, and others |

---

## The Attack

The campaign began before dawn on September 18 with a probe wave of roughly 600,000 requests, followed by a brief pause, then a full assault starting at 06:12 UTC. Traffic peaked at approximately **610,000 requests per minute** at 06:20 UTC — the site was returning 502 errors as the origin server was overwhelmed.

The botnet was deliberately designed to be hard to block:

- **Residential ISPs, not datacenters.** The attacking IPs came from consumer internet providers in Turkey, Egypt, Mexico, Iran, the Philippines, China, and elsewhere — not from cloud infrastructure. Blocking by IP range is ineffective against this type of botnet.
- **Fake browser signatures.** The bots presented themselves as common browsers (Chrome, Firefox) with plausible-looking user agent strings — some using old version numbers, some using truncated strings that no real browser produces.
- **One request per IP.** Many bots sent exactly one request and disconnected, making behavioral analysis difficult. A real user's session builds up over multiple requests; these bots never stayed long enough to be scored.
- **Rotation.** Between waves, the botnet switched IP ranges, user agents, and network providers. No single vector stayed the same for more than a few minutes.

The attack continued — with brief pauses — through the night of Sep 18 and into the morning of Sep 19, when it finally stopped at 07:49 UTC. Sep 20 (Election Day 3) was completely quiet.

---

## The First Detection Gap

The attack initially evaded automated detection. In the week before the election, organic traffic to v2026.org had grown enormously — from a handful of requests per minute on September 11 to tens of thousands per minute by election eve, as Russian citizens rushed to the site to follow the vote count.

Our detection system compares current traffic to a rolling baseline of recent traffic. By election morning, the baseline had absorbed this growth. The attack, while massive in absolute terms, did not look large *relative to the recent baseline* — it registered as a modest spike, below the threshold for automated action.

The Deflect team identified the attack manually via monitoring dashboards and mitigated the first wave within approximately 15 minutes using targeted blocks. The detection system was then reconfigured with a fixed reference point for v2026.org, so that subsequent spikes would be measured against a stable baseline rather than one inflated by election traffic.

---

## How the Response Evolved

### Wave 1 (06:12–06:27 UTC): Manual mitigation

The first wave was mitigated by hand. The team identified the top attacking networks and user agents from traffic logs and issued blocks. Attack volume dropped by approximately 97% once the main botnet networks were blocked.

### Wave 2 (07:25–12:00 UTC): AI takes over, then a new problem

After the detection system was reconfigured, Baskerville's AI first responder began handling subsequent waves autonomously. Over the next several hours it detected 19 separate incidents, analyzing each attack wave and issuing targeted blocks — identifying attacking networks, flagging fake browser signatures, and issuing per-IP challenges.

However, a deeper problem emerged around 11:00 UTC: the bots were requesting URLs that had already been cached at the edge. When a cached response is served, the JavaScript challenge is bypassed — the bot receives content without ever being asked to prove it's a browser. This explained why individual IP and user agent blocks were insufficient against this botnet: the bots were receiving responses without ever triggering a challenge.

### The decisive fix: JavaScript wall

At approximately 11:15 UTC, the infrastructure team enabled **`challenge_all`** — a mode that forces every single request to complete a JavaScript challenge *before* it reaches the cache. The effect was immediate:

- Bots that cannot execute JavaScript (the vast majority of automated traffic): blocked with HTTP 429
- Real users with browsers: complete the challenge invisibly in the background, receive the page normally

From 12:00 UTC onward, the site was fully accessible. No further downtime was recorded.

### Overnight (18:40 UTC Sep 18 – 07:49 UTC Sep 19): AI monitors, botnet gives up

The attack resumed in the evening with a new wave and continued through the night. Baskerville detected 26 more incidents overnight, autonomously identifying and blocking attacking networks and user agent patterns as they evolved. Peak traffic reached 251,000 requests/minute.

With the JavaScript wall in place, none of this caused any downtime. The botnet could not reach the origin. At 07:49 UTC on September 19, the last incident closed and traffic returned to normal. The attackers appear to have abandoned the campaign.

---

## Downtime Record

All downtime occurred in the first hours of Election Day 1, before the JavaScript wall was in place:

| Time (UTC, Sep 18) | Duration |
|---|---|
| 10:43 | 2m 10s |
| 10:49 | 2m 18s |
| 10:52 | 5m 29s |
| 11:02 | 7m 07s |
| 11:12 | 2m 17s |
| **After 11:15** | **0** |

**Total: ~19 minutes.** After the JavaScript wall was activated, the site stayed up through 26 hours of continuous attack.

---

## Who Was Behind It?

We cannot attribute the attack to a specific actor with certainty. What the traffic data shows:

- A botnet spanning residential internet providers across Turkey, Egypt, Mexico, Iran, the Philippines, China, Indonesia, Brazil, and others — consistent with a for-hire botnet using compromised home routers and consumer devices
- Coordinated behavior: bots rotated user agents and IP ranges on a schedule, adapted to blocks within minutes, and used consistent TLS fingerprints suggesting a shared toolset
- The timing — launching before dawn on Russian Election Day 1, targeting an independent election monitoring platform — speaks for itself

---

## What We Learned

**Election sites need special handling.** A monitoring platform that goes from 5 requests/minute to 50,000 requests/minute in a week presents a genuine detection challenge. Standard relative-threshold detection struggles when legitimate traffic is itself explosive. For sites under active political threat around known events, fixed reference baselines should be configured in advance.

**Against distributed residential botnets, a JavaScript wall is the decisive tool.** Per-IP and per-network blocking cannot scale against a botnet with hundreds of rotating addresses. A JavaScript challenge stops bots that cannot execute JavaScript — which is the vast majority of volumetric botnet traffic — at a single stroke, regardless of how many IPs or networks they use.

**The site held.** Independent election monitoring continued to function throughout the Russian parliamentary elections. The attackers failed.

---

*Deflect is a project of eQualitie, protecting civil society websites and media since 2011.*
