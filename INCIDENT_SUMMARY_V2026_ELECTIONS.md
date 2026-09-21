# Summary: DDoS Attack on v2026.org During Russian Parliamentary Elections
**Sep 18–20, 2026 | Deflect / Baskerville AI Security Team**

---

Over the three days of Russia's parliamentary elections, v2026.org — the independent election monitoring platform "Nablyudaem Vmeste" — sustained a large-scale DDoS campaign from a distributed global botnet.

**The site remained accessible throughout the elections. Total downtime: ~19 minutes.**

---

## The Attack

The campaign began at 06:12 UTC on Election Day 1 (Sep 18) with a peak of **610,000 requests per minute** — one of the largest volumetric attacks Deflect has mitigated. The botnet was highly distributed: residential ISPs across Turkey, Egypt, Mexico, China, Indonesia, Brazil, and 10+ other countries, using rotating IP addresses and fake browser user agents to evade detection. No datacenter infrastructure — entirely residential, making IP-level blocking ineffective at scale.

The attack continued through the night and into Election Day 2 (Sep 19), stopping at 07:49 UTC. Election Day 3 (Sep 20) was completely clean — the operators appear to have abandoned the campaign after two days of continuous mitigation.

---

## The Response

**Initial detection gap:** The attack initially evaded Baskerville's automated detection. In the days before the election, organic traffic to the site had grown from ~5 req/min to ~50,000 req/min as public interest surged. The spike detector uses a rolling baseline — by election morning, the baseline had absorbed this growth, and the attack at ~150,000 req/min registered as only 3× above baseline, below the detection threshold. Manual intervention by the Deflect team mitigated the first wave within ~15 minutes.

**Fix:** A synthetic baseline cap of 500 req/min was applied for v2026.org for the duration of elections, ensuring any spike above 2,000 req/min would trigger detection regardless of organic traffic growth.

**Automated AI response:** Once detection was restored, Baskerville's AI first responder took over — analyzing each attack wave and issuing targeted blocks autonomously:
- Blocked fake browser UAs (non-existent Firefox/147–148, truncated Chrome signatures)
- Blocked attacking ASNs with no legitimate audience overlap (OVH, Chinanet, Turk Telekom, M247, High Speed For Internet Services, and others)
- Issued per-IP challenges at ~60 commands/minute

**Decisive mitigation:** At ~11:15 UTC on Sep 18, `challenge_all` was enabled via the banjax dashboard — forcing a JavaScript challenge for all traffic before it reaches the cache. Bots without JavaScript received HTTP 429 and could not reach the origin. Real users solved the challenge transparently and continued normally. From that point on, **zero downtime** despite the attack continuing at 200,000–580,000 req/min through the night.

---

## By the Numbers

| Metric | Value |
|---|---|
| Attack duration | ~26 hours (Sep 18 06:12 – Sep 19 07:49 UTC) |
| Peak traffic | ~610,000 req/min |
| Total downtime | ~19 minutes |
| Downtime after `challenge_all` | **0** |
| Incidents detected (AI) | 50+ |
| AI blocking actions | ASN blocks, UA blocks, 3,600+ per-IP challenges |
| Botnet size | Hundreds of IPs across 15+ countries |

---

## Key Takeaways

1. **Election sites need a pre-configured baseline cap.** Organic traffic growth can inflate the rolling baseline and mask real attacks. A synthetic threshold ensures detection even when legitimate traffic is high.

2. **`challenge_all` is the decisive tool against distributed residential botnets.** Per-IP and per-ASN blocking cannot scale against hundreds of rotating IPs. A JS wall stops bots that lack JavaScript execution — which is the vast majority of volumetric botnet traffic.

3. **The AI first responder handled the sustained phase autonomously** — 26 hours of continuous attack, rotating infrastructure, 50+ incidents, with no additional manual intervention after `challenge_all` was deployed.
