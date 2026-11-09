@dmitri , @jeremy  
I want to reiterate:

ALTCHA will help, but **WILL NEVER SOLVE** the problem with browser botnets like the one on `tapol.org`.

As we all agreed yesterday, we need an INTERACTIVE challenge.

Here are the options from my perspective.


| Option | Pros | Cons |
|---|---|---|
| **1. Third party** (Turnstile, reCAPTCHA, hCaptcha) | Best detection, free, no work | Not for us obviously |
| **2. ALTCHA Sentinel** (€99/month central, €799/month on every edge) | Ready now, adaptive, accessible (audio, 44 languages), self-hosted | Paid, proprietary. Central service: latency, single point of failure. Image codes are solvable by AI |
| **3. ALTCHA + our own image/audio server** (in banjax) | free, on every edge | Image codes are solvable by AI. Few audio languages. We maintain it |
| **4. Our own interaction challenge** ("press and hold" → ML) | Least friction for humans, fully ours, hardest for bots to fake | Most effort: data, labelling, model, maintenance |

**My suggestion:** 
2 - ASAP, start on the free 30-day trial like next week

3 - slowly migrating to our own server, like in 2-3 months

4 - The best, long term. Whenever we have resources to develop.

NB: We had this exact conversation in 2025. I even built a prototype challenge ("I am not a robot" checkbox) that collects the mouse interactions and device signals.