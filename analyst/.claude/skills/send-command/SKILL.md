---
name: send-command
description: Send a banjax command by hand (challenge/block an IP, subnet, session or UA, challenge_all a site, or clear_rules to remove one). Use when the analyst asks to challenge, block, unblock or clear something on the edge.
---

# Send a banjax command

Everything goes through `bin/send_command.sh`. It runs inside the First Responder pod, routes the
command to the host's dnet, applies the host whitelist and the block kill switch, and logs the send
in `manual_commands`.

Run it as a single command from the `analyst/` folder: `bin/send_command.sh ...`, with no `cd`,
`&&`, `;` or pipes around it. Only that exact form matches the permission rule that lets it run
without a prompt.

## Before sending

1. Know exactly what and why: the host, the target (IP / subnet / session / exact UA), the TTL and
   a one-line reason. If any of it is unclear, ask the analyst. Don't guess targets.
2. Run with `--dry-run` first and show the analyst the message and the NOTE lines.
3. Then send the same command without `--dry-run`.

## Commands

| Command | Target | Notes |
|---|---|---|
| `challenge_ip`, `block_ip`, `altcha_challenge_ip` | `--value <ip>` | Applies on **every** site of the edge |
| `challenge_subnet`, `block_subnet`, `altcha_challenge_subnet` | `--value <a.b.c.0/24>` | IPv4, not wider than /16. Every site |
| `challenge_session`, `block_session`, `altcha_challenge_session` | `--session-id <cookie>` | |
| `challenge_ua`, `block_ua`, `altcha_challenge_ua` | `--ua "<exact full UA>"` | Only on `--host` |
| `challenge_all`, `altcha_challenge_all` | the host | Everyone on the site gets the challenge, including feeds, link previews and monitoring |
| `clear_rules` | see below | Removes a decision. There is no separate unblock |

All except `clear_rules` need `--ttl` in seconds (max 86400 = 24 h). `--host` is always required:
it picks the dnet.

**Blocks.** While `BASKERVILLE_BLOCKS_DISABLED=true`, `block_ip/subnet/session` are sent as
`challenge_*` (the script prints a NOTE). That kill switch exists because automatic blocks hurt real
users. To send a **real** block, add `--allow-block`. Use it only when the analyst explicitly asks for a
block (not a challenge), e.g. a client that solves the challenge and keeps attacking. Before sending, tell
them that `block_ip`/`block_subnet` block the address on **every** site of that edge, and keep the TTL
short (minutes to a few hours). `block_ua` is per host and is never converted. `clear_rules` removes a
block like any other decision.

`altcha_*` needs banjax v0.13.0 on the host's dnet.

## Removing a decision (`clear_rules`)

| To remove | Arguments |
|---|---|
| an IP challenge/block (all sites) | `--value <ip>` |
| a subnet decision | `--value <cidr>` |
| a session decision | `--session-id <id>` |
| a UA decision on all sites | `--ua "<UA>"` |
| `challenge_all` on the host | `--clear-host` |
| a UA decision only on this host | `--ua "<UA>" --clear-host`: banjax then **also** removes the host's `challenge_all` |

Decisions also expire on their own when the TTL runs out. Rules from banjax config (regex rules,
global lists) are not affected by `clear_rules`, only decisions sent through Kafka.

## Examples

```bash
bin/send_command.sh challenge_ip --host example.org --value 203.0.113.7 --ttl 3600 \
  --reason "credential stuffing on /wp-login.php, 900 req in 10 min" --dry-run

bin/send_command.sh challenge_all --host example.org --ttl 1800 --reason "L7 flood, 40x spike, customer asked"

bin/send_command.sh block_ip --host example.org --value 203.0.113.7 --ttl 1800 --allow-block \
  --reason "solves the challenge, 20k req/h to /search, analyst asked for a block"

bin/send_command.sh clear_rules --host example.org --value 198.51.100.20 \
  --reason "customer's office IP, false positive from Bad bot rule"
```

## After sending

A challenge is invisible to a normal browser: the SHA proof-of-work solves itself in a few seconds.
Don't judge by "I opened the site and saw nothing". Check `banjax_decision` in Kibana `deflect.log-*`
for the IP: `ExpiringChallenge` (429) then `ShaChallengePassed` = the challenge works; `NginxBlock` /
403 = blocked; `NoMention` after `clear_rules` = the decision is gone. banjax applies a command 5–15 s
after it is sent.

- Say what was sent (the printed message), to which dnet, the TTL, and the `manual_commands.id`.
- To check it reached banjax: Kibana `baskerville_banjax_command-*` with `source: manual*`, and
  in `deflect.log-*` the `banjax_decision` for that IP/host should change within a minute.
- History of manual sends: `bin/q.sh "SELECT * FROM manual_commands ORDER BY id DESC LIMIT 20;"`
