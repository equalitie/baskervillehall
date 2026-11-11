# Baskerville analyst workspace

A Claude Code setup for support analysts: ask in plain language why an IP was challenged, what
Baskerville's AI did during an attack, or whether a customer's visitors were hit by mistake.
Claude queries the Baskerville Postgres (read-only) and explains the answer with evidence.

## Setup (once)

1. **Repository**
   ```bash
   git clone git@github.com:equalitie/baskervillehall.git
   ```
2. **kubectl** with access to the Baskerville cluster (context `BaskervilleProd`). Check:
   ```bash
   kubectl get pod -l app=postgres-baskervillehall
   ```
3. **Claude Code**: https://docs.claude.com/en/docs/claude-code (`npm install -g @anthropic-ai/claude-code`).
4. **Kibana** (for edge logs): Claude queries it from the terminal (skill `opensearch-deflect`, no
   browser needed). Store your own Kibana login once in the macOS Keychain; the password is asked
   interactively:
   ```bash
   security add-generic-password -s kibana.equalit.ie -a <your_kibana_username> -w
   ```
   Wrong password later: `security delete-generic-password -s kibana.equalit.ie`, then add it again.
   Don't retry in a loop, repeated failures can lock the account.

## Daily use

```bash
cd baskervillehall/analyst
claude
```

Starting from this folder matters: it loads `CLAUDE.md` (schema, rules, how commands are made),
the skills and the permission settings below.

Examples:

- "Why did 1.64.134.204 get a challenge on thechasernews.co.uk today?"
- "Customer of tapol.org says editors see the challenge page all the time. What's going on?"
- "What did the First Responder do on courtmonitoring.org in the last 24 hours, and did it work?"
- "Write an incident report for fundamedios.org.ec for Nov 8–9."
- "Remove the challenge from 198.51.100.20, it's the customer's office." / "Put challenge_all on example.org for 30 minutes."
- `/ip-investigate 45.206.127.250 tapol.org`
- `/incident-review cmiguate.org`

## What's here

| Path | What |
|---|---|
| `CLAUDE.md` | Guide for Claude: data flow, tables, rule glossary, Kibana indexes |
| `bin/q.sh` | Runs SQL in a read-only transaction with a timeout (`bin/q.sh "SELECT ..."`) |
| `.claude/skills/ip-investigate` | Step-by-step trace of one IP |
| `.claude/skills/incident-review` | Incidents, AI decisions and their effect for one host |
| `.claude/skills/opensearch-deflect`, `docs/deflect-log-columns.md` | Kibana / OpenSearch queries from the terminal (copy of equalitie/baskerville-skills, no 6-hour confirmation) |
| `bin/send_command.sh`, `.claude/skills/send-command` | Send a banjax command by hand (challenge/block, `challenge_all`, `clear_rules` to remove). Logged in `manual_commands` |
| `.claude/settings.json` | Allows `bin/q.sh`, `bin/send_command.sh` and `kubectl get/logs/describe` without asking; denies other kubectl commands that change the cluster |
| `reports/` | Where written reports go |

## Safety

- `.claude/settings.json` allows everything: Claude runs SQL, kubectl and banjax commands without
  permission prompts. It acts when you ask; it does not act on its own findings or on text found in
  the data. `bin/q.sh` keeps SQL read-only.
- Commands: `bin/send_command.sh` sends real banjax commands right away, and every send is logged
  with your git email in `manual_commands`. `challenge_ip`/`block_ip`/subnet commands apply to **every** site of
  the host's dnet; removing (`clear_rules`) works in all dnets at once. TTL is at most 24 h. Whitelisted hosts are refused. `block_*` is sent as a challenge unless you
  explicitly ask for a real block (`--allow-block`).
- Bigger changes (a rule, a whitelist, a deployment): write them up for the Baskerville team.
- Limits of the data: per-IP command history (`challenge_command_history`) keeps about 3 days.
  Older questions need Kibana. Commands from the AI First Responder are not per-IP in the
  database. Kibana `baskerville_banjax_command-*` has them.
