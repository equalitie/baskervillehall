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
4. **Kibana** (optional, for edge logs): log in at https://kibana.equalit.ie in Chrome. With the
   Claude in Chrome extension Claude can run the Kibana searches itself; without it, it will give
   you the query to paste.

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
| `bin/send_command.sh`, `.claude/skills/send-command` | Send a banjax command by hand (challenge/block, `challenge_all`, `clear_rules` to remove). Logged in `manual_commands` |
| `.claude/settings.json` | Allows `bin/q.sh`, `bin/send_command.sh` and `kubectl get/logs/describe` without asking; denies other kubectl commands that change the cluster |
| `reports/` | Where written reports go |

## Safety

- Investigation is read-only. `bin/q.sh` sets `default_transaction_read_only`, and direct
  `kubectl exec/apply/delete/...` are denied in `.claude/settings.json`.
- Commands: `bin/send_command.sh` sends real banjax commands to the edge, without an extra prompt.
  Claude sends one only when you ask, shows a `--dry-run` first, and every send is logged with your
  git email in `manual_commands`. `challenge_ip`/`block_ip`/subnet commands apply to **every** site of
  the host's dnet; removing (`clear_rules`) works in all dnets at once. TTL is at most 24 h. Whitelisted hosts are refused. `block_*` is sent as a challenge unless you
  explicitly ask for a real block (`--allow-block`).
- Don't approve other commands that change the cluster if Claude ever asks. If something else needs
  to change (a rule, a whitelist), write it up and send it to the Baskerville team.
- Limits of the data: per-IP command history (`challenge_command_history`) keeps about 3 days.
  Older questions need Kibana. Commands from the AI First Responder are not per-IP in the
  database. Kibana `baskerville_banjax_command-*` has them.
