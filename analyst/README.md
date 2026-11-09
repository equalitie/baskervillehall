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
- `/ip-investigate 45.206.127.250 tapol.org`
- `/incident-review cmiguate.org`

## What's here

| Path | What |
|---|---|
| `CLAUDE.md` | Guide for Claude: data flow, tables, rule glossary, Kibana indexes |
| `bin/q.sh` | Runs SQL in a read-only transaction with a timeout (`bin/q.sh "SELECT ..."`) |
| `.claude/skills/ip-investigate` | Step-by-step trace of one IP |
| `.claude/skills/incident-review` | Incidents, AI decisions and their effect for one host |
| `.claude/settings.json` | Allows `bin/q.sh` and `kubectl get/logs/describe` without asking; denies anything that changes the cluster |
| `reports/` | Where written reports go |

## Safety

- Everything is read-only. `bin/q.sh` sets `default_transaction_read_only`, and direct
  `kubectl exec/apply/delete/...` are denied in `.claude/settings.json`.
- Don't approve commands that change the cluster if Claude ever asks. If something needs to change
  (a rule, a whitelist), write it up and send it to the Baskerville team.
- Limits of the data: per-IP command history (`challenge_command_history`) keeps about 3 days.
  Older questions need Kibana. Commands from the AI First Responder are not per-IP in the
  database. Kibana `baskerville_banjax_command-*` has them.
