# IncidentFirstResponder — LLM-powered DDoS Response Agent

## Overview

IncidentFirstResponder is an autonomous DDoS response agent that runs as a separate pod in the
same Kubernetes cluster as the Predictor. When a new incident is detected by `IncidentDetector`
(in `deflect_alerts`), the Responder analyzes the attack data using a local LLM (Ollama /
qwen2.5:7b) and writes a targeted mitigation action to the `first_responder_actions` Postgres table.
The Predictor polls this table and automatically enforces the action for the duration of its TTL.

## Architecture

```
IncidentDetector (deflect_alerts)
  writes → incidents, incident_country_stats, incident_asn_stats

IncidentFirstResponder (poll every 30s)
  reads  → incidents WHERE first_responder_processed = FALSE
  reads  → incident_country_stats, incident_asn_stats (attack distribution)
  reads  → sessions (7-day baseline = normal traffic distribution)
  calls  → Ollama LLM (qwen2.5:7b)
  writes → first_responder_actions (action + target + TTL)
  marks  → incidents.first_responder_processed = TRUE

Predictor (poll every 30s, alongside attack response)
  reads  → first_responder_actions WHERE expires_at > NOW()
  blocks → sessions matching the action's target (block_country / block_asn)
```

## Actions

| Action | Meaning | Predictor behavior |
|---|---|---|
| `block_country` | Block all sessions from target countries | `block_ip` for matching `country` |
| `block_asn` | Block all sessions from target ASNs | `block_ip` for matching `asn_name` |
| `raise_threshold` | Raise challenge threshold | Already handled by Attack Response Mode |
| `monitor_only` | No automated action | No block applied |

## Survey Country Protection

`survey_country` (declared by the site owner) is **always excluded** from blocks — both in
IncidentFirstResponder's LLM prompt (system rule) and in the Predictor's enforcement logic.
Sessions from the site's primary audience are downgraded to `challenge_ip` even when a
responder action is active.

## Postgres Tables

### `first_responder_actions`

```sql
CREATE TABLE IF NOT EXISTS first_responder_actions (
    id           BIGSERIAL PRIMARY KEY,
    incident_id  BIGINT NOT NULL REFERENCES incidents(id),
    host         TEXT NOT NULL,
    action       TEXT NOT NULL,   -- block_country | block_asn | raise_threshold | monitor_only
    target       TEXT NOT NULL,   -- comma-separated: "CN,RU" or "Hetzner Online GmbH,OVH SAS"
    confidence   TEXT NOT NULL,   -- high | medium | low
    reasoning    TEXT,
    ttl_minutes  INTEGER NOT NULL DEFAULT 30,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at   TIMESTAMPTZ NOT NULL,
    applied      BOOLEAN NOT NULL DEFAULT FALSE
);
```

### Added to `incidents`

```sql
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS first_responder_processed BOOLEAN DEFAULT FALSE;
```

Run `deployment/postgres/catch_up.sql` to migrate an existing database.

## LLM Decision Logic

The LLM receives:
- Attack distribution by country (from `incident_country_stats`)
- Attack distribution by ASN — including `datacenter` flag (from `incident_asn_stats`)
- 7-day normal traffic baseline by country and ASN (from `sessions` table)
- Site's `survey_country` (primary audience — never to be blocked)
- Botnet overlap with previous incidents

Decision rules (baked into the system prompt):
1. Prefer `block_asn` over `block_country` — less collateral damage
2. Use `block_asn` if datacenter ASNs dominate the attack
3. Use `block_country` if a country accounts for >`FIRST_RESPONDER_MIN_ATTACK_PCT`% of the attack
   AND represents <`FIRST_RESPONDER_MAX_NORMAL_PCT`% of normal traffic
4. Never include `survey_country` in the block target
5. Use `monitor_only` if collateral damage would exceed `FIRST_RESPONDER_MAX_NORMAL_PCT`%
6. Set `confidence=high` only when a single pattern dominates (>70% of attack)

## Configuration

All parameters are in `config_baskervillehall.yaml`:

| Parameter | Default | Description |
|---|---|---|
| `OLLAMA_URL` | `http://ollama-service.default.svc.cluster.local:11434` | Ollama API endpoint |
| `LLM_MODEL` | `qwen2.5:7b` | Model to use for inference |
| `FIRST_RESPONDER_CHECK_INTERVAL` | `30` | Seconds between Postgres polls |
| `FIRST_RESPONDER_MIN_ATTACK_PCT` | `50.0` | Min attack concentration (%) to recommend block |
| `FIRST_RESPONDER_MAX_NORMAL_PCT` | `20.0` | Max normal traffic (%) to still recommend block |
| `FIRST_RESPONDER_TTL_MINUTES` | `30` | Default block duration in minutes |

## Deployment

```bash
# Deploy Ollama (model is pulled automatically via initContainer)
kubectl apply -f ollama_service.yaml
kubectl apply -f ollama_deployment.yaml

# Wait for Ollama to be ready (model pull can take a few minutes)
kubectl rollout status deployment/ollama

# Deploy IncidentFirstResponder
kubectl apply -f incident_first_responder_deployment.yaml
```

Verify:
```bash
# Check Ollama has the model
kubectl exec deployment/ollama -- ollama list

# Check IncidentFirstResponder logs
kubectl logs deployment/incident-first-responder -f

# Query first_responder_actions after an incident
psql -c "SELECT id, host, action, target, confidence, expires_at FROM first_responder_actions ORDER BY created_at DESC LIMIT 10;"
```

## Relevant Files

| File | Role |
|---|---|
| `src/baskervillehall/incident_first_responder.py` | Main agent class |
| `src/baskervillehall/baskervillehall_predictor.py` | `_refresh_first_responder_actions()`, enforcement in `_apply_decision_and_send()` |
| `src/baskervillehall/main.py` | `respond` pipeline entry point |
| `config_baskervillehall.yaml` | `OLLAMA_*` and `FIRST_RESPONDER_*` config parameters |
| `incident_first_responder_deployment.yaml` | K8s Deployment for IncidentFirstResponder |
| `ollama_deployment.yaml` | K8s Deployment for Ollama |
| `ollama_service.yaml` | K8s Service exposing Ollama within the cluster |
| `deployment/postgres/catch_up.sql` | DB migration: `first_responder_actions` table + `incidents` column |
| `ATTACK_RESPONSE_MODE.md` | Documents the graduated attack response (complementary feature) |
