# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Baskerville is an intelligent analytics engine for Layer 7 DDoS attack detection and mitigation. It analyzes web request behavior in real-time using machine learning to distinguish between legitimate traffic (human users, verified bots) and malicious activity (malicious bots, AI crawlers). The system processes web logs, groups them into sessions, extracts features, and uses both supervised ML models and unsupervised anomaly detection to classify and challenge suspicious traffic.

## Core Architecture

The system consists of four main pipeline components:

1. **Session Pipeline** (`baskervillehall_session.py`) - Groups incoming web requests into sessions based on host, IP, and session cookies
2. **Trainer Pipeline** (`baskervillehall_trainer.py`) - Trains Isolation Forest and AutoEncoder models using session data from Kafka
3. **Predictor Pipeline** (`baskervillehall_predictor.py`) - Uses trained models to classify sessions and emit challenge commands
4. **Storage Pipeline** (`storage_base.py`) - Handles data persistence to PostgreSQL

### Key Components

- **Feature Extraction** (`feature_extractor.py`) - Computes behavioral features from sessions (request rates, path patterns, user agent analysis, etc.)
- **Model Storage** (`model_storage.py`) - Manages ML model persistence to S3
- **Isolation Forest** (`baskervillehall_isolation_forest.py`) - Anomaly detection for human vs automated traffic
- **AutoEncoder** (`baskervillehall_auto_encoder.py`) - Deep learning anomaly detection with PyTorch
- **Bot Verification** (`bot_verificator.py`) - Validates legitimate crawlers (Googlebot, Bingbot, etc.)

## Development Commands

### Running Tests
```bash
python -m pytest tests/
python -m pytest tests/test_bashervillehall_model.py  # Single test file
```

### Building Docker Images
```bash
# Base image
docker buildx build --platform linux/amd64 -t equalitie/baskervillehall:base .

# Latest image
docker buildx build --platform linux/amd64 -f ./Dockerfile_latest . -t equalitie/baskervillehall:latest
```

### Kubernetes Deployment
```bash
# Configuration
kubectl apply -f config_baskervillehall.yaml

# Pipeline deployments
kubectl apply -f session_deployment.yaml
kubectl apply -f predictor_deployment.yaml
kubectl apply -f trainer_deployment.yaml
kubectl apply -f storage_deployment.yaml
```

## Configuration

The system is configured via Kubernetes ConfigMap (`config_baskervillehall.yaml`) with key settings:

- **Kafka Topics**: `TOPIC_WEBLOGS`, `TOPIC_SESSIONS`, `TOPIC_COMMANDS`
- **Features**: Configurable feature set and categorical features
- **Model Parameters**: Isolation Forest parameters (`N_ESTIMATORS`, `CONTAMINATION`, etc.)
- **Training**: Batch sizes, dataset delays, model TTL
- **S3 Storage**: Model storage path configuration

## Key Features and Settings

- **Session Features**: 29 behavioral features including request rates, path patterns, user agent analysis, timing intervals
- **Categorical Features**: Country, session type, cipher, datacenter ASN, timezone
- **Model Types**: Separate models for human/automated traffic classification
- **Multiprocessing**: Predictor uses ProcessPoolExecutor for parallel model inference
- **Caching**: TTL-based caching for IP whitelists and model decisions

## Data Flow

1. Web logs → Kafka (`TOPIC_WEBLOGS`)
2. Session grouping → Kafka (`TOPIC_SESSIONS`)  
3. Training pipeline consumes sessions, trains models, stores to S3
4. Predictor pipeline loads models, classifies sessions, emits commands → Kafka (`TOPIC_COMMANDS`)
5. Storage pipeline persists results to PostgreSQL

## Dependencies

Core Python dependencies (from `requirements.txt`):
- `kafka-python==2.0.2` - Kafka integration
- `scikit-learn==1.3.0` - Isolation Forest models
- `tensorflow` - AutoEncoder models
- `psycopg2-binary` - PostgreSQL connection
- `boto3` - S3 model storage
- `kubernetes==27.2.0` - K8s integration

---

## Analyst Guide

This section is for security analysts working with incident data, generating reports, and investigating attacks. No code changes needed — all work happens via kubectl and PostgreSQL queries.

### Connecting to PostgreSQL

The database is Kubernetes-internal. Use kubectl exec:

```bash
kubectl exec $(kubectl get pod -l app=postgres-baskervillehall -o jsonpath='{.items[0].metadata.name}') -- \
  psql -U postgres -d baskerville -c "<SQL HERE>"
```

### Key Concepts

**Incident** — created when traffic to a site spikes above its rolling baseline (default threshold: 4×). Each incident tracks the spike magnitude, attack volume, geographic distribution, and the AI's response.

**spike_ratio** — how many times larger the attack traffic was vs the normal baseline. 10× = 10 times normal. 500× = extreme attack.

**baseline_avg** — average normal traffic (req/min) for the site in recent history. If artificially low (e.g. near zero), spike_ratio may be unreliable.

**challenge_count** — how many bot sessions were scored by ML during the incident. Low values (0–50) mean bots were evading ML by sending only 1 request per IP.

**immature_ratio** — fraction of bot sessions too short for ML scoring. >0.8 means 80%+ of attack traffic is single-request bots — cache-busting pattern, very hard to score.

**first_responder_actions** — AI LLM decisions: `block_asn`, `block_ua`, `block_ip`, `block_country`, `raise_threshold`, `monitor_only`. Each has `reasoning` explaining the decision.

### Common Analysis Queries

**Recent incidents for a specific site:**
```sql
SELECT id, started_at, ended_at,
       ROUND(spike_ratio::numeric,1) AS spike_ratio,
       ROUND(baseline_avg::numeric,0) AS baseline,
       traffic_peak_count AS peak_req_min,
       challenge_count
FROM incidents
WHERE host = 'example.org'
  AND started_at >= NOW() - INTERVAL '7 days'
ORDER BY started_at DESC;
```

**What did the AI do for an incident:**
```sql
SELECT action, target, confidence, reasoning, created_at
FROM first_responder_actions
WHERE incident_id = <ID>
ORDER BY created_at;
```

**Attack breakdown (country/ASN/UA/fingerprint) for an incident:**
```sql
SELECT 'country' AS type, country AS key, cmd_count FROM incident_country_stats WHERE incident_id = <ID>
UNION ALL
SELECT 'asn', asn_name, cmd_count FROM incident_asn_stats WHERE incident_id = <ID>
UNION ALL
SELECT 'ua', ua, cmd_count FROM incident_ua_stats WHERE incident_id = <ID>
UNION ALL
SELECT 'fingerprint', fingerprint, cmd_count FROM incident_fingerprint_stats WHERE incident_id = <ID>
ORDER BY type, cmd_count DESC;
```

**Most attacked sites last 7 days:**
```sql
SELECT host, COUNT(*) AS incidents,
       ROUND(MAX(spike_ratio)::numeric,0) AS max_spike,
       SUM(challenge_count) AS total_challenges,
       MAX(traffic_peak_count) AS max_peak_req_min
FROM incidents
WHERE started_at >= NOW() - INTERVAL '7 days'
GROUP BY host
ORDER BY incidents DESC
LIMIT 20;
```

**Sites currently under attack (open incidents):**
```sql
SELECT id, host, started_at,
       ROUND(spike_ratio::numeric,1) AS spike_ratio,
       traffic_peak_count AS peak_req_min,
       challenge_count
FROM incidents
WHERE ended_at IS NULL
ORDER BY started_at DESC;
```

**AI actions summary for a site over a period:**
```sql
SELECT a.action, a.target, a.confidence,
       i.started_at, i.spike_ratio::numeric(8,1),
       a.reasoning
FROM first_responder_actions a
JOIN incidents i ON i.id = a.incident_id
WHERE i.host = 'example.org'
  AND i.started_at >= NOW() - INTERVAL '7 days'
  AND a.action NOT IN ('monitor_only', 'raise_threshold')
ORDER BY i.started_at DESC;
```

**LLM cost today by source:**
```sql
SELECT source, COUNT(*) AS calls,
       SUM(input_tokens) AS input_tokens,
       ROUND(SUM(cost_usd)::numeric, 4) AS cost_usd
FROM llm_usage_log
WHERE ts >= NOW() - INTERVAL '24 hours'
GROUP BY source ORDER BY cost_usd DESC;
```

### Checking Pod/System Status

```bash
# Are all pipelines running?
kubectl get pods | grep baskervillehall

# First responder logs (AI decisions in real time)
kubectl logs deployment/incident-first-responder --tail=50

# Active incidents count
kubectl exec $(kubectl get pod -l app=postgres-baskervillehall -o jsonpath='{.items[0].metadata.name}') -- \
  psql -U postgres -d baskerville -c "SELECT COUNT(*) FROM incidents WHERE ended_at IS NULL;"
```

### Generating Incident Reports

When a customer or team asks for an incident report:

1. Query incidents for the site and time period
2. For each significant incident, get the breakdown (country/ASN/UA/fingerprint)
3. Get first_responder_actions to show what was blocked and why
4. Structure report as: Summary → Attack Profile → Timeline → AI Response → Result

See existing reports in this repo (`INCIDENT_REPORT_*.md`) as templates.

### Attack Pattern Reference

| Signal | Meaning |
|---|---|
| `challenge_count = 0` + `spike_ratio > 10` | Single-request bots, cache-busting, ML can't score |
| `spike_ratio > 100` | Extreme volumetric attack |
| TLS fingerprint uniformity > 80% | Single tool/operator |
| UA distribution flat (all UAs ~equal %) | Bot UA rotation, not organic |
| All attacks from residential ISPs | Hired botnet (compromised home routers) |
| `block_criteria` empty in action | No stable fingerprint to block by |
| `action = monitor_only` | AI skipped — volume too low or cooldown active |