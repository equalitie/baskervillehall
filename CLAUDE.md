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