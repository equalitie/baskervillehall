## What is Baskerville

Baskerville is an intelligent analytics engine designed to detect and mitigate Layer 7 (application layer) DDoS attacks by analyzing web request behavior in real time. Originally developed for the Deflect platform, it enables infrastructure to respond proactively to suspicious or malicious traffic before it causes disruption.

Baskerville uses machine learning to distinguish between normal and abnormal traffic patterns at the session level. It classifies traffic into categories like human users, legitimate bots, malicious bots, and AI-driven crawlers.

### Inputs and Outputs

Baskerville receives structured web logs as input, typically from Deflect edge nodes, Cloudflare Workers, or AWS Lambda.

Using this data, Baskerville analyzes each session and, when necessary, emits challenge commands as output. These commands can then be consumed and executed by the same platforms — Deflect, Cloudflare, or AWS Lambda — to block, rate-limit, or further inspect suspicious IPs.

In addition, Baskerville offers a user-facing dashboard for monitoring traffic, analyzing attack patterns, reviewing challenge decisions, and managing configuration policies.

### Key Challenges Baskerville Solves

- Fast Detection  
  Real-time analysis ensures suspicious activity is caught early enough to prevent damage.

- Traffic Adaptability  
  Designed to scale with volatile and diverse traffic loads using technologies like Apache Kafka (and optionally Apache Spark).

- Actionable Predictions  
  For each suspicious session or IP, Baskerville issues a prediction and can trigger a challenge, such as a block, redirect, or CAPTCHA—customizable per deployment.

- Human vs Bot Identification  
  Combines heuristics and ML to reliably distinguish human visitors from automated agents.

- AI Crawler Detection  
  Tracks advanced scraping and probing behaviors characteristic of LLM-based or stealth crawlers.

- Prediction Reliability  
  A feedback loop and probation period mechanism reduce false positives and improve model accuracy over time.

- Learning from Imperfect Data  
  With limited labeled data, Baskerville relies on unsupervised anomaly detection, trained on mostly normal behavior, yet robust to minor contamination.



# Baskerville – Web Request Intelligence & Bot Mitigation Engine

Baskerville is an intelligent traffic analysis engine that classifies incoming web traffic into categories such as:

- Normal human connections
- Verified crawlers (Google, Bing, DuckDuckGo, etc.)
- Malicious bots
- AI crawlers

It enables website operators to detect and mitigate suspicious or harmful sessions in real time.



## How It Works

### 1. Session Grouping
Incoming web requests are grouped into sessions based on:

- Requested host
- Client IP address
- Session cookie

### 2. Feature Extraction
Each session is analyzed through a set of computed features, including:

- Average URL path depth  
- Number of unique queries  
- User agent reputation score  
- HTML-to-image content ratio  
- ...and more

### 3. Traffic Classification
Sessions are classified into:

- Human traffic
- Automated traffic

This classification uses heuristic rules, logical checks, and supervised ML models.

### 4. Anomaly Detection
Separate unsupervised anomaly detection models are trained for each class (human vs. automated) to identify outliers.



## Operating Modes

Baskerville supports two primary modes of operation:

- War Mode  
  Aggressively challenges or blocks all non-verified bots, allowing only trusted crawlers (e.g., Googlebot).

- Peace Mode  
  Targets only anomalous sessions, applying challenges selectively to reduce friction for normal users.



## Architecture Overview

- Input Sources:
  - Deflect platform logs
  - Cloudflare Workers
  - AWS Lambda

- Output Actions:
  - Challenge or block commands
  - Sent to Banjax, Cloudflare Worker, or AWS Lambda

- Deployment:
  - Run in online mode for real-time processing and mitigation



## Key Features

- Session-level analysis of web traffic
- Supervised and unsupervised ML-based classification
- AI crawler detection and blocking
- VPN and Tor exit node detection
- Configurable response strategy (challenge/block/allow)
- Custom challenge engine support
- REST dashboard for analytics, statistics, and configuration


## Tech Stack

Baskerville is built on a modern, scalable analytics stack designed for high-throughput environments:

- **Apache Kafka** – Used for distributed messaging and stream processing
- **PostgreSQL** – Stores session metadata, model outputs, and feedback
- **Python** – Core language for orchestration, feature extraction, and machine learning logic
- **Scikit-learn** – Used for supervised classification models (e.g. human vs bot)
- **TensorFlow** – Used for anomaly detection and deep learning components
- **Kubernetes** – Baskerville services are deployed and orchestrated within a Kubernetes cluster for resilience and scalability

## Dashboard

Baskerville includes a web dashboard for:

- Visualizing live and historical traffic statistics
- Monitoring detected threats
- Reviewing challenge decisions
- Managing configuration and response policies


## Use Cases
- Defending high-risk websites from botnet attacks
- Protecting content from AI web scrapers
- Reducing false positives by classifying sessions intelligently
- Augmenting WAFs with behavioral intelligence
