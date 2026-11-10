#!/usr/bin/env bash
# Creates/updates the Kubernetes Secret "baskervillehall-secrets" that holds everything that must
# never be in git. Workloads read it with envFrom.secretRef next to the "baskervillehall" ConfigMap.
#
#   scripts/create_baskervillehall_secrets.sh <env-file>
#
# <env-file> (KEY=value per line, keep it outside git: *.env is gitignored) must define:
REQUIRED="DEFLECT_CONFIG_AUTH DEFLECT_CONFIG_AUTH_IP_WHITELIST DEFLECT_CONFIG_AUTH_GLOBAL_ALLOWLIST
DEFLECT_CONFIG_AUTH_ORIGIN_IPS POSTGRES_PASSWORD ALERT_WEBHOOK TYPESAFE_API_KEY"
set -euo pipefail
ENV_FILE="${1:?usage: $0 <env-file>}"
for k in $REQUIRED; do
  grep -q "^$k=." "$ENV_FILE" || { echo "missing $k in $ENV_FILE" >&2; exit 1; }
done
kubectl create secret generic baskervillehall-secrets --from-env-file="$ENV_FILE" \
  --dry-run=client -o yaml | kubectl apply -f -
echo "baskervillehall-secrets updated; restart the workloads to pick up new values"
