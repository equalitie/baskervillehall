#!/usr/bin/env bash
# Read-only query against the Baskerville postgres (via kubectl exec).
#   bin/q.sh "SELECT ..."
#   bin/q.sh < query.sql
# Env: TIMEOUT (default 120s), FORMAT=csv for CSV output.
set -euo pipefail
POD=$(kubectl get pod -l app=postgres-baskervillehall -o jsonpath='{.items[0].metadata.name}')
if [ $# -gt 0 ]; then SQL="$1"; else SQL=$(cat); fi
OPTS=(-U postgres -d baskerville -X -q -v ON_ERROR_STOP=1 -P pager=off)
[ "${FORMAT:-}" = csv ] && OPTS+=(--csv)
{
  echo "SET default_transaction_read_only = on;"
  echo "SET statement_timeout = '${TIMEOUT:-120s}';"
  echo "$SQL"
} | kubectl exec -i "$POD" -c postgres -- psql "${OPTS[@]}"
