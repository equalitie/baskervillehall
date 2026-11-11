#!/usr/bin/env bash
# Read-only OpenSearch queries through the OpenSearch Dashboards console proxy.
#
# Usage:
#   osq.sh METHOD PATH [BODY]
#     METHOD  GET or POST
#     PATH    <index-pattern>/<endpoint>[?params]
#             endpoint: _search | _count | _msearch | _field_caps | _mapping
#             e.g. 'deflect.log-*/_search'  'banjax-*/_field_caps?fields=trigger,action'
#     BODY    JSON string, '@file.json' to read a file, or '-' to read stdin
#
# Load guard (check_query.py): every query must bound @timestamp; > 7 days is refused;
# > 1 day needs another query condition.
#
# _search responses are trimmed with a default filter_path unless PATH already has
# one or OSQ_RAW=1. Credentials come from the macOS Keychain item with service
# "kibana.equalit.ie". The password goes to curl on stdin, never on argv.
#
# Env: OSQ_URL (default https://kibana.equalit.ie), OSQ_KEYCHAIN_SERVICE,
#      OSQ_TIMEOUT (curl seconds, default 60), OSQ_RAW=1 (no default filter_path)
set -euo pipefail

BASE_URL="${OSQ_URL:-https://kibana.equalit.ie}"
SERVICE="${OSQ_KEYCHAIN_SERVICE:-kibana.equalit.ie}"
TIMEOUT="${OSQ_TIMEOUT:-60}"
DEFAULT_FILTER="took,timed_out,_shards.failed,hits.total,hits.hits._index,hits.hits._source,aggregations,count"

die() { echo "osq: $*" >&2; exit 2; }

if [[ $# -lt 2 ]]; then
  sed -n '2,21p' "$0" | sed 's/^# \{0,1\}//' >&2
  exit 2
fi

METHOD="$(printf '%s' "$1" | tr '[:lower:]' '[:upper:]')"
API_PATH="${2#/}"
BODY_ARG="${3:-}"

# --- read-only guard -------------------------------------------------------
[[ "$METHOD" == "GET" || "$METHOD" == "POST" ]] || die "only GET and POST are allowed (got $METHOD)"
PATH_ONLY="${API_PATH%%\?*}"
QUERY=""
[[ "$API_PATH" == *\?* ]] && QUERY="${API_PATH#*\?}"
[[ "$PATH_ONLY" =~ ^[A-Za-z0-9._*,-]+/(_search|_count|_msearch|_field_caps|_mapping)$ ]] \
  || die "path not allowed: '$PATH_ONLY'. Use <index-pattern>/(_search|_count|_msearch|_field_caps|_mapping)"
ENDPOINT="${PATH_ONLY##*/}"
if [[ "$QUERY" =~ (^|&)scroll= ]]; then die "scroll is not allowed; use search_after or a smaller time window"; fi

if [[ "$ENDPOINT" == "_search" && -z "${OSQ_RAW:-}" && ! "$QUERY" =~ (^|&)filter_path= ]]; then
  QUERY="${QUERY:+$QUERY&}filter_path=$DEFAULT_FILTER"
fi
FULL_PATH="$PATH_ONLY${QUERY:+?$QUERY}"

# --- body -------------------------------------------------------------------
TMPDIR_OSQ="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_OSQ"' EXIT
BODY_FILE="$TMPDIR_OSQ/body"
OUT_FILE="$TMPDIR_OSQ/out"
case "$BODY_ARG" in
  "")  : > "$BODY_FILE" ;;
  -)   cat > "$BODY_FILE" ;;
  @*)  cat "${BODY_ARG#@}" > "$BODY_FILE" ;;
  *)   printf '%s' "$BODY_ARG" > "$BODY_FILE" ;;
esac
# _msearch NDJSON must end with a newline.
if [[ "$ENDPOINT" == "_msearch" && -s "$BODY_FILE" && -n "$(tail -c1 "$BODY_FILE")" ]]; then
  echo >> "$BODY_FILE"
fi

# --- load guard: time range limits (see check_query.py) ---------------------
RC=0
python3 -I "$(cd "$(dirname "$0")" && pwd)/check_query.py" "$ENDPOINT" "$BODY_FILE" || RC=$?
[[ $RC -eq 0 ]] || exit $RC

# --- credentials ------------------------------------------------------------
ACCOUNT="$(security find-generic-password -s "$SERVICE" 2>/dev/null \
  | sed -n 's/^ *"acct"<blob>="\(.*\)"$/\1/p' | head -n1)" || true
if [[ -z "$ACCOUNT" ]]; then
  echo "osq: no Keychain item for service '$SERVICE'." >&2
  echo "     Create it with: security add-generic-password -s $SERVICE -a <username> -w" >&2
  exit 3
fi
PASSWORD="$(security find-generic-password -s "$SERVICE" -a "$ACCOUNT" -w)"
case "$ACCOUNT$PASSWORD" in *$'\n'*|*$'\r'*) die "Keychain username or password contains a newline";; esac
esc() { printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'; }

# --- request ----------------------------------------------------------------
ENC_PATH="$(python3 -c 'import sys,urllib.parse;print(urllib.parse.quote(sys.argv[1],safe=""))' "$FULL_PATH")"
URL="$BASE_URL/api/console/proxy?path=$ENC_PATH&method=$METHOD"

CURL_ARGS=(-sS -m "$TIMEOUT" -o "$OUT_FILE" -w '%{http_code}' -K - -X POST -H 'osd-xsrf: true' "$URL")
if [[ -s "$BODY_FILE" ]]; then
  CT="application/json"; [[ "$ENDPOINT" == "_msearch" ]] && CT="application/x-ndjson"
  CURL_ARGS+=(-H "Content-Type: $CT" --data-binary "@$BODY_FILE")
fi

HTTP_CODE="$(printf 'user = "%s:%s"\n' "$(esc "$ACCOUNT")" "$(esc "$PASSWORD")" | curl "${CURL_ARGS[@]}")" || {
  unset PASSWORD; echo "osq: curl failed (timeout ${TIMEOUT}s or network error)" >&2; exit 1; }
unset PASSWORD

if [[ ! "$HTTP_CODE" =~ ^2 ]]; then
  echo "osq: HTTP $HTTP_CODE" >&2
  head -c 2000 "$OUT_FILE" >&2; echo >&2
  case "$HTTP_CODE" in
    401) echo "osq: credentials rejected. Re-store the Keychain item (delete the old one first)." >&2 ;;
    403) echo "osq: your OpenSearch role lacks permission for this request." >&2 ;;
  esac
  exit 1
fi

cat "$OUT_FILE"; echo

# Warn on partial results so they are not reported as complete.
python3 - "$OUT_FILE" <<'PY' || true
import json, sys
try:
    d = json.load(open(sys.argv[1]))
except Exception:
    sys.exit(0)
for r in (d.get("responses") or [d]):
    if not isinstance(r, dict):
        continue
    if r.get("timed_out"):
        print("osq: WARNING timed_out=true, results are partial", file=sys.stderr)
    failed = (r.get("_shards") or {}).get("failed", 0)
    if failed:
        print(f"osq: WARNING {failed} shard(s) failed, results are partial", file=sys.stderr)
PY
