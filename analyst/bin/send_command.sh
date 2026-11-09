#!/usr/bin/env bash
# Send one banjax command through Kafka (runs inside the incident-first-responder pod).
#   bin/send_command.sh challenge_ip --host example.org --value 1.2.3.4 --ttl 3600 --reason "..."
#   bin/send_command.sh clear_rules --host example.org --value 1.2.3.4 --reason "false positive"
#   add --dry-run to see the message without sending it.
# The operator is your git email (or BASKERVILLE_OPERATOR); every send is logged in manual_commands.
set -euo pipefail
OPERATOR="${BASKERVILLE_OPERATOR:-$(git config user.email 2>/dev/null || whoami)}"
exec kubectl exec -i deploy/incident-first-responder -c incident-first-responder -- \
  python /usr/bin/baskervillehall/src/baskervillehall/manual_command.py "$@" --operator "$OPERATOR"
