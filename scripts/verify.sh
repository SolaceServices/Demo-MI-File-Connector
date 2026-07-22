#!/usr/bin/env bash
# Quick health check of the running stack (no passwords needed).
#   ./scripts/verify.sh
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== Connector health (expect {\"status\":\"UP\"}) =="
for p in 8090:source 8091:sink-a 8092:sink-b 8093:sink-c; do
  port="${p%%:*}"; name="${p##*:}"
  printf "  %-8s " "$name"
  curl -s "http://localhost:${port}/actuator/health" || echo "(no response)"
  echo
done

echo
echo "== Broker queues + subscriptions =="
./broker/semp-setup.sh show

echo
echo "To watch routing:   docker logs -f fc-source 2>&1 | grep -E 'Blob Found|custom topic:.*/dest|Moved GCS|files processed|error'"
echo "To see files arrive: connect FileZilla (SFTP) to 127.0.0.1:2222/2223/2224, user/pass from .env, folder /outbound"
