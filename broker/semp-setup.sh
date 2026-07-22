#!/usr/bin/env bash
# =============================================================================
# Provision the Solace broker for the File Connector fan-out demo.
#
#   ./broker/semp-setup.sh           # create queues + subscriptions (idempotent)
#   ./broker/semp-setup.sh teardown  # delete everything this script created
#   ./broker/semp-setup.sh show      # list the demo queues (read-only)
#
# Reads connection details from ../.env (SOLACE_SEMP_URL, SOLACE_MANAGEMENT_USER/PWD,
# SOLACE_VPN_NAME, FC_BASE_TOPIC, FC_SOURCE_LVQ, FC_DEST_{A,B,C}_QUEUE).
#
# Creates, on the broker:
#   - LVQ            $FC_SOURCE_LVQ   (source checkpoint store; exclusive)
#       subs: $FC_BASE_TOPIC/checkpoint  and  $FC_BASE_TOPIC
#   - dest queues    $FC_DEST_A/B/C_QUEUE  (one per destination; non-exclusive)
#       sub:  $FC_BASE_TOPIC
# =============================================================================
set -euo pipefail

cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

SEMP="${SOLACE_SEMP_URL}/SEMP/v2/config"
VPN="${SOLACE_VPN_NAME}"
AUTH=(-u "${SOLACE_MANAGEMENT_USER}:${SOLACE_MANAGEMENT_PWD}")
CURL=(curl -sS -m 30 "${AUTH[@]}" -H "Content-Type: application/json")

# urlencode a topic (SEMP path segments must be encoded: / -> %2F, > -> %3E, * -> %2A)
enc() { local s="$1"; s="${s//%/%25}"; s="${s//\//%2F}"; s="${s//>/%3E}"; s="${s//\*/%2A}"; s="${s// /%20}"; printf '%s' "$s"; }

# POST that treats "already exists" (400 ALREADY_EXISTS) as success → idempotent
post() { # $1 = collection path, $2 = json
  local path="$1" body="$2" resp code
  resp=$("${CURL[@]}" -w $'\n%{http_code}' -X POST "${SEMP}/${path}" -d "${body}")
  code="${resp##*$'\n'}"; body="${resp%$'\n'*}"
  if [[ "$code" == 2* ]]; then return 0; fi
  if grep -q "ALREADY_EXISTS" <<<"$body"; then return 0; fi
  echo "  ! POST ${path} -> HTTP ${code}: $(grep -o '\"description\":\"[^\"]*\"' <<<"$body" | head -1)" >&2
  return 1
}

create_queue() { # $1 = name, $2 = accessType (exclusive|non-exclusive)
  local name="$1" access="$2"
  echo "  queue: ${name} (${access})"
  post "msgVpns/${VPN}/queues" "$(cat <<JSON
{ "queueName": "${name}", "accessType": "${access}", "permission": "consume",
  "ingressEnabled": true, "egressEnabled": true, "maxMsgSpoolUsage": 1000,
  "respectTtlEnabled": false }
JSON
)"
}

add_sub() { # $1 = queue, $2 = topic
  local q="$1" t="$2"
  echo "    sub: ${q} <- ${t}"
  post "msgVpns/${VPN}/queues/${q}/subscriptions" "{ \"subscriptionTopic\": \"${t}\" }"
}

del_queue() { # $1 = name
  echo "  delete queue: $1"
  "${CURL[@]}" -X DELETE "${SEMP}/msgVpns/${VPN}/queues/$(enc "$1")" >/dev/null || true
}

QUEUES=( "${FC_SOURCE_LVQ}" "${FC_DEST_A_QUEUE}" "${FC_DEST_B_QUEUE}" "${FC_DEST_C_QUEUE}" )

setup() {
  echo "Provisioning VPN '${VPN}' on ${SOLACE_SEMP_URL} ..."

  # --- Source checkpoint LVQ ---
  # NOT pre-created: the source connector provisions ${FC_SOURCE_LVQ} itself with the
  # exact LVQ properties + checkpoint subscription it needs. Pre-creating it causes a
  # JCSMP PropertyMismatchException (quota). `teardown` still deletes it.

  # --- One queue per destination, each subscribed to its own destId topic ---
  # The source publishes file events to ${FC_BASE_TOPIC}/<destId>, where <destId> is the
  # source sub-folder name (${FILE(FNAME_P)}). So a file in /inbound/destA reaches ONLY
  # the queue subscribed to ${FC_BASE_TOPIC}/destA. destId == folder name == routing key.
  #
  # TWO subscriptions are required per dest queue:
  #   1. ${FC_BASE_TOPIC}/<destId>   -> the file DATA multiparts (routing key = folder name)
  #   2. ${FC_BASE_TOPIC}            -> the RUN START/COMPLETE events (col3 = customBaseDestinationTopic).
  #                                     The START event (EVENT_TYPE=START) carries the RUN_ID and
  #                                     INITIALISES the sink assembler (FileOutboundMessageHandler L328).
  # Without the base-topic subscription the sink gets only orphan data parts and dies with
  # FA_ERROR_MISMATCH_IN_RUNID. This is NOT stated in the User Guide subscription table — see
  # docs/FINDINGS.md. (Verified working: q.fc.dest-a subscribed to both, per broker Subscriptions tab.)
  create_queue "${FC_DEST_A_QUEUE}" "non-exclusive"
  add_sub "${FC_DEST_A_QUEUE}" "${FC_BASE_TOPIC}/destA"; add_sub "${FC_DEST_A_QUEUE}" "${FC_BASE_TOPIC}"
  create_queue "${FC_DEST_B_QUEUE}" "non-exclusive"
  add_sub "${FC_DEST_B_QUEUE}" "${FC_BASE_TOPIC}/destB"; add_sub "${FC_DEST_B_QUEUE}" "${FC_BASE_TOPIC}"
  create_queue "${FC_DEST_C_QUEUE}" "non-exclusive"
  add_sub "${FC_DEST_C_QUEUE}" "${FC_BASE_TOPIC}/destC"; add_sub "${FC_DEST_C_QUEUE}" "${FC_BASE_TOPIC}"

  echo "Done. 3 destination queues (data topic + base topic for START/COMPLETE); LVQ auto-provisioned by the source."
}

teardown() {
  echo "Tearing down demo queues on VPN '${VPN}' ..."
  for q in "${QUEUES[@]}"; do del_queue "$q"; done
  echo "Done."
}

show() {
  echo "Demo queues on VPN '${VPN}':"
  for q in "${QUEUES[@]}"; do
    "${CURL[@]}" "${SEMP}/msgVpns/${VPN}/queues/$(enc "$q")?select=queueName,accessType" \
      | grep -oE '"(queueName|accessType)":"[^"]*"' | tr '\n' ' ' ; echo
  done
}

case "${1:-setup}" in
  setup)    setup ;;
  teardown) teardown ;;
  show)     show ;;
  *) echo "usage: $0 [setup|teardown|show]"; exit 2 ;;
esac
