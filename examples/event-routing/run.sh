#!/usr/bin/env bash
# Prove the routing: post three hooks to a running stack and read where each one
# landed. Needs the stack up (`docker compose up --build -d --wait`), curl and
# docker compose. Exits 0 with an OK line, or non-zero with the logs.
#
#   hook 1  billing   {"type":"invoice.paid"}     -> topic events.invoice.paid, and the receiver
#   hook 2  billing   {"event":"customer.created"} -> topic events.other only (no `type`: the
#                     source reads no event type, the topic template takes its fallback and
#                     the receiver's filter does not match)
#   hook 3  invoices  {"event":"customer.created"} -> 201 "filtered":true: its only sink's filter
#                     selects nothing, so nothing is stored
set -euo pipefail
cd "$(dirname "$0")"

INGEST="http://localhost:${INGEST_PORT:-4000}"   # the host port docker-compose.yml publishes
WAIT_SECONDS="${WAIT_SECONDS:-90}"
RUN="$(date +%s)$RANDOM"   # makes this run's lines unique in logs that keep earlier runs
PAID="paid-$RUN"
CUST="cust-$RUN"

fail() {
  echo "FAIL: $*" >&2
  echo "--- worker" >&2
  docker compose logs --no-log-prefix --tail 30 worker >&2 || true
  echo "--- receiver" >&2
  docker compose logs --no-log-prefix --tail 30 receiver >&2 || true
  echo "--- ankusa" >&2
  docker compose logs --no-log-prefix --tail 30 ankusa >&2 || true
  exit 1
}

send() {
  local source=$1 json=$2 out
  out=$(curl -sS -w '\n%{http_code}' -X POST "$INGEST/webhooks/$source" \
    -H 'content-type: application/json' -d "$json") || fail "POST /webhooks/$source did not connect"
  RESPONSE_BODY=$(printf '%s' "$out" | sed '$d')
  RESPONSE_STATUS=$(printf '%s' "$out" | tail -n 1)
  echo "POST /webhooks/$source $json -> $RESPONSE_STATUS $RESPONSE_BODY"
}

logs() { docker compose logs --no-log-prefix "$1" 2>&1; }

send billing "{\"type\":\"invoice.paid\",\"ref\":\"$PAID\"}"
[ "$RESPONSE_STATUS" = 201 ] || fail "hook 1: expected 201, got $RESPONSE_STATUS"
send billing "{\"event\":\"customer.created\",\"ref\":\"$CUST\"}"
[ "$RESPONSE_STATUS" = 201 ] || fail "hook 2: expected 201, got $RESPONSE_STATUS"
send invoices "{\"event\":\"customer.created\",\"ref\":\"$CUST\"}"
[ "$RESPONSE_STATUS" = 201 ] || fail "hook 3: expected 201, got $RESPONSE_STATUS"
case "$RESPONSE_BODY" in
  *'"filtered":true'*) ;;
  *) fail "hook 3: expected \"filtered\":true in $RESPONSE_BODY" ;;
esac

# Wait for the three deliveries that must happen.
deadline=$((SECONDS + WAIT_SECONDS))
until logs worker | grep "topic=events.invoice.paid " | grep -q "$PAID" \
  && logs worker | grep "topic=events.other " | grep -q "$CUST" \
  && logs receiver | grep -q "$PAID"; do
  [ "$SECONDS" -lt "$deadline" ] || fail "no delivery within ${WAIT_SECONDS}s"
  sleep 1
done

# Both hooks have been consumed, so a delivery that should not have happened
# had the same time to show up. Give a wrong one a moment, then look.
sleep 3

logs worker | grep "topic=events.invoice.paid " | grep -q "event_type=invoice.paid.*$PAID" \
  || fail "hook 1 reached events.invoice.paid without event_type=invoice.paid"
logs worker | grep "topic=events.other " | grep -q "event_type=null.*$CUST" \
  || fail "hook 2 reached events.other with an event_type"
logs receiver | grep "$PAID" | grep -q "event_type=invoice.paid verified=true" \
  || fail "hook 1 reached the receiver unverified or without x-ankusa-event-type"

if logs worker | grep "topic=events.other " | grep -q "$PAID"; then fail "hook 1 reached events.other"; fi
if logs worker | grep "topic=events.invoice.paid " | grep -q "$CUST"; then fail "hook 2 reached events.invoice.paid"; fi
if logs receiver | grep -q "$CUST"; then fail "hook 2 or 3 reached the receiver"; fi

echo "OK: invoice.paid on events.invoice.paid and at receiver; customer.created on events.other only"
