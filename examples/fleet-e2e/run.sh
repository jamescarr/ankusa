#!/usr/bin/env bash
# Fleet end to end: load balancer -> two Ankusa edge nodes -> RabbitMQ ->
# consumer -> Postgres, with Redis route/source stores, a shared S3 bucket and
# a claim-check gateway. Drives tools/loadgen through a steady phase, a chaos
# phase (a node killed and restarted, the consumer killed, a provider retry
# sent to the other node) and a burst phase, and fails on any acked hook the
# consumer never recorded.
#
#   examples/fleet-e2e/run.sh
#
# Needs docker (with compose), mix, curl, jq. See README.md.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

# Env knobs (override by exporting before invoking this script).
: "${RATE:=200}"
: "${DURATION:=45}"
: "${BURST_SECONDS:=10}"
: "${CONCURRENCY:=32}"
: "${KEEP:=0}"
: "${BUILD_ANKUSA:=1}"
: "${ANKUSA_IMAGE:=ankusa-fleet-e2e:local}"
: "${OUT_DIR:=examples/fleet-e2e/.e2e-out}"
export ANKUSA_IMAGE

for tool in docker mix curl jq; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "error: required tool '$tool' not found on PATH" >&2
    exit 2
  fi
done

if [ "$DURATION" -lt 40 ]; then
  echo "error: DURATION must be at least 40 (the chaos timeline runs to +35s)" >&2
  exit 2
fi

LB=http://127.0.0.1:18080
NODE_A=http://127.0.0.1:14001
NODE_B=http://127.0.0.1:14002
DB_URL=postgres://ankusa:ankusa@127.0.0.1:15433/consumer

compose() { docker compose -f examples/fleet-e2e/docker-compose.yml "$@"; }
psql_q() { compose exec -T postgres psql -U ankusa -d consumer -Atc "$1"; }
rand_id() { od -An -N8 -tx1 /dev/urandom | tr -d ' \n'; }

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

# post_hook URL BODY_FILE -> "<status> <hook id>"
post_hook() {
  local out code
  out="$(mktemp)"
  code="$(curl -s -o "$out" -w '%{http_code}' --max-time 15 -XPOST "$1" \
    -H 'content-type: application/json' --data-binary "@$2" || true)"
  echo "$code $(jq -r '.id // empty' "$out" 2>/dev/null || true)"
  rm -f "$out"
}

# wait_for SECONDS DESCRIPTION CMD... -- polls CMD every second until it succeeds.
wait_for() {
  local secs=$1 desc=$2
  shift 2
  for _ in $(seq 1 "$secs"); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "error: timed out after ${secs}s waiting for $desc" >&2
  return 1
}

node_ready() { curl -sf --max-time 2 "$1/ready"; }

# wait_row SECONDS DESCRIPTION SQL -- polls until SQL (a count) returns 1.
wait_row() {
  local secs=$1 desc=$2 sql=$3
  for _ in $(seq 1 "$secs"); do
    if [ "$(psql_q "$sql" 2>/dev/null || true)" = "1" ]; then return 0; fi
    sleep 1
  done
  echo "error: timed out after ${secs}s waiting for $desc" >&2
  return 1
}

FAILED=0
LOADGEN_PID=""

cleanup() {
  rc=$?
  if [ -n "$LOADGEN_PID" ] && kill -0 "$LOADGEN_PID" 2>/dev/null; then kill "$LOADGEN_PID" 2>/dev/null || true; fi
  if [ "$rc" -ne 0 ]; then
    echo "==> run failed (exit $rc); service logs follow" >&2
    for s in ankusa-a ankusa-b claim-check consumer nginx; do
      echo "--- $s ---" >&2
      compose logs --no-color --tail 60 "$s" >&2 || true
    done
  fi
  if [ "$KEEP" != "1" ]; then
    echo "==> tearing down the stack"
    compose down -v --remove-orphans >/dev/null 2>&1 || true
  else
    echo "==> KEEP=1 set, leaving the stack up (load balancer $LB)"
  fi
  exit "$rc"
}
trap cleanup EXIT

mkdir -p "$OUT_DIR"
OUT_ABS="$(pwd)/$OUT_DIR"
rm -f "$OUT_ABS"/*.csv "$OUT_ABS"/*.json

echo "==> clearing any previous stack"
compose down -v --remove-orphans >/dev/null 2>&1 || true

if [ "$BUILD_ANKUSA" = "1" ]; then
  echo "==> building $ANKUSA_IMAGE"
  docker build -f packages/ankusa_server/Dockerfile -t "$ANKUSA_IMAGE" .
fi

echo "==> starting the fleet"
compose up -d --build

echo "==> waiting for both nodes and the load balancer"
wait_for 180 "ankusa-a /ready" node_ready "$NODE_A"
wait_for 180 "ankusa-b /ready" node_ready "$NODE_B"
wait_for 60 "a hook through the load balancer" \
  curl -sf --max-time 5 -o /dev/null -XPOST "$LB/webhooks/demo" -d '{"id":"warmup"}'

echo "==> routes are shared: a path nobody defined is a 404 on every node"
for url in "$NODE_A" "$NODE_B" "$LB" "$LB"; do
  code="$(curl -s -o /dev/null -w '%{http_code}' -XPOST "$url/webhooks/nope" -d '{"id":"x"}')"
  [ "$code" = "404" ] || { echo "error: POST $url/webhooks/nope returned $code, want 404" >&2; exit 1; }
done

echo "==> installing loadgen deps"
(cd tools/loadgen && mix deps.get >/dev/null)

# run_loadgen NAME [loadgen.run flags...]
run_loadgen() {
  local name=$1
  shift
  (cd tools/loadgen && mix loadgen.run --url "$LB/webhooks/demo" "$@" \
    --out "$OUT_ABS/$name.csv" --report "$OUT_ABS/$name-report.json")
}

# verify_phase NAME TIMEOUT -- loadgen.verify; sets FAILED on loss or a sha mismatch.
verify_phase() {
  (cd tools/loadgen && mix loadgen.verify --acked "$OUT_ABS/$1.csv" --database-url "$DB_URL" \
    --timeout "$2" --report "$OUT_ABS/$1-verify.json") || FAILED=1
}

echo "==> phase 1/4: steady ($RATE/s for ${DURATION}s through the load balancer)"
run_loadgen steady --rate "$RATE" --duration "$DURATION" --concurrency 64
verify_phase steady 300

echo "==> phase 2/4: fat hooks from each node, redeemed through the gateway"
CLAIM_OK=true
for pair in "a $NODE_A" "b $NODE_B"; do
  set -- $pair
  node=$1
  url=$2
  fat_id="fat-$node-$(rand_id)"
  fat_file="$OUT_ABS/fat-$node.json"
  jq -nc --arg id "$fat_id" --arg pad "$(head -c 20000 /dev/zero | tr '\0' x)" '{id: $id, pad: $pad}' >"$fat_file"
  read -r code hook_id <<<"$(post_hook "$url/webhooks/demo" "$fat_file")"
  if [ "$code" != "201" ] || [ -z "$hook_id" ]; then
    echo "error: fat hook to node $node returned $code" >&2
    CLAIM_OK=false
    continue
  fi
  want_sha="$(sha256 "$fat_file")"
  if wait_row 90 "fat hook $hook_id (node $node) in processed_webhooks" \
    "SELECT count(*) FROM processed_webhooks WHERE ankusa_id = '$hook_id' AND via LIKE 'claim:%' AND body_sha256 = '$want_sha' AND processed_at IS NOT NULL"; then
    echo "    node $node: $hook_id redeemed via the gateway, sha256 matches"
  else
    echo "error: fat hook $hook_id from node $node never arrived via a claim with the right sha256" >&2
    CLAIM_OK=false
  fi
done
[ "$CLAIM_OK" = "true" ] || FAILED=1

echo "==> phase 3/4: chaos (kill node a, retry on the other node, kill the consumer)"
run_loadgen chaos --rate "$RATE" --duration "$DURATION" --concurrency 64 &
LOADGEN_PID=$!

retry_id="retry-$(rand_id)"
retry_file="$OUT_ABS/retry.json"
jq -nc --arg id "$retry_id" '{id: $id, event: "provider-retry"}' >"$retry_file"
retry_key="default:demo:$retry_id"

sleep 5
echo "    +5s   provider delivers event $retry_id to node b"
read -r code_b hook_b <<<"$(post_hook "$NODE_B/webhooks/demo" "$retry_file")"

sleep 5
echo "    +10s  killing ankusa-a"
compose kill ankusa-a

sleep 10
echo "    +20s  starting ankusa-a again"
compose start ankusa-a
wait_for 120 "ankusa-a /ready after restart" node_ready "$NODE_A"

echo "    the provider saw no answer in time and retries the same event on node a"
read -r code_a hook_a <<<"$(post_hook "$NODE_A/webhooks/demo" "$retry_file")"
if [ "$code_b" != "201" ] || [ "$code_a" != "201" ]; then
  echo "error: provider retry answered $code_b (node b) and $code_a (node a), want 201 and 201" >&2
  FAILED=1
fi

sleep 5
echo "    +30s  killing the consumer"
compose kill consumer
sleep 3
compose start consumer

wait "$LOADGEN_PID" || FAILED=1
LOADGEN_PID=""
verify_phase chaos 300

# The retry is one idempotency key. The dedupe index is per node, so the two
# hooks have different ids and the consumer sees the event twice; one row, with
# `deliveries` counting both.
RETRY_EXTRA=0
if [ -n "$hook_a" ] && [ -n "$hook_b" ]; then
  want_deliveries=2
  [ "$hook_a" = "$hook_b" ] && want_deliveries=1
  if wait_row 90 "the retried event ($retry_key) with $want_deliveries deliveries" \
    "SELECT count(*) FROM processed_webhooks WHERE idempotency_key = '$retry_key' AND deliveries >= $want_deliveries AND processed_at IS NOT NULL"; then
    RETRY_EXTRA="$(psql_q "SELECT deliveries - 1 FROM processed_webhooks WHERE idempotency_key = '$retry_key'")"
    echo "    $retry_key: one row, $((RETRY_EXTRA + 1)) delivery(ies) (hook ids $hook_b, $hook_a)"
  else
    FAILED=1
  fi
else
  FAILED=1
fi

echo "==> phase 4/4: burst ($CONCURRENCY workers for ${BURST_SECONDS}s)"
run_loadgen burst --concurrency "$CONCURRENCY" --duration "$BURST_SECONDS"
verify_phase burst 900

# Sum the per-phase verify reports. `missing` in a report is a sample of up to
# ten ids, so the count is acked - processed.
MISSING=0
SHA_MISMATCHES=0
EXTRA=$RETRY_EXTRA
for phase in steady chaos burst; do
  report="$OUT_ABS/$phase-verify.json"
  if [ ! -f "$report" ]; then
    echo "error: no verify report for phase $phase" >&2
    FAILED=1
    continue
  fi
  MISSING=$((MISSING + $(jq '.acked - .processed' "$report")))
  SHA_MISMATCHES=$((SHA_MISMATCHES + $(jq '.sha_mismatches' "$report")))
  EXTRA=$((EXTRA + $(jq '.extra_deliveries' "$report")))
done

echo
echo "verify: missing=$MISSING sha_mismatches=$SHA_MISMATCHES extra_deliveries=$EXTRA claim_redeemed_cross_node=$CLAIM_OK"

if [ "$MISSING" != 0 ] || [ "$SHA_MISMATCHES" != 0 ]; then FAILED=1; fi
if [ "$FAILED" != "0" ]; then
  echo "==> the fleet lost or corrupted a hook, or an assertion failed; see $OUT_DIR/*-verify.json" >&2
  exit 1
fi
echo "==> every acked hook was processed; reports in $OUT_DIR"
