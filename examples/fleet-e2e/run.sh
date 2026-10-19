#!/usr/bin/env bash
# Fleet end to end: load balancer -> two Ankusa edge nodes -> RabbitMQ ->
# consumer -> Postgres, with Redis route/source stores, a shared S3 bucket and
# a claim-check gateway. Drives tools/loadgen through a steady phase, a chaos
# phase (a node killed and restarted, the consumer killed, a provider retry
# sent to the other node) and a burst phase, and fails on any acked hook the
# consumer never recorded.
#
#   examples/fleet-e2e/run.sh                          # wal.type: disk, the default drill
#   examples/fleet-e2e/run.sh --wal dual               # wal.type: dual (a three-broker Kafka intake log)
#   examples/fleet-e2e/run.sh --wal dual --kill-node   # + host loss, the log's brokers down, the secondary backup
#   examples/fleet-e2e/run.sh --wal disk --kill-node   # the same host loss under `disk`: it LOSES hooks, as promised
#
# `--kill-node` turns the backup on (every 15 s, a primary and a secondary
# bucket) and adds, after the chaos phase:
#
#   host loss        RabbitMQ is paused so acked hooks pile up undelivered, `ankusa-a` is
#                    killed AND its volume deleted (the host, not the process), and a
#                    replacement starts on an empty volume with the same node id.
#                    `dual`: it restores the backup and replays the intake log, so every
#                    201 id reaches the consumer (missing=0). `disk`: only the backup
#                    comes back, so the hooks acked since it are gone, and the drill
#                    fails unless it sees that loss.
#   broker outage    (`dual`) the three intake brokers stop: every node refuses with
#                    `503 intake_unavailable` and not one request is acked; they restart
#                    and the nodes answer 201 again.
#   secondary only   (`dual`) the primary backup is deleted, then the host is lost again:
#                    the replacement restores from the secondary bucket and `recover`
#                    (the image's command, over the stopped store) replays the log.
#
# Needs docker (with compose), mix, curl, jq. See README.md.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

usage() {
  echo "usage: $0 [--wal disk|dual] [--kill-node]" >&2
  exit 2
}

WAL=disk
KILL_NODE=0
while [ $# -gt 0 ]; do
  case "$1" in
    --wal)
      [ $# -ge 2 ] || usage
      WAL=$2
      shift 2
      ;;
    --kill-node)
      KILL_NODE=1
      shift
      ;;
    *) usage ;;
  esac
done
case "$WAL" in disk | dual) ;; *) usage ;; esac

# Env knobs (override by exporting before invoking this script).
: "${RATE:=200}"
# the host-loss phases run at the plan's per-node rate (1 000 req/s); the steady and chaos
# phases keep RATE, so the fleet dedupe store is not starved on a small runner
: "${HOSTLOSS_RATE:=1000}"
: "${DURATION:=45}"
: "${BURST_SECONDS:=10}"
: "${CONCURRENCY:=32}"
: "${KEEP:=0}"
: "${BUILD_ANKUSA:=1}"
: "${ANKUSA_IMAGE:=ankusa-fleet-e2e:local}"
: "${OUT_DIR:=examples/fleet-e2e/.e2e-out}"
export ANKUSA_IMAGE
export WAL
if [ "$KILL_NODE" = 1 ]; then export BACKUP=true; else export BACKUP=false; fi

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
PROJECT=ankusa-fleet-e2e

# `compose` sees the services of this run's mode; `compose_all` every profile (to tear down).
COMPOSE_FILE=examples/fleet-e2e/docker-compose.yml
compose() {
  if [ "$WAL" = dual ]; then
    docker compose -f "$COMPOSE_FILE" --profile dual "$@"
  else
    docker compose -f "$COMPOSE_FILE" "$@"
  fi
}
compose_all() { docker compose -f "$COMPOSE_FILE" --profile dual "$@"; }
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
node_not_ready() { ! curl -sf --max-time 2 "$1/ready"; }

# The consumer owns its queue: until it has declared it and is consuming,
# the exchange has nothing to route to and every publish is unroutable.
consumer_consuming() {
  compose exec -T rabbitmq rabbitmqctl -q list_queues name consumers | grep -Eq '^ankusa-fleet-consumer[[:space:]]+[1-9]'
}

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
  # a run that dies with RabbitMQ paused or the brokers stopped must not leave them so
  compose unpause rabbitmq >/dev/null 2>&1 || true
  if [ "$rc" -ne 0 ]; then
    echo "==> run failed (exit $rc); service logs follow" >&2
    for s in ankusa-a ankusa-b claim-check consumer nginx; do
      echo "--- $s ---" >&2
      compose logs --no-color --tail 60 "$s" >&2 || true
    done
    if [ "$WAL" = dual ]; then
      for s in kafka-1 kafka-2 kafka-3; do
        echo "--- $s ---" >&2
        compose logs --no-color --tail 20 "$s" >&2 || true
      done
    fi
  fi
  if [ "$KEEP" != "1" ]; then
    echo "==> tearing down the stack"
    compose_all down -v --remove-orphans >/dev/null 2>&1 || true
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
compose_all down -v --remove-orphans >/dev/null 2>&1 || true

if [ "$BUILD_ANKUSA" = "1" ]; then
  echo "==> building $ANKUSA_IMAGE"
  docker build -f packages/ankusa_server/Dockerfile -t "$ANKUSA_IMAGE" .
fi

echo "==> starting the fleet (wal.type: $WAL$([ "$KILL_NODE" = 1 ] && echo ', backup on'))"
compose up -d --build

echo "==> waiting for both nodes and the load balancer"
wait_for 240 "ankusa-a /ready" node_ready "$NODE_A"
wait_for 240 "ankusa-b /ready" node_ready "$NODE_B"
wait_for 180 "the consumer to be consuming" consumer_consuming
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

# verify_phase_lossy NAME TIMEOUT -- the same, for a phase that is expected to lose hooks.
verify_phase_lossy() {
  (cd tools/loadgen && mix loadgen.verify --acked "$OUT_ABS/$1.csv" --database-url "$DB_URL" \
    --timeout "$2" --report "$OUT_ABS/$1-verify.json") || true
}

PHASES=4
if [ "$KILL_NODE" = 1 ]; then
  if [ "$WAL" = dual ]; then PHASES=7; else PHASES=5; fi
fi
PHASE=0
phase() {
  PHASE=$((PHASE + 1))
  echo "==> phase $PHASE/$PHASES: $1"
}

phase "steady ($RATE/s for ${DURATION}s through the load balancer)"
run_loadgen steady --rate "$RATE" --duration "$DURATION" --concurrency 64
verify_phase steady 300
if [ "$FAILED" != "0" ]; then
  echo "error: the steady phase already lost or corrupted hooks; not driving chaos against a broken fleet" >&2
  exit 1
fi

phase "fat hooks from each node, redeemed through the gateway"
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

phase "chaos (kill node a, retry on the other node, kill the consumer)"
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
wait_for 240 "ankusa-a /ready after restart" node_ready "$NODE_A"

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

# The retry is one idempotency key, delivered to two nodes. `dedupe.scope: fleet`
# makes the second node answer with the first node's hook id, so it is one hook:
# one row, one hook id, one delivery. (If it were two hooks the consumer would see
# the event twice, one row with `deliveries` counting both: `duplicate_hooks`
# counts exactly that, and a RabbitMQ redelivery cannot add to it.)
RETRY_EXTRA=0
RETRY_DUP_HOOKS=0
if [ -n "$hook_a" ] && [ -n "$hook_b" ]; then
  want_deliveries=2
  [ "$hook_a" = "$hook_b" ] && want_deliveries=1
  if wait_row 90 "the retried event ($retry_key) with $want_deliveries deliveries" \
    "SELECT count(*) FROM processed_webhooks WHERE idempotency_key = '$retry_key' AND deliveries >= $want_deliveries AND processed_at IS NOT NULL"; then
    RETRY_EXTRA="$(psql_q "SELECT deliveries - 1 FROM processed_webhooks WHERE idempotency_key = '$retry_key'")"
    RETRY_DUP_HOOKS="$(psql_q "SELECT cardinality(hook_ids) - 1 FROM processed_webhooks WHERE idempotency_key = '$retry_key'")"
    echo "    $retry_key: one row, $((RETRY_EXTRA + 1)) delivery(ies), $((RETRY_DUP_HOOKS + 1)) hook(s) (hook ids $hook_b, $hook_a)"
  else
    FAILED=1
  fi
else
  FAILED=1
fi

# ── --kill-node: the host is lost, not the process ────────────────────────────

# The object store, through the bootstrap image's aws cli.
s3() { compose run --rm --no-deps -T --entrypoint aws minio-bootstrap "$@"; }

# A backup of ankusa-a exists in the primary bucket and in the secondary.
backups_present() {
  s3 s3 ls s3://ankusa-fleet/node-a/backup/LATEST &&
    s3 s3 ls s3://ankusa-fleet-dr/node-a/backup/LATEST
}

# The host of ankusa-a is gone: the container and the volume with its store.
lose_host() {
  compose kill ankusa-a
  compose rm -sf ankusa-a >/dev/null
  docker volume rm -f "${PROJECT}_ankusa-a-data" >/dev/null
}

kafka_up() {
  local n
  for n in 1 2 3; do
    compose exec -T "kafka-$n" /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 || return 1
  done
}

# accepting URL -- the node answers 201 to a fresh event.
accepting() {
  [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 -XPOST "$1/webhooks/demo" \
    -H 'content-type: application/json' -d "{\"id\":\"after-$(rand_id)\"}")" = "201" ]
}

# probe URL -> "<status>|<body>|<retry-after header>" for a fresh event.
probe() {
  local body headers code
  body="$(mktemp)"
  headers="$(mktemp)"
  code="$(curl -s -o "$body" -D "$headers" -w '%{http_code}' --max-time 20 -XPOST "$1/webhooks/demo" \
    -H 'content-type: application/json' -d "{\"id\":\"probe-$(rand_id)\"}" || true)"
  echo "$code|$(tr -d '\n' <"$body")|$(grep -i '^retry-after:' "$headers" | tr -d '\r\n ' || true)"
  rm -f "$body" "$headers"
}

HOSTLOSS_MISSING=0
OUTAGE_ACKED=0
OUTAGE_REFUSED=0
SECONDARY_APPLIED=0
if [ "$KILL_NODE" = 1 ]; then
  phase "host loss: ankusa-a and its volume are lost under load ($HOSTLOSS_RATE/s, RabbitMQ paused so acked hooks are undelivered)"
  wait_for 240 "a backup of ankusa-a in the primary and the secondary bucket" backups_present
  run_loadgen hostloss --rate "$HOSTLOSS_RATE" --duration "$DURATION" --concurrency 64 &
  LOADGEN_PID=$!
  sleep 5
  echo "    +5s   pausing RabbitMQ: hooks acked from now on wait on their node"
  compose pause rabbitmq
  sleep 7
  echo "    +12s  host loss: ankusa-a is killed and its volume deleted"
  lose_host
  sleep 2
  echo "    +14s  a replacement starts with the node id ankusa-a on an empty volume"
  compose up -d --no-deps ankusa-a
  sleep 6
  echo "    +20s  resuming RabbitMQ"
  compose unpause rabbitmq
  wait "$LOADGEN_PID" || FAILED=1
  LOADGEN_PID=""
  wait_for 300 "the replacement ankusa-a to be ready (restore, then replay)" node_ready "$NODE_A"

  if [ "$WAL" = dual ]; then
    verify_phase hostloss 300
  else
    # `disk` promises nothing past the last backup: the hooks acked since it were
    # on the lost volume, undelivered. The drill proves the difference by seeing them go.
    verify_phase_lossy hostloss 90
    HOSTLOSS_MISSING=$(jq '.acked - .processed' "$OUT_ABS/hostloss-verify.json")
    echo "    wal.type: disk lost $HOSTLOSS_MISSING acked hook(s) with the host"
    if [ "$HOSTLOSS_MISSING" -le 0 ]; then
      echo "error: wal.type: disk lost nothing with the host: the drill did not make hooks undelivered at the kill, so it proves nothing" >&2
      FAILED=1
    fi
  fi
fi

if [ "$KILL_NODE" = 1 ] && [ "$WAL" = dual ]; then
  phase "the intake brokers stop: every node refuses, nothing is acked"
  compose stop kafka-1 kafka-2 kafka-3 >/dev/null
  RETRY_AFTER_SEEN=false
  for _ in $(seq 1 8); do
    for url in "$NODE_A" "$NODE_B"; do
      result="$(probe "$url")"
      status="${result%%|*}"
      rest="${result#*|}"
      body="${rest%%|*}"
      retry_after="${rest#*|}"
      if [ "$status" = "201" ]; then
        OUTAGE_ACKED=$((OUTAGE_ACKED + 1))
      elif [ "$status" = "503" ] && printf '%s' "$body" | grep -q 'intake_unavailable'; then
        OUTAGE_REFUSED=$((OUTAGE_REFUSED + 1))
        [ -n "$retry_after" ] && RETRY_AFTER_SEEN=true
      else
        echo "error: during the outage a node answered $status $body, want 503 intake_unavailable" >&2
        FAILED=1
      fi
    done
  done
  echo "    16 requests: $OUTAGE_REFUSED refused with 503 intake_unavailable, $OUTAGE_ACKED acked (Retry-After sent: $RETRY_AFTER_SEEN)"
  if [ "$OUTAGE_ACKED" != 0 ] || [ "$OUTAGE_REFUSED" != 16 ] || [ "$RETRY_AFTER_SEEN" != true ]; then
    echo "error: with the intake log down a node acked or did not refuse with Retry-After" >&2
    FAILED=1
  fi
  wait_for 60 "ankusa-a to report not ready while the log is down" node_not_ready "$NODE_A"
  wait_for 60 "ankusa-b to report not ready while the log is down" node_not_ready "$NODE_B"

  compose start kafka-1 kafka-2 kafka-3 >/dev/null
  wait_for 240 "the three intake brokers" kafka_up
  wait_for 240 "ankusa-a to be ready again" node_ready "$NODE_A"
  wait_for 240 "ankusa-b to be ready again" node_ready "$NODE_B"
  wait_for 240 "ankusa-a to ack again" accepting "$NODE_A"
  wait_for 240 "ankusa-b to ack again" accepting "$NODE_B"
  echo "    both nodes ack again"

  phase "host loss with only the secondary backup: the primary is deleted, \`recover\` replays the log"
  run_loadgen secondary --rate "$HOSTLOSS_RATE" --duration "$DURATION" --concurrency 64 &
  LOADGEN_PID=$!
  sleep 20
  echo "    +20s  pausing RabbitMQ"
  compose pause rabbitmq
  sleep 6
  echo "    +26s  the primary backup is deleted, then the host is lost"
  s3 s3 rm s3://ankusa-fleet/node-a/backup/ --recursive >/dev/null
  lose_host
  compose unpause rabbitmq
  echo "    the replacement's store is restored from the secondary and brought forward from the log by \`recover\`"
  recover_out="$(compose run --rm --no-deps -T ankusa-a recover 2>&1)" || {
    echo "$recover_out" >&2
    echo "error: recover failed" >&2
    FAILED=1
  }
  echo "    recover: $(printf '%s\n' "$recover_out" | tail -1)"
  SECONDARY_APPLIED="$(printf '%s\n' "$recover_out" | tail -1 | sed -n 's/.* applied=\([0-9]*\).*/\1/p')"
  if [ -z "$SECONDARY_APPLIED" ] || [ "$SECONDARY_APPLIED" -le 0 ]; then
    echo "error: recover applied no hook from the log: the drill did not lose anything the log had to restore" >&2
    FAILED=1
    SECONDARY_APPLIED=${SECONDARY_APPLIED:-0}
  fi
  compose up -d --no-deps ankusa-a
  wait "$LOADGEN_PID" || FAILED=1
  LOADGEN_PID=""
  wait_for 300 "the replacement ankusa-a to be ready" node_ready "$NODE_A"
  verify_phase secondary 300
fi

phase "burst ($CONCURRENCY workers for ${BURST_SECONDS}s)"
run_loadgen burst --concurrency "$CONCURRENCY" --duration "$BURST_SECONDS"
verify_phase burst 900

# Sum the per-phase verify reports. `missing` in a report is a sample of up to
# ten ids, so the count is acked - processed.
#
# `extra_deliveries` is consumer receipts beyond the first per idempotency key:
# duplicate hooks (what fleet dedupe removes) plus the hooks RabbitMQ redelivers
# after the consumer kill (which ingest cannot remove) plus, after a host loss,
# the hooks the lost node had delivered that its replacement redelivers from the
# backup and the log (same hook id, same idempotency key). `duplicate_hooks` is
# the first component alone, distinct hook ids per key minus one, and must be 0.
MISSING=0
SHA_MISMATCHES=0
EXTRA=$RETRY_EXTRA
DUPLICATE_HOOKS=$RETRY_DUP_HOOKS
PHASE_LIST="steady chaos burst"
if [ "$KILL_NODE" = 1 ] && [ "$WAL" = dual ]; then PHASE_LIST="$PHASE_LIST hostloss secondary"; fi
if [ "$KILL_NODE" = 1 ] && [ "$WAL" = disk ]; then PHASE_LIST="$PHASE_LIST hostloss"; fi
for phase_name in $PHASE_LIST; do
  report="$OUT_ABS/$phase_name-verify.json"
  if [ ! -f "$report" ]; then
    echo "error: no verify report for phase $phase_name" >&2
    FAILED=1
    continue
  fi
  phase_missing=$(jq '.acked - .processed' "$report")
  # the loss `disk` promises is reported on its own line, not as a failure
  if [ "$phase_name" = hostloss ] && [ "$WAL" = disk ]; then phase_missing=0; fi
  MISSING=$((MISSING + phase_missing))
  SHA_MISMATCHES=$((SHA_MISMATCHES + $(jq '.sha_mismatches' "$report")))
  EXTRA=$((EXTRA + $(jq '.extra_deliveries' "$report")))
  dup="$(jq '.duplicate_hooks // empty' "$report")"
  if [ -z "$dup" ]; then
    echo "error: $phase_name-verify.json has no duplicate_hooks (the consumer's table has no hook_ids)" >&2
    FAILED=1
    dup=0
  fi
  DUPLICATE_HOOKS=$((DUPLICATE_HOOKS + dup))
done

echo
echo "verify: wal=$WAL missing=$MISSING sha_mismatches=$SHA_MISMATCHES extra_deliveries=$EXTRA claim_redeemed_cross_node=$CLAIM_OK duplicate_hooks=$DUPLICATE_HOOKS"
if [ "$KILL_NODE" = 1 ]; then
  echo "host loss: wal=$WAL lost_with_the_host=$HOSTLOSS_MISSING outage_acked=$OUTAGE_ACKED outage_refused=$OUTAGE_REFUSED secondary_recovered=$SECONDARY_APPLIED"
fi

if [ "$MISSING" != 0 ] || [ "$SHA_MISMATCHES" != 0 ]; then FAILED=1; fi
if [ "$DUPLICATE_HOOKS" != 0 ]; then
  echo "error: $DUPLICATE_HOOKS provider retr(ies) were stored as a second hook: dedupe.scope: fleet did not collapse them" >&2
  FAILED=1
fi
if [ "$FAILED" != "0" ]; then
  echo "==> the fleet lost or corrupted a hook, or an assertion failed; see $OUT_DIR/*-verify.json" >&2
  exit 1
fi
if [ "$KILL_NODE" = 1 ] && [ "$WAL" = disk ]; then
  echo "==> wal.type: disk lost $HOSTLOSS_MISSING acked hooks with the host, as it promises; every other phase kept every hook; reports in $OUT_DIR"
else
  echo "==> every acked hook was processed; reports in $OUT_DIR"
fi
