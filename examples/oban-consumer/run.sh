#!/usr/bin/env bash
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

# Env knobs (override by exporting before invoking this script).
# RATE defaults to 300/s. The old single-envelope dispatch ceiling (~125/s on
# the reference machine) is gone: Ankusa.Dispatch.Pipeline delivers
# concurrently (dispatch.concurrency, 32 by default), so the steady phase is
# bounded by the consumer fleet, not by sink latency. Measured numbers on the
# reference machine are recorded in docs/testing.md.
: "${CLUSTER:=ankusa-e2e}"
: "${RATE:=300}"
: "${DURATION:=60}"
: "${BURST_SECONDS:=15}"
: "${CONCURRENCY:=64}"
: "${KEEP:=0}"
: "${OUT_DIR:=examples/oban-consumer/.e2e-out}"
# Which shared WAL the fleet runs on. `ra` is the recommended fleet WAL: a
# majority-replicated Raft cluster of its own. `postgres` is still supported
# and still exercised, because a WAL adapter nobody runs is a WAL adapter that
# rots.
: "${WAL:=ra}"

ANKUSA_WAL_MEMBERS="ankusa@ankusa-wal-0.ankusa-wal,ankusa@ankusa-wal-1.ankusa-wal,ankusa@ankusa-wal-2.ankusa-wal"

for tool in kind kubectl docker mix; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "error: required tool '$tool' not found on PATH" >&2
    exit 2
  fi
done

FAILED=0

# Which worker pod currently holds the dispatch lease. Both workers report it
# through the WAL: with Ra the lease lives in replicated state, so any wal pod
# can answer. Falls back to ankusa-worker-0 when the answer cannot be read —
# the lease makes killing the standby safe either way, it just proves less.
active_worker_pod() {
  local holder
  # `rpc` runs one expression on the *running* node and prints only what that
  # expression prints — it does not echo the value. So the expression prints it.
  holder="$(kubectl -n ankusa-e2e exec ankusa-wal-0 -- /app/bin/ingest rpc \
    'IO.inspect(elem(:ra.consistent_aux({:ankusa_wal_default, node()}, :overview, 5000), 1).leases[:dispatch].holder)' \
    2>/dev/null | sed -n 's/.*\(ankusa-worker-[0-9]\).*/\1/p' | head -1 || true)"

  if [ -n "$holder" ]; then
    echo "$holder"
  else
    echo "ankusa-worker-0"
  fi
}

# The wal pod that is currently Raft leader. `:ra.members/2` answers
# `{:ok, members, leader}`; the leader is that third element, and `nil` until one
# is elected. (Taking the last member the list names would be a different pod.)
wal_leader_pod() {
  local node
  node="$(kubectl -n ankusa-e2e exec ankusa-wal-0 -- /app/bin/ingest rpc \
    'case :ra.members({:ankusa_wal_default, node()}) do {:ok, _, {_c, n}} when n != nil -> IO.puts(:erlang.atom_to_list(n)); _ -> IO.puts("none") end' \
    2>/dev/null | tail -1 || true)"

  case "$node" in
    *@ankusa-wal-*) echo "${node##*@}" ;;
    *) echo "" ;;
  esac
}

cleanup() {
  if [ "$KEEP" != "1" ]; then
    echo "==> tearing down kind cluster '$CLUSTER'"
    kind delete cluster --name "$CLUSTER" || true
  else
    echo "==> KEEP=1 set, leaving cluster '$CLUSTER' running"
  fi
}
trap cleanup EXIT

echo "==> ensuring kind cluster '$CLUSTER' exists"
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "    cluster '$CLUSTER' already exists, skipping creation"
else
  kind create cluster --name "$CLUSTER" --config examples/oban-consumer/k8s/kind.yaml
fi

echo "==> building images"
docker build --platform linux/arm64 -t ankusa-oban-ingest:e2e -f examples/oban-consumer/ingest_app/Dockerfile .
docker build --platform linux/arm64 -t ankusa-oban-consumer:e2e examples/oban-consumer/consumer_app

echo "==> loading images into kind"
kind load docker-image ankusa-oban-ingest:e2e --name "$CLUSTER"
kind load docker-image ankusa-oban-consumer:e2e --name "$CLUSTER"

echo "==> applying namespace + postgres"
kubectl apply -f examples/oban-consumer/k8s/00-namespace.yaml -f examples/oban-consumer/k8s/01-postgres.yaml
kubectl -n ankusa-e2e rollout status statefulset/postgres --timeout=180s

if [ "$WAL" = "ra" ]; then
  echo "==> applying Ra WAL cluster (3 members)"
  kubectl apply -f examples/oban-consumer/k8s/01-ankusa-wal.yaml
  kubectl -n ankusa-e2e rollout status statefulset/ankusa-wal --timeout=300s
  echo "    waiting for a Raft leader"
  leader_seen=0
  for _ in $(seq 1 30); do
    if [ "$(kubectl -n ankusa-e2e exec ankusa-wal-0 -- /app/bin/ingest rpc \
         'IO.puts(match?({:ok, _, l} when l != nil, :ra.members({:ankusa_wal_default, node()})))' \
         2>/dev/null | tail -1 || true)" = "true" ]; then
      leader_seen=1
      break
    fi
    sleep 2
  done
  if [ "$leader_seen" != "1" ]; then
    echo "error: the Ra WAL cluster never reported a leader" >&2
    exit 1
  fi
fi

echo "==> running migrations"
kubectl apply -f examples/oban-consumer/k8s/10-migrate.yaml
kubectl -n ankusa-e2e wait --for=condition=complete job/ankusa-migrate job/consumer-migrate --timeout=180s

echo "==> applying consumer + ankusa fleet"
kubectl -n ankusa-e2e create configmap ankusa-wal \
  --from-literal=WAL="$WAL" \
  --from-literal=WAL_RA_MEMBERS="$ANKUSA_WAL_MEMBERS" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f examples/oban-consumer/k8s/20-consumer.yaml -f examples/oban-consumer/k8s/30-ankusa.yaml
kubectl -n ankusa-e2e rollout status deployment/consumer --timeout=180s
kubectl -n ankusa-e2e rollout status deployment/ankusa-edge --timeout=180s
kubectl -n ankusa-e2e rollout status statefulset/ankusa-worker --timeout=180s

echo "==> waiting for edge health check on localhost:8080"
edge_up=0
for _ in $(seq 1 30); do
  if curl -sf localhost:8080/health >/dev/null 2>&1; then
    edge_up=1
    break
  fi
  sleep 2
done
if [ "$edge_up" != "1" ]; then
  echo "error: localhost:8080/health never became healthy after 60s" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"
OUT_ABS="$(pwd)/$OUT_DIR"

echo "==> phase 1/3: steady"
(cd tools/loadgen && mix loadgen.run \
  --url http://localhost:8080/webhooks/demo \
  --rate "$RATE" \
  --duration "$DURATION" \
  --out "$OUT_ABS/steady.csv" \
  --report "$OUT_ABS/steady-report.json")
mix_dir_ok=0
(cd tools/loadgen && mix loadgen.verify \
  --acked "$OUT_ABS/steady.csv" \
  --database-url postgres://ankusa:ankusa@localhost:15432/consumer \
  --timeout 300 \
  --report "$OUT_ABS/steady-verify.json") || FAILED=1

echo "==> phase 2/3: chaos"
(cd tools/loadgen && mix loadgen.run \
  --url http://localhost:8080/webhooks/demo \
  --rate "$RATE" \
  --duration "$DURATION" \
  --out "$OUT_ABS/chaos.csv" \
  --report "$OUT_ABS/chaos-report.json") &
chaos_pid=$!

sleep 10
edge_pod="$(kubectl -n ankusa-e2e get pods -l app=ankusa-edge -o jsonpath='{.items[0].metadata.name}')"
echo "    killing edge pod $edge_pod"
kubectl -n ankusa-e2e delete pod "$edge_pod" --wait=false

sleep 10
echo "    killing worker pod ankusa-worker-0"
kubectl -n ankusa-e2e delete pod ankusa-worker-0 --wait=false

sleep 5
# The active worker is whichever one holds the dispatch lease; the other is a
# hot standby. Killing the *active* one is what proves the lease fails over —
# killing a standby would prove nothing.
worker_pod="$(active_worker_pod)"
echo "    killing active worker pod $worker_pod"
kubectl -n ankusa-e2e delete pod "$worker_pod" --wait=false

sleep 5
if [ "$WAL" = "ra" ]; then
  # Kill the Raft leader itself: a majority remains, so the WAL elects a new
  # leader and every ack made before the kill is still readable after it.
  leader_pod="$(wal_leader_pod)"
  echo "    killing Ra WAL leader pod ${leader_pod:-<unknown>}"
  # `[ -n ... ] && cmd` as a script's last statement aborts it under `set -e`
  # whenever the test is false.
  if [ -n "$leader_pod" ]; then
    kubectl -n ankusa-e2e delete pod "$leader_pod" --wait=false
  fi
fi

sleep 10
consumer_pod="$(kubectl -n ankusa-e2e get pods -l app=consumer -o jsonpath='{.items[0].metadata.name}')"
echo "    killing consumer pod $consumer_pod"
kubectl -n ankusa-e2e delete pod "$consumer_pod" --wait=false

wait "$chaos_pid"

(cd tools/loadgen && mix loadgen.verify \
  --acked "$OUT_ABS/chaos.csv" \
  --database-url postgres://ankusa:ankusa@localhost:15432/consumer \
  --timeout 300 \
  --report "$OUT_ABS/chaos-verify.json") || FAILED=1

echo "==> phase 3/3: burst"
(cd tools/loadgen && mix loadgen.run \
  --url http://localhost:8080/webhooks/demo \
  --concurrency "$CONCURRENCY" \
  --duration "$BURST_SECONDS" \
  --out "$OUT_ABS/burst.csv" \
  --report "$OUT_ABS/burst-report.json")
(cd tools/loadgen && mix loadgen.verify \
  --acked "$OUT_ABS/burst.csv" \
  --database-url postgres://ankusa:ankusa@localhost:15432/consumer \
  --timeout 900 \
  --report "$OUT_ABS/burst-verify.json") || FAILED=1

if [ "$FAILED" != "0" ]; then
  echo "==> one or more verify phases reported loss/mismatch; see $OUT_DIR/*-verify.json" >&2
  exit 1
fi

echo "==> all phases verified with zero loss; reports in $OUT_DIR"
