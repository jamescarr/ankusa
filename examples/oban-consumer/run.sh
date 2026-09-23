#!/usr/bin/env bash
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

# Env knobs (override by exporting before invoking this script).
# RATE default is half the measured single-instance dispatch drain rate
# (~125 envelopes/s on the reference machine, see docs/testing.md) —
# Ankusa.Dispatch.Pipeline delivers one envelope at a time (see
# docs/deployment.md#dispatch-throughput), so a steady rate above that
# ceiling builds a backlog the verify timeout won't wait out.
: "${CLUSTER:=ankusa-e2e}"
: "${RATE:=60}"
: "${DURATION:=60}"
: "${BURST_SECONDS:=15}"
: "${CONCURRENCY:=64}"
: "${KEEP:=0}"
: "${OUT_DIR:=examples/oban-consumer/.e2e-out}"

for tool in kind kubectl docker mix; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "error: required tool '$tool' not found on PATH" >&2
    exit 2
  fi
done

FAILED=0

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

echo "==> running migrations"
kubectl apply -f examples/oban-consumer/k8s/10-migrate.yaml
kubectl -n ankusa-e2e wait --for=condition=complete job/ankusa-migrate job/consumer-migrate --timeout=180s

echo "==> applying consumer + ankusa fleet"
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
