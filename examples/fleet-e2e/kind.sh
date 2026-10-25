#!/usr/bin/env bash
# The fleet end to end, run from the Helm chart on kind (OPS-8): the fleet of
# docker-compose.yml as two StatefulSet pods of `deploy/helm/ankusa` (a PVC each,
# read-only root file system, restricted Pod Security Standard enforced), a
# claim-check gateway, Redis, MinIO, RabbitMQ, a consumer and Postgres. Drives
# tools/loadgen through a steady phase, fat hooks redeemed at the gateway, a chaos
# phase (one pod deleted gracefully, the other SIGKILLed, the consumer deleted) and a
# burst, and fails on any acked hook the consumer never recorded.
#
#   examples/fleet-e2e/kind.sh          # also `mise run e2e fleet`
#
# Needs docker, kind, kubectl, helm, mix, curl, jq. See README.md.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

# Env knobs (override by exporting before invoking this script).
: "${CLUSTER:=ankusa-fleet-e2e}"
: "${RATE:=200}"
: "${DURATION:=45}"
: "${BURST_SECONDS:=10}"
: "${CONCURRENCY:=32}"
: "${KEEP:=0}"
: "${BUILD_ANKUSA:=1}"
: "${ANKUSA_IMAGE:=ankusa-fleet-e2e:kind}"
: "${CONSUMER_IMAGE:=ankusa-fleet-e2e-consumer:kind}"
: "${OUT_DIR:=examples/fleet-e2e/.e2e-out-kind}"

for tool in docker kind kubectl helm mix curl jq; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "error: required tool '$tool' not found on PATH" >&2
    exit 2
  fi
done
if [ "$DURATION" -lt 40 ]; then
  echo "error: DURATION must be at least 40 (the chaos timeline runs to +36s)" >&2
  exit 2
fi

NS=ankusa-fleet
INFRA_NS=ankusa-fleet-infra
LB=http://127.0.0.1:18180
DB_URL=postgres://ankusa:ankusa@127.0.0.1:15434/consumer
CHART=deploy/helm/ankusa
KIND_DIR=examples/fleet-e2e/kind

k() { kubectl --context "kind-$CLUSTER" "$@"; }
psql_q() { k -n "$INFRA_NS" exec deploy/postgres -- psql -U ankusa -d consumer -Atc "$1"; }
rand_id() { od -An -N8 -tx1 /dev/urandom | tr -d ' \n'; }
sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

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

# Every pod of the StatefulSet is Ready and none is terminating.
fleet_ready() {
  k -n "$NS" rollout status statefulset/ankusa --timeout=1s
}

FAILED=0
LOADGEN_PID=""

cleanup() {
  rc=$?
  if [ -n "$LOADGEN_PID" ] && kill -0 "$LOADGEN_PID" 2>/dev/null; then kill "$LOADGEN_PID" 2>/dev/null || true; fi
  if [ "$rc" -ne 0 ] && kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
    echo "==> run failed (exit $rc); cluster state follows" >&2
    k get pods -A -o wide >&2 || true
    for p in $(k -n "$NS" get pods -o name 2>/dev/null); do
      echo "--- $p ---" >&2
      k -n "$NS" logs "$p" --tail 60 >&2 || true
    done
    echo "--- consumer ---" >&2
    k -n "$INFRA_NS" logs deploy/consumer --tail 60 >&2 || true
  fi
  if [ "$KEEP" != "1" ]; then
    echo "==> deleting kind cluster '$CLUSTER'"
    kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  else
    echo "==> KEEP=1 set, leaving cluster '$CLUSTER' running (load balancer $LB)"
  fi
  exit "$rc"
}
trap cleanup EXIT

mkdir -p "$OUT_DIR"
OUT_ABS="$(pwd)/$OUT_DIR"
rm -f "$OUT_ABS"/*.csv "$OUT_ABS"/*.json

echo "==> kind cluster '$CLUSTER'"
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "    cluster '$CLUSTER' already exists, skipping creation"
else
  kind create cluster --name "$CLUSTER" --config "$KIND_DIR/kind.yaml"
fi

if [ "$BUILD_ANKUSA" = "1" ]; then
  echo "==> building $ANKUSA_IMAGE"
  docker build -f packages/ankusa_server/Dockerfile -t "$ANKUSA_IMAGE" .
fi
echo "==> building $CONSUMER_IMAGE"
docker build -f examples/fleet-e2e/consumer/Dockerfile -t "$CONSUMER_IMAGE" .
echo "==> loading the images into kind"
kind load docker-image "$ANKUSA_IMAGE" --name "$CLUSTER"
kind load docker-image "$CONSUMER_IMAGE" --name "$CLUSTER"

echo "==> the dependencies (Redis, MinIO, RabbitMQ, Postgres) and the consumer"
k create namespace "$INFRA_NS" --dry-run=client -o yaml | k apply -f -
k -n "$INFRA_NS" create configmap postgres-schema --from-file=schema.sql=examples/fleet-e2e/consumer/schema.sql \
  --dry-run=client -o yaml | k apply -f -
k apply -n "$INFRA_NS" -f "$KIND_DIR/infra.yaml"
for d in redis minio rabbitmq postgres; do
  k -n "$INFRA_NS" rollout status "deployment/$d" --timeout=240s
done
k -n "$INFRA_NS" wait --for=condition=complete job/minio-bootstrap --timeout=240s
# The consumer owns its queue: until it is consuming, the exchange has nothing to
# route to and every publish is unroutable. It goes up before the nodes do.
k -n "$INFRA_NS" rollout status deployment/consumer --timeout=240s

echo "==> the chart, in a namespace that enforces the restricted Pod Security Standard"
k create namespace "$NS" --dry-run=client -o yaml | k apply -f -
k label namespace "$NS" pod-security.kubernetes.io/enforce=restricted pod-security.kubernetes.io/enforce-version=latest --overwrite
helm upgrade --install ankusa "$CHART" --kube-context "kind-$CLUSTER" -n "$NS" \
  -f "$KIND_DIR/values.yaml" --set "image.tag=${ANKUSA_IMAGE##*:}" --set "image.repository=${ANKUSA_IMAGE%:*}" \
  --wait --timeout 5m

echo "==> the pods are hardened"
for pod in ankusa-0 ankusa-1; do
  uid="$(k -n "$NS" exec "$pod" -c ankusa -- id -u)"
  if k -n "$NS" exec "$pod" -c ankusa -- sh -c 'touch /rootfs-write-test' 2>/dev/null; then ro=writable; else ro=read-only; fi
  grace="$(k -n "$NS" get pod "$pod" -o jsonpath='{.spec.terminationGracePeriodSeconds}')"
  echo "    $pod: uid=$uid root file system $ro terminationGracePeriodSeconds=$grace"
  if [ "$uid" != "100" ] || [ "$ro" != "read-only" ]; then
    echo "error: $pod does not run as uid 100 on a read-only root file system" >&2
    FAILED=1
  fi
done
[ "$FAILED" = "0" ] || exit 1

echo "==> routes are shared: a path nobody defined is a 404 through the Service"
wait_for 60 "a hook through the Service" \
  curl -sf --max-time 5 -o /dev/null -XPOST "$LB/webhooks/demo" -d '{"id":"warmup"}'
for _ in 1 2 3 4; do
  code="$(curl -s -o /dev/null -w '%{http_code}' -XPOST "$LB/webhooks/nope" -d '{"id":"x"}')"
  [ "$code" = "404" ] || { echo "error: POST $LB/webhooks/nope returned $code, want 404" >&2; exit 1; }
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

echo "==> phase 1/4: steady ($RATE/s for ${DURATION}s)"
run_loadgen steady --rate "$RATE" --duration "$DURATION" --concurrency 64
verify_phase steady 300
if [ "$FAILED" != "0" ]; then
  echo "error: the steady phase already lost or corrupted hooks; not driving chaos against a broken fleet" >&2
  exit 1
fi

echo "==> phase 2/4: fat hooks, redeemed through the gateway"
CLAIM_OK=true
for i in 1 2 3 4 5 6; do
  fat_id="fat-$i-$(rand_id)"
  fat_file="$OUT_ABS/fat-$i.json"
  jq -nc --arg id "$fat_id" --arg pad "$(head -c 20000 /dev/zero | tr '\0' x)" '{id: $id, pad: $pad}' >"$fat_file"
  body="$(mktemp)"
  code="$(curl -s -o "$body" -w '%{http_code}' --max-time 15 -XPOST "$LB/webhooks/demo" \
    -H 'content-type: application/json' --data-binary "@$fat_file" || true)"
  hook_id="$(jq -r '.id // empty' "$body" 2>/dev/null || true)"
  rm -f "$body"
  if [ "$code" != "201" ] || [ -z "$hook_id" ]; then
    echo "error: fat hook $i returned $code" >&2
    CLAIM_OK=false
    continue
  fi
  want_sha="$(sha256 "$fat_file")"
  if wait_row 90 "fat hook $hook_id in processed_webhooks" \
    "SELECT count(*) FROM processed_webhooks WHERE ankusa_id = '$hook_id' AND via LIKE 'claim:%' AND body_sha256 = '$want_sha' AND processed_at IS NOT NULL"; then
    echo "    $hook_id redeemed via the gateway, sha256 matches"
  else
    echo "error: fat hook $hook_id never arrived via a claim with the right sha256" >&2
    CLAIM_OK=false
  fi
done
[ "$CLAIM_OK" = "true" ] || FAILED=1

echo "==> phase 3/4: chaos (delete a pod, SIGKILL the other, delete the consumer)"
run_loadgen chaos --rate "$RATE" --duration "$DURATION" --concurrency 64 &
LOADGEN_PID=$!
sleep 8
echo "    +8s   deleting ankusa-0: SIGTERM, the drain runs (/ready 503, the listener closes after its requests)"
k -n "$NS" delete pod ankusa-0 --wait=false
sleep 20
echo "    +28s  SIGKILL of ankusa-1 (grace period 0): the store recovers from its write-ahead log on the PVC"
k -n "$NS" delete pod ankusa-1 --grace-period=0 --force --wait=false
sleep 8
echo "    +36s  deleting the consumer"
k -n "$INFRA_NS" delete pod -l app=consumer --wait=false
wait "$LOADGEN_PID" || FAILED=1
LOADGEN_PID=""
wait_for 240 "the fleet to be ready again" fleet_ready
k -n "$INFRA_NS" rollout status deployment/consumer --timeout=240s
verify_phase chaos 300

echo "==> phase 4/4: burst ($CONCURRENCY workers for ${BURST_SECONDS}s)"
run_loadgen burst --concurrency "$CONCURRENCY" --duration "$BURST_SECONDS"
verify_phase burst 900

# Sum the per-phase verify reports (see run.sh): `missing` in a report is a sample
# of up to ten ids, so the count is acked - processed. `duplicate_hooks` must be 0:
# a provider retry that reached the other node collapses at ingest.
MISSING=0
SHA_MISMATCHES=0
EXTRA=0
DUPLICATE_HOOKS=0
for phase_name in steady chaos burst; do
  report="$OUT_ABS/$phase_name-verify.json"
  if [ ! -f "$report" ]; then
    echo "error: no verify report for phase $phase_name" >&2
    FAILED=1
    continue
  fi
  MISSING=$((MISSING + $(jq '.acked - .processed' "$report")))
  SHA_MISMATCHES=$((SHA_MISMATCHES + $(jq '.sha_mismatches' "$report")))
  EXTRA=$((EXTRA + $(jq '.extra_deliveries' "$report")))
  DUPLICATE_HOOKS=$((DUPLICATE_HOOKS + $(jq '.duplicate_hooks // 0' "$report")))
done

echo
echo "verify: missing=$MISSING sha_mismatches=$SHA_MISMATCHES extra_deliveries=$EXTRA claim_redeemed=$CLAIM_OK duplicate_hooks=$DUPLICATE_HOOKS"

if [ "$MISSING" != 0 ] || [ "$SHA_MISMATCHES" != 0 ] || [ "$DUPLICATE_HOOKS" != 0 ]; then FAILED=1; fi
if [ "$FAILED" != "0" ]; then
  echo "==> the fleet lost or corrupted a hook, or an assertion failed; see $OUT_DIR/*-verify.json" >&2
  exit 1
fi
echo "==> every acked hook was processed; reports in $OUT_DIR"
