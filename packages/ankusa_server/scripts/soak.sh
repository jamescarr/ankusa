#!/usr/bin/env bash
#
# The soak gate: run the built image at a steady rate for DURATION seconds with
# tools/loadgen, sample /metrics and the container's resident memory every
# SAMPLE_INTERVAL seconds, let the queue drain, and fail when an accepted hook
# never reached the sink (`missing > 0`) or the container's RSS grew by more
# than MAX_RSS_GROWTH (5 %) between the early and the late part of the run.
#
#   packages/ankusa_server/scripts/soak.sh jamescarr/ankusa:dev
#
# Used by `mise run bench:soak` and .github/workflows/soak.yml. Needs docker,
# curl, awk, and the Elixir toolchain from .mise.toml (for tools/loadgen).
#
# The image runs its baked demo config: one `demo` source, `verify: none`, one
# `log` sink. A hook is delivered when `ankusa_dispatch_deliveries_total{result="ok"}`
# counts it, so on a fresh container `missing = accepted - delivered`.
#
# Env knobs:
#
#   DURATION=3600         seconds of load
#   RATE=200              requests per second (open loop)
#   CONCURRENCY=64        loadgen workers
#   BODY_BYTES=1024       pad size of each body
#   SAMPLE_INTERVAL=60    seconds between /metrics + RSS samples
#   DRAIN_TIMEOUT=600     seconds to wait for the queue to empty after the load
#   MAX_RSS_GROWTH=0.05   allowed RSS growth, early window to late window
#   SOAK_OUT=<dir>        where samples, metrics scrapes and soak.json go
#                         (default: <repo>/soak-out)
#   SOAK_HTTP_PORT=4000, SOAK_ADMIN_PORT=4002   host ports
#
# RSS growth compares the lowest sample in the last sixth of the run with the
# lowest in the second sixth (the first sixth is warm-up: the BEAM's allocators
# and the store's memtables are still growing to their working size). The
# floor, not a median or mean: the container's RSS is a sawtooth (it climbs
# ~10 % and drops back every few minutes), and a window floor is the live set
# the sawtooth returns to, which is what a leak raises.
set -euo pipefail

IMAGE="${1:?usage: soak.sh IMAGE}"
NAME=ankusa-soak
DURATION="${DURATION:-3600}"
RATE="${RATE:-200}"
CONCURRENCY="${CONCURRENCY:-64}"
BODY_BYTES="${BODY_BYTES:-1024}"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-60}"
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-600}"
MAX_RSS_GROWTH="${MAX_RSS_GROWTH:-0.05}"
HTTP_PORT="${SOAK_HTTP_PORT:-4000}"
ADMIN_PORT="${SOAK_ADMIN_PORT:-4002}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
OUT="${SOAK_OUT:-$ROOT/soak-out}"
ADMIN_URL=http://127.0.0.1:$ADMIN_PORT

fail() { echo "FAIL: $*" >&2; exit 1; }

rm -rf "$OUT"
mkdir -p "$OUT"
LOADGEN_PID=""

cleanup() {
  rc=$?
  if [ -n "$LOADGEN_PID" ]; then kill "$LOADGEN_PID" >/dev/null 2>&1 || true; fi
  if [ "$rc" -ne 0 ]; then
    echo "--- container logs ($NAME, last 100 lines) ---" >&2
    docker logs --tail 100 "$NAME" >&2 || true
  fi
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  exit "$rc"
}
trap cleanup EXIT

# Sum of /metrics lines `<name>{...<label filter>...} <value>`; 0 when none.
metric_sum() { # FILE NAME [LABEL_SUBSTRING]
  awk -v name="$2" -v want="${3:-}" '
    index($0, name "{") == 1 && (want == "" || index($0, want) > 0) { s += $NF }
    END { printf "%d\n", s }
  ' "$1"
}

# Total RSS (kB) of every process in the container: the BEAM and tini.
container_rss_kb() {
  docker exec "$NAME" sh -c "awk '/^VmRSS:/ { s += \$2 } END { print s }' /proc/[0-9]*/status"
}

# Smallest of the integers, one per line, on stdin.
floor() {
  awk 'NR == 1 || $1 < m { m = $1 } END { print m }'
}

docker rm -f "$NAME" >/dev/null 2>&1 || true

echo "==> starting $IMAGE"
docker run -d --name "$NAME" \
  --log-opt max-size=10m --log-opt max-file=2 \
  -p "127.0.0.1:$HTTP_PORT:4000" -p "127.0.0.1:$ADMIN_PORT:4002" \
  -e ANKUSA_ADMIN_IP=0.0.0.0 "$IMAGE" >/dev/null

echo "==> waiting for /health"
ready=false
for _ in $(seq 1 30); do
  if [ "$(curl -s -o /dev/null -w '%{http_code}' "$ADMIN_URL/health")" = "200" ]; then
    ready=true
    break
  fi
  [ "$(docker inspect -f '{{.State.Running}}' "$NAME")" = "true" ] || fail "the container exited during startup"
  sleep 1
done
[ "$ready" = "true" ] || fail "/health never returned 200 within 30s"

echo "==> loadgen: $RATE req/s, $CONCURRENCY workers, ${BODY_BYTES}B bodies, ${DURATION}s"
(cd "$ROOT/tools/loadgen" && mix deps.get --check-locked >/dev/null)
(
  cd "$ROOT/tools/loadgen"
  exec mix loadgen.run --url "http://127.0.0.1:$HTTP_PORT/webhooks/demo" \
    --concurrency "$CONCURRENCY" --duration "$DURATION" --rate "$RATE" --body-bytes "$BODY_BYTES" \
    --out "$OUT/acked.csv" --report "$OUT/loadgen-report.json"
) &
LOADGEN_PID=$!

echo "elapsed_s,rss_kb" >"$OUT/samples.csv"
start=$SECONDS
next=$((SECONDS + SAMPLE_INTERVAL))
n=0
while kill -0 "$LOADGEN_PID" 2>/dev/null; do
  sleep 1
  if [ "$SECONDS" -ge "$next" ]; then
    n=$((n + 1))
    next=$((next + SAMPLE_INTERVAL))
    curl -s "$ADMIN_URL/metrics" >"$OUT/metrics-$n.prom"
    rss=$(container_rss_kb)
    echo "$((SECONDS - start)),$rss" >>"$OUT/samples.csv"
    echo "    sample $n at $((SECONDS - start))s: rss ${rss} kB, delivered $(metric_sum "$OUT/metrics-$n.prom" ankusa_dispatch_deliveries_total 'result="ok"')"
  fi
done
loadgen_rc=0
wait "$LOADGEN_PID" || loadgen_rc=$?
LOADGEN_PID=""
[ "$loadgen_rc" -eq 0 ] || fail "loadgen exited $loadgen_rc"

field() { sed -n "s/.*\"$1\":\([0-9]*\).*/\1/p" "$OUT/loadgen-report.json"; }
accepted=$(field accepted)
errors=$(field errors)
shed=$(field shed)

echo "==> draining (accepted $accepted)"
drain_deadline=$((SECONDS + DRAIN_TIMEOUT))
while :; do
  curl -s "$ADMIN_URL/metrics" >"$OUT/metrics-final.prom"
  backlog=$(($(metric_sum "$OUT/metrics-final.prom" ankusa_queue_pending) + $(metric_sum "$OUT/metrics-final.prom" ankusa_queue_scheduled) + $(metric_sum "$OUT/metrics-final.prom" ankusa_queue_inflight)))
  delivered=$(metric_sum "$OUT/metrics-final.prom" ankusa_dispatch_deliveries_total 'result="ok"')
  if [ "$backlog" -eq 0 ] && [ "$delivered" -ge "$accepted" ]; then break; fi
  [ "$SECONDS" -lt "$drain_deadline" ] || break
  sleep 2
done

missing=$((accepted - delivered))
[ "$missing" -ge 0 ] || missing=0
dead=$(metric_sum "$OUT/metrics-final.prom" ankusa_queue_dead)

samples=$(($(wc -l <"$OUT/samples.csv") - 1))
[ "$samples" -ge 3 ] || fail "only $samples RSS samples (need 3+): raise DURATION or lower SAMPLE_INTERVAL"
w=$((samples / 6))
[ "$w" -ge 1 ] || w=1
# samples.csv has a header, so sample k is line k + 1.
early=$(sed -n "$((w + 2)),$((2 * w + 1))p" "$OUT/samples.csv" | cut -d, -f2 | floor)
late=$(sed -n "$((samples - w + 2)),$((samples + 1))p" "$OUT/samples.csv" | cut -d, -f2 | floor)
growth=$(awk -v e="$early" -v l="$late" 'BEGIN { printf "%.4f", (e > 0 ? l / e - 1 : 0) }')

cat >"$OUT/soak.json" <<EOF
{"image":"$IMAGE","duration_s":$DURATION,"rate":$RATE,"accepted":$accepted,"shed":$shed,"errors":$errors,"delivered":$delivered,"missing":$missing,"dead":$dead,"samples":$samples,"rss_early_kb":$early,"rss_late_kb":$late,"rss_growth":$growth}
EOF

echo
echo "soak summary"
echo "------------"
echo "accepted     $accepted (shed $shed, errors $errors)"
echo "delivered    $delivered"
echo "missing      $missing"
echo "dead         $dead"
echo "rss early    $early kB (lowest of samples $((w + 1))-$((2 * w)))"
echo "rss late     $late kB (lowest of samples $((samples - w + 1))-$samples)"
echo "rss growth   $growth (limit $MAX_RSS_GROWTH)"

[ "$missing" -eq 0 ] || fail "missing $missing: accepted hooks that no sink delivered"
awk -v g="$growth" -v m="$MAX_RSS_GROWTH" 'BEGIN { exit !(g > m) }' && fail "rss grew $growth > $MAX_RSS_GROWTH"

echo "soak OK: $IMAGE"
