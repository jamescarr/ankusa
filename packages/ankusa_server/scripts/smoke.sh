#!/usr/bin/env bash
#
# The image gate: start the built image and check what an operator checks in the
# first minute — the demo hook is accepted, metrics and config answer, and a
# broken config exits 78 instead of crash-looping.
#
#   packages/ankusa_server/scripts/smoke.sh jamescarr/ankusa:dev
#
# Used by both `mise run docker:smoke` and .github/workflows/docker.yml, so it
# depends on nothing but docker, curl, and bash.
set -euo pipefail

IMAGE="${1:?usage: smoke.sh IMAGE}"
NAME=ankusa-smoke
# Host ports; override when 4000/4002 are taken on this machine.
HTTP_PORT="${SMOKE_HTTP_PORT:-4000}"
ADMIN_PORT="${SMOKE_ADMIN_PORT:-4002}"
BASE_URL=http://127.0.0.1:$HTTP_PORT
ADMIN_URL=http://127.0.0.1:$ADMIN_PORT

WORK="$(mktemp -d)"

cleanup() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "--- container logs ($NAME) ---" >&2
    docker logs "$NAME" >&2 || true
  fi
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  rm -rf "$WORK"
  exit "$rc"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

# http_code OUT CURL_ARGS... — body to OUT, status code to stdout.
http_code() {
  local out=$1
  shift
  curl -s -o "$out" -w '%{http_code}' "$@"
}

# A leftover container from an interrupted run would make every check below lie.
docker rm -f "$NAME" >/dev/null 2>&1 || true

echo "==> starting $IMAGE"
docker run -d --name "$NAME" -p "127.0.0.1:$HTTP_PORT:4000" -p "127.0.0.1:$ADMIN_PORT:4002" \
  -e ANKUSA_ADMIN_IP=0.0.0.0 "$IMAGE" >/dev/null

echo "==> waiting for the admin API"
ready=false
for _ in $(seq 1 30); do
  if [ "$(http_code "$WORK/health" "$ADMIN_URL/health")" = "200" ]; then
    ready=true
    break
  fi

  if [ "$(docker inspect -f '{{.State.Running}}' "$NAME")" != "true" ]; then
    fail "the container exited during startup"
  fi

  sleep 1
done

[ "$ready" = "true" ] || fail "/health never returned 200 within 30s"

echo "==> ingest: accepted"
code=$(http_code "$WORK/first" -XPOST "$BASE_URL/webhooks/demo" -d '{"id":"evt_smoke"}')
[ "$code" = "201" ] || fail "first POST returned $code: $(cat "$WORK/first")"
grep -q '"status":"accepted"' "$WORK/first" || fail "first POST was not accepted: $(cat "$WORK/first")"

echo "==> ready: the store takes a synced write"
code=$(http_code "$WORK/ready" "$BASE_URL/ready")
[ "$code" = "200" ] || fail "/ready returned $code: $(cat "$WORK/ready")"
grep -q '"store":"ok"' "$WORK/ready" || fail "/ready did not report the store: $(cat "$WORK/ready")"

echo "==> the image's HEALTHCHECK (GET /ready) turns healthy"
health=""
for _ in $(seq 1 40); do
  health=$(docker inspect -f '{{.State.Health.Status}}' "$NAME")
  [ "$health" = "healthy" ] && break
  sleep 1
done
[ "$health" = "healthy" ] || fail "container health is '$health' after 40s"

echo "==> metrics"
curl -s "$ADMIN_URL/metrics" >"$WORK/metrics"
grep -q 'ankusa_ingest_requests_total' "$WORK/metrics" ||
  fail "/metrics has no ingest counter"

echo "==> config, with no credentials at all"
code=$(http_code "$WORK/config" "$ADMIN_URL/v1/config")
[ "$code" = "200" ] || fail "/v1/config returned $code: $(cat "$WORK/config")"
grep -q '"demo"' "$WORK/config" || fail "/v1/config does not mention the demo source"

echo "==> a missing config file exits 78 (EX_CONFIG)"
set +e
docker run --rm -e ANKUSA_CONFIG=/nope "$IMAGE" check-config >"$WORK/check" 2>&1
code=$?
set -e
[ "$code" = "78" ] || fail "check-config with a missing file exited $code, expected 78: $(cat "$WORK/check")"

echo "smoke OK: $IMAGE"
