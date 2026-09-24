#!/usr/bin/env bash
#
# The image gate: start the built image and check what an operator checks in the
# first minute — the demo hook round-trips (accepted, then deduped), metrics and
# config answer, and a broken config exits 78 instead of crash-looping.
#
#   ankusa_server/scripts/smoke.sh ankusa/ankusa:dev
#
# Used by both `mise run docker:smoke` and .github/workflows/docker.yml, so it
# depends on nothing but docker, curl, and bash.
set -euo pipefail

IMAGE="${1:?usage: smoke.sh IMAGE}"
NAME=ankusa-smoke
BASE_URL=http://127.0.0.1:4000
ADMIN_URL=http://127.0.0.1:4002

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
docker run -d --name "$NAME" -p 127.0.0.1:4000:4000 -p 127.0.0.1:4002:4002 "$IMAGE" >/dev/null

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

echo "==> ingest: accepted once, deduped the second time"
hook='{"id":"evt_smoke"}'

code=$(http_code "$WORK/first" -XPOST "$BASE_URL/webhooks/demo" -d "$hook")
[ "$code" = "201" ] || fail "first POST returned $code: $(cat "$WORK/first")"
grep -q '"status":"accepted"' "$WORK/first" || fail "first POST was not accepted: $(cat "$WORK/first")"

code=$(http_code "$WORK/second" -XPOST "$BASE_URL/webhooks/demo" -d "$hook")
[ "$code" = "200" ] || fail "duplicate POST returned $code: $(cat "$WORK/second")"
grep -q '"status":"duplicate"' "$WORK/second" ||
  fail "duplicate POST was not deduped: $(cat "$WORK/second")"

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
