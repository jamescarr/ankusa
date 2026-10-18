#!/usr/bin/env bash
#
# The image gate: start the built image and check what an operator checks in the
# first minute — the demo hook is accepted, metrics and config answer, and a
# broken config exits 78 instead of crash-looping. A published image (a
# `repo@sha256:…` reference, or any image named by SMOKE_COSIGN_IMAGE) is first
# checked for the keyless cosign signature .github/workflows/docker.yml makes.
#
#   packages/ankusa_server/scripts/smoke.sh jamescarr/ankusa:dev
#   packages/ankusa_server/scripts/smoke.sh jamescarr/ankusa@sha256:…   # + cosign verify
#
# Used by both `mise run docker:smoke` and .github/workflows/docker.yml, so it
# depends on nothing but docker, curl, and bash (and cosign, to verify one).
set -euo pipefail

IMAGE="${1:?usage: smoke.sh IMAGE}"
NAME=ankusa-smoke
# Host ports; override when 4000/4002 are taken on this machine.
HTTP_PORT="${SMOKE_HTTP_PORT:-4000}"
ADMIN_PORT="${SMOKE_ADMIN_PORT:-4002}"
BASE_URL=http://127.0.0.1:$HTTP_PORT
ADMIN_URL=http://127.0.0.1:$ADMIN_PORT
# The admin API listens off loopback in the container, so it needs a credential:
# a throwaway token for this run (`ANKUSA_ADMIN_TOKEN` gives it every scope).
ADMIN_TOKEN="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
# The mutual-TLS container's ingest and metrics ports.
MTLS_PORT="${SMOKE_MTLS_PORT:-4010}"
METRICS_PORT="${SMOKE_METRICS_PORT:-4014}"
# Who may have signed a published image: this repo's docker.yml, on main or on a
# server tag, with GitHub's OIDC issuer. Override for a fork.
COSIGN_IDENTITY="${SMOKE_COSIGN_IDENTITY:-^https://github\.com/jamescarr/ankusa/\.github/workflows/docker\.yml@refs/(heads/main|tags/ankusa_server-v.+)$}"
COSIGN_ISSUER="${SMOKE_COSIGN_ISSUER:-https://token.actions.githubusercontent.com}"

WORK="$(mktemp -d)"

cleanup() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "--- container logs ($NAME) ---" >&2
    docker logs "$NAME" >&2 || true
    docker logs "$NAME-mtls" >&2 2>&1 || true
  fi
  docker rm -f "$NAME" "$NAME-mtls" >/dev/null 2>&1 || true
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

# The signature first: nothing below should run an image nobody signed.
cosign_target="${SMOKE_COSIGN_IMAGE:-}"
case "$IMAGE" in
  *@sha256:*) cosign_target="${cosign_target:-$IMAGE}" ;;
esac
if [ -n "$cosign_target" ]; then
  command -v cosign >/dev/null || fail "cosign is not installed, so $cosign_target cannot be verified"
  cosign verify --certificate-identity-regexp "$COSIGN_IDENTITY" \
    --certificate-oidc-issuer "$COSIGN_ISSUER" "$cosign_target" >"$WORK/cosign" 2>&1 ||
    fail "cosign verify failed for $cosign_target: $(cat "$WORK/cosign")"
  echo "cosign verify: OK ($cosign_target)"
else
  echo "cosign verify: skipped (a locally built image is unsigned; pass a repo@sha256: reference or SMOKE_COSIGN_IMAGE)"
fi

# A leftover container from an interrupted run would make every check below lie.
docker rm -f "$NAME" >/dev/null 2>&1 || true

echo "==> starting $IMAGE"
docker run -d --name "$NAME" -p "127.0.0.1:$HTTP_PORT:4000" -p "127.0.0.1:$ADMIN_PORT:4002" \
  -e ANKUSA_ADMIN_IP=0.0.0.0 -e ANKUSA_ADMIN_TOKEN="$ADMIN_TOKEN" "$IMAGE" >/dev/null

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
curl -s -H "Authorization: Bearer $ADMIN_TOKEN" "$ADMIN_URL/metrics" >"$WORK/metrics"
grep -q 'ankusa_ingest_requests_total' "$WORK/metrics" ||
  fail "/metrics has no ingest counter"

echo "==> config: 401 without a credential or with a wrong token, 200 with the token"
code=$(http_code "$WORK/denied" "$ADMIN_URL/v1/config")
[ "$code" = "401" ] || fail "/v1/config with no credential returned $code, expected 401"
code=$(http_code "$WORK/denied" -H "Authorization: Bearer wrong-token" "$ADMIN_URL/v1/config")
[ "$code" = "401" ] || fail "/v1/config with a wrong token returned $code, expected 401"
code=$(http_code "$WORK/denied" "$ADMIN_URL/metrics")
[ "$code" = "401" ] || fail "/metrics with no credential returned $code, expected 401"
code=$(http_code "$WORK/config" -H "Authorization: Bearer $ADMIN_TOKEN" "$ADMIN_URL/v1/config")
[ "$code" = "200" ] || fail "/v1/config with the token returned $code: $(cat "$WORK/config")"
grep -q '"demo"' "$WORK/config" || fail "/v1/config does not mention the demo source"
! grep -q "$ADMIN_TOKEN" "$WORK/config" || fail "/v1/config shows the admin token"

echo "==> mutual TLS on the ingest listener, and a metrics listener that serves only metrics"
# The same TLS fixtures core's tests use: a throwaway CA, a server certificate
# for localhost, a client certificate that CA signed, and one a different CA did.
TLS_DIR="$(cd "$(dirname "$0")/../../ankusa/test/fixtures/tls" && pwd)"
cat >"$WORK/mtls.yml" <<'EOF'
node: {data_dir: /var/lib/ankusa}
listener:
  tls:
    certfile: /tls/server.pem
    keyfile: /tls/server-key.pem
    cacertfile: /tls/ca.pem
    verify: verify_peer
metrics: {enabled: true, ip: 0.0.0.0}
sources:
  demo: {verify: {type: none}, on_verify_failure: accept_flag, sinks: [{type: log}]}
EOF
docker rm -f "$NAME-mtls" >/dev/null 2>&1 || true
docker run -d --name "$NAME-mtls" \
  -p "127.0.0.1:$MTLS_PORT:4000" -p "127.0.0.1:$METRICS_PORT:4004" \
  -v "$WORK/mtls.yml:/etc/ankusa/ankusa.yml:ro" -v "$TLS_DIR:/tls:ro" "$IMAGE" >/dev/null

ready=false
for _ in $(seq 1 30); do
  if [ "$(http_code "$WORK/mready" "http://127.0.0.1:$METRICS_PORT/ready")" = "200" ]; then
    ready=true
    break
  fi
  [ "$(docker inspect -f '{{.State.Running}}' "$NAME-mtls")" = "true" ] ||
    fail "the mTLS container exited during startup"
  sleep 1
done
[ "$ready" = "true" ] || fail "the metrics listener's /ready never returned 200 within 30s"

# One hook POSTed over the TLS listener with `openssl s_client`; prints the
# response. Extra arguments carry (or withhold) the client certificate.
tls_post() {
  printf 'POST /webhooks/demo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 17\r\nConnection: close\r\n\r\n{"id":"evt_mtls"}' |
    openssl s_client -quiet -connect "127.0.0.1:$MTLS_PORT" -servername localhost \
      -CAfile "$TLS_DIR/ca.pem" -verify_return_error "$@" 2>/dev/null || true
}

reply=$(tls_post -cert "$TLS_DIR/client.pem" -key "$TLS_DIR/client-key.pem")
echo "$reply" | grep -q '^HTTP/1.1 201' || fail "the pinned client certificate was not accepted: $reply"
echo "$reply" | grep -q '"status":"accepted"' || fail "the mTLS POST was not accepted: $reply"

reply=$(tls_post)
! echo "$reply" | grep -q '^HTTP/1.1' || fail "a client with no certificate was served: $reply"

reply=$(tls_post -cert "$TLS_DIR/stranger.pem" -key "$TLS_DIR/stranger-key.pem")
! echo "$reply" | grep -q '^HTTP/1.1' || fail "a client certificate from another CA was served: $reply"

# Plain HTTP to a TLS port gets nothing.
code=$(http_code "$WORK/plain" --max-time 5 "http://127.0.0.1:$MTLS_PORT/ready" || true)
[ "$code" = "000" ] || fail "plain HTTP to the TLS listener returned $code"

code=$(http_code "$WORK/mmetrics" "http://127.0.0.1:$METRICS_PORT/metrics")
[ "$code" = "200" ] || fail "the metrics listener's /metrics returned $code"
for path in /v1/config /v1/dlq /v1/wal; do
  code=$(http_code "$WORK/m404" "http://127.0.0.1:$METRICS_PORT$path")
  [ "$code" = "404" ] || fail "the metrics listener answered $path with $code, expected 404"
done
docker rm -f "$NAME-mtls" >/dev/null 2>&1 || true

echo "==> a missing config file exits 78 (EX_CONFIG)"
set +e
docker run --rm -e ANKUSA_CONFIG=/nope "$IMAGE" check-config >"$WORK/check" 2>&1
code=$?
set -e
[ "$code" = "78" ] || fail "check-config with a missing file exited $code, expected 78: $(cat "$WORK/check")"

echo "smoke OK: $IMAGE"
