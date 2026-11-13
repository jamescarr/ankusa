#!/usr/bin/env bash
#
# The gateway image gate: start the built image and check what an operator
# checks in the first minute — the probes answer, the claims API refuses a
# caller without the token and serves one with it, a claim checked in on the
# node reads back, the printed config shows no secret, and a broken config
# exits 78 instead of crash-looping. A published image (a `repo@sha256:…`
# reference, or any image named by SMOKE_COSIGN_IMAGE) is first checked for the
# keyless cosign signature .github/workflows/docker.yml makes.
#
#   packages/ankusa_gateway_server/scripts/smoke.sh jamescarr/ankusa-gateway:dev
#   packages/ankusa_gateway_server/scripts/smoke.sh jamescarr/ankusa-gateway@sha256:…   # + cosign verify
#
# Used by both `mise run docker:smoke ankusa_gateway_server` and
# .github/workflows/docker.yml, so it depends on nothing but docker, curl, and
# bash (and cosign, to verify one).
set -euo pipefail

IMAGE="${1:?usage: smoke.sh IMAGE}"
NAME=ankusa-gateway-smoke
# Host port; override when 4001 is taken on this machine.
PORT="${SMOKE_GATEWAY_PORT:-4001}"
URL=http://127.0.0.1:$PORT
# The gateway listens off loopback in the container, so it needs a credential:
# a throwaway token for this run (`ANKUSA_CLAIM_CHECK_TOKEN` holds claims:read
# and tenant:*).
TOKEN="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
# Who may have signed a published image: this repo's docker.yml, on main or on a
# gateway tag, with GitHub's OIDC issuer. Override for a fork.
COSIGN_IDENTITY="${SMOKE_COSIGN_IDENTITY:-^https://github\.com/jamescarr/ankusa/\.github/workflows/docker\.yml@refs/(heads/main|tags/ankusa_gateway_server-v.+)$}"
COSIGN_ISSUER="${SMOKE_COSIGN_ISSUER:-https://token.actions.githubusercontent.com}"

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

# The signature first: nothing below should run an image nobody signed.
cosign_target="${SMOKE_COSIGN_IMAGE:-}"
case "$IMAGE" in
  *@sha256:*) cosign_target="${cosign_target:-$IMAGE}" ;;
esac
if [ -n "$cosign_target" ]; then
  command -v cosign >/dev/null || fail "cosign is not installed, so $cosign_target cannot be verified"
  # Docker Hub can serve a just-pushed signature tag late: up to 30 attempts,
  # 10 s apart.
  attempt=1
  until cosign verify --certificate-identity-regexp "$COSIGN_IDENTITY" \
    --certificate-oidc-issuer "$COSIGN_ISSUER" "$cosign_target" >"$WORK/cosign" 2>&1; do
    [ "$attempt" -lt 30 ] || fail "cosign verify failed for $cosign_target: $(cat "$WORK/cosign")"
    echo "cosign verify: attempt $attempt/30 failed: $(tail -n 1 "$WORK/cosign"); retrying in 10 s" >&2
    attempt=$((attempt + 1))
    sleep 10
  done
  echo "cosign verify: OK ($cosign_target)"
else
  echo "cosign verify: skipped (a locally built image is unsigned; pass a repo@sha256: reference or SMOKE_COSIGN_IMAGE)"
fi

# A leftover container from an interrupted run would make every check below lie.
docker rm -f "$NAME" >/dev/null 2>&1 || true

echo "==> starting $IMAGE"
docker run -d --name "$NAME" -p "127.0.0.1:$PORT:4001" \
  -e ANKUSA_CLAIM_CHECK_IP=0.0.0.0 -e ANKUSA_CLAIM_CHECK_TOKEN="$TOKEN" "$IMAGE" >/dev/null

echo "==> waiting for /health"
ready=false
for _ in $(seq 1 30); do
  if [ "$(http_code "$WORK/health" "$URL/health")" = "200" ]; then
    ready=true
    break
  fi

  if [ "$(docker inspect -f '{{.State.Running}}' "$NAME")" != "true" ]; then
    fail "the container exited during startup"
  fi

  sleep 1
done

[ "$ready" = "true" ] || fail "/health never returned 200 within 30s"

echo "==> ready"
code=$(http_code "$WORK/ready" "$URL/ready")
[ "$code" = "200" ] || fail "/ready returned $code: $(cat "$WORK/ready")"
grep -q '"gateway":"ok"' "$WORK/ready" || fail "/ready does not report the gateway: $(cat "$WORK/ready")"

echo "==> the image's HEALTHCHECK (GET /ready) turns healthy"
health=""
for _ in $(seq 1 40); do
  health=$(docker inspect -f '{{.State.Health.Status}}' "$NAME")
  [ "$health" = "healthy" ] && break
  sleep 1
done
[ "$health" = "healthy" ] || fail "container health is '$health' after 40s"

echo "==> claims: 401 without the token, 404 for a missing claim with it"
missing=/v1/claims/acme/01M39VMD8RA3C5HR4RBV67Y002
code=$(http_code "$WORK/denied" "$URL$missing")
[ "$code" = "401" ] || fail "a claim with no credential returned $code, expected 401"
code=$(http_code "$WORK/missing" -H "Authorization: Bearer $TOKEN" "$URL$missing")
[ "$code" = "404" ] || fail "a missing claim returned $code: $(cat "$WORK/missing")"
grep -q '"error":"not_found"' "$WORK/missing" || fail "a missing claim's body: $(cat "$WORK/missing")"

echo "==> a claim checked in on the node reads back"
# shellcheck disable=SC2016
path=$(docker exec "$NAME" /usr/local/bin/docker-entrypoint rpc \
  '{:ok, c} = Ankusa.ClaimCheck.check_in(:default, "acme", [%{id: "a", body: "hello"}]); IO.puts(Ankusa.ClaimCheck.Ref.path(c["a"].ref))' |
  tr -d '\r' | tail -n 1)
case "$path" in
  /v1/claims/acme/*) ;;
  *) fail "check_in printed no claim path: '$path'" ;;
esac
code=$(http_code "$WORK/claim" -D "$WORK/claim.headers" -H "Authorization: Bearer $TOKEN" "$URL$path")
[ "$code" = "200" ] || fail "the claim returned $code: $(cat "$WORK/claim")"
[ "$(cat "$WORK/claim")" = "hello" ] || fail "the claim's bytes: $(cat "$WORK/claim")"
grep -qi '^x-ankusa-sha256: ' "$WORK/claim.headers" || fail "the claim carries no x-ankusa-sha256"

echo "==> print-config: the gateway, and no secret"
docker exec "$NAME" /usr/local/bin/docker-entrypoint print-config >"$WORK/print"
grep -q '"gateway"' "$WORK/print" || fail "print-config has no gateway: $(cat "$WORK/print")"
! grep -q "$TOKEN" "$WORK/print" || fail "print-config shows the token"

echo "==> check-config: a missing file exits 78"
rc=0
docker run --rm -e ANKUSA_CONFIG=/nope "$IMAGE" check-config >"$WORK/nope" 2>&1 || rc=$?
[ "$rc" = "78" ] || fail "check-config with a missing file exited $rc: $(cat "$WORK/nope")"

echo "==> check-config: a section of the full image exits 78, naming it"
printf 'config_version: 2\nsources: {demo: {verify: {type: none}, sinks: [{type: log}]}}\n' >"$WORK/full.yml"
chmod 0644 "$WORK/full.yml"
rc=0
docker run --rm -v "$WORK/full.yml:/etc/ankusa/gateway.yml:ro" "$IMAGE" check-config >"$WORK/full" 2>&1 || rc=$?
[ "$rc" = "78" ] || fail "check-config with a sources: key exited $rc: $(cat "$WORK/full")"
grep -q 'sources: not available in the gateway image' "$WORK/full" ||
  fail "check-config did not name sources: $(cat "$WORK/full")"

echo "smoke OK"
