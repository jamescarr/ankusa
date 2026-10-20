#!/usr/bin/env bash
#
# Run the crash-consistency drill in a container:
#
#   scripts/drills/crash.sh tmpfs   ENOSPC on a 192 MiB tmpfs, then kill -9 under load
#   scripts/drills/crash.sh dm      I/O error windows and a power-loss cut on ext4
#                                   over device-mapper (docker --privileged; exit 2
#                                   when the host kernel cannot do that)
#
# `mise run test:crash` is the tmpfs drill (CI's `crash` job); flakey.sh is the
# dm one (release:preflight). The steps and the line printed at the end are in
# crash_drill.sh. The image is the one the server image builds with, so the
# drill runs on the Elixir/OTP release ankusa ships on. Compiled deps are kept
# in .cache/crash between runs.
set -euo pipefail

mode=${1:?usage: crash.sh tmpfs|dm}
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

# The `FROM` line carries the tag and, since SEC-8, the digest the image is
# pinned to (`elixir:1.20.4-alpine@sha256:…`); `docker run` takes it whole.
image=$(sed -n 's/^FROM \(elixir:[^ @]*-alpine\(@sha256:[0-9a-f]\{64\}\)\{0,1\}\) AS build$/\1/p' "$root/packages/ankusa_server/Dockerfile")
[ -n "$image" ] || {
  echo "FAIL: no 'FROM elixir:…-alpine[@sha256:…] AS build' line in packages/ankusa_server/Dockerfile" >&2
  exit 1
}

mkdir -p "$root/.cache/crash"

args=(--rm --init
  -v "$root:/repo:ro"
  -v "$root/.cache/crash:/cache"
  -e "RATE=${RATE:-1000}"
  -e "BODY_BYTES=${BODY_BYTES:-10240}"
  -e "FAULT_AT=${FAULT_AT:-5}"
  -e "KILL_AT=${KILL_AT:-20}")

case $mode in
  tmpfs) args+=(--tmpfs /data:size=192m -e FAULT=enospc) ;;
  dm) args+=(--privileged -e FAULT=eio) ;;
  *)
    echo "usage: crash.sh tmpfs|dm" >&2
    exit 1
    ;;
esac

exec docker run "${args[@]}" "$image" sh -c 'apk add --no-cache bash >/dev/null && exec bash /repo/scripts/drills/crash_drill.sh'
