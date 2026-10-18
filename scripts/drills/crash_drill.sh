#!/usr/bin/env bash
#
# The in-container half of the crash-consistency drill. Run by crash.sh, which
# starts the container; not meant to be run on the host.
#
# One instance (packages/ankusa/test/support/crash_harness.exs) ingests at
# RATE hooks/s of BODY_BYTES each (default 1000 of 10 KiB) on /data, a fault
# hits the disk under it, and the instance is
# killed under load. Every id the harness printed as acked must read back from
# the reopened store, and the node must have refused (503 store_unavailable,
# /ready 503) while the fault lasted and resumed (201, /ready 200) once it
# cleared.
#
#   FAULT=enospc  /data is a 96 MiB tmpfs. At +5 s a filler file takes every
#                 free byte. RocksDB pre-allocates its WAL, so the node keeps
#                 acking until that is used up; the filler stays until the
#                 node has answered 503 and /ready 503 (and 2 s longer), then
#                 it is deleted. kill -9 once the node has served again for
#                 5 s and at least 20 s into the run.
#   FAULT=eio     /data is ext4 on a device-mapper device over a loop device
#                 (needs --privileged). Two 2 s windows (+5 s, +10 s) where
#                 every I/O on the device fails, then at +20 s a power-loss cut:
#                 the device fails every I/O, the instance is killed, the
#                 filesystem is unmounted, repaired and remounted, so whatever
#                 was not fsynced is gone. A tmpfs cannot show a missing fsync
#                 (kill -9 loses nothing the kernel already holds); this can.
#
# Prints one line:
#   acked=<n> recovered=<n> missing=<n> <enospc|eio>_503=<k> resumed=true
# and exits 0 only when missing=0, the node refused and resumed, and hooks were
# acked after it resumed. Exit 2: the kernel cannot do device-mapper here.
set -euo pipefail

fault=${FAULT:?FAULT must be enospc or eio}
rate=${RATE:-1000}
body_bytes=${BODY_BYTES:-10240}
port=4000
fault_at=${FAULT_AT:-5}
kill_at=${KILL_AT:-20}

base=http://127.0.0.1:$port
data=/data
store=$data/ankusa
work=/work
harness=$work/packages/ankusa/test/support/crash_harness.exs
acks=/tmp/acks.txt
body=/tmp/probe.body

export MIX_ENV=test MIX_DEPS_PATH=/cache/deps MIX_BUILD_ROOT=/cache/build
export MIX_HOME=/cache/mix HEX_HOME=/cache/hex
export ERL_LIBS=$MIX_BUILD_ROOT/test/lib

fail() { echo "FAIL: $*" >&2; exit 1; }
log() { echo "==> $*" >&2; }
now_ms() { local t=${EPOCHREALTIME/./}; echo $((t / 1000)); }
# Seconds since the load started, as an integer.
elapsed() { echo $((($(now_ms) - t0) / 1000)); }
wait_until() { while [ "$(elapsed)" -lt "$1" ]; do sleep 0.2; done; }

post_code() { curl -s -m 5 -o "$body" -w '%{http_code}' -XPOST "$base/webhooks/load" -d '{"probe":1}' || true; }
ready_code() { curl -s -m 5 -o /dev/null -w '%{http_code}' "$base/ready" || true; }
acked_now() { grep -c '^ACK ' "$acks" || true; }

harness_pid=""
dm_name=ankusa_crash_$$
loop_dev=""
sectors=""

cleanup() {
  rc=$?
  if [ -n "$harness_pid" ]; then kill -9 "$harness_pid" 2>/dev/null || true; fi
  if [ "$fault" = eio ]; then
    umount -l "$data" 2>/dev/null || true
    dmsetup remove -f "$dm_name" 2>/dev/null || true
    if [ -n "$loop_dev" ]; then losetup -d "$loop_dev" 2>/dev/null || true; fi
  fi
  if [ "$rc" -ne 0 ] && [ -f /tmp/harness.err ]; then
    echo "--- harness stderr (last 40 lines) ---" >&2
    tail -n 40 /tmp/harness.err >&2 || true
  fi
  exit "$rc"
}
trap cleanup EXIT

# ── device-mapper (FAULT=eio) ───────────────────────────────────────────────

dm_table() {
  dmsetup suspend --noflush "$dm_name"
  dmsetup reload "$dm_name" --table "0 $sectors $1"
  dmsetup resume "$dm_name"
}
dm_fail() { dm_table error; }
dm_heal() { dm_table "linear $loop_dev 0"; }

dm_setup() {
  if [ ! -e /dev/loop-control ] || ! dmsetup targets 2>/dev/null | grep -q '^error'; then
    echo "SKIP: no loop devices or device-mapper in this container/kernel" >&2
    exit 2
  fi
  truncate -s 256M /tmp/crash.img
  loop_dev=$(losetup -f --show /tmp/crash.img) || {
    echo "SKIP: cannot attach a loop device" >&2
    exit 2
  }
  sectors=$(blockdev --getsz "$loop_dev")
  dmsetup create "$dm_name" --table "0 $sectors linear $loop_dev 0"
  dmsetup mknodes
  mkfs.ext4 -q -F "/dev/mapper/$dm_name"
  mkdir -p "$data"
  # `errors=continue`: a failed write window must not remount the filesystem
  # read-only for good, the way a transient device error would not.
  mount -t ext4 -o errors=continue "/dev/mapper/$dm_name" "$data"
}

# ── build ───────────────────────────────────────────────────────────────────

log "installing build dependencies"
apk add --no-cache bash curl build-base cmake git linux-headers openssl-dev zstd-dev \
  device-mapper losetup e2fsprogs >/dev/null

if [ "$fault" = eio ]; then dm_setup; fi

log "compiling ankusa (MIX_ENV=test) into /cache"
# A copy, not the read-only mount: the build writes next to the sources.
mkdir -p "$work"
tar -C /repo --exclude=_build --exclude=deps -cf - packages/ankusa packages/async_api_spex |
  tar -C "$work" -xf -
mix local.hex --force >/dev/null
mix local.rebar --force >/dev/null
(cd "$work/packages/ankusa" && mix deps.get --check-locked >/dev/null && mix compile)

# ── load ────────────────────────────────────────────────────────────────────

log "starting the instance: $rate hooks/s on $store ($fault)"
mkdir -p "$store"
DATA_DIR=$store HTTP_PORT=$port RATE=$rate BODY_BYTES=$body_bytes elixir "$harness" serve >"$acks" 2>/tmp/harness.err &
harness_pid=$!

for _ in $(seq 1 240); do
  [ "$(ready_code)" = 200 ] && break
  kill -0 "$harness_pid" 2>/dev/null || fail "the instance exited during startup"
  sleep 0.5
done
[ "$(ready_code)" = 200 ] || fail "/ready never returned 200"
t0=$(now_ms)

# ── fault ───────────────────────────────────────────────────────────────────

refused=0 # POSTs answered 503 store_unavailable while the fault was on
not_ready=0 # /ready probes answered 503 while the fault was on

# One look at what the node says: a POST and /ready.
probe_once() {
  if [ "$(post_code)" = 503 ] && grep -q '"error":"store_unavailable"' "$body"; then
    refused=$((refused + 1))
  fi
  if [ "$(ready_code)" = 503 ]; then not_ready=$((not_ready + 1)); fi
}

# Probe until `$1` seconds into the run.
probe_until() {
  while [ "$(elapsed)" -lt "$1" ]; do
    probe_once
    sleep 0.2
  done
}

# Probe until the node has refused a POST and answered /ready 503, or `$1`
# seconds into the run.
probe_until_refused() {
  while [ "$(elapsed)" -lt "$1" ]; do
    probe_once
    if [ "$refused" -gt 0 ] && [ "$not_ready" -gt 0 ]; then return 0; fi
    sleep 0.2
  done
}

# The node serves again: a 201 and /ready 200, twice in a row, within 40 s.
await_resume() {
  local ok=0 deadline=$(($(now_ms) + 40000))
  while [ "$(now_ms)" -lt "$deadline" ]; do
    if [ "$(post_code)" = 201 ] && [ "$(ready_code)" = 200 ]; then
      ok=$((ok + 1))
      [ "$ok" -ge 2 ] && return 0
    else
      ok=0
    fi
    sleep 0.5
  done
  return 1
}

wait_until "$fault_at"
case $fault in
  enospc)
    log "+$(elapsed)s: filling the disk"
    dd if=/dev/zero of="$data/filler" bs=1M 2>/dev/null || true # ends on ENOSPC
    df -k "$data" >&2
    probe_until_refused $((fault_at + 30))
    probe_until $(($(elapsed) + 2))
    log "+$(elapsed)s: deleting the filler (503s seen: $refused)"
    rm -f "$data/filler"
    ;;
  eio)
    for window in 1 2; do
      log "+$(elapsed)s: I/O errors, window $window"
      dm_fail
      probe_until $(($(elapsed) + 2))
      dm_heal
      log "+$(elapsed)s: device healthy"
      if [ "$window" = 1 ]; then
        # The second window starts 5 s after the first one began.
        await_resume || fail "the node did not resume after window 1"
        wait_until $((fault_at + 5))
      fi
    done
    ;;
  *) fail "FAULT must be enospc or eio, not $fault" ;;
esac

[ "$refused" -gt 0 ] || fail "no POST was answered 503 store_unavailable while the fault was on"
[ "$not_ready" -gt 0 ] || fail "/ready never answered 503 while the fault was on"

await_resume || fail "the node did not resume: a 201 and /ready 200 within 40 s of the fault clearing"
acked_at_resume=$(acked_now)
log "+$(elapsed)s: resumed (acked so far: $acked_at_resume)"

# ── kill under load ─────────────────────────────────────────────────────────

# At least 5 s of load after the node resumed, whatever the fault cost.
resumed_s=$(elapsed)
wait_until $((resumed_s + 5 > kill_at ? resumed_s + 5 : kill_at))

if [ "$fault" = eio ]; then
  log "+$(elapsed)s: power cut"
  dm_fail
fi
log "+$(elapsed)s: kill -9"
kill -9 "$harness_pid"
wait "$harness_pid" 2>/dev/null || true
harness_pid=""

if [ "$fault" = eio ]; then
  umount "$data"
  dm_heal
  e2fsck -fy "/dev/mapper/$dm_name" >/tmp/fsck.out 2>&1 || [ $? -lt 4 ] || {
    cat /tmp/fsck.out >&2
    fail "e2fsck could not repair the filesystem after the cut"
  }
  mount -t ext4 "/dev/mapper/$dm_name" "$data"
fi

acked=$(acked_now)
[ "$acked" -gt "$acked_at_resume" ] || fail "no hook was acked after the node resumed (acked=$acked)"

# ── verify ──────────────────────────────────────────────────────────────────

log "reopening the store alone and reading every acked id back"
status=0
verdict=$(DATA_DIR=$store ACKS_FILE=$acks elixir "$harness" verify 2>/tmp/verify.err) || status=$?
echo "$verdict" >&2
if [ "$status" -ne 0 ] && ! grep -q '^acked=' <<<"$verdict"; then
  cat /tmp/verify.err >&2
  fail "verify could not read the store back (exit $status)"
fi

summary=$(grep '^acked=' <<<"$verdict")
echo "$summary ${fault}_503=$refused resumed=true"
[ "$status" -eq 0 ] || fail "acked hooks are missing from the reopened store"
