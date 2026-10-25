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
# /ready 503 naming a `reason`) while the fault lasted and resumed (201, /ready
# 200) once it cleared, with the store's failure series (`ankusa_store_*`, read
# from the metrics listener) showing the write failures and the reopen.
#
#   FAULT=enospc  /data is a 192 MiB tmpfs. At +5 s a filler file takes every
#                 free byte. RocksDB pre-allocates its WAL, so the node keeps
#                 acking until that is used up; the filler stays until the
#                 node has answered 503 and /ready 503 (and 2 s longer), then
#                 it is deleted. kill -9 once the node has served again for
#                 5 s and at least 20 s into the run.
#   FAULT=eio     /data is ext4 on a device-mapper device over a loop device
#                 (needs --privileged). Two 2 s windows (+5 s, +10 s) where
#                 every I/O on the file data area fails (the journal and the
#                 metadata at the start of the volume stay up, see dm_setup),
#                 then at +20 s a power-loss cut:
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
metrics_port=4001

base=http://127.0.0.1:$port
data=/data
store=$data/ankusa
work=/work
harness=$work/packages/ankusa/test/support/crash_harness.exs
acks=/tmp/acks.txt
body=/tmp/probe.body
ready_body=/tmp/ready.body

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
ready_code() { curl -s -m 5 -o "$ready_body" -w '%{http_code}' "$base/ready" || true; }
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
    # A refused commit logs once per batch, a thousand lines a second: what the
    # store itself said is in the lines that are left.
    echo "--- harness stderr, store lines (last 80 of $(wc -l </tmp/harness.err) lines) ---" >&2
    grep -v -e 'store commit' -e 'is not running' /tmp/harness.err | tail -n 80 >&2 || true
  fi
  if [ "$rc" -ne 0 ] && [ "$fault" = eio ]; then
    echo "--- kernel log, filesystem lines (last 40) ---" >&2
    dmesg 2>/dev/null | grep -i -e ext4 -e jbd2 -e 'dm-' -e remount | tail -n 40 >&2 || true
  fi
  exit "$rc"
}
trap cleanup EXIT

# ── device-mapper (FAULT=eio) ───────────────────────────────────────────────

# `$1` is a whole table, one segment per line.
dm_table() {
  dmsetup suspend --noflush "$dm_name"
  dmsetup reload "$dm_name" --table "$1"
  dmsetup resume "$dm_name"
}
# Every I/O on the device fails: the power cut.
dm_fail() { dm_table "0 $sectors error"; }
# The file data area fails, the journal and the metadata at the start of the
# volume (see `dm_setup`) keep working: a disk that has lost sectors, not one
# the filesystem has to give up on.
dm_fail_data() {
  dm_table "0 $meta_sectors linear $loop_dev 0
$meta_sectors $((sectors - meta_sectors)) error"
}
dm_heal() { dm_table "0 $sectors linear $loop_dev 0"; }

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
  # The error windows must not fail the journal. A journal that cannot be
  # written is aborted, and ext4 then remounts the volume read-only for good
  # (`Aborting journal on device`, `Remounting filesystem read-only`, whatever
  # `errors=` says): no application can reopen a store on that, so whether a
  # run resumed depended on whether a journal commit happened to land in a
  # window. `packed_meta_blocks` puts the journal, the bitmaps and the inode
  # tables in one run at the start of the volume, and `dm_fail_data` fails
  # only what follows it. (No journal at all is no answer: without ordered
  # data and metadata a failed window leaves a WAL no recovery mode accepts.)
  mkfs.ext4 -q -F -b 4096 -E packed_meta_blocks=1 -O ^flex_bg "/dev/mapper/$dm_name"
  first_data_block=$(dumpe2fs "/dev/mapper/$dm_name" |
    sed -n '/^Group 0:/,/^Group 1:/s/^ *Free blocks: *\([0-9][0-9]*\).*/\1/p')
  [ -n "$first_data_block" ] || fail "cannot tell where the file data area of the volume starts"
  meta_sectors=$((first_data_block * 8))
  mkdir -p "$data"
  mount -t ext4 -o errors=continue "/dev/mapper/$dm_name" "$data"
}

# ── build ───────────────────────────────────────────────────────────────────

log "installing build dependencies"
apk add --no-cache bash curl build-base cmake git linux-headers openssl-dev zstd-dev \
  device-mapper losetup e2fsprogs e2fsprogs-extra >/dev/null

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
DATA_DIR=$store HTTP_PORT=$port METRICS_PORT=$metrics_port RATE=$rate BODY_BYTES=$body_bytes \
  elixir "$harness" serve >"$acks" 2>/tmp/harness.err &
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
named=0 # of those, the ones whose body named a `reasons.store`

# One look at what the node says: a POST and /ready.
probe_once() {
  if [ "$(post_code)" = 503 ] && grep -q '"error":"store_unavailable"' "$body"; then
    refused=$((refused + 1))
  fi
  if [ "$(ready_code)" = 503 ]; then
    not_ready=$((not_ready + 1))
    if grep -q '"reasons":{[^}]*"store":"[a-z_]*"' "$ready_body"; then named=$((named + 1)); fi
  fi
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

# The node serves again: a 201 and /ready 200, twice in a row, within 40 s. On
# failure `last_resume_probe` is what the node said last.
last_resume_probe=""
await_resume() {
  local ok=0 deadline=$(($(now_ms) + 40000)) post ready
  while [ "$(now_ms)" -lt "$deadline" ]; do
    post=$(post_code)
    ready=$(ready_code)
    last_resume_probe="POST $post $(head -c 200 "$body" 2>/dev/null), /ready $ready $(head -c 200 "$ready_body" 2>/dev/null)"
    if [ "$post" = 201 ] && [ "$ready" = 200 ]; then
      ok=$((ok + 1))
      [ "$ok" -ge 2 ] && return 0
    else
      ok=0
    fi
    sleep 0.5
  done
  # A volume the kernel remounted read-only cannot be reopened by any store.
  if [ "$fault" = eio ] && grep -q " $data ext4 ro[ ,]" /proc/mounts; then
    last_resume_probe="$last_resume_probe; the kernel remounted $data read-only"
  fi
  return 1
}

# The store's failure series, from the metrics listener.
store_series() { curl -s -m 5 "http://127.0.0.1:$metrics_port/metrics" | grep '^ankusa_store_' || true; }

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
      dm_fail_data
      probe_until $(($(elapsed) + 2))
      dm_heal
      log "+$(elapsed)s: device healthy"
      if [ "$window" = 1 ]; then
        # The second window starts 5 s after the first one began.
        await_resume || fail "the node did not resume after window 1 (last answer: $last_resume_probe)"
        wait_until $((fault_at + 5))
      fi
    done
    ;;
  *) fail "FAULT must be enospc or eio, not $fault" ;;
esac

[ "$refused" -gt 0 ] || fail "no POST was answered 503 store_unavailable while the fault was on"
[ "$not_ready" -gt 0 ] || fail "/ready never answered 503 while the fault was on"
[ "$named" -gt 0 ] || fail "/ready answered 503 but never named a reason"

await_resume || fail "the node did not resume: a 201 and /ready 200 within 40 s of the fault clearing (last answer: $last_resume_probe)"
acked_at_resume=$(acked_now)
log "+$(elapsed)s: resumed (acked so far: $acked_at_resume)"

series=$(store_series)
echo "$series" >&2
grep -q '^ankusa_store_write_failures_total{' <<<"$series" || fail "no ankusa_store_write_failures_total series after the fault"
grep -q '^ankusa_store_reopen_total{' <<<"$series" || fail "no ankusa_store_reopen_total series after the fault"

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

if [ "$fault" = enospc ]; then
  # How much room the load left on the volume the store has to reopen on.
  df -k "$data" >&2
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
