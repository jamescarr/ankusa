#!/usr/bin/env bash
# Give one member a wrong clock, for real. libfaketime (via LD_PRELOAD) shifts
# the whole BEAM's clock, so the member's `system_time` — the clock the machine
# reads for lease fencing — is genuinely off by SKEW_MS. A simulated
# `time_offset_ms` in the machine would not test the OS clock, which is the
# thing a real host has wrong after an NTP step.
#
# What this proves: a member with a clock 5s off keeps its place in the cluster
# (quorum, leadership, commits) and the cluster keeps acking. Token fencing is
# I9, and the harness emits no lease events, so that is checked where it can be
# observed exactly — the `clock-skew` drill in `wal_ra_faults_test.exs`.
source /scenarios/lib.sh
WINDOW="${FAULT_WINDOW_S:-120}"
SKEW_MS="${SKEW_MS:-5000}"

leader="$(wal_leader_container)"
log "skewing $leader by +${SKEW_MS}ms"
SKEW_S=$((SKEW_MS / 1000))

# Recreate only the leader with a faked clock. `recreate_service` passes the
# FAKETIME/LD_PRELOAD environment through the compose file (see x-wal-env).
# The Alpine package installs the library under /usr/lib/faketime, which is not
# on the loader's default path, so the full path is given. libfaketime's
# FAKETIME offset is whole seconds, so the skew is rounded down.
FAKETIME="+${SKEW_S}" LD_PRELOAD="/usr/lib/faketime/libfaketime.so.1" recreate_service "$leader"

# The skew must be observable or the fault never took: fail rather than pass on
# a member whose clock is actually right. The offset is applied in whole
# seconds, so the observed delta must be in that band — neither unobserved nor
# wildly larger than asked.
skewed="$(docker exec "$(container_name "$leader")" date +%s%3N)"
reference="$(date +%s%3N)"
delta=$((skewed - reference))
if [ "$delta" -lt "$((SKEW_MS / 2))" ] || [ "$delta" -gt "$((SKEW_MS * 2))" ]; then
  log "clock skew not observed (delta ${delta}ms); aborting"
  exit 1
fi
log "observed +${delta}ms skew on $leader"

sleep "$WINDOW"

# Heal: recreate the member with the clock back at the host's.
recreate_service "$leader"
wait_for_wal_leader 90
log "clock-skew done"
