#!/usr/bin/env bash
# Give the leader and the active dispatcher wrong clocks, for real.
#
# `faketime` cannot help here: it changes the clock of processes it *starts*, and
# the BEAM is already running. What the machine actually reads is its own view of
# the clock, `meta.system_time + time_offset_ms`, so the offset is the honest way
# to make a member's clock wrong — the same knob the Level-2 drill uses.
#
# What this proves: a member with a clock 30s off keeps its place in the cluster
# (quorum, leadership, commits) and the cluster keeps acking. Token fencing is
# I9, and the harness emits no lease events, so that is checked where it can be
# observed exactly — the `clock-skew` drill in `wal_ra_faults_test.exs`.
source /scenarios/lib.sh
WINDOW="${FAULT_WINDOW_S:-120}"
SKEW_MS="${SKEW_MS:-30000}"

leader="$(wal_leader_container)"
worker="$(active_worker_container)"
log "skewing $leader by +${SKEW_MS}ms and $worker by -${SKEW_MS}ms"

WAL_RA_TIME_OFFSET_MS="$SKEW_MS" recreate_service "$leader"
WAL_RA_TIME_OFFSET_MS="-$SKEW_MS" recreate_service "$worker"

sleep "$WINDOW"

# Heal: recreate both with the offset back at zero.
recreate_service "$leader"
recreate_service "$worker"
wait_for_wal_leader 90
log "clock-skew done"
