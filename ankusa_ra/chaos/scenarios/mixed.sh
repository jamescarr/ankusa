#!/usr/bin/env bash
# The nightly schedule: a sequence of faults, back to back, so recovery from
# one overlaps the next. This is the scenario the nightly job runs.
source /scenarios/lib.sh
FAULT_WINDOW_S="${FAULT_WINDOW_S:-120}"
log "mixed: kill-leader"
FAULT_WINDOW_S=$((FAULT_WINDOW_S / 4)) INTERVAL=20 /scenarios/kill-leader.sh
log "mixed: pause-leader"
FAULT_WINDOW_S=$((FAULT_WINDOW_S / 4)) PAUSE_S=10 /scenarios/pause-leader.sh
log "mixed: kill-dispatch-active"
FAULT_WINDOW_S=$((FAULT_WINDOW_S / 4)) INTERVAL=25 /scenarios/kill-dispatch-active.sh
log "mixed: netem"
FAULT_WINDOW_S=$((FAULT_WINDOW_S / 4)) /scenarios/netem.sh
log "mixed done"
