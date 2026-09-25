#!/usr/bin/env bash
# Large bodies under plain load: `run.sh` passes `--body-bytes 1048576..8388608
# --big-ratio 0.2` to the load generator for this scenario, so the cluster must
# fsync and replicate multi-megabyte records for the whole window. No fault is
# injected — the point is that big bodies survive a plain run without stalling
# the pipeline or blowing the segment budget.
source /scenarios/lib.sh
WINDOW="${FAULT_WINDOW_S:-120}"
sleep "$WINDOW"
log "big-bodies done"
