#!/usr/bin/env bash
# Latency and loss on the WAL network: commands still commit, slower, and the
# client's own deadline is what decides when an append is reported as failed.
source /scenarios/lib.sh
WINDOW="${FAULT_WINDOW_S:-120}"
for w in "${WAL_NODES[@]}"; do
  latency "$w" 100ms 50ms 5%
done
sleep "$WINDOW"
for w in "${WAL_NODES[@]}"; do clear_latency "$w"; done
log "netem cleared"
