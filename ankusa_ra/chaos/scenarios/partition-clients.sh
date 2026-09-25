#!/usr/bin/env bash
# Isolate the edges from the WAL. The edges cannot commit, so they must answer
# 503 with a Retry-After — never 2xx (I8) — and resume when the partition heals.
source /scenarios/lib.sh
WINDOW="${FAULT_WINDOW_S:-120}"
mark_fault_begin
for e in "${EDGES[@]}"; do
  for w in "${WAL_NODES[@]}"; do
    partition "$e" "$w"
    partition "$w" "$e"
  done
done
sleep "$WINDOW"
heal_all
sleep 2
mark_fault_end
write_fault_windows "/out/${SCENARIO_NAME}-faults.json"
log "partition-clients healed"
