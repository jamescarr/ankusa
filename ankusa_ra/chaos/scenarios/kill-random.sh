#!/usr/bin/env bash
# Kill a random member every INTERVAL seconds. Weaker than kill-leader per
# event, but it hits followers too — including the one about to be promoted.
source /scenarios/lib.sh
INTERVAL="${INTERVAL:-15}"
WINDOW="${FAULT_WINDOW_S:-120}"
end=$((SECONDS + WINDOW))
while [ "$SECONDS" -lt "$end" ]; do
  victim="${WAL_NODES[$((RANDOM % ${#WAL_NODES[@]}))]}"
  kill_container "$victim"
  sleep "$INTERVAL"
done
log "kill-random done"
