#!/usr/bin/env bash
# Kill the Raft leader outright, every INTERVAL seconds, for the fault window.
# A majority remains, so the WAL elects a new leader and every ack made before
# the kill must still be readable after it (I1).
source /scenarios/lib.sh
INTERVAL="${INTERVAL:-20}"
WINDOW="${FAULT_WINDOW_S:-120}"
end=$((SECONDS + WINDOW))
while [ "$SECONDS" -lt "$end" ]; do
  victim="$(wal_leader_container)"
  kill_container "$victim"
  sleep "$INTERVAL"
  revive "$victim"
done
log "kill-leader done"
