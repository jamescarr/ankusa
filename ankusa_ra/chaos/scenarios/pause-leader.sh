#!/usr/bin/env bash
# A frozen leader stops sending heartbeats, so the followers elect a new one
# while the old one is still convinced it leads. On resume it must find out.
source /scenarios/lib.sh
PAUSE_S="${PAUSE_S:-10}"   # > 2 x the election timeout
WINDOW="${FAULT_WINDOW_S:-120}"
end=$((SECONDS + WINDOW))
while [ "$SECONDS" -lt "$end" ]; do
  pause_container "$(wal_leader_container)" "$PAUSE_S"
  sleep 5
done
log "pause-leader done"
