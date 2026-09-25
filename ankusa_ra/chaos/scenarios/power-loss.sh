#!/usr/bin/env bash
# Kill every member at once, then bring them back. This is the drill that
# separates "replicated" from "durable": everything acked before the kill must
# still be readable afterwards (I1), and nothing un-acked may appear.
#
# No quorum window is marked here. `disk-full` is the I8 (shedding) drill; this
# one is durability. Its outage is short — the members come straight back — so
# the edge holds in-flight appends until quorum returns instead of shedding,
# and marking a window would leave I8 with no evidence and fail the gate on
# "not exercised".
source /scenarios/lib.sh
for w in "${WAL_NODES[@]}"; do kill_container "$w"; done
sleep 5
for w in "${WAL_NODES[@]}"; do
  id="$(docker ps -aq -f "label=com.docker.compose.project=${PROJECT}" -f "label=com.docker.compose.service=$w" | head -1)"
  [ -n "$id" ] && docker start "$id" >/dev/null || true
done
wait_for_wal_leader 90
log "power-loss: all members restarted"
