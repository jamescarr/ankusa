#!/usr/bin/env bash
# Kill every member at once, then bring them back. This is the drill that
# separates "replicated" from "durable": everything acked before the kill must
# still be readable afterwards (I1), and nothing un-acked may appear.
source /scenarios/lib.sh
mark_fault_begin
for w in "${WAL_NODES[@]}"; do kill_container "$w"; done
sleep 5
for w in "${WAL_NODES[@]}"; do
  id="$(docker ps -aq -f "label=com.docker.compose.project=${PROJECT}" -f "label=com.docker.compose.service=$w" | head -1)"
  [ -n "$id" ] && docker start "$id" >/dev/null || true
done
wait_for_wal_leader 90
mark_fault_end
write_fault_windows "/out/${SCENARIO_NAME}-faults.json"
log "power-loss: all members restarted"
