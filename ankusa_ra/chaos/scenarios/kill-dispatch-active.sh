#!/usr/bin/env bash
# Kill the worker holding the dispatch lease. Its standby must take over within
# ttl + ttl/3 (I9), and no acked record may be lost (I1).
source /scenarios/lib.sh
INTERVAL="${INTERVAL:-25}"
WINDOW="${FAULT_WINDOW_S:-120}"
end=$((SECONDS + WINDOW))
while [ "$SECONDS" -lt "$end" ]; do
  victim="$(active_worker_container)"
  kill_container "$victim"
  sleep "$INTERVAL"
  revive "$victim"
done
log "kill-dispatch-active done"
