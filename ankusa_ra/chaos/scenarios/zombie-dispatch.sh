#!/usr/bin/env bash
# Freeze the active dispatcher past its TTL, let the standby take over, then
# thaw it. The cluster must keep dispatching throughout, and the thawed zombie
# must not take the lease back.
#
# The fencing itself (I6/I9) needs cursor and lease writes in the history, which
# this harness does not produce — it is drill 6 of the Level-2 suite. What is
# checked here is the part the end-to-end view can see: every ack still reaches
# the sink while the lease changes hands twice.
source /scenarios/lib.sh
TTL_S="${TTL_S:-15}"
WINDOW="${FAULT_WINDOW_S:-120}"
end=$((SECONDS + WINDOW))
while [ "$SECONDS" -lt "$end" ]; do
  pause_container "$(active_worker_container)" "$((TTL_S + 5))"
  sleep 5
done
log "zombie-dispatch done"
