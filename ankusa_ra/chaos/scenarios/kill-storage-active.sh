#!/usr/bin/env bash
# Kill the worker holding the storage lease, including mid-segment: the
# compactor writes the segment, then the sidecar, then the cursor, so a kill
# anywhere in there leaves the cursor behind and the work is simply redone.
source /scenarios/lib.sh
INTERVAL="${INTERVAL:-25}"
WINDOW="${FAULT_WINDOW_S:-120}"
end=$((SECONDS + WINDOW))
while [ "$SECONDS" -lt "$end" ]; do
  holder="$(docker exec "$(container_name wal-0)" /app/bin/ingest rpc \
    'IO.inspect(elem(:ra.consistent_aux({:ankusa_wal_default, node()}, :overview, 5000), 1).leases[:storage].holder)' \
    2>/dev/null | sed -n 's/.*\(worker-[0-9]\).*/\1/p' | head -1 || true)"
  kill_container "${holder:-worker-0}"
  sleep "$INTERVAL"
  revive "${holder:-worker-0}"
done
log "kill-storage-active done"
