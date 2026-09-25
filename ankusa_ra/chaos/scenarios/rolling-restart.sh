#!/usr/bin/env bash
# Restart every member and every worker, one at a time. Nothing may be lost and
# no seq may be reused.
source /scenarios/lib.sh
for c in "${WAL_NODES[@]}"; do
  restart_container "$c"
  sleep 15
done
for c in "${WORKERS[@]}"; do
  restart_container "$c"
  sleep 10
done
for c in "${EDGES[@]}"; do
  restart_container "$c"
  sleep 5
done
log "rolling-restart done"
