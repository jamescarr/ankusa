#!/usr/bin/env bash
# Large bodies during a partition: the report records the election count, so a
# run that thrashed the cluster is visible rather than silently slow.
source /scenarios/lib.sh
WINDOW="${FAULT_WINDOW_S:-120}"
leader="$(wal_leader_container)"
others=()
for n in "${WAL_NODES[@]}"; do [ "$n" = "$leader" ] || others+=("$n"); done
for b in "${others[@]}"; do
  partition "$leader" "$b"
  partition "$b" "$leader"
done
sleep "$WINDOW"
for b in "${others[@]}"; do
  heal "$leader" "$b"
  heal "$b" "$leader"
done
heal_all
log "big-bodies done"
