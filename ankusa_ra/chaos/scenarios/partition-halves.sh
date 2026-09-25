#!/usr/bin/env bash
# Split the cluster so the leader is in the minority: it must stop acking, and
# the majority must elect a new leader and keep serving.
source /scenarios/lib.sh
WINDOW="${FAULT_WINDOW_S:-120}"
leader="$(wal_leader_container)"
others=()
for n in "${WAL_NODES[@]}"; do [ "$n" = "$leader" ] || others+=("$n"); done
log "leader $leader in the minority, majority: ${others[*]}"
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
log "partition-halves healed"
