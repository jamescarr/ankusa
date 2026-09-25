#!/usr/bin/env bash
# Cut only the leader → one follower link: the majority can still elect, but the
# leader's log is now fed by a different set of peers.
source /scenarios/lib.sh
WINDOW="${FAULT_WINDOW_S:-120}"
leader="$(wal_leader_container)"
other="$(wal_members | grep -v "^${leader}$" | head -1)"
partition "$leader" "$other"
sleep "$WINDOW"
heal "$leader" "$other"
heal_all
log "partition-leader-bridge healed"
