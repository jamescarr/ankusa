#!/usr/bin/env bash
# Remove a member, wipe its volume, and add a fresh one: the newcomer cannot
# have the log, so it must catch up through snapshot install (I1).
#
# The container has to be *removed* before its volume can be: a stopped
# container still holds the volume, so `docker volume rm` would silently do
# nothing and the "fresh" member would come back with its old data — the
# scenario would prove nothing while appearing to pass.
source /scenarios/lib.sh
leader="$(wal_leader_container)"
victim="$(wal_members | grep -v "^${leader}$" | head -1)"

if [ -z "$victim" ]; then
  log "no member other than the leader ($leader): nothing to replace"
  exit 0
fi

log "replacing $victim"
docker exec "$(container_name wal-0)" /app/bin/ingest rpc \
  "IO.inspect(:ra.remove_member({:ankusa_wal_default, node()}, {:ankusa_wal_default, :\"ankusa@${victim}\"}, 30000))" \
  >/dev/null 2>&1 || true

# Take the container away, then wipe its named volume, then let compose bring
# it back. A stopped container still holds the volume, so the explicit
# `docker volume rm` is what makes the newcomer genuinely fresh: it has never
# seen the log and must be caught up by a snapshot.
compose_ref rm -sf "$victim" >/dev/null 2>&1 || true
docker volume rm "${PROJECT}_${victim}-data" >/dev/null 2>&1 ||
  log "warning: could not remove ${PROJECT}_${victim}-data (still in use?)"

compose_ref up -d --no-deps "$victim" >/dev/null 2>&1 || log "warning: $victim did not come back"

# The re-added member has to be in the member list to be caught up.
sleep 10
docker exec "$(container_name wal-0)" /app/bin/ingest rpc \
  "IO.inspect(:ra.add_member({:ankusa_wal_default, node()}, {:ankusa_wal_default, :\"ankusa@${victim}\"}, 30000))" \
  >/dev/null 2>&1 || true

wait_for_wal_leader 90
log "replace-member done"
