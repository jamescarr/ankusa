#!/usr/bin/env bash
# Fill one member's volume, then two. A member that cannot fsync must fail
# rather than ack; with one full, the majority still commits.
source /scenarios/lib.sh
WINDOW="${FAULT_WINDOW_S:-120}"
fill() {
  local c="$1"
  log "filling the volume of $c"
  # Write until ENOSPC; `dd` keeps what it wrote (fallocate rolls back on failure).
  docker exec "$(container_id "$c")" sh -c 'dd if=/dev/zero of=/data/fill bs=1M 2>/dev/null' || true
  avail="$(docker exec "$(container_id "$c")" df -P /data | awk 'NR==2 {print $4}')"
  if [ "$avail" -gt 1024 ]; then
    log "disk fill did not take on $c (${avail}KiB free); aborting"
    exit 1
  fi
}
clean() {
  local c="$1"
  docker exec "$(container_id "$c")" rm -f /data/fill 2>/dev/null || true
}
leader="$(wal_leader_container)"
victim="$(wal_members | grep -v "^${leader}$" | head -1)"
fill "$victim"
sleep $((WINDOW / 2))
second="$(wal_members | grep -v "^${leader}$" | grep -v "^${victim}$" | head -1)"
if [ -n "$second" ]; then fill "$second"; fi
# Two of three members cannot persist: the leader is a minority and cannot
# commit. That is the interval I8 applies to.
mark_fault_begin
sleep $((WINDOW / 2))
mark_fault_end
clean "$victim"
if [ -n "$second" ]; then clean "$second"; fi
write_fault_windows "/out/${SCENARIO_NAME}-faults.json"
log "disk-full cleaned"
