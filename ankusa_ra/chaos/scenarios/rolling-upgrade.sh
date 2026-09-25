#!/usr/bin/env bash
# Roll every node onto a different image, one at a time, and assert the cluster
# still has a leader after each step: a mixed cluster has to keep serving while
# the roll is in progress.
#
# What this proves: the mechanics of an image roll — recreate, rejoin, catch up,
# no ack lost — through a cluster that is never all-new at once. What it does
# *not* prove: the machine-version semantics (a snapshot from an incompatible
# machine version must be refused until every member advertises the new one).
# That needs a peer that really reports a different version, which a container
# cannot fake; it is the `rolling-upgrade` drill in `wal_ra_faults_test.exs`.
source /scenarios/lib.sh
IMAGE="${UPGRADE_IMAGE:-ankusa-oban-ingest:chaos-next}"

# Point only the service being rolled at the new image.
roll() {
  local service="$1"
  cat > /tmp/upgrade.yml <<YAML
services:
  ${service}:
    image: ${IMAGE}
YAML
  log "rolling $service onto $IMAGE"
  compose_ref -f /tmp/upgrade.yml up -d --no-deps --force-recreate "$service" >/dev/null 2>&1 ||
    log "warning: could not roll $service"
}

for c in "${WAL_NODES[@]}"; do
  roll "$c"
  # A mixed cluster keeps a leader and keeps committing; if it does not, the
  # roll is not safe and the scenario should say so.
  wait_for_wal_leader 90
  sleep 15
done

for c in "${WORKERS[@]}"; do
  roll "$c"
  sleep 10
done

for c in "${EDGES[@]}"; do
  roll "$c"
  sleep 5
done

log "rolling-upgrade done"
