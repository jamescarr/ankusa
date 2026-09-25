#!/usr/bin/env bash
# Shared helpers for the chaos scenarios. Sourced by each scenario script, which
# runs *inside* the nemesis container: it can reach the other containers by
# name over the compose network, tc/iptables them, and kill them through the
# mounted Docker socket.
set -euo pipefail

PROJECT="${PROJECT:-ankusa-chaos}"
# Which members exist in this run. `SINGLE=1` (see run.sh) is one node, and the
# scenarios must not try to kill members that were never started.
WAL_NODES=(${WAL_NODES:-wal-0 wal-1 wal-2})
WORKERS=("worker-0" "worker-1")
EDGES=("edge-1" "edge-2" "edge-3")

log() { echo "[nemesis] $*" >&2; }

# The scenario name, so a scenario can name its own evidence files.
SCENARIO_NAME="${SCENARIO_NAME:-$(basename "${0:-scenario}" .sh)}"

container_id() { docker ps -q -f "label=com.docker.compose.project=${PROJECT}" -f "label=com.docker.compose.service=$1" | head -1; }
container_name() { docker ps --format '{{.Names}}' -f "label=com.docker.compose.project=${PROJECT}" -f "label=com.docker.compose.service=$1" | head -1; }

# The node the cluster currently calls leader, asked of the cluster itself.
# `:ra.members/2` answers `{:ok, members, leader}` and the leader is that third
# element — `nil` when there is none. (There is no `:ra.leader/1`; a grep over
# the members list would hand back whichever member happens to be listed last,
# which is not the same thing and makes `kill-leader` kill a follower.)
WAL_LEADER_EXPR='case :ra.members({:ankusa_wal_default, node()}) do {:ok, _, {_c, n}} when n != nil -> IO.puts(:erlang.atom_to_list(n)); _ -> IO.puts("none") end'

wal_leader_node() {
  docker exec "$(container_name wal-0)" /app/bin/ingest rpc "$WAL_LEADER_EXPR" 2>/dev/null \
    | tail -1 || true
}

# That node's container name: `ankusa@wal-1` -> `wal-1`.
wal_leader_container() {
  local node
  node="$(wal_leader_node)"
  case "$node" in
    *@wal-*) echo "${node##*@}" ;;
    *) echo "wal-0" ;;
  esac
}

# The worker that holds the dispatch lease. Both workers report it through the
# replicated state, so any wal node can answer.
active_worker_container() {
  local holder
  holder="$(docker exec "$(container_name wal-0)" /app/bin/ingest rpc \
      'IO.inspect(elem(:ra.consistent_aux({:ankusa_wal_default, node()}, :overview, 5000), 1).leases[:dispatch].holder)' \
      2>/dev/null | sed -n 's/.*\(worker-[0-9]\).*/\1/p' | head -1 || true)"
  echo "${holder:-worker-0}"
}

# ── quorum-outage windows ─────────────────────────────────────────────────
# The interval during which *no* quorum can exist, as epoch milliseconds. The
# checker needs it for I8: inside such a window an edge must answer 503 and
# must never 2xx. Only the scenarios that take the whole cluster (or the
# edges' whole path to it) out of service mark a window — a partition that
# leaves a majority on one side is *not* a no-quorum interval, because the
# majority elects a leader and legitimately keeps acking. Marking it would
# turn a correct ack into a false violation; the Level-2 suite covers the
# minority-leader case, where it can know the exact moment the leader was
# fenced.
FAULT_WINDOWS=()
_fault_open=""

mark_fault_begin() {
  _fault_open="$(date +%s%3N)"
  log "quorum outage opens at $_fault_open"
}

mark_fault_end() {
  [ -n "$_fault_open" ] || return 0
  local t
  t="$(date +%s%3N)"
  FAULT_WINDOWS+=("{\"from\":${_fault_open},\"to\":${t}}")
  _fault_open=""
  log "quorum outage closes at $t"
}

write_fault_windows() {
  local out="${1:?}"
  local IFS=,
  printf '[%s]\n' "${FAULT_WINDOWS[*]:-}" > "$out"
  log "wrote $(wc -c < "$out") bytes of outage windows to $out"
}

# Wait until the cluster has a leader again, so a window closes when quorum is
# genuinely back rather than when the containers were merely restarted.
wal_has_leader() {
  [ "$(wal_leader_node)" != "none" ]
}

wait_for_wal_leader() {
  local deadline=$((SECONDS + ${1:-60}))
  until wal_has_leader; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      log "no leader after ${1:-60}s"
      return 1
    fi
    sleep 1
  done
  log "leader elected"
}

# The stack's compose file, mounted read-only at /chaos (see docker-compose.yml).
# Needed to *recreate* a service — a fresh volume, a changed env — which the
# Docker socket alone cannot do without reconstructing the container by hand.
COMPOSE_REF="/chaos/docker-compose.yml"

compose_ref() { docker compose -p "$PROJECT" -f "$COMPOSE_REF" "$@"; }

# Recreate one service with a fresh container (and optionally a changed env),
# keeping its named volume unless `fresh_volume` says otherwise.
recreate_service() {
  local service="$1"
  shift
  log "recreating $service $*"
  compose_ref up -d --no-deps --force-recreate "$@" "$service" >/dev/null 2>&1 ||
    log "warning: could not recreate $service"
}

kill_container() {
  local id
  id="$(container_id "$1")"
  [ -n "$id" ] || { log "$1 not found"; return 0; }
  log "kill -9 $1"
  docker kill --signal=KILL "$id" >/dev/null || true
}

pause_container() {
  local id
  id="$(container_id "$1")"
  [ -n "$id" ] || return 0
  log "SIGSTOP $1 for ${2:-5}s"
  docker kill --signal=STOP "$id" >/dev/null || true
  sleep "${2:-5}"
  log "SIGCONT $1"
  docker kill --signal=CONT "$id" >/dev/null || true
}

restart_container() {
  local id
  id="$(container_id "$1")"
  [ -n "$id" ] || return 0
  log "restart $1"
  docker restart "$id" >/dev/null
}

# The wal members in the order the cluster reports them.
wal_members() {
  docker exec "$(container_name wal-0)" /app/bin/ingest rpc \
    'IO.inspect(:ra.members({:ankusa_wal_default, node()}))' 2>/dev/null \
    | grep -o 'wal-[0-9]' | sort -u
}

# ── network faults ─────────────────────────────────────────────────────────

# Drop traffic between two containers, in both directions. The containers are
# addressed by IP because iptables does not resolve compose service names.
ip_of() { getent hosts "$1" | awk '{print $1}' | head -1; }

partition() {
  local a="$1" b="$2"
  local aid bid
  aid="$(container_id "$a")"
  bid="$(ip_of "$b")"
  log "partition $a <-> $b"
  docker exec "$aid" iptables -A INPUT -s "$bid" -j DROP || true
  docker exec "$aid" iptables -A OUTPUT -d "$bid" -j DROP || true
}

heal() {
  local a="$1" b="$2"
  local aid bid
  aid="$(container_id "$a")"
  bid="$(ip_of "$b")"
  log "heal $a <-> $b"
  docker exec "$aid" iptables -D INPUT -s "$bid" -j DROP 2>/dev/null || true
  docker exec "$aid" iptables -D OUTPUT -d "$bid" -j DROP 2>/dev/null || true
}

heal_all() {
  for c in "${WAL_NODES[@]}" "${WORKERS[@]}" "${EDGES[@]}"; do
    local id
    id="$(container_id "$c")"
    [ -n "$id" ] || continue
    docker exec "$id" iptables -F 2>/dev/null || true
  done
}

latency() {
  local c="$1" delay="${2:-100ms}" jitter="${3:-50ms}" loss="${4:-5%}"
  local id
  id="$(container_id "$c")"
  log "netem on $c: delay $delay jitter $jitter loss $loss"
  docker exec "$id" tc qdisc replace dev eth0 root netem delay "$delay" "$jitter" loss "$loss" || true
}

clear_latency() {
  local id
  id="$(container_id "$1")"
  docker exec "$id" tc qdisc del dev eth0 root 2>/dev/null || true
}
