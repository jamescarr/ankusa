#!/usr/bin/env bash
# The chaos gate.
#
#   ./run.sh all          every scenario, one after another
#   ./run.sh mixed        the nightly schedule (a sequence of faults)
#   ./run.sh kill-leader  one scenario
#   ./run.sh power-loss   ...
#
# For each scenario: bring up the stack, run load, inject the fault from the
# `nemesis` container, then check the evidence — `mix loadgen.verify` against
# what the sink actually recorded, and `Ankusa.WAL.Checker` against the event
# history and the final scan. A report lands in `out/<scenario>-report.json`,
# and any violated invariant is a non-zero exit.
set -euo pipefail

cd "$(dirname "$0")"
ROOT="$(cd ../.. && pwd)"
COMPOSE=(docker compose -f docker-compose.yml)
PROJECT=ankusa-chaos
OUT=out
# The load generator runs from another directory, so it needs an absolute path —
# and it is the *same* directory the nemesis writes to over the /out mount
# (`./out:/out`), which `$OUT` alone would get wrong.
OUT_ABS="$(pwd)/$OUT"

: "${RATE:=200}"
: "${DURATION:=300}"
: "${FAULT_WINDOW_S:=120}"
: "${KEEP:=0}"
: "${SINK_DB:=postgres://ankusa:ankusa@localhost:5434/chaos}"
# The members a run has; `SINGLE=1` below narrows it to one.
: "${WAL_NODES:=wal-0 wal-1 wal-2}"

SCENARIOS=(
  kill-leader kill-random pause-leader
  partition-halves partition-leader-bridge partition-clients
  netem disk-full clock-skew
  kill-dispatch-active kill-storage-active zombie-dispatch
  rolling-restart rolling-upgrade
  power-loss replace-member big-bodies
)

# `SINGLE=1` runs the whole gate against one WAL node — the documented laptop
# shape, and a *one-member* Raft cluster, which elects itself and commits
# immediately. It needs no Erlang distribution between containers, so the gate
# can run where three-node distribution does not work (a sandbox, a laptop), and
# it still exercises everything the gate is made of: the load generator, the
# edge's 503-when-there-is-no-quorum path, the observer, the final scan, the
# checker and the two verifiers.
#
# What it does *not* exercise: replication itself — a majority surviving one
# member's loss. The scenarios that only make sense with peers (electing a new
# leader after a kill, partition majorities, membership change) are not part of
# this list; `kill-leader` and `kill-random` kill a member and leave it down,
# which a one-member cluster cannot recover from.
if [ "${SINGLE:-0}" = "1" ]; then
  export WAL_RA_MEMBERS="ankusa@wal-0"
  export WAL_NODES="wal-0"
  SCENARIOS=(power-loss rolling-restart)
  echo "==> SINGLE=1: one WAL member (no distribution needed); scenarios: ${SCENARIOS[*]}"
fi

mkdir -p "$OUT"

cleanup() {
  if [ "$KEEP" != "1" ]; then
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_for_stack() {
  echo "    waiting for the stack"
  # A fresh id per attempt, never a fixed one: the edge dedups, so a probe id
  # that has already been acked comes back `200` (duplicate) forever after and a
  # loop waiting for `201` can never be satisfied again — which is how this hung
  # on a stack that was serving perfectly well.
  for attempt in $(seq 1 90); do
    code="$(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8080/webhooks/demo \
      -H 'content-type: application/json' \
      -d "{\"id\":\"probe-${attempt}-${RANDOM}\",\"pad\":\"\"}" || true)"
    # A 201 means an append went through, which means quorum and a leader; the
    # second check just says which half is lagging. `:ra.members/2` answers
    # `{:ok, members, leader}` and the leader is nil until one is elected.
    leader="$(docker exec "$("${COMPOSE[@]}" ps -q wal-0)" /app/bin/ingest rpc \
      'IO.puts(match?({:ok, _, l} when l != nil, :ra.members({:ankusa_wal_default, node()})))' \
      2>/dev/null | tail -1 || true)"
    if [ "$code" = "201" ] && [ "${leader:-false}" = "true" ]; then
      return 0
    fi
    sleep 2
  done
  echo "error: the stack never became ready (last status $code)" >&2
  return 1
}

run_scenario() {
  scenario="$1"
  echo "==> scenario $scenario"

  # Per-scenario evidence, cleared first: some writers append (the observer, the
  # stats sampler), so a rerun without this mixes two runs into one history and
  # the checker reports gaps that are really the previous run's seqs.
  #
  # Named one suffix at a time rather than with `"$OUT/${scenario}"-*.jsonl`,
  # which would also match a *longer* scenario's files (`kill-leader` matching
  # `kill-leader-bridge-*`).
  for suffix in observer.jsonl loadgen-events.jsonl stats.jsonl events.jsonl \
    loadgen.json final.json faults.json deliveries.json report.json fault.log; do
    rm -f "$OUT/${scenario}-${suffix}"
  done

  rm -f "$OUT/${scenario}.csv"

  "${COMPOSE[@]}" up -d --build >/dev/null

  # `rolling-upgrade` rolls onto a second image tag. Tag the freshly built image
  # as that tag so the roll is a real image change (a new image id on the
  # container) rather than a restart of the same one.
  if [ "$scenario" = "rolling-upgrade" ]; then
    docker tag ankusa-oban-ingest:chaos "${UPGRADE_IMAGE:-ankusa-oban-ingest:chaos-next}"
  fi

  # The members that are not part of the cluster would boot with a `:members`
  # list that excludes themselves; stop them rather than let them crash-loop.
  if [ "${SINGLE:-0}" = "1" ]; then
    "${COMPOSE[@]}" stop wal-1 wal-2 >/dev/null 2>&1 || true
  fi

  wait_for_stack

  # The observers run inside the network, next to the WAL, and write into the
  # mounted out/ directory.
  docker exec "$("${COMPOSE[@]}" ps -q nemesis)" \
    bash -c "OBSERVER_WINDOW_S=$((DURATION + FAULT_WINDOW_S + 120)) /scenarios/observer.sh /out/${scenario}-observer.jsonl" &
  observer_pid=$!
  docker exec "$("${COMPOSE[@]}" ps -q nemesis)" \
    bash -c "SAMPLER_WINDOW_S=$((DURATION + FAULT_WINDOW_S + 120)) /scenarios/stats-sampler.sh /out/${scenario}-stats.jsonl" &
  sampler_pid=$!

  # The fault runs concurrently with the load: that is the whole point.
  docker exec "$("${COMPOSE[@]}" ps -q nemesis)" \
    bash -c "WAL_NODES='$WAL_NODES' FAULT_WINDOW_S=$FAULT_WINDOW_S /scenarios/${scenario}.sh" \
    > "$OUT/${scenario}-fault.log" 2>&1 &
  fault_pid=$!

  (cd "$ROOT/tools/loadgen" && mix loadgen.run \
    --url http://localhost:8080/webhooks/demo \
    --rate "$RATE" \
    --duration "$DURATION" \
    --dup-ratio 0.1 \
    --nil-key-ratio 0.2 \
    --out "$OUT_ABS/${scenario}.csv" \
    --report "$OUT_ABS/${scenario}-loadgen.json" \
    --events "$OUT_ABS/${scenario}-loadgen-events.jsonl")

  wait "$fault_pid" || true
  kill "$observer_pid" "$sampler_pid" 2>/dev/null || true
  wait "$observer_pid" "$sampler_pid" 2>/dev/null || true

  echo "    verifying"
  (cd "$ROOT/tools/loadgen" && mix loadgen.verify \
    --acked "$OUT_ABS/${scenario}.csv" \
    --database-url "$SINK_DB" \
    --timeout 300 \
    --report "$OUT_ABS/${scenario}-deliveries.json")

  docker exec "$("${COMPOSE[@]}" ps -q nemesis)" bash -c "/scenarios/final-scan.sh /out/${scenario}-final.json"

  # One event history: what the edges answered (the loadgen) and what a
  # cursor-following reader saw (the observer), ordered by when it happened.
  # The observer only writes when it sees something, so the file may not exist.
  touch "$OUT/${scenario}-observer.jsonl"
  # `cat … | jq -s` rather than `jq -s file file`: jaq slurps each *file* into its
  # own array while jq slurps the whole input, so only the piped form gives one
  # array — and therefore one merged, time-ordered history — on both.
  cat "$OUT/${scenario}-loadgen-events.jsonl" "$OUT/${scenario}-observer.jsonl" \
    | jq -s -c 'sort_by(.invoked_at)[]' \
    > "$OUT/${scenario}-events.jsonl"

  # macOS ships bash 3.2, where an empty array under `set -u` is an unbound
  # variable; `[@]+` expansion is the portable way to pass zero arguments.
  FAULT_ARG=()
  if [ -f "$OUT/${scenario}-faults.json" ]; then
    FAULT_ARG=(--fault "$OUT_ABS/${scenario}-faults.json")
  fi

  # Absolute paths: this runs from `ankusa_ra/`, not from here, so a relative
  # `$OUT` would name a directory that nothing wrote to.
  (cd "$ROOT/ankusa_ra" && mix ankusa.chaos.verify \
    --events "$OUT_ABS/${scenario}-events.jsonl" \
    --acked "$OUT_ABS/${scenario}.csv" \
    --final "$OUT_ABS/${scenario}-final.json" \
    "${FAULT_ARG[@]+"${FAULT_ARG[@]}"}" \
    --report "$OUT_ABS/${scenario}-report.json")

  echo "    $scenario: passed"
}

if [ "${1:-mixed}" = "all" ]; then
  for s in "${SCENARIOS[@]}"; do
    run_scenario "$s"
    cleanup
  done
else
  run_scenario "${1:-mixed}"
fi

echo "==> every scenario passed; reports in $OUT/"
