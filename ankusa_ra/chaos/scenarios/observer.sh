#!/usr/bin/env bash
# Follow the log with a cursor and record every seq it sees, in order, as
# `{:observe, seqs}` events. This is the evidence for I3: a reader that follows
# the log must see strictly increasing seqs and, at the end, must have missed
# nothing below the highest seq it saw.
#
# The plan has no bodies in it — reading the bytes is a separate step — so these
# events carry seqs only and feed I3 alone.
source /scenarios/lib.sh
OUT="${1:-/out/observer.jsonl}"
# Start from empty: this file is appended to as the run proceeds, and a rerun
# that inherited the previous run's observations would show a cursor going
# backwards (the new run starts at seq 1 again), which reads as a reader gap.
: > "$OUT"
cursor=0
end=$((SECONDS + ${OBSERVER_WINDOW_S:-600}))
while [ "$SECONDS" -lt "$end" ]; do
  now="$(date +%s%3N)"
  plan="$(docker exec "$(container_name wal-0)" /app/bin/ingest rpc \
    "IO.inspect(Ankusa.WAL.Ra.remote_aux([{:ankusa_wal_default, node()}], {:read_plan, ${cursor}, 200}))" \
    2>/dev/null || true)"
  # `{:ok, [{seq, index, pos}, ...]}` — the seqs, in plan order.
  seqs="$(printf '%s' "$plan" | grep -oE '\{ *[0-9]+,' | grep -oE '[0-9]+' || true)"
  next="$cursor"
  for s in $seqs; do
    [ "$s" -gt "$next" ] && next="$s"
  done
  if [ "$next" -gt "$cursor" ]; then
    list="$(printf '%s' "$seqs" | paste -sd, -)"
    printf '{"client":"observer","op":{"tag":"observe","0":[%s]},"invoked_at":%s,"completed_at":%s,"result":null}\n' \
      "$list" "$now" "$now" >> "$OUT"
    cursor="$next"
  fi
  sleep 0.25
done
