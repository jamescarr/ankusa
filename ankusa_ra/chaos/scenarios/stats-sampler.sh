#!/usr/bin/env bash
# Sample `stats/1` every 250ms: next_seq, records, floor and the leases. The
# series is what shows a run's shape (a stall, a failover) next to its faults.
source /scenarios/lib.sh
OUT="${1:-/out/stats.jsonl}"
end=$((SECONDS + ${SAMPLER_WINDOW_S:-600}))
while [ "$SECONDS" -lt "$end" ]; do
  stats="$(docker exec "$(container_name wal-0)" /app/bin/ingest rpc \
    'IO.inspect(Ankusa.WAL.Ra.remote_aux([{:ankusa_wal_default, node()}], :overview))' 2>/dev/null || true)"
  printf '{"t":%s,"stats":%s}\n' "$(date +%s%3N)" \
    "$(printf '%s' "$stats" | jq -Rs .)" >> "$OUT"
  sleep 0.25
done
