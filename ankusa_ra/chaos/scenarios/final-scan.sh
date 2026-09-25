#!/usr/bin/env bash
# The final scan: every live record, as JSON, from a wal member. This is what
# `mix ankusa.chaos.verify --final` checks the acks against (I1/I2).
#
# `rpc` runs one expression on the running node and prints only what that
# expression prints, so the dump is printed explicitly with `IO.puts` — dump/1
# returns a JSON string, and the checker parses it with JSON.decode!.
#
# A scan that fails must fail the *scenario*: writing `[]` instead would report
# every ack as missing and send the reader looking for a data-loss bug that is
# really a transient read failure. So this retries a moment, shows the reason,
# and exits non-zero.
source /scenarios/lib.sh
OUT="${1:-/out/final.json}"
ERR=/tmp/final-scan.err

for _ in 1 2 3 4 5 6; do
  if docker exec "$(container_name wal-0)" /app/bin/ingest rpc \
       'IO.puts(Ankusa.WAL.Chaos.dump())' > "$OUT" 2>"$ERR"; then
    # More than the two bytes of an empty JSON array: the load ran, so there is
    # something to scan.
    if [ "$(wc -c < "$OUT")" -gt 3 ]; then
      exit 0
    fi
  fi
  sleep 5
done

echo "error: the final scan never returned any records" >&2
cat "$ERR" >&2
exit 1
