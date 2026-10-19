# The queue gauge tick against the size of the queue (ADR 0001, Proof).
#
#     MIX_ENV=test mix run bench/gauges_bench.exs
#
# `Ankusa.Metrics.Gauges.measure/1` reads the node's counters and the first of
# the active index, so its cost must not depend on how many rows are queued or
# how many tenants exist. Two axes, each cell a fresh store with the index and
# counters a commit would have written, measured while a process commits 3 000
# batches of counter merges a second:
#
#   rows     10^3, 10^4, 10^5, 10^6 due rows over 10 tenants
#   tenants  10, 10^3, 10^5 tenants, 8 of them with due rows (the others' rows
#            are scheduled a day ahead)
#
# Passes only if the slowest cell is within 2x of the fastest (counted as at
# least 1 ms: see `Bench.TenantSeed.verdict/2` for why a store a thousand times
# bigger may cost a little more per seek) and under 50 ms (a cell is the 10th
# percentile of 1 000 ticks, the lowest of 3 rounds: see
# `Bench.TenantSeed.quiet_ms/3`). Exits 1 otherwise. `mise run bench:gate` runs
# it before the ingest gate.

Logger.configure(level: :warning)
Code.require_file("tenant_seed.exs", __DIR__)

cells =
  Bench.TenantSeed.cells([1_000, 10_000, 100_000, 1_000_000], [10, 1_000, 100_000], fn instance,
                                                                                       _ords ->
    :ok = Ankusa.Metrics.Gauges.measure(instance)
  end)

Bench.TenantSeed.verdict("gauge tick (Ankusa.Metrics.Gauges.measure/1)", cells)
