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
# Passes only if the slowest cell is within 1.5x of the fastest and under 50 ms
# (lowest of 5 medians of 200 ticks per cell). Exits 1 otherwise. `mise run bench:gate` runs
# it after the ingest gate.

Logger.configure(level: :warning)
Code.require_file("tenant_seed.exs", __DIR__)

cells =
  Bench.TenantSeed.cells([1_000, 10_000, 100_000, 1_000_000], [10, 1_000, 100_000], fn instance,
                                                                                   _ords ->
    :ok = Ankusa.Metrics.Gauges.measure(instance)
  end)

Bench.TenantSeed.verdict("gauge tick (Ankusa.Metrics.Gauges.measure/1)", cells)
