# The dispatch scan's reads against the size of the queue (ADR 0001, Proof).
#
#     MIX_ENV=test mix run bench/dispatch_tick_bench.exs
#
# A scan is `Deliveries.active_tenants/3` (the tenants with a due-row marker
# at or before now) plus `Deliveries.due_for_tenant/6` for each of them, up to a
# page of 128 rows. It must cost the same however many rows are queued and
# however many tenants exist: the number of tenants with work due is what
# counts. The same two axes and the same pass rule as `gauges_bench.exs`,
# measured under the same load of counter merges.

Logger.configure(level: :warning)
Code.require_file("tenant_seed.exs", __DIR__)

cells =
  Bench.TenantSeed.cells([1_000, 10_000, 100_000, 1_000_000], [10, 1_000, 100_000], fn instance,
                                                                                       _ords ->
    now = System.system_time(:millisecond)
    {:ok, active} = Ankusa.Queue.Deliveries.active_tenants(instance, now, 4_096)

    for {ord, _buckets} <- active do
      {:ok, rows} =
        Ankusa.Queue.Deliveries.due_for_tenant(instance, ord, now, 0, 128, 134_217_728)

      true = rows != []
    end
  end)

Bench.TenantSeed.verdict("dispatch tick (active_tenants + due_for_tenant)", cells)
