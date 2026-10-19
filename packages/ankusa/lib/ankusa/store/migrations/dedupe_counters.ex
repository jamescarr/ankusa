defmodule Ankusa.Store.Migrations.DedupeCounters do
  @moduledoc false

  # Store format 3 -> 4 (DUR-2): the per-tenant `dedupe` counter (field 7 of the
  # `counters` family), the count of live ingest-dedupe keys that
  # `dedupe.max_entries` and `dedupe.max_entries_per_tenant` are checked against
  # in one read.
  #
  # The keys themselves keep their layout. What a format 3 store lacks is the
  # count: every `?e` expiry key is one live `?u` key (the writer puts and
  # deletes them together), so one step reads up to `@batch` expiry keys after
  # its cursor (the last `?e` key it counted; `nil` on the first step), resolves
  # their tenants' ordinals, and merges `+1` into each tenant's and the node's
  # `dedupe` counter, the same merge path as runtime. `Ankusa.Store.Migrate`
  # commits a step's ops and its cursor in one synced batch, so a step is either
  # counted whole or not at all and a resumed migration never counts a key
  # twice. A tenant with no ordinal (none can have a key, but a hand-edited
  # store might) is counted in the node aggregate alone.

  @behaviour Ankusa.Store.Migration

  alias Ankusa.Queue.Deliveries
  alias Ankusa.Queue.Deliveries.Counters
  alias Ankusa.Queue.Tenants
  alias Ankusa.Store
  alias Ankusa.Store.Keys

  @batch 500

  @impl true
  def describe, do: "count the live ingest-dedupe keys per tenant (counter `dedupe`)"

  @impl true
  def step(instance, cursor) do
    %{lo: lo, hi: hi} = Keys.family(:dedupe_expiry)
    from = if is_nil(cursor), do: lo, else: cursor <> <<0>>

    with {:ok, keys} <- expiry_keys(instance, {from, hi}),
         {:ok, ops} <- count(instance, keys) do
      if length(keys) < @batch,
        do: {:ok, ops, :done},
        else: {:ok, ops, {:more, List.last(keys)}}
    end
  end

  defp expiry_keys(instance, range) do
    result =
      Store.fold(instance, :dedupe_expiry, range, [], fn key, _value, acc ->
        acc = [key | acc]
        if length(acc) >= @batch, do: {:halt, acc}, else: {:cont, acc}
      end)

    with {:ok, keys} <- result, do: {:ok, Enum.reverse(keys)}
  end

  defp count(_instance, []), do: {:ok, []}

  defp count(instance, keys) do
    tenants =
      for key <- keys,
          {_at, tenant, _source, _key} <- [Keys.decode_dedupe_expiry(key)],
          do: Tenants.normalize(tenant)

    with {:ok, ords} <- Tenants.lookup(instance, Enum.uniq(tenants)) do
      {known, unknown} =
        tenants
        |> Enum.map(&Map.get(ords, &1))
        |> Enum.split_with(&is_integer/1)

      deltas = Enum.map(known, &{&1, :dedupe, 1})

      {:ok,
       Deliveries.coalesce(Counters.ops(deltas) ++ Counters.node_ops(dedupe: length(unknown)))}
    end
  end
end
