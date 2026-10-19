defmodule Ankusa.Store.Migrations.DlqBytes do
  @moduledoc false

  # Store format 4 -> 5 (DUR-2.b): the per-tenant `dead_bytes` counter (field 8 of
  # the `counters` family), the stored size of the dead-letter queue that
  # `queue.dlq.max_bytes` is checked against in one read.
  #
  # The keys keep their layout. What a format 4 store lacks is the byte count: one
  # step reads up to `@batch` dead keys after its cursor (the last `?x` key it
  # counted; `nil` on the first step), reads their delivery rows for the stored
  # size, and merges it into the tenant's and the node's `dead_bytes` counter, the
  # same merge path as runtime. A dead key carries its tenant ordinal in its
  # value, so no tenant lookup is needed. `Ankusa.Store.Migrate` commits a step's
  # ops and its cursor in one synced batch, so a step is counted whole or not at
  # all and a resumed migration never counts a row twice. A dead row whose
  # delivery row is gone counts 0 (the row was an orphan; it counted nothing in
  # `bytes` either).

  @behaviour Ankusa.Store.Migration

  alias Ankusa.Queue.Deliveries
  alias Ankusa.Queue.Deliveries.Counters
  alias Ankusa.Store
  alias Ankusa.Store.Keys

  @batch 500

  @impl true
  def describe,
    do: "count the stored bytes of the dead-letter queue per tenant (counter `dead_bytes`)"

  @impl true
  def step(instance, cursor) do
    %{lo: lo, hi: hi} = Keys.family(:dead)
    from = if is_nil(cursor), do: lo, else: cursor <> <<0>>

    with {:ok, rows} <- dead_rows(instance, {from, hi}),
         {:ok, ops} <- count(instance, rows) do
      if length(rows) < @batch,
        do: {:ok, ops, :done},
        else: {:ok, ops, {:more, rows |> List.last() |> elem(0)}}
    end
  end

  # `[{dead_key, ord}]`, oldest first.
  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  defp dead_rows(instance, range) do
    result =
      Store.fold(instance, :dead, range, [], fn key, value, acc ->
        {_source_id, _id, ord} = :erlang.binary_to_term(value)
        acc = [{key, ord} | acc]
        if length(acc) >= @batch, do: {:halt, acc}, else: {:cont, acc}
      end)

    with {:ok, rows} <- result, do: {:ok, Enum.reverse(rows)}
  end

  defp count(_instance, []), do: {:ok, []}

  defp count(instance, rows) do
    keys =
      Enum.map(rows, fn {key, _ord} ->
        {_at, seq, sink} = Keys.decode_dead(key)
        Keys.delivery(seq, sink)
      end)

    with {:ok, delivery_rows} <- Store.multi_get(instance, :deliveries, keys) do
      deltas =
        rows
        |> Enum.zip(delivery_rows)
        |> Enum.map(fn {{_key, ord}, row} -> {ord, :dead_bytes, size(row)} end)

      {:ok, Deliveries.coalesce(Counters.ops(deltas))}
    end
  end

  defp size({:ok, bin}) do
    case Deliveries.decode_row(bin) do
      {:ok, %{size: size}} when is_integer(size) -> size
      _ -> 0
    end
  end

  defp size(:not_found), do: 0
end
