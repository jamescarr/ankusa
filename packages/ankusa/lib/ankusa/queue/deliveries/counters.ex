defmodule Ankusa.Queue.Deliveries.Counters do
  @moduledoc false

  # Per-tenant queue counters (ADR 0001, section 4): seven `counters` rows per
  # tenant, plus the same seven under ordinal 0 for the node. Every row is changed
  # only by blind merge operands (`{:merge, :counters, key, delta}`), in the same
  # batch as the index change it describes, so concurrent writers (the Writer,
  # the Pipeline, the Replayer, the archive compactor) cannot lose a count and a
  # crash leaves rows and counts together or neither.

  alias Ankusa.Store
  alias Ankusa.Store.Keys

  @fields %{
    pending: 1,
    scheduled: 2,
    inflight: 3,
    dead: 4,
    bytes: 5,
    archive_pending: 6,
    dedupe: 7
  }

  @names Map.keys(@fields)

  # `dedupe` counts the live `?u` keys (DUR-2): the writer adds one for every
  # key it inserts, the `Ankusa.Queue.DedupeSweeper` takes one off for every
  # key it expires, so `dedupe.max_entries` is checked from one counter read.
  @type field ::
          :pending | :scheduled | :inflight | :dead | :bytes | :archive_pending | :dedupe
  @type delta :: {ord :: non_neg_integer(), field(), integer()}

  @doc "The counter fields, in `Keys.counter/2` order."
  @spec fields() :: [field()]
  def fields, do: Enum.sort_by(@names, &Map.fetch!(@fields, &1))

  @doc """
  The merge ops for `deltas`, coalesced per `{ord, field}`, plus the same
  deltas added into the node aggregate (ordinal 0). Zero deltas write nothing.
  """
  @spec ops([delta()]) :: [Store.op()]
  def ops([]), do: []

  def ops(deltas) do
    per_tenant =
      Enum.reduce(deltas, %{}, fn {ord, field, delta}, acc ->
        Map.update(acc, {ord, field}, delta, &(&1 + delta))
      end)

    aggregate =
      Enum.reduce(per_tenant, %{}, fn {{_ord, field}, delta}, acc ->
        Map.update(acc, field, delta, &(&1 + delta))
      end)

    merges(per_tenant, fn {ord, field} -> {ord, field} end) ++
      merges(aggregate, fn field -> {0, field} end)
  end

  @doc """
  Merge ops on the node aggregate (ordinal 0) alone, for a change whose tenant
  has no ordinal: `node_ops(dedupe: -1)`.
  """
  @spec node_ops([{field(), integer()}]) :: [Store.op()]
  def node_ops(deltas) do
    for {field, delta} <- deltas, delta != 0 do
      {:merge, :counters, Keys.counter(0, Map.fetch!(@fields, field)), delta}
    end
  end

  defp merges(sums, to_key) do
    for {key, delta} <- sums, delta != 0 do
      {ord, field} = to_key.(key)
      {:merge, :counters, Keys.counter(ord, Map.fetch!(@fields, field)), delta}
    end
  end

  @doc "The seven counters of `ord` (0 is the node), each `0` when never written."
  @spec read(atom(), non_neg_integer()) :: {:ok, %{field() => integer()}} | {:error, term()}
  def read(instance, ord) do
    names = fields()

    with {:ok, values} <-
           Store.multi_get(
             instance,
             :counters,
             Enum.map(names, &Keys.counter(ord, Map.fetch!(@fields, &1)))
           ) do
      {:ok, names |> Enum.zip(Enum.map(values, &decode/1)) |> Map.new()}
    end
  end

  @doc "One counter of each of `ords`, in order: `{:ok, [integer()]}`."
  @spec read_field(atom(), [non_neg_integer()], field()) :: {:ok, [integer()]} | {:error, term()}
  def read_field(_instance, [], _field), do: {:ok, []}

  def read_field(instance, ords, field) do
    index = Map.fetch!(@fields, field)

    with {:ok, values} <-
           Store.multi_get(instance, :counters, Enum.map(ords, &Keys.counter(&1, index))) do
      {:ok, Enum.map(values, &decode/1)}
    end
  end

  # Trusted, internally written data (this node's own store rows); never request input.
  # sobelow_skip ["Misc.BinToTerm"]
  defp decode({:ok, bin}), do: :erlang.binary_to_term(bin)
  defp decode(:not_found), do: 0
end
