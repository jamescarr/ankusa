defmodule Ankusa.Gateway.Objects.Budget do
  @moduledoc """
  The object bytes the objects API holds in memory at once, across requests,
  bounded by `gateway.objects.max_inflight_bytes`.

  A public ETS set of `{pid, bytes}` rows (what each request holds) and an
  `:atomics` total, both created by `Ankusa.Gateway.Supervisor` and living as
  long as it does. `reserve/2` adds to both and backs out when the total would
  pass the bound (`:shed`, a `503 overloaded`); `release/1`, in an `after`,
  removes the caller's row and subtracts it. `sweep/1` (every
  `Ankusa.Gateway.Gauges` tick) releases the rows of dead processes, so a
  request killed mid-flight never leaks its bytes.
  """

  @doc false
  # Create the table and the counter (`Ankusa.Gateway.Supervisor.init/1`).
  @spec setup(atom()) :: :ok
  def setup(instance) do
    table = table(instance)

    if :ets.whereis(table) == :undefined,
      do: :ets.new(table, [:named_table, :public, :set, write_concurrency: true])

    :persistent_term.put({Ankusa.Gateway, :inflight_bytes, instance}, :atomics.new(1, []))
    :ok
  end

  @doc "Hold `bytes` more for the calling process, unless that passes `max`."
  @spec reserve(atom(), non_neg_integer(), pos_integer()) :: :ok | :shed
  def reserve(_instance, 0, _max), do: :ok

  def reserve(instance, bytes, max) do
    case counter(instance) do
      {:ok, counter} ->
        if :atomics.add_get(counter, 1, bytes) > max do
          :atomics.sub(counter, 1, bytes)
          :shed
        else
          :ets.update_counter(table(instance), self(), bytes, {self(), 0})
          :ok
        end

      :error ->
        :ok
    end
  end

  @doc "Give back everything the calling process holds."
  @spec release(atom()) :: :ok
  def release(instance), do: release(instance, self())

  defp release(instance, pid) do
    with {:ok, counter} <- counter(instance),
         [{^pid, bytes}] <- :ets.take(table(instance), pid) do
      :atomics.sub(counter, 1, bytes)
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "Release the rows of processes that are gone; returns the bytes held now."
  @spec sweep(atom()) :: non_neg_integer()
  def sweep(instance) do
    table = table(instance)

    for {pid, _bytes} <- :ets.tab2list(table), not Process.alive?(pid), do: release(instance, pid)

    inflight(instance)
  rescue
    ArgumentError -> 0
  end

  @doc "The bytes held now."
  @spec inflight(atom()) :: non_neg_integer()
  def inflight(instance) do
    case counter(instance) do
      {:ok, counter} -> :atomics.get(counter, 1)
      :error -> 0
    end
  end

  defp counter(instance) do
    case :persistent_term.get({Ankusa.Gateway, :inflight_bytes, instance}, nil) do
      nil -> :error
      counter -> {:ok, counter}
    end
  end

  # sobelow_skip ["DOS.BinToAtom"]
  defp table(instance), do: :"ankusa_gateway_bytes_#{instance}"
end
