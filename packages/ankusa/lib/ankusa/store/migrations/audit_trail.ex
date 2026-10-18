defmodule Ankusa.Store.Migrations.AuditTrail do
  @moduledoc false

  # Store format 1 -> 2 (SEC-2.a): the `audit` column family and per-source
  # revisions.
  #
  # The column family needs no step: RocksDB creates it on open. What a format 1
  # store lacks is a revision for each API-managed source, which every audited
  # write from now on increments. Every existing source is revision 1; one that
  # already has a revision key (a step replayed after a kill) keeps it.

  @behaviour Ankusa.Store.Migration

  alias Ankusa.Store
  alias Ankusa.Store.Keys

  @batch 100

  @impl true
  def describe,
    do: "give every API-managed source a revision (audit trail, column family `audit`)"

  @impl true
  def step(instance, cursor) do
    %{lo: lo, hi: hi} = Keys.family(:sources)
    from = if is_nil(cursor), do: lo, else: cursor <> <<0>>

    with {:ok, keys} <- source_keys(instance, {from, hi}),
         {:ok, ops} <- stamp(instance, keys) do
      if length(keys) < @batch do
        {:ok, ops, :done}
      else
        {:ok, ops, {:more, List.last(keys)}}
      end
    end
  end

  defp source_keys(instance, range) do
    Store.fold(instance, :sources, range, [], fn key, _value, acc ->
      acc = [key | acc]
      if length(acc) >= @batch, do: {:halt, acc}, else: {:cont, acc}
    end)
    |> case do
      {:ok, keys} -> {:ok, Enum.reverse(keys)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp stamp(instance, keys) do
    Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, ops} ->
      {tenant, name} = Keys.decode_source(key)
      revision_key = Keys.source_revision(tenant, name)

      case Store.get(instance, :default, revision_key) do
        :not_found -> {:cont, {:ok, [{:put, :default, revision_key, "1"} | ops]}}
        {:ok, _existing} -> {:cont, {:ok, ops}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, ops} -> {:ok, Enum.reverse(ops)}
      {:error, reason} -> {:error, reason}
    end
  end
end
