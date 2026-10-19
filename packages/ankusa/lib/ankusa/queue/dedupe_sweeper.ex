defmodule Ankusa.Queue.DedupeSweeper do
  @moduledoc """
  Expires ingest-dedupe keys (`?u`, with their `?e` expiry index) once their ttl
  has passed, so the dedupe index stays bounded by `dedupe.max_entries`.

  One process per instance, next to `Ankusa.Queue.Writer` and never inside it.
  The sweep itself (the scan of `?e`, the key decoding, the tenant ordinals, the
  batching) runs here: every `@interval_ms`, and then back to back for as long as
  a full chunk of expired keys keeps coming, so it keeps pace with an ingest rate
  of tens of thousands of keys a second. What it hands the writer is one batch of
  at most `@chunk` keys at a time (`{:expire_dedupe, entries}`), because a
  refresh and a delete of one key must not race: the writer deletes a key only
  if it still holds the expiry the sweep read, and takes the key off the
  tenant's and the node's `dedupe` counter in the same batch.

  Emits `[:ankusa, :dedupe, :pruned]` (`count`) for every batch that removed
  keys. A failed chunk is logged and retried on the next tick; `sweep/1` runs
  the sweep to completion, for a test or an operator.
  """

  use GenServer

  require Logger

  alias Ankusa.Queue.Tenants
  alias Ankusa.Store
  alias Ankusa.Store.Keys
  alias Ankusa.Telemetry

  @chunk 1_000
  @interval_ms 10_000
  @backoff_ms 20

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :dedupe_sweeper))
  end

  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc "Sweep every key expired as of now, synchronously: `{:ok, deleted}`."
  @spec sweep(atom()) :: {:ok, non_neg_integer()} | {:error, term()}
  def sweep(instance),
    do: GenServer.call(Ankusa.via(instance, :dedupe_sweeper), :sweep, :infinity)

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    Process.send_after(self(), :tick, @interval_ms)
    {:ok, %{instance: instance}}
  end

  @impl true
  def handle_call(:sweep, _from, state) do
    {:reply, sweep_all(state.instance, 0), state}
  end

  @impl true
  def handle_info(:tick, state) do
    next =
      case sweep_chunk(state.instance) do
        {:ok, _deleted, true} ->
          @backoff_ms

        {:ok, _deleted, false} ->
          @interval_ms

        {:error, reason} ->
          Logger.warning("[ankusa] dedupe sweep failed, retrying: #{inspect(reason)}")
          @interval_ms
      end

    Process.send_after(self(), :tick, next)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp sweep_all(instance, total) do
    case sweep_chunk(instance) do
      {:ok, deleted, true} -> sweep_all(instance, total + deleted)
      {:ok, deleted, false} -> {:ok, total + deleted}
      {:error, reason} -> {:error, reason}
    end
  end

  # One chunk: `{:ok, deleted, more?}`.
  defp sweep_chunk(instance) do
    now = System.system_time(:millisecond)
    range = {<<?e>>, <<?e, now + 1::64>>}

    scan =
      Store.fold(instance, :dedupe_expiry, range, {0, []}, fn key, _value, {n, acc} ->
        if n + 1 >= @chunk, do: {:halt, {n + 1, [key | acc]}}, else: {:cont, {n + 1, [key | acc]}}
      end)

    with {:ok, {taken, expiry_keys}} <- scan,
         decoded = expiry_keys |> Enum.reverse() |> Enum.map(&decode/1),
         {:ok, ords} <- ordinals(instance, decoded),
         {:ok, deleted} <- apply_batch(instance, entries(decoded, ords)) do
      if deleted > 0,
        do: Telemetry.emit([:dedupe, :pruned], %{count: deleted}, %{instance: instance})

      {:ok, deleted, taken >= @chunk}
    end
  end

  defp decode(expiry_key) do
    case Keys.decode_dedupe_expiry(expiry_key) do
      {at, tenant, source_id, key} -> {:ok, expiry_key, at, tenant, source_id, key}
      :error -> {:undecodable, expiry_key}
    end
  end

  # The ordinal of every distinct tenant of the chunk, in one `multi_get`.
  defp ordinals(instance, decoded) do
    tenants =
      for({:ok, _expiry, _at, tenant, _source, _key} <- decoded, do: Tenants.normalize(tenant))
      |> Enum.uniq()

    Tenants.lookup(instance, tenants)
  end

  defp entries(decoded, ords) do
    Enum.map(decoded, fn
      {:ok, expiry_key, at, tenant, source_id, key} ->
        {:expired, expiry_key, Keys.dedupe(tenant, source_id, key), at,
         Map.get(ords, Tenants.normalize(tenant))}

      {:undecodable, expiry_key} ->
        {:undecodable, expiry_key}
    end)
  end

  defp apply_batch(_instance, []), do: {:ok, 0}

  defp apply_batch(instance, entries) do
    case Ankusa.whereis(instance, :queue_writer) do
      pid when is_pid(pid) ->
        try do
          GenServer.call(pid, {:expire_dedupe, entries}, 30_000)
        catch
          :exit, reason -> {:error, {:writer_unavailable, reason}}
        end

      nil ->
        {:error, :writer_unavailable}
    end
  end
end
