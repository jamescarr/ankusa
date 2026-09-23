defmodule Ankusa.Storage.Compactor do
  @moduledoc """
  Packs committed WAL records into immutable segments and reclaims the WAL.

  Each tick reads every WAL record past the compactor cursor, encodes them into
  a single segment via `config.storage.codec`, `PUT`s the segment through the
  blob store, appends the per-record rows to `Ankusa.Storage.Index`, and advances
  the durable compactor cursor.

  The WAL is then truncated through `min(compactor_seq, dispatch_seq)`: records
  the dispatch pipeline has not yet consumed are never dropped, preserving
  at-least-once delivery.
  """

  use GenServer

  alias Ankusa.{Config, Envelope}
  alias Ankusa.Storage.Index

  # one segment can absorb a very large replay window in a single tick
  @read_limit 1_000_000

  # ── lifecycle ─────────────────────────────────────────────────────────────

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :compactor))
  end

  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc "Run one compaction synchronously; returns the number of segments written (0 or 1)."
  @spec tick(atom()) :: {:ok, non_neg_integer()}
  def tick(instance), do: GenServer.call(Ankusa.via(instance, :compactor), :tick)

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    %Config{} = config = Keyword.fetch!(opts, :config)
    cursor = Ankusa.WAL.get_cursor(instance, :compactor)
    interval = config.storage.interval_ms

    # This process owns the live index: it is the only writer, it reloads the
    # table from the file whenever it (re)starts, and lookups elsewhere read it.
    Index.open(config)

    schedule(interval)
    {:ok, %{instance: instance, config: config, cursor: cursor, interval: interval}}
  end

  # ── ticks ─────────────────────────────────────────────────────────────────

  @impl true
  def handle_call(:tick, _from, state) do
    {n, state} = compact(state)
    {:reply, {:ok, n}, state}
  end

  @impl true
  def handle_info(:tick, state) do
    {_n, state} = compact(state)
    schedule(state.interval)
    {:noreply, state}
  end

  # ── compaction ────────────────────────────────────────────────────────────

  defp compact(%{instance: instance, config: config, cursor: cursor} = state) do
    started = System.monotonic_time()

    case Ankusa.WAL.read(instance, cursor, @read_limit) do
      [] ->
        {0, state}

      envelopes ->
        first_seq = hd(envelopes).seq
        last_seq = List.last(envelopes).seq
        {codec, _} = config.storage.codec

        records =
          Enum.map(envelopes, fn env -> %{key: env.id, payload: Envelope.to_binary(env)} end)

        {segment, index} = codec.encode(records)

        key = "seg/#{pad(first_seq)}-#{pad(last_seq)}.seg"
        :ok = Ankusa.BlobStore.put(instance, key, segment)

        rows =
          Enum.zip(envelopes, index)
          |> Enum.map(fn {env, entry} ->
            %{
              event_id: env.id,
              source_id: env.source_id,
              tenant_id: env.tenant_id,
              received_at: env.received_at,
              seq: env.seq,
              segment_key: key,
              offset: entry.offset,
              length: entry.length
            }
          end)

        :ok = Index.append(config, rows)

        :ok = Ankusa.WAL.put_cursor(instance, :compactor, last_seq)

        # never truncate past what dispatch has consumed — at-least-once
        dispatch_seq = Ankusa.WAL.get_cursor(instance, :dispatch)
        :ok = Ankusa.WAL.truncate_through(instance, min(last_seq, dispatch_seq))

        Ankusa.Telemetry.emit(
          [:compact, :stop],
          %{
            records: length(envelopes),
            bytes: byte_size(segment),
            duration: System.monotonic_time() - started
          },
          %{instance: instance}
        )

        {1, %{state | cursor: last_seq}}
    end
  end

  defp schedule(interval) when is_integer(interval) and interval > 0 do
    Process.send_after(self(), :tick, interval)
  end

  defp schedule(_), do: :ok

  defp pad(seq), do: seq |> Integer.to_string() |> String.pad_leading(20, "0")
end
