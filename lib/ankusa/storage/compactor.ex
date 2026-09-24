defmodule Ankusa.Storage.Compactor do
  @moduledoc """
  Packs committed WAL records into immutable segments and reclaims the WAL.

  Each tick reads WAL records past the compactor cursor in bounded chunks and
  packs them into segments of at most `config.storage.roll_bytes` of payload.
  A backlog therefore becomes *several* segments per tick instead of one
  unbounded one: peak memory is a chunk plus one segment, whatever the backlog
  and however long a storage node was unavailable.

  Each segment is encoded via `config.storage.codec`, `PUT` through the blob
  store, and followed by its per-record rows in `Ankusa.Storage.Index` and an
  advance of the durable compactor cursor.

  The WAL is then truncated through `min(compactor_seq, dispatch_seq)`: records
  the dispatch pipeline has not yet consumed are never dropped, preserving
  at-least-once delivery.
  """

  use GenServer

  alias Ankusa.{Config, Envelope}
  alias Ankusa.Storage.Index

  # Read granularity: bounds one read's memory, and how much the roll check can
  # overshoot `roll_bytes` by at most.
  @read_chunk 256

  # ── lifecycle ─────────────────────────────────────────────────────────────

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :compactor))
  end

  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc "Run one compaction synchronously; returns the number of segments written."
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

  defp compact(state), do: compact(state, 0)

  defp compact(state, written) do
    {entries, next_cursor, more?} =
      collect(state, state.cursor, state.config.storage.roll_bytes)

    case entries do
      [] ->
        {written, state}

      entries ->
        write_segment(state, entries)
        state = %{state | cursor: next_cursor}

        if more? do
          compact(state, written + 1)
        else
          {written + 1, state}
        end
    end
  end

  # Collect envelopes up to a payload-byte budget. `more?` says whether the WAL
  # may still hold records past the returned cursor, which is what lets a tick
  # write several segments instead of stopping at the first one.
  defp collect(state, cursor, roll_bytes) do
    collect(state, cursor, roll_bytes, [], 0, false)
  end

  defp collect(state, cursor, roll_bytes, acc, bytes, more?) do
    # `acc != []` matters: with a budget of 0 the first check would otherwise
    # return nothing at all and no segment would ever be written.
    if acc != [] and bytes >= roll_bytes do
      {Enum.reverse(acc), cursor, more?}
    else
      case Ankusa.WAL.read(state.instance, cursor, @read_chunk) do
        [] ->
          {Enum.reverse(acc), cursor, more?}

        envelopes ->
          {entries, taken, leftover?} = take_within_budget(envelopes, roll_bytes - bytes)

          cursor =
            if taken == 0,
              do: cursor,
              else: envelopes |> Enum.at(taken - 1) |> Map.fetch!(:seq)

          bytes =
            bytes +
              Enum.reduce(entries, 0, fn {_env, record}, sum ->
                sum + byte_size(record.payload)
              end)

          more? = leftover? or length(envelopes) == @read_chunk

          collect(state, cursor, roll_bytes, Enum.reverse(entries) ++ acc, bytes, more?)
      end
    end
  end

  # Take records until the budget is spent — always at least one, so a tick
  # always makes progress even when a single record exceeds `roll_bytes`.
  defp take_within_budget(envelopes, budget), do: take_within_budget(envelopes, budget, [], 0)

  defp take_within_budget([env | rest], budget, acc, bytes) do
    record = %{key: env.id, payload: Envelope.to_binary(env)}
    acc = [{env, record} | acc]
    bytes = bytes + byte_size(record.payload)

    if bytes >= budget or rest == [] do
      {Enum.reverse(acc), length(acc), rest != []}
    else
      take_within_budget(rest, budget, acc, bytes)
    end
  end

  defp take_within_budget([], _budget, acc, _bytes), do: {Enum.reverse(acc), length(acc), false}

  defp write_segment(state, entries) do
    instance = state.instance
    config = state.config
    started = System.monotonic_time()

    first_seq = entries |> hd() |> elem(0) |> Map.fetch!(:seq)
    last_seq = entries |> List.last() |> elem(0) |> Map.fetch!(:seq)
    {codec, _} = config.storage.codec

    {segment, index} = codec.encode(Enum.map(entries, fn {_env, record} -> record end))

    key = "seg/#{pad(first_seq)}-#{pad(last_seq)}.seg"
    :ok = Ankusa.BlobStore.put(instance, key, segment)

    rows =
      Enum.zip(entries, index)
      |> Enum.map(fn {{env, _record}, entry} ->
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
        records: length(entries),
        bytes: byte_size(segment),
        duration: System.monotonic_time() - started
      },
      %{instance: instance}
    )
  end

  defp schedule(interval) when is_integer(interval) and interval > 0 do
    Process.send_after(self(), :tick, interval)
  end

  defp schedule(_), do: :ok

  defp pad(seq), do: seq |> Integer.to_string() |> String.pad_leading(20, "0")
end
