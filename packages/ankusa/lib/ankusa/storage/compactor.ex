defmodule Ankusa.Storage.Compactor do
  @moduledoc """
  Packs committed hooks into immutable segments and clears their archive
  obligations.

  A hook committed while the `:storage` role runs carries an *archive
  obligation* (`Ankusa.Queue`). Each tick takes obligations in `seq` order until
  their stored sizes reach `config.storage.roll_bytes`, packs those hooks into
  one segment, and goes on: a backlog becomes *several* segments per tick
  instead of one unbounded one, so peak memory is one segment whatever the
  backlog and however long a storage node was unavailable.

  A segment is encoded via `config.storage.codec` and `PUT` through the blob
  store as `seg/<first>-<last>.seg`, followed by an index object
  (`seg/<first>-<last>.idx`: event id to offset, length and seq) and the
  segment's catalogue row in the store. Only then do its obligations clear. A
  hook is deleted when its *last* obligation — archive or delivery — clears, so
  an archive that is behind or switched off never blocks delivery, and delivery
  never blocks the archive.

  A failed write ends the tick, nothing crashes, and the same hooks are
  written again next tick under the same keys.
  """

  use GenServer

  require Logger

  alias Ankusa.{Config, Envelope}
  alias Ankusa.Queue.{Archive, Reclaim}
  alias Ankusa.Store.Keys

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
    interval = config.storage.interval_ms

    # Markers a crash left between "obligation cleared" and "hook deleted".
    with {:error, reason} <- Reclaim.sweep(instance) do
      Logger.warning("[ankusa] hook reclaim sweep failed: #{inspect(reason)}")
    end

    schedule(interval)
    {:ok, %{instance: instance, config: config, interval: interval, after_seq: 0}}
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
    case archive_batch(state) do
      {:ok, :none} ->
        {written, state}

      {:ok, {last_seq, more?}} ->
        state = %{state | after_seq: last_seq}
        if more?, do: compact(state, written + 1), else: {written + 1, state}

      {:error, reason} ->
        # `after_seq` did not move, so the same hooks (and the same segment
        # key) are written again next tick.
        Logger.warning(
          "[ankusa] archive segment after seq #{state.after_seq} not written, " <>
            "retried next tick: #{inspect(reason)}"
        )

        {written, state}
    end
  end

  defp archive_batch(state) do
    roll_bytes = state.config.storage.roll_bytes

    with {:ok, pending, more?} <- Archive.pending(state.instance, state.after_seq, roll_bytes),
         {:ok, hooks} <- Archive.hooks(state.instance, Enum.map(pending, &elem(&1, 0))) do
      case pending do
        [] ->
          {:ok, :none}

        pending ->
          {present, missing} = Enum.split_with(hooks, fn {_seq, bin} -> bin != nil end)
          missing_seqs = Enum.map(missing, &elem(&1, 0))

          if missing_seqs != [] do
            Logger.error(
              "[ankusa] archive obligation(s) for hook(s) #{inspect(missing_seqs)} have no hook; dropping them"
            )
          end

          with :ok <- write_segment(state, present, missing_seqs) do
            {:ok, {pending |> List.last() |> elem(0), more?}}
          end
      end
    end
  end

  # Every hook of the batch was already gone: nothing to write, only
  # obligations to drop.
  defp write_segment(state, [], missing_seqs) do
    Archive.archived(state.instance, nil, [], missing_seqs)
  end

  defp write_segment(state, present, missing_seqs) do
    instance = state.instance
    started = System.monotonic_time()
    {codec, _opts} = state.config.storage.codec

    entries = Enum.map(present, fn {seq, bin} -> {seq, Envelope.from_binary(bin).id, bin} end)

    {segment, index} =
      codec.encode(Enum.map(entries, fn {_seq, id, bin} -> %{key: id, payload: bin} end))

    seqs = Enum.map(entries, &elem(&1, 0))
    ids = Enum.map(entries, &elem(&1, 1))
    first_seq = hd(seqs)
    last_seq = List.last(seqs)
    key = "seg/#{pad(first_seq)}-#{pad(last_seq)}.seg"
    idx_key = "seg/#{pad(first_seq)}-#{pad(last_seq)}.idx"

    idx =
      entries
      |> Enum.zip(index)
      |> Map.new(fn {{seq, id, _bin}, entry} -> {id, {entry.offset, entry.length, seq}} end)

    row = %{
      key: key,
      idx_key: idx_key,
      first_seq: first_seq,
      last_seq: last_seq,
      min_id: Enum.min(ids),
      max_id: Enum.max(ids),
      count: length(entries),
      bytes: byte_size(segment)
    }

    with :ok <- put(instance, key, segment),
         :ok <- put(instance, idx_key, :erlang.term_to_binary(idx)),
         :ok <- Archive.archived(instance, row, seqs, missing_seqs) do
      markers = Enum.map(seqs, fn seq -> {seq, Keys.cleared(seq, 1, 0)} end)

      # A failed reclaim only defers: the markers stay and the sweep finds them.
      with {:error, reason} <- Reclaim.run(instance, markers) do
        Logger.warning("[ankusa] hook reclaim deferred to the next sweep: #{inspect(reason)}")
      end

      Ankusa.Telemetry.emit(
        [:compact, :stop],
        %{
          records: length(entries),
          bytes: byte_size(segment),
          duration: System.monotonic_time() - started
        },
        %{instance: instance}
      )

      :ok
    end
  end

  # A blob store is user code (S3, GCS, a custom adapter): an error return and a
  # raise are the same failure — this tick is retried, nothing crashes.
  defp put(instance, key, data) do
    case Ankusa.BlobStore.put(instance, key, data) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  rescue
    error -> {:error, {:raised, error}}
  end

  defp schedule(interval) when is_integer(interval) and interval > 0 do
    Process.send_after(self(), :tick, interval)
  end

  defp schedule(_), do: :ok

  defp pad(seq), do: seq |> Integer.to_string() |> String.pad_leading(20, "0")
end
