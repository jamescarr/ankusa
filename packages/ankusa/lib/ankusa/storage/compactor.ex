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

  A failed write ends the tick, nothing crashes, and the same hooks are written
  again — after `storage.interval_ms` doubled per consecutive failure (jittered,
  capped at 60 s) — under the same keys.
  """

  use GenServer

  require Logger

  alias Ankusa.{Config, Envelope}
  alias Ankusa.Queue.{Archive, Reclaim}
  alias Ankusa.Store.Keys

  # The catalogue sweep: hourly, `@catalogue_batch` segments a batch.
  @retention_interval_ms 3_600_000
  @catalogue_batch 100
  @catalogue_batches 20
  @day_ms 86_400_000

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

  @doc """
  Delete the catalogue rows past retention: a segment's row (`?S`) once its newest
  hook is older than `storage.catalogue_retention_days` (or, with
  `archive.retention_days` set, that many days, deleting the segment object and
  its index through the blob store as well), and a 0.3 import's location row
  (`?L`) once its hook is. Runs hourly in a process of its own; callable on its
  own.

  What a replay or a delivery still needs stays: a segment an archive replay job
  (`running` or `paused`) can still read (its newest hook is at or after the
  job's `from`), and a segment holding the hook of a delivery row that is still
  owed (pending or dead): the DLQ reaper evicts that row only while the catalogue
  can find its hook. The sweep deletes nothing when the replayer cannot say what
  its jobs need. Emits `[:ankusa, :archive, :pruned]` (`segments`, `objects`,
  `legacy`); `more` says the pass's budget, not the catalogue, ran out.
  """
  @spec prune_catalogue(atom()) ::
          {:ok,
           %{
             segments: non_neg_integer(),
             objects: non_neg_integer(),
             legacy: non_neg_integer(),
             more: boolean()
           }}
          | {:error, term()}
  def prune_catalogue(instance) do
    %Config{storage: storage, archive: archive} = Ankusa.config(instance)
    objects? = archive.retention_days != nil
    days = archive.retention_days || storage.catalogue_retention_days
    cutoff = System.system_time(:millisecond) - days * @day_ms

    with {:ok, floor} <- Ankusa.Replay.live_floor(instance, :archive, :from) do
      cutoff = if floor == :infinity, do: cutoff, else: min(cutoff, floor - 1_000)
      done = %{segments: 0, objects: 0, more: false}

      with {:ok, done} <- prune_segments(instance, cutoff, objects?, 0, @catalogue_batches, done),
           {:ok, legacy} <- prune_legacy(instance, cutoff, @catalogue_batches, 0) do
        done = Map.put(done, :legacy, legacy)

        if done.segments + legacy > 0,
          do:
            Ankusa.Telemetry.emit(
              [:archive, :pruned],
              Map.take(done, [:segments, :objects, :legacy]),
              %{instance: instance}
            )

        {:ok, done}
      end
    end
  end

  defp prune_segments(_instance, _cutoff, _objects?, _after, 0, done),
    do: {:ok, %{done | more: true}}

  defp prune_segments(instance, cutoff, objects?, after_seq, left, done) do
    with {:ok, rows} <- Archive.expired_segments(instance, cutoff, @catalogue_batch, after_seq),
         {:ok, doomed} <- not_owed(instance, rows),
         :ok <- delete_objects(instance, doomed, objects?),
         :ok <- Archive.delete_rows(instance, Enum.map(doomed, &elem(&1, 0))) do
      done = %{
        done
        | segments: done.segments + length(doomed),
          objects: done.objects + if(objects?, do: 2 * length(doomed), else: 0)
      }

      if length(rows) < @catalogue_batch do
        {:ok, done}
      else
        {_key, last} = List.last(rows)
        prune_segments(instance, cutoff, objects?, last.first_seq, left - 1, done)
      end
    end
  end

  # The rows no delivery row is still owed for: `{:ok, [{key, row}]}`.
  defp not_owed(instance, rows) do
    Enum.reduce_while(rows, {:ok, []}, fn {_key, row} = entry, {:ok, keep} ->
      case Archive.owed?(instance, row) do
        {:ok, true} -> {:cont, {:ok, keep}}
        {:ok, false} -> {:cont, {:ok, [entry | keep]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, keep} -> {:ok, Enum.reverse(keep)}
      error -> error
    end
  end

  defp delete_objects(_instance, _doomed, false), do: :ok

  defp delete_objects(instance, doomed, true) do
    for {_key, row} <- doomed, key <- [row.key, row.idx_key], reduce: :ok do
      :ok -> delete_object(instance, key)
      error -> error
    end
  end

  # A blob store is user code: a raise or an exit is this sweep's failure, retried
  # on the next pass (the catalogue row stays until its objects are gone).
  defp delete_object(instance, key) do
    Ankusa.BlobStore.delete(instance, :segments, key)
    :ok
  rescue
    error -> {:error, {:raised, error}}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp prune_legacy(_instance, _cutoff, 0, total), do: {:ok, total}

  defp prune_legacy(instance, cutoff, left, total) do
    with {:ok, keys} <- Archive.expired_legacy(instance, cutoff, @catalogue_batch * 10),
         :ok <- Archive.delete_rows(instance, keys) do
      if length(keys) < @catalogue_batch * 10,
        do: {:ok, total + length(keys)},
        else: prune_legacy(instance, cutoff, left - 1, total + length(keys))
    end
  end

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    %Config{} = config = Ankusa.config(instance)
    interval = config.storage.interval_ms

    # Markers a crash left between "obligation cleared" and "hook deleted".
    with {:error, reason} <- Reclaim.sweep(instance) do
      Logger.warning("[ankusa] hook reclaim sweep failed: #{inspect(reason)}")
    end

    schedule(interval)
    Process.send_after(self(), :retention, @retention_interval_ms)

    {:ok,
     %{
       instance: instance,
       config: config,
       interval: interval,
       after_seq: 0,
       failures: 0,
       pruning: nil
     }}
  end

  # Crash reports print the state; the config carries the object store's
  # credentials.
  @impl true
  def format_status(%{state: %{config: _} = state} = status),
    do: %{status | state: %{state | config: :redacted}}

  def format_status(status), do: status

  # ── ticks ─────────────────────────────────────────────────────────────────

  @impl true
  def handle_call(:tick, _from, state) do
    {n, state} = compact(state)
    {:reply, {:ok, n}, state}
  end

  @impl true
  def handle_info(:tick, state) do
    {_n, state} = compact(state)
    schedule(next_delay(state))
    {:noreply, state}
  end

  # The catalogue sweep runs in a process of its own (`prune_catalogue/1`): it
  # asks the replayer for its jobs and may delete objects, neither of which the
  # compactor's tick should wait on.
  def handle_info(:retention, %{pruning: nil} = state) do
    instance = state.instance

    {_pid, ref} =
      spawn_monitor(fn ->
        case prune_catalogue(instance) do
          {:ok, %{more: true}} ->
            exit({:shutdown, :more})

          {:ok, _done} ->
            exit(:normal)

          {:error, reason} ->
            Logger.warning("[ankusa] catalogue retention failed, retrying: #{inspect(reason)}")
            exit(:normal)
        end
      end)

    {:noreply, %{state | pruning: ref}}
  end

  def handle_info(:retention, state) do
    Process.send_after(self(), :retention, @retention_interval_ms)
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{pruning: ref} = state) do
    next = if reason == {:shutdown, :more}, do: 1_000, else: @retention_interval_ms
    Process.send_after(self(), :retention, next)
    {:noreply, %{state | pruning: nil}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  # Same shape as Ankusa.Instance.Isolated's restart backoff, plus the jitter
  # RetryPolicy.Exponential uses: an unreachable object store is retried at
  # interval·2^n, capped at a minute, never faster than the interval.
  @max_backoff_ms 60_000

  defp next_delay(%{failures: 0, interval: interval}), do: interval

  defp next_delay(%{failures: n, interval: interval}) do
    capped = (interval * Integer.pow(2, min(n, 16))) |> min(@max_backoff_ms) |> max(interval)
    max(interval, round(capped * (0.5 + :rand.uniform() * 0.5)))
  end

  # ── compaction ────────────────────────────────────────────────────────────

  defp compact(state), do: compact(state, 0)

  defp compact(state, written) do
    case archive_batch(state) do
      {:ok, :none} ->
        {written, %{state | failures: 0}}

      {:ok, {last_seq, more?}} ->
        state = %{state | after_seq: last_seq, failures: 0}
        if more?, do: compact(state, written + 1), else: {written + 1, state}

      {:error, reason} ->
        # `after_seq` did not move, so the same hooks (and the same segment
        # key) are written again next tick.
        Logger.warning(
          "[ankusa] archive segment after seq #{state.after_seq} not written " <>
            "(#{state.failures + 1} in a row), retried with backoff: #{inspect(reason)}"
        )

        Ankusa.Telemetry.emit([:archive, :failure], %{}, %{instance: state.instance})

        {written, %{state | failures: state.failures + 1}}
    end
  end

  defp archive_batch(state) do
    roll_bytes = state.config.storage.roll_bytes

    with {:ok, pending, more?} <- Archive.pending(state.instance, state.after_seq, roll_bytes),
         {:ok, hooks} <- Archive.hooks(state.instance, Enum.map(pending, &elem(&1, 0))) do
      case pending do
        [] ->
          emit_archive_state(state.instance, [])
          {:ok, :none}

        pending ->
          {present, missing} = Enum.split_with(hooks, fn {_seq, bin} -> bin != nil end)
          {entries, undecodable} = decode_ids(present)
          emit_archive_state(state.instance, entries)
          missing_seqs = Enum.map(missing, &elem(&1, 0))

          if missing_seqs != [] do
            Logger.error(
              "[ankusa] archive obligation(s) for hook(s) #{inspect(missing_seqs)} have no hook; dropping them"
            )
          end

          # A stored hook that does not decode can never be archived; retrying it
          # would stop the archive behind it for good. Its obligation goes, the
          # hook itself stays for whatever deliveries it still has.
          if undecodable != [] do
            Logger.error(
              "[ankusa] hook(s) #{inspect(undecodable)} do not decode and cannot be archived; " <>
                "dropping their archive obligation(s)"
            )
          end

          with :ok <- write_segment(state, entries, missing_seqs ++ undecodable) do
            {:ok, {pending |> List.last() |> elem(0), more?}}
          end
      end
    end
  end

  # How long the oldest hook still owed a segment has been waiting, for the age
  # gauge: from the batch this tick already read (the oldest obligation is the
  # first, and the first hook is decoded once more for its `received_at`), so
  # it costs no scan, and it is reported before the write, so a store that
  # refuses it shows as growth.
  defp emit_archive_state(instance, entries) do
    age_ms =
      with [{_seq, _id, bin} | _] <- entries,
           {:ok, %Envelope{received_at: received_at}} <- Envelope.from_binary(bin) do
        max(System.system_time(:millisecond) - received_at, 0)
      else
        _none -> 0
      end

    Ankusa.Telemetry.emit([:archive, :state], %{oldest_unarchived_age_ms: age_ms}, %{
      instance: instance
    })
  end

  # Every hook of the batch was already gone: nothing to write, only
  # obligations to drop.
  defp write_segment(state, [], missing_seqs) do
    Archive.archived(state.instance, nil, [], missing_seqs)
  end

  defp write_segment(state, entries, missing_seqs) do
    started = System.monotonic_time()
    {codec, _opts} = state.config.storage.codec

    with {:ok, {segment, index}} <- encode(codec, entries) do
      write_encoded(state, entries, missing_seqs, segment, index, started)
    end
  end

  defp write_encoded(state, entries, missing_seqs, segment, index, started) do
    instance = state.instance

    seqs = Enum.map(entries, &elem(&1, 0))
    ids = Enum.map(entries, &elem(&1, 1))
    first_seq = hd(seqs)
    last_seq = List.last(seqs)
    # Full keys, `storage.key_prefix` included: the catalogue records exactly
    # what was written, so a later prefix change never strands this segment.
    name = "seg/#{pad(first_seq)}-#{pad(last_seq)}"
    key = Ankusa.BlobStore.object_key(instance, :segments, name <> ".seg")
    idx_key = Ankusa.BlobStore.object_key(instance, :segments, name <> ".idx")

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

  # `{seq, id, bin}` for every hook that decodes, and the seqs of those that do not.
  defp decode_ids(present) do
    {entries, undecodable} =
      Enum.reduce(present, {[], []}, fn {seq, bin}, {ok, bad} ->
        case Envelope.from_binary(bin) do
          {:ok, env} -> {[{seq, env.id, bin} | ok], bad}
          {:error, _reason} -> {ok, [seq | bad]}
        end
      end)

    {Enum.reverse(entries), Enum.reverse(undecodable)}
  end

  # The codec is configurable code: a raise is this tick's failure, retried next
  # tick like a failed blob write, never a crashed compactor.
  defp encode(codec, entries) do
    {:ok, codec.encode(Enum.map(entries, fn {_seq, id, bin} -> %{key: id, payload: bin} end))}
  rescue
    error -> {:error, {:raised, error}}
  end

  # A blob store is user code (S3, GCS, a custom adapter): an error return, a
  # raise, an exit and a throw are the same failure — this tick is retried,
  # nothing crashes. A return outside `:ok | {:error, _}` raises here and lands
  # in the `rescue`.
  defp put(instance, key, data) do
    case Ankusa.BlobStore.put(instance, :segments, key, data) do
      :ok -> :ok
      {:error, _reason} = error -> error
    end
  rescue
    error -> {:error, {:raised, error}}
  catch
    :exit, reason -> {:error, {:exit, reason}}
    :throw, value -> {:error, {:throw, value}}
  end

  defp schedule(interval) when is_integer(interval) and interval > 0 do
    Process.send_after(self(), :tick, interval)
  end

  defp schedule(_), do: :ok

  defp pad(seq), do: seq |> Integer.to_string() |> String.pad_leading(20, "0")
end
