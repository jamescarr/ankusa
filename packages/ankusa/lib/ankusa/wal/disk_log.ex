defmodule Ankusa.WAL.DiskLog do
  @moduledoc """
  Default WAL: a durable append-only log on local disk. Zero external
  dependencies — it survives process crash and power loss on *this box*.

  It does **not** survive loss of the box. The startup log says so, honestly.

  ## On-disk format

  A length-prefixed record stream. Each frame:

      <<magic::16, version::8, flags::8, seq::64, crc32::32, len::32, payload::binary>>

  `payload` is a serialized `Ankusa.Envelope`; `crc32` covers the payload. Replay
  validates every CRC and **drops a torn trailing frame** — a commit that never
  `fsync`'d and therefore was never acked. That closes the crash-before-commit
  window: no un-acked write is ever surfaced.

  ## Group commit

  `append/2` writes every record in one `:file.pwrite`, then a single
  `:file.datasync` (fsync) covers the whole batch. Hundreds of hooks, one fsync.

  ## Truncation

  Truncation is **logical first**: `truncate_through/2` writes the seq floor to
  `<name>.truncated` (fsynced, then renamed into place) and drops the affected
  entries from the in-memory index. Dropping a prefix never has to touch the
  file, so a compaction tick that reclaims a few records costs a few ETS
  deletes, not a rewrite of the whole log — and never blocks appends.

  That floor is also what keeps seqs from being reused: after a restart,
  `next_seq` is the maximum of the last replayed frame, **the truncation floor**,
  and every persisted cursor. A node that restarts with a fully reclaimed log
  therefore continues at the floor, instead of restarting at 1 while dispatch's
  cursor sits in the thousands.

  The file itself is rewritten only when the dead prefix reaches
  `:rewrite_min_bytes` (default 64 MiB) *and* is at least as large as the live
  suffix it would have to copy — at which point the snapshot, a chunked copy of
  the live suffix, and an index re-offset are worth it.

  """

  @behaviour Ankusa.WAL
  use GenServer

  require Logger
  alias Ankusa.{Config, Envelope}

  @magic 0x484B
  @version 1
  @header_bytes 20
  # Copy buffer for a physical rewrite. Bounds peak memory during a rewrite
  # regardless of how large the live suffix is.
  @rewrite_chunk 8 * 1024 * 1024

  # ── lifecycle ─────────────────────────────────────────────────────────────

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :wal))
  end

  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @impl true
  def init(opts) do
    config = Keyword.fetch!(opts, :config)
    dir = Config.path(config, "wal")
    File.mkdir_p!(dir)
    path = Path.join(dir, "ankusa.wal")

    index = :ets.new(:ankusa_wal_index, [:ordered_set, :protected])

    # Load the persisted state that bounds `next_seq` before replaying, so a
    # reclaimed (or fully rewritten) log still continues where it left off.
    truncated_through = load_truncated_through(path <> ".truncated")
    cursors = load_cursors(path <> ".cursors")

    {:ok, fd} = :file.open(path, [:read, :write, :raw, :binary])
    {valid_end, replay_next} = replay(fd, index, truncated_through)
    {:ok, _} = :file.position(fd, valid_end)
    :ok = :file.truncate(fd)

    # Never hand out a seq that was already handed out: take the maximum over
    # the last replayed frame, the truncation floor, and every persisted cursor
    # (`cursor + 1` is the next unconsumed seq). The floor covers a log whose
    # frames were all physically reclaimed; the cursors cover a deployment
    # upgraded from a pre-fix empty log.
    next_seq =
      Enum.max([replay_next, truncated_through + 1 | Enum.map(Map.values(cursors), &(&1 + 1))])

    rewrite_min_bytes = Keyword.get(elem(config.wal, 1), :rewrite_min_bytes, 64 * 1024 * 1024)

    Logger.info(
      "[ankusa] DiskLog WAL at #{path}: recovered #{:ets.info(index, :size)} record(s), " <>
        "next_seq=#{next_seq}. Durable to power loss on THIS host only."
    )

    {:ok,
     %{
       instance: Keyword.fetch!(opts, :instance),
       path: path,
       fd: fd,
       write_pos: valid_end,
       next_seq: next_seq,
       index: index,
       cursors: cursors,
       truncated_through: truncated_through,
       rewrite_min_bytes: rewrite_min_bytes
     }}
  end

  @impl true
  def terminate(_reason, %{fd: fd}), do: :file.close(fd)

  # ── behaviour: facade calls arrive here as GenServer calls ────────────────

  @impl Ankusa.WAL
  def append(server, records), do: GenServer.call(server, {:append, records})

  @impl Ankusa.WAL
  def read(server, after_seq, limit), do: GenServer.call(server, {:read, after_seq, limit})

  @impl Ankusa.WAL
  def get_cursor(server, name), do: GenServer.call(server, {:get_cursor, name})

  @impl Ankusa.WAL
  def put_cursor(server, name, seq), do: GenServer.call(server, {:put_cursor, name, seq})

  @impl Ankusa.WAL
  def truncate_through(server, seq), do: GenServer.call(server, {:truncate_through, seq})

  @impl Ankusa.WAL
  def stats(server), do: GenServer.call(server, :stats)

  # ── group commit ──────────────────────────────────────────────────────────

  @impl true
  def handle_call({:append, records}, _from, state) do
    {results, iodata, inserts, next_seq, bytes, pos} = build_batch(records, state)

    case commit(state, iodata, length(inserts), bytes) do
      :ok ->
        :ets.insert(state.index, inserts)

        {:reply, {:ok, results}, %{state | write_pos: pos, next_seq: next_seq}}

      {:error, reason} ->
        Logger.error(
          "[ankusa] WAL append of #{length(inserts)} record(s) failed, nothing acked: #{inspect(reason)}"
        )

        case discard_tail(state) do
          {:ok, state} ->
            {:reply, {:error, reason}, state}

          {:error, discard_reason} ->
            {:stop, {:wal_write_failed, reason, discard_reason}, {:error, reason}, state}
        end
    end
  end

  def handle_call({:read, after_seq, limit}, _from, state) do
    envelopes =
      state.index
      |> select_after(after_seq, limit)
      |> Enum.map(fn {_seq, {off, len}} ->
        {:ok, payload} = :file.pread(state.fd, off, len)
        Envelope.from_binary(payload)
      end)

    {:reply, envelopes, state}
  end

  def handle_call({:get_cursor, name}, _from, state) do
    {:reply, Map.get(state.cursors, name, 0), state}
  end

  def handle_call({:put_cursor, name, seq}, _from, state) do
    cursors = Map.put(state.cursors, name, seq)

    case persist_term(state.path <> ".cursors", cursors) do
      :ok ->
        {:reply, :ok, %{state | cursors: cursors}}

      {:error, reason} ->
        Logger.warning(
          "[ankusa] WAL could not persist cursor #{inspect(name)}=#{seq}: #{inspect(reason)}"
        )

        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:truncate_through, seq}, _from, %{truncated_through: floor} = state)
      when seq <= floor do
    {:reply, :ok, state}
  end

  def handle_call({:truncate_through, seq}, _from, state) do
    # Durably record the floor *before* dropping anything: a crash between the
    # two must not let a restarted node reuse seqs it already handed out.
    case persist_term(state.path <> ".truncated", seq) do
      :ok ->
        :ets.select_delete(state.index, [{{:"$1", :_}, [{:"=<", :"$1", seq}], [true]}])

        {:reply, :ok, maybe_rewrite(%{state | truncated_through: seq})}

      {:error, reason} ->
        Logger.warning(
          "[ankusa] WAL could not persist truncation floor #{seq}: #{inspect(reason)}"
        )

        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:stats, _from, state) do
    {min_seq, max_seq} = seq_bounds(state.index)

    {:reply,
     %{
       records: :ets.info(state.index, :size),
       bytes: state.write_pos,
       next_seq: state.next_seq,
       min_seq: min_seq,
       max_seq: max_seq,
       cursors: state.cursors
     }, state}
  end

  # ── commit ────────────────────────────────────────────────────────────────

  # One group commit: a single `pwrite` and a single fsync (`datasync`) cover
  # the whole batch. Anything but `:ok` from either means the batch is not
  # durable — the span then emits `[:ankusa, :commit, :exception]` (never a
  # `:stop`), so the commit duration/batch-size series keep counting only real
  # commits.
  defp commit(state, iodata, batch_size, bytes) do
    Ankusa.Telemetry.span([:commit], %{instance: state.instance}, fn ->
      with :ok <- :file.pwrite(state.fd, state.write_pos, iodata),
           :ok <- :file.datasync(state.fd) do
        # measurements, then metadata: `:duration` is added by the span itself.
        {:ok, %{batch_size: batch_size, bytes: bytes}, %{}}
      else
        {:error, reason} -> throw({:wal_commit_failed, reason})
      end
    end)
  catch
    :throw, {:wal_commit_failed, reason} -> {:error, reason}
  end

  # The failed batch's bytes may be partially on disk past `write_pos`; a later
  # crash must not replay a frame that was never acked, and a next batch that is
  # shorter would not overwrite all of it. Truncating at `write_pos` drops the
  # tail. The fresh descriptor is the point: after a failed fsync the old one's
  # error state is not something to build on. Truncation only frees space, so it
  # still works on a full disk; if it does not, the disk is genuinely broken and
  # a restart (which truncates at the replayed end) is the only honest recovery.
  defp discard_tail(state) do
    :file.close(state.fd)

    with {:ok, fd} <- :file.open(state.path, [:read, :write, :raw, :binary]) do
      case truncate_at(fd, state.write_pos) do
        :ok ->
          {:ok, %{state | fd: fd}}

        {:error, reason} ->
          :file.close(fd)
          {:error, reason}
      end
    end
  end

  defp truncate_at(fd, pos) do
    with {:ok, _} <- :file.position(fd, pos),
         :ok <- :file.truncate(fd),
         :ok <- :file.datasync(fd),
         do: :ok
  end

  # ── batch building ────────────────────────────────────────────────────────

  defp build_batch(records, state) do
    init = {[], [], [], state.next_seq, 0, state.write_pos}

    {results, iodata, inserts, next_seq, bytes, pos} =
      Enum.reduce(records, init, fn %{envelope: env}, acc ->
        {results, iodata, inserts, seq, bytes, pos} = acc
        env = %{env | seq: seq}
        payload = Envelope.to_binary(env)
        frame = frame(seq, payload)
        plen = byte_size(payload)
        entry = {seq, {pos + @header_bytes, plen}}
        fsize = @header_bytes + plen

        {[{:committed, env} | results], [iodata, frame], [entry | inserts], seq + 1,
         bytes + fsize, pos + fsize}
      end)

    {Enum.reverse(results), iodata, inserts, next_seq, bytes, pos}
  end

  # ── truncation ────────────────────────────────────────────────────────────

  # Logical truncation (the floor above) is already done; this only decides
  # whether the file itself is worth rewriting. A rewrite copies the live
  # suffix, so it pays off exactly when the dead prefix is both large in
  # absolute terms and at least as large as what would be copied. A log that is
  # compacted every tick therefore never rewrites: the dead prefix stays small.
  defp maybe_rewrite(state) do
    dead = dead_prefix(state)
    live = state.write_pos - dead

    if dead > 0 and dead >= state.rewrite_min_bytes and dead >= live do
      rewrite(state, dead, live)
    else
      state
    end
  end

  # Header offset of the first live frame. Live frames are always a contiguous
  # file suffix — truncation only ever drops a prefix — so everything before
  # this offset is dead bytes. An empty index means every frame is dead.
  defp dead_prefix(%{index: index, write_pos: write_pos}) do
    case :ets.first(index) do
      :"$end_of_table" ->
        write_pos

      first ->
        [{_seq, {off, _len}}] = :ets.lookup(index, first)
        off - @header_bytes
    end
  end

  # The rewrite is best effort: the logical floor is already durable, so a
  # rewrite that cannot proceed (a full disk while copying) just leaves the dead
  # prefix in place, and the next `truncate_through/2` with a higher seq retries
  # it.
  defp rewrite(state, dead, live) do
    tmp = state.path <> ".compact"
    # A `.compact` left by a crash mid-rewrite may be longer than `live`, and
    # opening it does not truncate: its stale tail would follow the copied
    # frames into the new log.
    _ = File.rm(tmp)

    case :file.open(tmp, [:read, :write, :raw, :binary]) do
      {:ok, tfd} ->
        # Rename while `state.fd` is still open: the rename is atomic, so a
        # failure up to here leaves the old file (and every index offset) as it
        # was. After it, `tfd` *is* the log — same inode — so it is adopted
        # rather than reopened.
        with :ok <- copy_range(state.fd, tfd, dead, live, 0),
             :ok <- :file.datasync(tfd),
             :ok <- :file.rename(tmp, state.path) do
          :file.close(state.fd)

          # Re-offset every live frame by the bytes now in front of them.
          entries =
            for {seq, {off, len}} <- :ets.tab2list(state.index), do: {seq, {off - dead, len}}

          :ets.delete_all_objects(state.index)
          if entries != [], do: :ets.insert(state.index, entries)

          %{state | fd: tfd, write_pos: live}
        else
          {:error, reason} ->
            :file.close(tfd)
            skip_rewrite(state, tmp, reason)
        end

      {:error, reason} ->
        skip_rewrite(state, tmp, reason)
    end
  end

  defp skip_rewrite(state, tmp, reason) do
    _ = File.rm(tmp)

    Logger.warning(
      "[ankusa] WAL rewrite skipped, logical truncation through #{state.truncated_through} " <>
        "is durable: #{inspect(reason)}"
    )

    state
  end

  defp copy_range(_src, _dst, _from, 0, _pos), do: :ok

  defp copy_range(src, dst, from, remaining, pos) do
    chunk = min(remaining, @rewrite_chunk)

    case :file.pread(src, from + pos, chunk) do
      {:ok, data} ->
        case :file.pwrite(dst, pos, data) do
          :ok -> copy_range(src, dst, from, remaining - chunk, pos + chunk)
          {:error, _} = error -> error
        end

      :eof ->
        {:error, :unexpected_eof}

      {:error, _} = error ->
        error
    end
  end

  # ── replay ────────────────────────────────────────────────────────────────

  defp replay(fd, index, truncated_through) do
    {:ok, size} = :file.position(fd, :eof)
    :file.position(fd, :bof)
    data = if size > 0, do: elem(:file.pread(fd, 0, size), 1), else: <<>>
    # 1-based seqs: cursor 0 means "nothing consumed", and read/2 (strictly `>`)
    # surfaces seq 1 onward. Empty log => next_seq starts at 1.
    parse(data, 0, index, 1, truncated_through)
  end

  defp parse(bin, pos, index, next_seq, truncated_through) do
    case bin do
      <<@magic::16, @version::8, _flags::8, seq::64, crc::32, len::32, rest::binary>> ->
        case rest do
          <<payload::binary-size(^len), tail::binary>> ->
            if :erlang.crc32(payload) == crc do
              # Frames below the floor are still physically present (the file is
              # only rewritten once it is worth it) but logically gone; they
              # must not be readable again.
              if seq > truncated_through do
                :ets.insert(index, {seq, {pos + @header_bytes, len}})
              end

              parse(tail, pos + @header_bytes + len, index, seq + 1, truncated_through)
            else
              # torn/corrupt payload — stop; this write was never acked
              {pos, next_seq}
            end

          _ ->
            {pos, next_seq}
        end

      _ ->
        {pos, next_seq}
    end
  end

  # ── helpers ───────────────────────────────────────────────────────────────

  defp frame(seq, payload) do
    len = byte_size(payload)
    crc = :erlang.crc32(payload)
    <<@magic::16, @version::8, 0::8, seq::64, crc::32, len::32, payload::binary>>
  end

  # Walk with `:ets.next/2` from the first live key rather than
  # `:ets.select/3` with a `>` guard: the match spec makes ETS scan the whole
  # ordered_set from the front, so one read became O(WAL) — measured 371µs per
  # call with the cursor at the tail of a 20k-record log, against 0.05µs for
  # this keyed walk. That scan, run once per WAL read, was the dispatch
  # pipeline's ceiling.
  defp select_after(index, after_seq, limit) do
    collect_after(index, :ets.next(index, after_seq), limit, [])
  end

  defp collect_after(_index, :"$end_of_table", _remaining, acc), do: Enum.reverse(acc)

  defp collect_after(_index, _key, 0, acc), do: Enum.reverse(acc)

  defp collect_after(index, key, remaining, acc) do
    entry = {key, :ets.lookup_element(index, key, 2)}
    remaining = if remaining == :infinity, do: :infinity, else: remaining - 1
    collect_after(index, :ets.next(index, key), remaining, [entry | acc])
  end

  defp seq_bounds(index) do
    case :ets.first(index) do
      :"$end_of_table" -> {nil, nil}
      first -> {first, :ets.last(index)}
    end
  end

  defp load_truncated_through(path) do
    case File.read(path) do
      {:ok, bin} -> :erlang.binary_to_term(bin, [:safe])
      {:error, _} -> 0
    end
  end

  defp load_cursors(path) do
    case File.read(path) do
      {:ok, bin} -> :erlang.binary_to_term(bin, [:safe])
      {:error, _} -> %{}
    end
  end

  # Write-then-rename, with the data fsynced *before* the rename: after power
  # loss the destination is either the whole new term or the whole old one, so
  # `binary_to_term/2` at boot can never see a truncated file. `File.write!/2`
  # would leave the rename ordered ahead of the data.
  #
  # A leftover `.tmp` on failure is harmless: the next attempt opens it with
  # `:write`, which truncates.
  defp persist_term(path, term) do
    tmp = path <> ".tmp"

    with {:ok, fd} <- :file.open(tmp, [:write, :raw, :binary]) do
      written =
        try do
          with :ok <- :file.write(fd, :erlang.term_to_binary(term)), do: :file.datasync(fd)
        after
          :file.close(fd)
        end

      with :ok <- written, do: :file.rename(tmp, path)
    end
  end
end
