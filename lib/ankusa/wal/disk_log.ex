defmodule Ankusa.WAL.DiskLog do
  @moduledoc """
  Default WAL: a durable append-only log on local disk. Zero external
  dependencies — it survives process crash and power loss on *this box*.

  It does **not** survive loss of the box; that is what the Postgres/Kafka/Ra
  adapters are for. The startup log says so, honestly.

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

  ## Dedup durability

  Committed `dedup_key`s live in an in-memory ETS set rebuilt on start. Because
  the log is truncated after compaction, a snapshot of the dedup set is persisted
  to `<name>.dedup` at truncation time and reloaded before replay, so dedup stays
  correct across compaction *and* restart.

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

    dedup = :ets.new(:ankusa_wal_dedup, [:set, :protected, read_concurrency: true])
    index = :ets.new(:ankusa_wal_index, [:ordered_set, :protected])

    # dedup snapshot survives truncation; load it before replaying live frames
    load_dedup_snapshot(path <> ".dedup", dedup)

    # Load the persisted state that bounds `next_seq` before replaying, so a
    # reclaimed (or fully rewritten) log still continues where it left off.
    truncated_through = load_truncated_through(path <> ".truncated")
    cursors = load_cursors(path <> ".cursors")

    {:ok, fd} = :file.open(path, [:read, :write, :raw, :binary])
    {valid_end, replay_next} = replay(fd, index, dedup, truncated_through)
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
       dedup: dedup,
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
    {results, iodata, inserts, dedup_inserts, next_seq, bytes, pos} =
      build_batch(records, state)

    if iodata == [] do
      # all duplicates — nothing to write, no fsync
      {:reply, {:ok, results}, state}
    else
      Ankusa.Telemetry.span([:commit], %{instance: state.instance}, fn ->
        :ok = :file.pwrite(state.fd, state.write_pos, iodata)
        :ok = :file.datasync(state.fd)
        # measurements, then metadata: `:duration` is added by the span itself.
        {:ok, %{batch_size: length(inserts), bytes: bytes}, %{}}
      end)

      :ets.insert(state.index, inserts)
      if dedup_inserts != [], do: :ets.insert(state.dedup, dedup_inserts)

      {:reply, {:ok, results}, %{state | write_pos: pos, next_seq: next_seq}}
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
    persist_term(state.path <> ".cursors", cursors)
    {:reply, :ok, %{state | cursors: cursors}}
  end

  def handle_call({:truncate_through, seq}, _from, %{truncated_through: floor} = state)
      when seq <= floor do
    {:reply, :ok, state}
  end

  def handle_call({:truncate_through, seq}, _from, state) do
    # Durably record the floor *before* dropping anything: a crash between the
    # two must not let a restarted node reuse seqs it already handed out.
    persist_term(state.path <> ".truncated", seq)
    :ets.select_delete(state.index, [{{:"$1", :_}, [{:"=<", :"$1", seq}], [true]}])

    {:reply, :ok, maybe_rewrite(%{state | truncated_through: seq})}
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

  # ── batch building ────────────────────────────────────────────────────────

  defp build_batch(records, state) do
    init = {[], [], [], [], state.next_seq, 0, state.write_pos, %{}}

    {results, iodata, inserts, dedup_inserts, next_seq, bytes, pos, _seen} =
      Enum.reduce(records, init, fn %{envelope: env}, acc ->
        {results, iodata, inserts, dedup_inserts, seq, bytes, pos, seen} = acc
        key = dedup_lookup_key(env)
        existing = if key, do: committed_seq(state.dedup, seen, key), else: nil

        cond do
          existing != nil ->
            Ankusa.Telemetry.emit([:dedup, :hit], %{}, %{
              source_id: env.source_id,
              instance: state.instance
            })

            {[{:duplicate, existing} | results], iodata, inserts, dedup_inserts, seq, bytes, pos,
             seen}

          true ->
            env = %{env | seq: seq}
            payload = Envelope.to_binary(env)
            frame = frame(seq, payload)
            plen = byte_size(payload)
            entry = {seq, {pos + @header_bytes, plen}}
            fsize = @header_bytes + plen

            {seen, dedup_inserts} =
              if key,
                do: {Map.put(seen, key, seq), [{key, seq} | dedup_inserts]},
                else: {seen, dedup_inserts}

            {[{:committed, env} | results], [iodata, frame], [entry | inserts], dedup_inserts,
             seq + 1, bytes + fsize, pos + fsize, seen}
        end
      end)

    {Enum.reverse(results), iodata, inserts, dedup_inserts, next_seq, bytes, pos}
  end

  # `nil` dedup_key means "no idempotency key" — always accept.
  defp dedup_lookup_key(%Envelope{dedup_key: nil}), do: nil
  defp dedup_lookup_key(%Envelope{tenant_id: t, source_id: s, dedup_key: k}), do: {t, s, k}

  defp committed_seq(dedup, seen, key) do
    case Map.get(seen, key) do
      nil ->
        case :ets.lookup(dedup, key) do
          [{^key, seq}] -> seq
          [] -> nil
        end

      seq ->
        seq
    end
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

  defp rewrite(state, dead, live) do
    # Frames about to be dropped carry dedup keys that must outlive them; the
    # snapshot is the same durability step the old per-frame truncation took.
    persist_dedup_snapshot(state.path <> ".dedup", state.dedup)

    tmp = state.path <> ".compact"
    {:ok, tfd} = :file.open(tmp, [:read, :write, :raw, :binary])
    :ok = copy_range(state.fd, tfd, dead, live, 0)
    :ok = :file.datasync(tfd)
    :file.close(tfd)

    :file.close(state.fd)
    :ok = :file.rename(tmp, state.path)

    {:ok, fd} = :file.open(state.path, [:read, :write, :raw, :binary])

    # Re-offset every live frame by the bytes now in front of them.
    entries = for {seq, {off, len}} <- :ets.tab2list(state.index), do: {seq, {off - dead, len}}
    :ets.delete_all_objects(state.index)
    if entries != [], do: :ets.insert(state.index, entries)

    %{state | fd: fd, write_pos: live}
  end

  defp copy_range(_src, _dst, _from, 0, _pos), do: :ok

  defp copy_range(src, dst, from, remaining, pos) do
    chunk = min(remaining, @rewrite_chunk)
    {:ok, data} = :file.pread(src, from + pos, chunk)
    :ok = :file.pwrite(dst, pos, data)
    copy_range(src, dst, from, remaining - chunk, pos + chunk)
  end

  # ── replay ────────────────────────────────────────────────────────────────

  defp replay(fd, index, dedup, truncated_through) do
    {:ok, size} = :file.position(fd, :eof)
    :file.position(fd, :bof)
    data = if size > 0, do: elem(:file.pread(fd, 0, size), 1), else: <<>>
    # 1-based seqs: cursor 0 means "nothing consumed", and read/2 (strictly `>`)
    # surfaces seq 1 onward. Empty log => next_seq starts at 1.
    parse(data, 0, index, dedup, 1, truncated_through)
  end

  defp parse(bin, pos, index, dedup, next_seq, truncated_through) do
    case bin do
      <<@magic::16, @version::8, _flags::8, seq::64, crc::32, len::32, rest::binary>> ->
        case rest do
          <<payload::binary-size(^len), tail::binary>> ->
            if :erlang.crc32(payload) == crc do
              # Frames below the floor are still physically present (the file is
              # only rewritten once it is worth it) but logically gone; they
              # must not be readable again. Their dedup keys still count.
              if seq > truncated_through do
                :ets.insert(index, {seq, {pos + @header_bytes, len}})
              end

              case dedup_key_of(payload) do
                nil -> :ok
                key -> :ets.insert(dedup, {key, seq})
              end

              parse(
                tail,
                pos + @header_bytes + len,
                index,
                dedup,
                seq + 1,
                truncated_through
              )
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

  defp dedup_key_of(payload) do
    env = Envelope.from_binary(payload)
    dedup_lookup_key(env)
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
  defp persist_term(path, term) do
    tmp = path <> ".tmp"
    {:ok, fd} = :file.open(tmp, [:write, :raw, :binary])

    try do
      :ok = :file.write(fd, :erlang.term_to_binary(term))
      :ok = :file.datasync(fd)
    after
      :file.close(fd)
    end

    :ok = :file.rename(tmp, path)
  end

  defp load_dedup_snapshot(path, dedup) do
    case File.read(path) do
      {:ok, bin} ->
        for {key, seq} <- :erlang.binary_to_term(bin, [:safe]), do: :ets.insert(dedup, {key, seq})
        :ok

      {:error, _} ->
        :ok
    end
  end

  defp persist_dedup_snapshot(path, dedup) do
    persist_term(path, :ets.tab2list(dedup))
  end
end
