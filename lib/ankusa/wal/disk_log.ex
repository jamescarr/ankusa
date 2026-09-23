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
  """

  @behaviour Ankusa.WAL
  use GenServer

  require Logger
  alias Ankusa.{Config, Envelope}

  @magic 0x484B
  @version 1
  @header_bytes 20

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

    {:ok, fd} = :file.open(path, [:read, :write, :raw, :binary])
    {valid_end, next_seq} = replay(fd, index, dedup)
    {:ok, _} = :file.position(fd, valid_end)
    :ok = :file.truncate(fd)

    cursors = load_cursors(path <> ".cursors")

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
       cursors: cursors
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

  def handle_call({:truncate_through, seq}, _from, state) do
    {:reply, :ok, do_truncate(state, seq)}
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

  defp do_truncate(state, cutoff) do
    # snapshot dedup first so its coverage survives dropping the frames
    persist_dedup_snapshot(state.path <> ".dedup", state.dedup)

    survivors = select_after(state.index, cutoff, :infinity)

    tmp = state.path <> ".compact"
    {:ok, tfd} = :file.open(tmp, [:read, :write, :raw, :binary])

    {new_index, new_pos} =
      Enum.reduce(survivors, {[], 0}, fn {seq, {off, len}}, {acc, pos} ->
        {:ok, payload} = :file.pread(state.fd, off, len)
        :ok = :file.pwrite(tfd, pos, frame(seq, payload))
        {[{seq, {pos + @header_bytes, len}} | acc], pos + @header_bytes + len}
      end)

    :ok = :file.datasync(tfd)
    :file.close(tfd)
    :file.close(state.fd)
    :ok = :file.rename(tmp, state.path)

    {:ok, fd} = :file.open(state.path, [:read, :write, :raw, :binary])
    :ets.delete_all_objects(state.index)
    if new_index != [], do: :ets.insert(state.index, new_index)

    %{state | fd: fd, write_pos: new_pos}
  end

  # ── replay ────────────────────────────────────────────────────────────────

  defp replay(fd, index, dedup) do
    {:ok, size} = :file.position(fd, :eof)
    :file.position(fd, :bof)
    data = if size > 0, do: elem(:file.pread(fd, 0, size), 1), else: <<>>
    # 1-based seqs: cursor 0 means "nothing consumed", and read/2 (strictly `>`)
    # surfaces seq 1 onward. Empty log => next_seq starts at 1.
    parse(data, 0, index, dedup, 1)
  end

  defp parse(bin, pos, index, dedup, next_seq) do
    case bin do
      <<@magic::16, @version::8, _flags::8, seq::64, crc::32, len::32, rest::binary>> ->
        case rest do
          <<payload::binary-size(^len), tail::binary>> ->
            if :erlang.crc32(payload) == crc do
              :ets.insert(index, {seq, {pos + @header_bytes, len}})

              case dedup_key_of(payload) do
                nil -> :ok
                key -> :ets.insert(dedup, {key, seq})
              end

              parse(tail, pos + @header_bytes + len, index, dedup, seq + 1)
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

  defp select_after(index, after_seq, limit) do
    ms = [{{:"$1", :"$2"}, [{:>, :"$1", after_seq}], [{{:"$1", :"$2"}}]}]

    case limit do
      :infinity ->
        :ets.select(index, ms)

      n when is_integer(n) ->
        case :ets.select(index, ms, n) do
          {rows, _cont} -> rows
          :"$end_of_table" -> []
        end
    end
  end

  defp seq_bounds(index) do
    case :ets.first(index) do
      :"$end_of_table" -> {nil, nil}
      first -> {first, :ets.last(index)}
    end
  end

  defp load_cursors(path) do
    case File.read(path) do
      {:ok, bin} -> :erlang.binary_to_term(bin, [:safe])
      {:error, _} -> %{}
    end
  end

  defp persist_term(path, term) do
    tmp = path <> ".tmp"
    File.write!(tmp, :erlang.term_to_binary(term))
    File.rename!(tmp, path)
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
