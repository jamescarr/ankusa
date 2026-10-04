defmodule Ankusa.Store.Migrate do
  @moduledoc false

  # Imports a 0.3 data dir into the store, once, on first boot.
  #
  # 0.3 kept six artifacts under `<data_dir>/<instance>/`:
  #
  #   sources.json            API-managed sources
  #   rate_limits.json        rate-limit overrides
  #   quarantine/             the quarantine pen (`quarantine.log`)
  #   wal/                    the hooks (`ankusa.wal`) and their cursors
  #   dlq/                    the dead letters (`dlq.log`)
  #   segments/index.log      where each archived event lives in a segment
  #
  # Each is imported in that order, its marker written (synced) in the same batch
  # that finishes it, and then renamed to `<name>.migrated-<unix seconds>`, so a
  # crash anywhere leaves either a marker to honour or an artifact to import
  # again, never half of each. Nothing is deleted: the renamed artifacts are the
  # rollback.
  #
  # The one rule that matters: an artifact this node cannot read *completely and
  # trustworthily* stops the boot. A torn tail is the only damage that is
  # forgiven, because it is a write that was never acknowledged.

  require Logger

  alias Ankusa.{Config, Envelope, Fsync, Store}
  alias Ankusa.Queue.Deliveries
  alias Ankusa.Store.Keys

  @artifacts [:sources, :rate_limits, :quarantine, :wal, :dlq, :index]

  # What a `wal: :none` node cannot do anything with: it has no queue.
  @queue_artifacts [:wal, :dlq, :index]

  @wal_magic 0x484B
  @wal_version 1
  @wal_header 20

  @chunk 8 * 1024 * 1024
  @batch_records 1_000
  @batch_bytes 64 * 1024 * 1024
  # A frame header that claims more than this is not a frame.
  @max_frame 256 * 1024 * 1024

  @doc "Run the import. `:ok`, or `{:error, reason}` and the store must not start."
  @spec run(Config.t()) :: :ok | {:error, term()}
  def run(%Config{} = config) do
    ctx = %{config: config, instance: config.instance, stamp: System.system_time(:second)}

    with {:ok, done?} <- migration_done?(ctx) do
      if done?, do: tidy(ctx), else: import_all(ctx)
    end
  end

  # ── orchestration ────────────────────────────────────────────────────────

  defp migration_done?(ctx) do
    case Store.get(ctx.instance, :default, Keys.meta("migration")) do
      {:ok, "done"} -> {:ok, true}
      :not_found -> {:ok, false}
      {:ok, other} -> {:error, {:bad_migration_marker, other}}
      {:error, reason} -> {:error, reason}
    end
  end

  # The first boot of this store: import whatever 0.3 left, then record that the
  # migration is over. An artifact skipped because this node has no queue keeps
  # the migration open, so switching the node to `wal: :disk` later still finds
  # its backlog.
  defp import_all(ctx) do
    result =
      Enum.reduce_while(@artifacts, {:ok, 0}, fn artifact, {:ok, skipped} ->
        case import_artifact(ctx, artifact) do
          :ok -> {:cont, {:ok, skipped}}
          :skipped -> {:cont, {:ok, skipped + 1}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)

    case result do
      {:ok, 0} -> put_marker(ctx, Keys.meta("migration"), [])
      {:ok, _skipped} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp import_artifact(ctx, artifact) do
    path = path(ctx.config, artifact)
    marker = Keys.meta("imported:" <> Atom.to_string(artifact))

    cond do
      not File.exists?(path) ->
        :ok

      queue_artifact_without_queue?(ctx, artifact) ->
        Logger.warning(
          "[ankusa] #{path} is a 0.3 #{artifact} and this node has no queue (wal: :none); " <>
            "left in place, imported once the node runs wal: :disk"
        )

        :skipped

      marked?(ctx, marker) ->
        # Imported before a crash, never renamed.
        rename(ctx, artifact, path, "already imported")

      true ->
        with {:ok, count, extra_ops} <- do_import(ctx, artifact, path),
             :ok <- put_marker(ctx, marker, extra_ops) do
          rename(ctx, artifact, path, "imported #{count} #{noun(artifact)}")
        end
    end
  end

  # Once this store has been through its migration, a 0.3 artifact turning up is
  # not an upgrade: the node was already running 0.4. Never import over live
  # data; just say so.
  defp tidy(ctx) do
    Enum.each(@artifacts, fn artifact ->
      path = path(ctx.config, artifact)
      marker = Keys.meta("imported:" <> Atom.to_string(artifact))

      cond do
        not File.exists?(path) ->
          :ok

        marked?(ctx, marker) ->
          Logger.warning("[ankusa] #{path} was imported but not moved aside; moving it now")
          rename(ctx, artifact, path, "already imported")

        true ->
          Logger.error(
            "[ankusa] #{path} found after this store was created; not imported, left in place"
          )
      end
    end)

    :ok
  end

  defp queue_artifact_without_queue?(%{config: %Config{wal: :none}}, artifact),
    do: artifact in @queue_artifacts

  defp queue_artifact_without_queue?(_ctx, _artifact), do: false

  defp marked?(ctx, marker) do
    match?({:ok, _}, Store.get(ctx.instance, :default, marker))
  end

  defp put_marker(ctx, marker, extra_ops) do
    Store.write(ctx.instance, extra_ops ++ [{:put, :default, marker, "done"}], sync: true)
  end

  defp rename(ctx, artifact, path, what) do
    target = "#{path}.migrated-#{ctx.stamp}"

    case Fsync.rename(path, target) do
      :ok ->
        Logger.info("[ankusa] #{what} from #{path}; moved to #{target}")
        :ok

      {:error, reason} ->
        {:error, {:legacy_rename_failed, artifact, path, reason}}
    end
  end

  defp noun(:sources), do: "source(s)"
  defp noun(:rate_limits), do: "rate-limit override(s)"
  defp noun(:quarantine), do: "quarantined hook(s)"
  defp noun(:wal), do: "queued hook(s)"
  defp noun(:dlq), do: "dead letter(s)"
  defp noun(:index), do: "archive index row(s)"

  defp path(config, :sources), do: Config.path(config, "sources.json")
  defp path(config, :rate_limits), do: Config.path(config, "rate_limits.json")
  defp path(config, :quarantine), do: Config.path(config, "quarantine")
  defp path(config, :wal), do: Config.path(config, "wal")
  defp path(config, :dlq), do: Config.path(config, "dlq")
  defp path(config, :index), do: Config.path(config, "segments/index.log")

  defp do_import(ctx, :sources, path), do: import_sources(ctx, path)
  defp do_import(ctx, :rate_limits, path), do: import_rate_limits(ctx, path)
  defp do_import(ctx, :quarantine, path), do: import_quarantine(ctx, path)
  defp do_import(ctx, :wal, path), do: import_wal(ctx, path)
  defp do_import(ctx, :dlq, path), do: import_dlq(ctx, path)
  defp do_import(ctx, :index, path), do: import_index(ctx, path)

  # ── JSON artifacts ───────────────────────────────────────────────────────

  defp import_sources(ctx, path) do
    case read_json(path) do
      {:ok, %{"version" => 1, "sources" => entries}} when is_list(entries) ->
        ops = Enum.flat_map(entries, &source_ops/1)
        with :ok <- write(ctx, ops, :sync), do: {:ok, length(ops), []}

      {:error, {:legacy_read_failed, _, _}} = error ->
        error

      _ ->
        unreadable_json(path)
    end
  end

  defp import_rate_limits(ctx, path) do
    case read_json(path) do
      {:ok, %{"version" => 1, "tenants" => tenants}} when is_map(tenants) ->
        ops =
          Enum.flat_map(tenants, fn
            {tenant, attrs} when is_binary(tenant) and is_map(attrs) ->
              [{:put, :default, Keys.rate_limit(tenant), JSON.encode!(attrs)}]

            {tenant, _attrs} ->
              Logger.warning("[ankusa] skipping malformed rate limit for #{inspect(tenant)}")
              []
          end)

        with :ok <- write(ctx, ops, :sync), do: {:ok, length(ops), []}

      {:error, {:legacy_read_failed, _, _}} = error ->
        error

      _ ->
        unreadable_json(path)
    end
  end

  defp source_ops(%{"tenant" => tenant, "name" => name, "spec" => spec})
       when is_binary(tenant) and is_binary(name) and is_map(spec) do
    [{:put, :default, Keys.source(tenant, name), JSON.encode!(spec)}]
  end

  defp source_ops(entry) do
    Logger.warning("[ankusa] skipping malformed 0.3 source entry: #{inspect(entry)}")
    []
  end

  # A file that cannot be *read* is not the same as one that does not parse: a
  # permission or I/O error says nothing about its contents, so it refuses to
  # start like any other legacy read failure, instead of booting without the
  # sources or overrides it holds.
  defp read_json(path) do
    case File.read(path) do
      {:ok, body} -> JSON.decode(body)
      {:error, posix} -> {:error, {:legacy_read_failed, path, posix}}
    end
  end

  # A file this node read but cannot make sense of imports nothing, is kept
  # (renamed) for an operator, and does not stop the boot: 0.3 itself booted
  # without it.
  defp unreadable_json(path) do
    Logger.error("[ankusa] #{path}: unparseable or wrong shape; nothing imported from it")

    {:ok, 0, []}
  end

  # ── framed term logs ─────────────────────────────────────────────────────

  defp import_quarantine(ctx, path) do
    result =
      import_log(ctx, Path.join(path, "quarantine.log"), :unsafe, fn
        %{id: id, received_at: at} = record ->
          # The body value stays the 0.4 map; `Quarantine.envelope/2` rebuilds
          # an envelope from it. The summary gains the keys a current one has,
          # so the pen's byte cap counts imported entries too.
          held = :erlang.term_to_binary(Map.take(record, [:headers, :body]))

          summary =
            record
            |> Map.take([:id, :source_id, :received_at, :reason])
            |> Map.put(:tenant_id, nil)
            |> Ankusa.Edge.Quarantine.encode_summary(held)

          {:ok,
           [
             {:put, :quarantine, Keys.quarantine_summary(at, id), summary},
             {:put, :quarantine, Keys.quarantine_body(at, id), held}
           ], 0}

        _other ->
          :skip
      end)

    # Nothing here has a seq: no extra ops beyond the marker.
    with {:ok, count, _max_seq} <- result, do: {:ok, count, []}
  end

  defp import_index(ctx, path) do
    result =
      import_log(ctx, path, :safe, fn
        %{event_id: id, segment_key: key, offset: offset, length: length, seq: seq} ->
          value = :erlang.term_to_binary({key, offset, length, seq})
          {:ok, [{:put, :archive, Keys.legacy_location(id), value}], 0}

        _other ->
          :skip
      end)

    with {:ok, count, _max_seq} <- result, do: {:ok, count, []}
  end

  # 0.3 never removed a DLQ entry, so a hook dead-lettered twice (redelivered
  # after a restart, say) appears twice. There is one delivery row per hook and
  # sink, so the later entry wins and the earlier dead key is deleted: a dead
  # row has exactly one `?x` key.
  defp import_dlq(ctx, path) do
    seen = :ets.new(:ankusa_migrate_dlq_seen, [:set, :private])

    result =
      try do
        import_log(ctx, Path.join(path, "dlq.log"), :unsafe, fn
          %{envelope: %Envelope{seq: seq} = env, reason: reason, at: at} when is_integer(seq) ->
            bin = Envelope.to_binary(%{env | seq: nil})
            size = byte_size(bin)
            error = inspect(reason, limit: 50, printable_limit: 4096)
            unresolved = Deliveries.unresolved_dead()
            dead_key = Keys.dead(at, seq, unresolved)

            superseded =
              case :ets.lookup(seen, seq) do
                [{^seq, ^dead_key}] -> []
                [{^seq, old_key}] -> [{:delete, :index, old_key}]
                [] -> []
              end

            true = :ets.insert(seen, {seq, dead_key})

            row =
              Deliveries.encode_row(%{
                module: nil,
                state: :dead,
                attempts: 0,
                at: at,
                error: error,
                size: size
              })

            {:ok,
             superseded ++
               [
                 {:put, :hooks, Keys.hook(seq), bin},
                 {:put, :deliveries, Keys.delivery(seq, unresolved), row},
                 {:put, :index, dead_key, :erlang.term_to_binary({env.source_id, env.id})}
               ], seq}

          _other ->
            :skip
        end)
      after
        :ets.delete(seen)
      end

    with {:ok, count, max_seq} <- result do
      with {:ok, ops} <- next_seq_ops(ctx, max_seq + 1), do: {:ok, count, ops}
    end
  end

  # Reads `<<len::32, term::binary-size(len)>>` frames one at a time. A torn
  # final frame ends the import (an append that never completed); a term that
  # will not decode ends it too, loudly, and the file is still kept. `fun`
  # returns `{:ok, ops, max_seq}` or `:skip`.
  defp import_log(ctx, file, safety, fun) do
    case File.exists?(file) do
      false -> {:ok, 0, 0}
      true -> read_frames(ctx, file, safety, fun)
    end
  end

  defp read_frames(ctx, file, safety, fun) do
    with {:ok, reader} <- open_reader(file) do
      try do
        acc = %{ctx: ctx, batch: new_batch(), count: 0, max_seq: 0, file: file}
        frames(reader, 0, safety, fun, acc)
      after
        close_reader(reader)
      end
    end
  end

  defp frames(reader, pos, safety, fun, acc) do
    case read_at(reader, pos, 4) do
      {:eof, _} ->
        finish_log(acc)

      {<<len::32>>, _reader} when len > @max_frame ->
        # Not a length any 0.3 record had: the prefix itself is damaged. Stop
        # without reading `len` bytes (it could be most of a large file), and
        # say how much is left unread.
        Logger.error(
          "[ankusa] #{acc.file}: record at byte #{pos} claims #{len} bytes, which no 0.3 " <>
            "record had; stopped after #{acc.count} imported, #{reader.size - pos} bytes " <>
            "not imported (the file is kept)"
        )

        finish_log(acc)

      {<<len::32>>, reader} ->
        case read_at(reader, pos + 4, len) do
          {bin, reader} when is_binary(bin) and byte_size(bin) == len ->
            case decode(bin, safety) do
              {:ok, term} ->
                with {:ok, acc} <- apply_record(fun.(term), acc) do
                  frames(reader, pos + 4 + len, safety, fun, acc)
                end

              :error ->
                Logger.error(
                  "[ankusa] #{acc.file}: record at byte #{pos} does not decode; " <>
                    "stopped after #{acc.count} imported (the file is kept)"
                )

                finish_log(acc)
            end

          _torn ->
            torn(acc, pos, reader.size)
        end

      {_partial, _} ->
        torn(acc, pos, reader.size)
    end
  end

  # A record that runs past the end of the file: the append that never
  # finished, or a damaged length prefix. Either way nothing after it can be
  # read; the byte count lets an operator tell a few torn bytes from a lot.
  defp torn(acc, pos, size) do
    Logger.warning(
      "[ankusa] #{acc.file}: record at byte #{pos} runs past the end of the file; " <>
        "#{size - pos} trailing bytes ignored"
    )

    finish_log(acc)
  end

  defp apply_record(:skip, acc), do: {:ok, acc}

  defp apply_record({:ok, ops, seq}, acc) do
    acc = %{
      acc
      | batch: add_to_batch(acc.batch, ops),
        count: acc.count + 1,
        max_seq: max(acc.max_seq, seq)
    }

    with {:ok, batch} <- maybe_flush(acc.ctx, acc.batch), do: {:ok, %{acc | batch: batch}}
  end

  defp finish_log(acc) do
    with :ok <- flush(acc.ctx, acc.batch), do: {:ok, acc.count, acc.max_seq}
  end

  defp decode(bin, :unsafe) do
    {:ok, :erlang.binary_to_term(bin)}
  rescue
    ArgumentError -> :error
  end

  defp decode(bin, :safe) do
    {:ok, :erlang.binary_to_term(bin, [:safe])}
  rescue
    ArgumentError -> :error
  end

  # ── the WAL ──────────────────────────────────────────────────────────────

  defp import_wal(ctx, dir) do
    file = Path.join(dir, "ankusa.wal")

    with {:ok, floor} <- read_floor(Path.join(dir, "ankusa.wal.truncated"), dir),
         {:ok, cursors} <- read_cursors(Path.join(dir, "ankusa.wal.cursors"), dir) do
      dispatch = Map.get(cursors, :dispatch, 0)
      compactor = Map.get(cursors, :compactor, 0)
      archive? = Config.role?(ctx.config, :storage)
      start = max(floor, if(archive?, do: min(dispatch, compactor), else: dispatch))
      plan = %{start: start, dispatch: dispatch, compactor: compactor, archive?: archive?}

      with {:ok, count, last_seq} <- import_wal_file(ctx, file, plan) do
        high = Enum.max([last_seq, floor, dispatch, compactor])

        with {:ok, ops} <- next_seq_ops(ctx, high + 1), do: {:ok, count, ops}
      end
    end
  end

  # A cursor or floor this node cannot read must not be guessed: too low
  # redelivers hooks, too high skips them.
  defp read_cursors(path, dir) do
    with {:ok, term} <- read_sidecar(path, dir, %{}) do
      if is_map(term) and
           Enum.all?(term, fn {k, v} -> is_atom(k) and is_integer(v) and v >= 0 end),
         do: {:ok, term},
         else: corrupt_sidecar(path, dir)
    end
  end

  defp read_floor(path, dir) do
    with {:ok, term} <- read_sidecar(path, dir, 0) do
      if is_integer(term) and term >= 0, do: {:ok, term}, else: corrupt_sidecar(path, dir)
    end
  end

  defp read_sidecar(path, dir, default) do
    case File.read(path) do
      {:error, :enoent} ->
        {:ok, default}

      {:ok, bin} ->
        try do
          {:ok, :erlang.binary_to_term(bin, [:safe])}
        rescue
          ArgumentError -> corrupt_sidecar(path, dir)
        end

      {:error, posix} ->
        {:error, {:legacy_read_failed, path, posix}}
    end
  end

  defp corrupt_sidecar(path, dir) do
    Logger.error(
      "[ankusa] #{path} is unreadable or not what 0.3 wrote. Refusing to start: guessing a " <>
        "cursor could redeliver or skip hooks. Move #{dir} aside to start without it."
    )

    {:error, {:corrupt_legacy_sidecar, path}}
  end

  defp import_wal_file(ctx, file, plan) do
    if File.exists?(file) do
      with {:ok, reader} <- open_reader(file) do
        try do
          acc = %{ctx: ctx, batch: new_batch(), count: 0, last_seq: 0, plan: plan, file: file}
          wal_frames(reader, 0, acc)
        after
          close_reader(reader)
        end
      end
    else
      {:ok, 0, 0}
    end
  end

  defp wal_frames(reader, pos, acc) do
    case read_at(reader, pos, @wal_header) do
      {:eof, _} ->
        finish_wal(acc)

      {<<@wal_magic::16, @wal_version::8, _flags::8, seq::64, crc::32, len::32>>, reader}
      when len <= @max_frame ->
        cond do
          pos + @wal_header + len > reader.size ->
            torn_or_damaged(reader, pos, acc)

          seq <= acc.plan.start ->
            # Below the floor or already consumed everywhere: stepped over.
            wal_frames(reader, pos + @wal_header + len, %{acc | last_seq: max(acc.last_seq, seq)})

          true ->
            {payload, reader} = read_at(reader, pos + @wal_header, len)

            if :erlang.crc32(payload) == crc do
              with {:ok, acc} <- wal_record(seq, payload, acc) do
                wal_frames(reader, pos + @wal_header + len, acc)
              end
            else
              torn_or_damaged(reader, pos, acc)
            end
        end

      _bad_or_short ->
        torn_or_damaged(reader, pos, acc)
    end
  end

  defp wal_record(seq, payload, acc) do
    env = Envelope.from_binary(payload)
    bin = Envelope.to_binary(%{env | seq: nil})
    size = byte_size(bin)
    plan = acc.plan
    deliver? = seq > plan.dispatch
    archive? = plan.archive? and seq > plan.compactor

    ops =
      [{:put, :hooks, Keys.hook(seq), bin}] ++
        if(deliver?, do: pending_ops(seq, size), else: []) ++
        if archive?, do: [{:put, :index, Keys.archive_pending(seq), <<size::32>>}], else: []

    acc = %{
      acc
      | batch: add_to_batch(acc.batch, ops),
        count: acc.count + 1,
        last_seq: max(acc.last_seq, seq)
    }

    with {:ok, batch} <- maybe_flush(acc.ctx, acc.batch), do: {:ok, %{acc | batch: batch}}
  end

  # The sink a 0.3 hook was bound to was never recorded; the Pipeline expands
  # this row into one per current sink of the source when it claims it.
  defp pending_ops(seq, size) do
    unresolved = Deliveries.unresolved_pending()

    row =
      Deliveries.encode_row(%{
        module: nil,
        state: :pending,
        attempts: 0,
        at: 0,
        error: nil,
        size: size
      })

    [
      {:put, :deliveries, Keys.delivery(seq, unresolved), row},
      {:put, :index, Keys.due(0, seq, unresolved), <<size::32>>}
    ]
  end

  defp finish_wal(acc) do
    with :ok <- flush(acc.ctx, acc.batch), do: {:ok, acc.count, acc.last_seq}
  end

  # A frame that does not parse. If a valid frame follows anywhere later, this is
  # damage in the middle of the log: hooks that were acked sit after it, and
  # carrying on would drop them. If nothing valid follows, it is the torn tail of
  # a write that was never acked.
  defp torn_or_damaged(reader, pos, acc) do
    case find_valid_frame(reader, pos + 1) do
      {:ok, later} ->
        dir = Path.dirname(acc.file)

        Logger.error(
          "[ankusa] #{acc.file}: frame at byte #{pos} is damaged and a valid frame follows at " <>
            "byte #{later}. Refusing to start: acked hooks after the damage would be lost. " <>
            "Move #{dir} aside to start without it."
        )

        {:error, {:damaged_legacy_wal, acc.file, pos, later}}

      :none ->
        Logger.warning("[ankusa] #{acc.file}: torn final frame at byte #{pos}; ignored")
        finish_wal(acc)
    end
  end

  # Scan for the next offset at which a whole frame parses with a matching CRC.
  defp find_valid_frame(reader, from) do
    scan_frames(reader, from)
  end

  defp scan_frames(reader, from) when from >= reader.size, do: :none

  defp scan_frames(reader, from) do
    {chunk, reader} = read_at(reader, from, @chunk)
    magic = <<@wal_magic::16, @wal_version::8>>

    candidate =
      chunk
      |> :binary.matches(magic)
      |> Enum.find_value(fn {offset, _} ->
        at = from + offset
        if valid_frame_at?(reader, at), do: at
      end)

    cond do
      candidate != nil -> {:ok, candidate}
      # Step back a header so a frame straddling the chunk boundary is seen.
      byte_size(chunk) < @chunk -> :none
      true -> scan_frames(reader, from + @chunk - @wal_header)
    end
  end

  defp valid_frame_at?(reader, at) do
    case read_at(reader, at, @wal_header) do
      {<<@wal_magic::16, @wal_version::8, _flags::8, _seq::64, crc::32, len::32>>, reader}
      when len <= @max_frame ->
        if at + @wal_header + len <= reader.size do
          {payload, _} = read_at(reader, at + @wal_header, len)
          :erlang.crc32(payload) == crc
        else
          false
        end

      _ ->
        false
    end
  end

  # ── chunked reads ────────────────────────────────────────────────────────

  # A read-ahead window over a file, so a multi-gigabyte WAL is walked in
  # bounded memory. `read_at/3` serves from the window and refills it on a miss.
  defp open_reader(path) do
    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        case :file.position(fd, :eof) do
          {:ok, size} -> {:ok, %{fd: fd, size: size, buf: <<>>, buf_at: 0, path: path}}
          {:error, posix} -> {:error, {:legacy_read_failed, path, posix}}
        end

      {:error, posix} ->
        {:error, {:legacy_read_failed, path, posix}}
    end
  end

  defp close_reader(%{fd: fd}), do: :file.close(fd)

  # `{binary, reader}`: the bytes at `[pos, pos + n)`, short at the end of the
  # file; `{:eof, reader}` when `pos` is at or past the end. A read error is not
  # an end: it raises, and the boot fails rather than treating it as torn.
  defp read_at(%{size: size} = reader, pos, _n) when pos >= size, do: {:eof, reader}

  defp read_at(reader, pos, n) do
    n = min(n, reader.size - pos)

    if pos >= reader.buf_at and pos + n <= reader.buf_at + byte_size(reader.buf) do
      {binary_part(reader.buf, pos - reader.buf_at, n), reader}
    else
      want = min(max(n, @chunk), reader.size - pos)

      case :file.pread(reader.fd, pos, want) do
        {:ok, bin} ->
          reader = %{reader | buf: bin, buf_at: pos}
          {binary_part(bin, 0, min(n, byte_size(bin))), reader}

        :eof ->
          {:eof, reader}

        {:error, posix} ->
          raise "legacy read of #{reader.path} failed: #{inspect(posix)}"
      end
    end
  end

  # ── batches ──────────────────────────────────────────────────────────────

  # `ops` holds one list per record, newest first, and `flush/2` writes them
  # oldest first: a key written by two records in one batch ends up with the
  # later record's value, the same as across a flush.
  defp new_batch, do: %{ops: [], count: 0, bytes: 0}

  defp add_to_batch(batch, ops) do
    bytes =
      Enum.reduce(ops, 0, fn
        {:put, _cf, _k, v}, acc -> acc + byte_size(v)
        _other, acc -> acc
      end)

    %{batch | ops: [ops | batch.ops], count: batch.count + 1, bytes: batch.bytes + bytes}
  end

  defp maybe_flush(ctx, batch) do
    if batch.count >= @batch_records or batch.bytes >= @batch_bytes do
      with :ok <- flush(ctx, batch), do: {:ok, new_batch()}
    else
      {:ok, batch}
    end
  end

  defp flush(_ctx, %{ops: []}), do: :ok
  defp flush(ctx, batch), do: write(ctx, batch.ops |> Enum.reverse() |> Enum.concat(), :async)

  defp write(ctx, ops, mode) do
    case Store.write(ctx.instance, ops, sync: mode == :sync) do
      :ok -> :ok
      {:error, reason} -> {:error, {:import_write_failed, reason}}
    end
  end

  # `m:next_seq` never moves backwards, and a seq in the imported data is never
  # handed out again.
  defp next_seq_ops(ctx, candidate) do
    existing =
      case Store.get(ctx.instance, :default, Keys.meta("next_seq")) do
        {:ok, <<n::64>>} -> {:ok, n}
        :not_found -> {:ok, 1}
        {:error, reason} -> {:error, reason}
      end

    with {:ok, n} <- existing do
      {:ok, [{:put, :default, Keys.meta("next_seq"), <<max(n, candidate)::64>>}]}
    end
  end
end
