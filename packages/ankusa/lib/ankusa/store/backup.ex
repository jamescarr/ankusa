defmodule Ankusa.Store.Backup do
  @moduledoc """
  Continuous backup of the node's store to an object store, and its restore
  into an empty `data_dir` at boot. Off unless `backup.enabled`.

  Every `backup.interval_ms` this process asks the store for a checkpoint
  (`Ankusa.Store.checkpoint/2`: memtables flushed, files hard-linked) and
  uploads it through the `:backup` scope of `Ankusa.BlobStore` (the
  `backup.blob_store`, else the segment store, under `storage.key_prefix`):

    * `backup/sst/<name>` — every `.sst` and `.blob` file. RocksDB never
      rewrites one, so each is uploaded once and shared by every backup that
      lists it.
    * `backup/<id>/<name>` — every other file (`MANIFEST-*`, `CURRENT`,
      `OPTIONS-*`, the WAL), per backup.
    * `backup/<id>/manifest.json` — every file with its key, size and sha256,
      written after all of them.
    * `backup/LATEST` — the id, written last. A backup is the one `LATEST`
      names; anything an interrupted attempt left is ignored, then deleted.

  After each successful upload, backups beyond `backup.keep` and every shared
  file no kept backup lists are deleted. A failed attempt is logged and
  retried after the interval doubled per consecutive failure (jittered,
  capped at a minute), like the compactor.

  One store per prefix: the first backup gives the store an id (the
  `m:store_id` row, so every checkpoint and every restore carries it) and
  every manifest records it. When `LATEST` names another store's backup, the
  attempt fails with `{:foreign_backup, theirs, ours}` — nothing uploaded,
  nothing purged — because two stores on one prefix would delete each
  other's backups as stale. Two live nodes can also share one id (an old
  host back beside the replacement that restored its backup): once this
  process has written a backup, `LATEST` naming one it did not write fails
  the attempt with `{:prefix_shared, latest}`, so one of the two stops.

  At boot, `Ankusa.Store` calls `restore/2` when its directory holds no
  database: the `LATEST` backup is downloaded, every file checked against
  its size and sha256, `CURRENT` written last (a restore that fails part-way
  leaves no database, and the next boot starts it over). No backup at all
  starts an empty store; a backup location that cannot be read refuses the
  boot instead. `reconcile_archive/1` then squares the new store with the
  segments in the bucket. A restore (or a fresh start) leaves a
  `RESTORE-IN-PROGRESS` marker in the directory until that reconcile has
  succeeded, so a node that dies in between reconciles on its next boot
  (`reconcile_pending?/1`) rather than booting as an existing store.

  The recovery point is the last backup: hooks acked after it are lost with
  the host, except for the archive copies `reconcile_archive/1` finds.

  Emits `[:ankusa, :backup, :stop]` per attempt and `[:ankusa, :backup,
  :state]` (`age_seconds` since the last successful backup) after it.
  """

  use GenServer

  require Logger

  alias Ankusa.{BlobStore, Config, Store}
  alias Ankusa.Queue.{Archive, Reclaim}
  alias Ankusa.Store.Keys

  @manifest_version 1

  @shared_exts [".sst", ".blob"]

  # Same shape as the compactor's: interval·2^n, jittered, capped at a minute.
  @max_backoff_ms 60_000

  @id_re ~r/\A[0-9A-HJKMNP-TV-Z]{26}\z/
  @sha256_re ~r/\A[0-9a-f]{64}\z/
  @segment_re ~r"seg/(\d{20})-(\d{20})\.seg\z"

  # ── lifecycle ─────────────────────────────────────────────────────────────

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :backup))
  end

  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc """
  Take one backup now, synchronously: the number of files uploaded (shared
  files already in the object store are not) and their bytes.
  """
  @spec run(atom()) ::
          {:ok, %{id: String.t(), uploaded: non_neg_integer(), bytes: non_neg_integer()}}
          | {:error, term()}
  def run(instance), do: GenServer.call(Ankusa.via(instance, :backup), :run, 300_000)

  @doc "The checks `Ankusa.Config.new/1` leaves to this module."
  @spec validate_config!(Config.t()) :: :ok
  def validate_config!(%Config{backup: backup}) do
    unless is_boolean(Map.get(backup, :enabled)) do
      raise ArgumentError,
            "backup.enabled must be a boolean, got #{inspect(Map.get(backup, :enabled))}"
    end

    # The value is not printed: a malformed pair can still hold credentials.
    case Map.get(backup, :blob_store) do
      nil -> :ok
      {mod, opts} when is_atom(mod) and is_list(opts) -> :ok
      _other -> raise ArgumentError, "backup.blob_store must be nil or {module, opts}"
    end
  end

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    %Config{} = config = Ankusa.config(instance)
    interval = config.backup.interval_ms

    # What a crash mid-upload left behind.
    _ = File.rm_rf(checkpoint_dir(config))
    warn_if_local(instance, config)
    schedule(interval)

    state = %{
      instance: instance,
      config: config,
      interval: interval,
      failures: 0,
      # name => manifest entry of every shared file `LATEST` lists; loaded
      # after init, so a slow object store does not hold up the boot
      known: nil,
      last_success_ms: nil,
      # This process's writer nonce, recorded in every manifest it writes, and
      # whether one of its backups succeeded: once one has, a `LATEST` whose
      # manifest carries another writer means a second node moved it
      # (`check_owner/3`). An upload of ours that failed after `LATEST` landed
      # still carries our nonce, so it can never trip the check.
      writer: Base.encode32(:crypto.strong_rand_bytes(10), case: :lower, padding: false),
      wrote?: false,
      started_ms: System.system_time(:millisecond)
    }

    {:ok, state, {:continue, :load_latest}}
  end

  # Crash reports print the state; the config carries the object store's
  # credentials.
  @impl true
  def format_status(%{state: %{config: _} = state} = status),
    do: %{status | state: %{state | config: :redacted}}

  def format_status(status), do: status

  @impl true
  def handle_continue(:load_latest, state), do: {:noreply, %{state | known: load_known(state)}}

  @impl true
  def handle_call(:run, _from, state) do
    {result, state} = backup(state)
    {:reply, result, state}
  end

  @impl true
  def handle_info(:tick, state) do
    {_result, state} = backup(state)
    schedule(next_delay(state))
    {:noreply, state}
  end

  defp next_delay(%{failures: 0, interval: interval}), do: interval

  defp next_delay(%{failures: n, interval: interval}) do
    capped = (interval * Integer.pow(2, min(n, 16))) |> min(@max_backoff_ms) |> max(interval)
    max(interval, round(capped * (0.5 + :rand.uniform() * 0.5)))
  end

  defp schedule(interval), do: Process.send_after(self(), :tick, interval)

  defp warn_if_local(instance, config) do
    case BlobStore.resolve_config(config, :backup) do
      {Ankusa.BlobStore.LocalFS, opts, _prefix} ->
        root = Keyword.get(opts, :root) || Config.path(config, "segments")

        Logger.warning(
          "[ankusa] backup store for #{inspect(instance)} is the local filesystem (#{root}); " <>
            "it does not survive losing this host"
        )

      _ ->
        :ok
    end
  end

  defp load_known(state) do
    case load_latest(state.instance, BlobStore.resolve_config(state.config, :backup)) do
      {:ok, manifest} ->
        shared_entries(manifest)

      :none ->
        %{}

      {:error, reason} ->
        # Harmless: every file is uploaded again by the next backup.
        Logger.warning(
          "[ankusa] latest backup not read, its files will be uploaded again: #{inspect(reason)}"
        )

        %{}
    end
  end

  defp shared_entries(manifest) do
    for %{"kind" => "shared", "name" => name} = entry <- manifest["files"],
        into: %{},
        do: {name, entry}
  end

  # ── one backup ────────────────────────────────────────────────────────────

  defp backup(state) do
    started = System.monotonic_time()
    dir = checkpoint_dir(state.config)
    known = state.known || load_known(state)
    id = new_id()

    result =
      try do
        take(state, dir, known, id)
      rescue
        error -> {:error, {:raised, error}}
      catch
        :exit, reason -> {:error, {:exit, reason}}
        :throw, value -> {:error, {:throw, value}}
      after
        File.rm_rf(dir)
      end

    duration = System.monotonic_time() - started
    now = System.system_time(:millisecond)

    {reply, state} =
      case result do
        {:ok, %{id: id, uploaded: uploaded, bytes: bytes, manifest: manifest}} ->
          if state.failures > 0 or state.last_success_ms == nil do
            Logger.info(
              "[ankusa] store backed up: #{id}, #{uploaded} file(s), #{bytes} bytes uploaded"
            )
          end

          emit_stop(state, %{duration: duration, files: uploaded, bytes: bytes}, %{result: :ok})
          purge(state, id)

          {{:ok, %{id: id, uploaded: uploaded, bytes: bytes}},
           %{
             state
             | failures: 0,
               known: shared_entries(manifest),
               last_success_ms: now,
               wrote?: true
           }}

        {:error, reason} ->
          log_failure(state, store_root(state), reason)

          emit_stop(state, %{duration: duration, files: 0, bytes: 0}, %{
            result: :error,
            reason: reason
          })

          {{:error, reason}, %{state | failures: state.failures + 1, known: known}}
      end

    Ankusa.Telemetry.emit(
      [:backup, :state],
      %{age_seconds: (now - (state.last_success_ms || state.started_ms)) / 1000},
      %{instance: state.instance}
    )

    {reply, state}
  end

  defp log_failure(state, root, {:foreign_backup, other, store_id}) do
    Logger.error(
      "[ankusa] backup refused: #{root} already holds backups of store #{other} " <>
        "(this node is #{store_id}; #{state.failures + 1} in a row). Two nodes must not share " <>
        "a backup prefix: give this node its own storage.key_prefix or backup store, " <>
        "or delete #{root} there to start over."
    )
  end

  defp log_failure(state, root, {:prefix_shared, latest}) do
    Logger.error(
      "[ankusa] backup refused: #{root}LATEST names #{latest}, a backup of this store that " <>
        "this node did not write (#{state.failures + 1} in a row). Another node with the same " <>
        "store (an old host beside the replacement that restored it?) is backing up there; " <>
        "retire one of them and restart the other, or give it its own storage.key_prefix or " <>
        "backup store."
    )
  end

  defp log_failure(state, _root, reason) do
    Logger.warning(
      "[ankusa] store backup failed (#{state.failures + 1} in a row), " <>
        "retried with backoff: #{inspect(reason)}"
    )
  end

  defp store_root(state), do: key(BlobStore.resolve_config(state.config, :backup), "")

  defp emit_stop(state, measurements, meta) do
    Ankusa.Telemetry.emit(
      [:backup, :stop],
      measurements,
      Map.put(meta, :instance, state.instance)
    )
  end

  defp take(state, dir, known, id) do
    store = BlobStore.resolve_config(state.config, :backup)

    # The id goes into the store before the checkpoint, so a restore of this
    # backup inherits it and carries on backing up to the same place.
    with {:ok, store_id} <- ensure_store_id(state.instance),
         :ok <- check_owner(state, store, store_id),
         :ok <- remove(dir),
         :ok <- Store.checkpoint(state.instance, dir),
         {:ok, names} <- checkpoint_files(dir),
         {shared, private} = Enum.split_with(names, &shared?/1),
         {:ok, shared_entries, n1, b1} <- upload_shared(state.instance, store, dir, shared, known),
         {:ok, private_entries, n2, b2} <- upload_private(state.instance, store, dir, id, private),
         manifest = %{
           "version" => @manifest_version,
           "id" => id,
           "created_at_ms" => System.system_time(:millisecond),
           "store_id" => store_id,
           "writer" => state.writer,
           "files" => shared_entries ++ private_entries
         },
         :ok <-
           put(state.instance, store, key(store, "#{id}/manifest.json"), JSON.encode!(manifest)),
         :ok <- put(state.instance, store, key(store, "LATEST"), id) do
      {:ok, %{id: id, uploaded: n1 + n2, bytes: b1 + b2, manifest: manifest}}
    end
  end

  # Which store this is, for `check_owner/3`: written once, by the first
  # backup, and carried by every checkpoint from then on.
  defp ensure_store_id(instance) do
    key = Keys.meta("store_id")

    case Store.get(instance, :default, key) do
      {:ok, id} ->
        {:ok, id}

      :not_found ->
        id = Base.encode32(:crypto.strong_rand_bytes(10), case: :lower, padding: false)

        case Store.write(instance, [{:put, :default, key, id}], sync: true) do
          :ok -> {:ok, id}
          {:error, reason} -> {:error, {:store_id, reason}}
        end

      {:error, reason} ->
        {:error, {:store_id, reason}}
    end
  end

  # Two stores backing up under one prefix would each purge the other's
  # backups as stale. The latest backup there must be this store's (or
  # predate store ids, or not exist); otherwise nothing is uploaded and
  # nothing is deleted. A store restored from a backup inherits its id, so two
  # live nodes can share one (the old host back beside its replacement): once
  # this process has written a backup, a `LATEST` written by another process
  # of the same store means another writer moved it, and this one stops.
  defp check_owner(state, store, store_id) do
    case load_latest(state.instance, store) do
      {:ok, %{"store_id" => other}} when other != store_id ->
        {:error, {:foreign_backup, other, store_id}}

      {:ok, %{"id" => latest, "writer" => writer}} when state.wrote? and writer != state.writer ->
        {:error, {:prefix_shared, latest}}

      _ ->
        :ok
    end
  end

  # Time plus 64 random bits, as ULID text: ids sort by when they were taken.
  defp new_id,
    do:
      Ankusa.ClaimCheck.Ref.pack_id(
        System.system_time(:millisecond),
        :crypto.strong_rand_bytes(8)
      )

  defp checkpoint_files(dir) do
    with {:ok, names} <- File.ls(dir) do
      names = names |> Enum.filter(&File.regular?(Path.join(dir, &1))) |> Enum.sort()

      # RocksDB writes CURRENT last; a checkpoint without one is not a database.
      if "CURRENT" in names, do: {:ok, names}, else: {:error, :checkpoint_incomplete}
    end
  end

  defp shared?(name), do: Path.extname(name) in @shared_exts

  # A shared file the latest backup already lists is in the object store under
  # its name: RocksDB never reuses a file number, so the same name and size is
  # the same file.
  defp upload_shared(instance, store, dir, names, known) do
    Enum.reduce_while(names, {:ok, [], 0, 0}, fn name, {:ok, entries, n, bytes} ->
      path = Path.join(dir, name)

      with %{"size" => size} = entry <- Map.get(known, name),
           {:ok, %File.Stat{size: ^size}} <- File.stat(path) do
        {:cont, {:ok, [entry | entries], n, bytes}}
      else
        _ ->
          case upload(instance, store, path, name, "shared", key(store, "sst/" <> name)) do
            {:ok, entry} -> {:cont, {:ok, [entry | entries], n + 1, bytes + entry["size"]}}
            {:error, _} = error -> {:halt, error}
          end
      end
    end)
    |> reverse_entries()
  end

  defp upload_private(instance, store, dir, id, names) do
    Enum.reduce_while(names, {:ok, [], 0, 0}, fn name, {:ok, entries, n, bytes} ->
      path = Path.join(dir, name)

      case upload(instance, store, path, name, "private", key(store, "#{id}/#{name}")) do
        {:ok, entry} -> {:cont, {:ok, [entry | entries], n + 1, bytes + entry["size"]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> reverse_entries()
  end

  defp reverse_entries({:ok, entries, n, bytes}), do: {:ok, Enum.reverse(entries), n, bytes}
  defp reverse_entries(error), do: error

  # One file in memory at a time.
  defp upload(instance, store, path, name, kind, key) do
    with {:ok, bin} <- File.read(path),
         :ok <- put(instance, store, key, bin) do
      {:ok,
       %{
         "name" => name,
         "kind" => kind,
         "key" => key,
         "size" => byte_size(bin),
         "sha256" => sha256(bin)
       }}
    end
  end

  # ── retention ─────────────────────────────────────────────────────────────

  # Runs right after a successful backup, so every live shared file is listed
  # by `LATEST`. Nothing is deleted on a partial view: a listing or a kept
  # manifest that cannot be read skips the purge, and a failed delete is
  # retried by the next one (the key is still listed).
  defp purge(state, latest_id) do
    instance = state.instance
    {mod, opts, _prefix} = store = BlobStore.resolve_config(state.config, :backup)
    root = key(store, "")

    with {:ok, keys} <- call(fn -> mod.list(instance, root, opts) end),
         by_id = group_by_id(keys, root),
         kept = kept_ids(by_id, latest_id, state.config.backup.keep),
         {:ok, referenced} <- referenced_keys(instance, store, kept) do
      stale_backups =
        for {id, id_keys} <- by_id, id not in kept, key <- id_keys, do: key

      stale_shared =
        for key <- keys,
            String.starts_with?(key, root <> "sst/"),
            not MapSet.member?(referenced, key),
            do: key

      Enum.each(stale_backups ++ stale_shared, fn key ->
        call(fn -> mod.delete(instance, key, opts) end)
      end)
    else
      {:error, reason} ->
        Logger.warning("[ankusa] backup retention skipped: #{inspect(reason)}")
    end
  end

  # id => its keys, for every `backup/<id>/...` key (not `sst/`, not `LATEST`).
  defp group_by_id(keys, root) do
    keys
    |> Enum.flat_map(fn key ->
      case String.split(String.replace_prefix(key, root, ""), "/", parts: 2) do
        ["sst", _] -> []
        [id, _rest] -> [{id, key}]
        _ -> []
      end
    end)
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
  end

  # The backup just written, then the newest others that finished (have a
  # manifest), up to `keep`. The one just written is kept whatever its id
  # sorts as: a replacement host's clock may be behind the old one's.
  defp kept_ids(by_id, latest_id, keep) do
    complete =
      for {id, keys} <- by_id,
          id != latest_id,
          Enum.any?(keys, &String.ends_with?(&1, "/#{id}/manifest.json")),
          do: id

    Enum.take([latest_id | Enum.sort(complete, :desc)], keep)
  end

  defp referenced_keys(instance, store, ids) do
    Enum.reduce_while(ids, {:ok, MapSet.new()}, fn id, {:ok, acc} ->
      case load_manifest(instance, store, id) do
        {:ok, manifest} ->
          keys = for %{"kind" => "shared", "key" => key} <- manifest["files"], do: key
          {:cont, {:ok, MapSet.union(acc, MapSet.new(keys))}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  # ── restore ───────────────────────────────────────────────────────────────

  # Written before the first file of a restore (or before a fresh store is
  # created), and removed only once the archive has been reconciled with the
  # new store (`clear_restore_marker/1`). Without `CURRENT` beside it,
  # whatever sits there is a restore's own leftovers, safe to clear; with
  # `CURRENT`, the database is whole but has not been reconciled yet.
  @restore_marker "RESTORE-IN-PROGRESS"

  @doc """
  Restore the latest backup into `path`, a store directory that holds no
  database. `{:ok, :restored}`; `{:ok, :fresh}` when the backup location is
  readable and holds no backup; `{:error, reason}` otherwise
  (`{:backup_unreachable, _}`, `{:manifest_invalid, id, _}`,
  `{:download_failed, key, _}`, `{:size_mismatch, name}`,
  `{:checksum_mismatch, name}`, `{:write_failed, name, _}`,
  `{:unrecognized_store_files, names}`).

  A directory that holds database files but no `CURRENT`, and was not left by
  an interrupted restore, is refused rather than cleared: it may be a damaged
  store someone wants to recover, and deleting it is not this function's call.

  Called by `Ankusa.Store` before it opens the database, with the config it
  was started with: the instance need not be running.
  """
  @spec restore(Config.t(), Path.t()) :: {:ok, :restored | :fresh} | {:error, term()}
  def restore(%Config{instance: instance} = config, path) do
    store = BlobStore.resolve_config(config, :backup)

    with :ok <- check_restorable(path) do
      case load_latest(instance, store) do
        {:ok, manifest} ->
          restore_files(instance, store, manifest, path)

        :none ->
          with :ok <- clear_dir(path),
               :ok <- write_local(Path.join(path, @restore_marker), "fresh") do
            Logger.info(
              "[ankusa] no backup under #{key(store, "")}; starting with an empty store"
            )

            {:ok, :fresh}
          end

        {:error, _} = error ->
          error
      end
    end
  end

  @doc """
  Whether the store directory `path` holds a database that a restore (or a
  fresh start) created and the archive has not been reconciled with yet: the
  node died between the two. `Ankusa.Store` then reconciles before serving.
  """
  @spec reconcile_pending?(Path.t()) :: boolean()
  def reconcile_pending?(path), do: File.exists?(Path.join(path, @restore_marker))

  @doc "Remove the marker `reconcile_pending?/1` looks for, once the reconcile succeeded."
  @spec clear_restore_marker(Path.t()) :: :ok | {:error, term()}
  def clear_restore_marker(path) do
    case File.rm(Path.join(path, @restore_marker)) do
      ok when ok in [:ok, {:error, :enoent}] -> Ankusa.Fsync.fsync_dir(path)
      {:error, reason} -> {:error, reason}
    end
  end

  defp check_restorable(path) do
    with {:ok, names} <- File.ls(path) do
      data = Enum.filter(names, &data_file?/1)

      if data == [] or @restore_marker in names,
        do: :ok,
        else: {:error, {:unrecognized_store_files, Enum.take(Enum.sort(data), 5)}}
    end
  end

  # What RocksDB keeps data in. LOG, LOCK, IDENTITY and OPTIONS hold none, and
  # a first open that failed before writing CURRENT can leave them behind.
  defp data_file?(name),
    do: String.starts_with?(name, "MANIFEST-") or Path.extname(name) in [".sst", ".blob", ".log"]

  defp restore_files(instance, store, manifest, path) do
    marker = Path.join(path, @restore_marker)
    {current, rest} = Enum.split_with(manifest["files"], &(&1["name"] == "CURRENT"))

    with :ok <- clear_dir(path),
         :ok <- write_local(marker, manifest["id"]),
         {:ok, bytes} <- restore_all(instance, store, rest ++ current, path) do
      # CURRENT is down, so the directory is a database now. The marker stays
      # until the archive is reconciled (`Ankusa.Store` clears it): RocksDB
      # ignores it, and a boot that finds it beside CURRENT reconciles.
      _ = Ankusa.Fsync.fsync_dir(path)

      age_s = div(System.system_time(:millisecond) - manifest["created_at_ms"], 1000)

      Logger.warning(
        "[ankusa] store at #{path} restored from backup #{manifest["id"]} " <>
          "(#{length(manifest["files"])} files, #{bytes} bytes, taken #{age_s} s ago)"
      )

      {:ok, :restored}
    end
  end

  defp restore_all(instance, store, files, path) do
    Enum.reduce_while(files, {:ok, 0}, fn file, {:ok, bytes} ->
      case restore_file(instance, store, file, path) do
        :ok -> {:cont, {:ok, bytes + file["size"]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp clear_dir(path) do
    with {:ok, names} <- File.ls(path) do
      Enum.reduce_while(names, :ok, fn name, :ok ->
        file = Path.join(path, name)

        case File.regular?(file) && File.rm(file) do
          false -> {:cont, :ok}
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, {:clear_failed, name, reason}}}
        end
      end)
    end
  end

  defp restore_file(instance, {mod, opts, _prefix}, file, path) do
    %{"name" => name, "key" => key, "size" => size, "sha256" => sha} = file

    with {:ok, bin} <- download(instance, mod, opts, key),
         :ok <- check(byte_size(bin) == size, {:size_mismatch, name}),
         :ok <- check(sha256(bin) == sha, {:checksum_mismatch, name}) do
      write_local(Path.join(path, name), bin)
    end
  end

  defp write_local(file, data) do
    case Ankusa.Fsync.write_file(file, data) do
      :ok -> :ok
      {:error, reason} -> {:error, {:write_failed, Path.basename(file), reason}}
    end
  end

  defp download(instance, mod, opts, key) do
    case call(fn -> mod.get(instance, key, opts) end) do
      {:ok, bin} when is_binary(bin) -> {:ok, bin}
      {:error, reason} -> {:error, {:download_failed, key, reason}}
    end
  end

  defp check(true, _error), do: :ok
  defp check(false, error), do: {:error, error}

  # ── reading LATEST ────────────────────────────────────────────────────────

  # `{:ok, manifest}` for the backup `LATEST` names, `:none` when there is no
  # `LATEST` at all. A store that cannot be read is never `:none`.
  defp load_latest(instance, {mod, opts, _prefix} = store) do
    case call(fn -> mod.get(instance, key(store, "LATEST"), opts) end) do
      {:ok, body} when is_binary(body) ->
        id = String.trim(body)

        if Regex.match?(@id_re, id),
          do: load_manifest(instance, store, id),
          else: {:error, {:manifest_invalid, id, :bad_id}}

      {:error, :not_found} ->
        :none

      {:error, reason} ->
        {:error, {:backup_unreachable, reason}}
    end
  end

  defp load_manifest(instance, {mod, opts, _prefix} = store, id) do
    case call(fn -> mod.get(instance, key(store, "#{id}/manifest.json"), opts) end) do
      {:ok, bin} when is_binary(bin) ->
        with {:ok, manifest} <- decode_manifest(bin),
             :ok <- validate_manifest(manifest, id) do
          {:ok, manifest}
        else
          {:error, reason} -> {:error, {:manifest_invalid, id, reason}}
        end

      {:error, :not_found} ->
        {:error, {:manifest_invalid, id, :not_found}}

      {:error, reason} ->
        {:error, {:backup_unreachable, reason}}
    end
  end

  defp decode_manifest(bin) do
    case JSON.decode(bin) do
      {:ok, %{} = manifest} -> {:ok, manifest}
      {:ok, _other} -> {:error, :not_an_object}
      {:error, reason} -> {:error, {:json, reason}}
    end
  end

  defp validate_manifest(%{"version" => @manifest_version, "id" => id, "files" => files} = m, id)
       when is_list(files) and is_integer(:erlang.map_get("created_at_ms", m)) do
    cond do
      not Enum.all?(files, &valid_file?/1) -> {:error, :bad_file_entry}
      not Enum.any?(files, &(&1["name"] == "CURRENT")) -> {:error, :no_current}
      true -> :ok
    end
  end

  defp validate_manifest(%{"version" => version}, _id) when version != @manifest_version,
    do: {:error, {:unsupported_version, version}}

  defp validate_manifest(_manifest, _id), do: {:error, :malformed}

  # `name` becomes a file in the store directory, so it must be a bare name.
  defp valid_file?(%{
         "name" => name,
         "kind" => kind,
         "key" => key,
         "size" => size,
         "sha256" => sha
       })
       when is_binary(name) and kind in ["shared", "private"] and is_binary(key) and
              is_integer(size) and size >= 0 and is_binary(sha) do
    name not in ["", ".", ".."] and Path.basename(name) == name and
      not String.contains?(name, ["/", "\\", <<0>>]) and Regex.match?(@sha256_re, sha)
  end

  defp valid_file?(_file), do: false

  # ── archive reconciliation ────────────────────────────────────────────────

  @doc """
  Square a store that started over (restored, or created empty with a backup
  configured) with the archive segments already in the segment store. Called
  by `Ankusa.Store` once the database is open, before the queue writer starts.

    * `m:next_seq` moves past the highest archived seq, so no new segment key
      overwrites an archived one;
    * a segment the store has no catalogue row for is catalogued from its
      `.idx` object, and the archive obligations it settles are cleared, so
      the compactor does not write those hooks again under the same first seq.

  A segment whose index object is missing (its writer died between the two
  PUTs) is logged and skipped. Any other read failure is an error: the store
  must not start with seqs it may hand out twice.
  """
  @spec reconcile_archive(Config.t()) :: :ok | {:error, term()}
  def reconcile_archive(%Config{instance: instance} = config) do
    {mod, opts, prefix} = BlobStore.resolve_config(config, :segments)

    with {:ok, keys} <- call(fn -> mod.list(instance, prefix <> "seg/", opts) end) do
      segments =
        for key <- keys, [_, first, last] <- [Regex.run(@segment_re, key)] do
          {key, String.to_integer(first), String.to_integer(last)}
        end

      if segments == [] do
        :ok
      else
        with {:ok, n} <- recatalogue(instance, {mod, opts}, segments),
             :ok <- advance_seq(instance, segments) do
          if n > 0, do: Logger.warning("[ankusa] #{n} archived segment(s) re-catalogued")
          :ok
        end
      end
    end
  end

  defp recatalogue(instance, store, segments) do
    Enum.reduce_while(segments, {:ok, 0}, fn {key, first, _last} = segment, {:ok, n} ->
      case Store.get(instance, :archive, Keys.segment(first)) do
        {:ok, _row} ->
          {:cont, {:ok, n}}

        :not_found ->
          case catalogue(instance, store, segment) do
            :ok -> {:cont, {:ok, n + 1}}
            :skipped -> {:cont, {:ok, n}}
            {:error, reason} -> {:halt, {:error, {key, reason}}}
          end

        {:error, reason} ->
          {:halt, {:error, {key, reason}}}
      end
    end)
  end

  defp catalogue(instance, {mod, opts}, {key, first, last}) do
    idx_key = String.replace_suffix(key, ".seg", ".idx")

    case call(fn -> mod.get(instance, idx_key, opts) end) do
      {:ok, bin} ->
        with {:ok, idx} <- decode_idx(bin) do
          settle(instance, segment_row(key, idx_key, first, last, idx), idx)
        end

      {:error, :not_found} ->
        Logger.warning("[ankusa] segment #{key} has no index object; not catalogued")
        :skipped

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp decode_idx(bin) do
    case :erlang.binary_to_term(bin, [:safe]) do
      idx when is_map(idx) and map_size(idx) > 0 -> {:ok, idx}
      _ -> {:error, :bad_index}
    end
  rescue
    ArgumentError -> {:error, :bad_index}
  end

  # The fields `Ankusa.Storage.Compactor` records.
  defp segment_row(key, idx_key, first, last, idx) do
    ids = Map.keys(idx)

    %{
      key: key,
      idx_key: idx_key,
      first_seq: first,
      last_seq: last,
      min_id: Enum.min(ids),
      max_id: Enum.max(ids),
      count: map_size(idx),
      bytes:
        idx
        |> Map.values()
        |> Enum.map(fn {offset, length, _seq} -> offset + length end)
        |> Enum.max()
    }
  end

  # The row, and every obligation of this store the segment already settles,
  # in one batch, exactly as the compactor records a segment it wrote.
  defp settle(instance, row, idx) do
    seqs = idx |> Map.values() |> Enum.map(fn {_offset, _length, seq} -> seq end) |> Enum.sort()

    with {:ok, results} <-
           Store.multi_get(instance, :index, Enum.map(seqs, &Keys.archive_pending/1)),
         owed = for({seq, {:ok, _}} <- Enum.zip(seqs, results), do: seq),
         :ok <- Archive.archived(instance, row, owed) do
      markers = Enum.map(owed, &{&1, Keys.cleared(&1, 1, 0)})

      # A failed reclaim only defers: the markers stay and the sweep finds them.
      with {:error, reason} <- Reclaim.run(instance, markers) do
        Logger.warning("[ankusa] hook reclaim deferred to the next sweep: #{inspect(reason)}")
      end

      :ok
    end
  end

  # Synced, after every catalogue row: the batches before it are durable once
  # it is, and the queue writer starts only after the store's init returns.
  defp advance_seq(instance, segments) do
    next = (segments |> Enum.map(&elem(&1, 2)) |> Enum.max()) + 1

    marker =
      case Store.get(instance, :default, Keys.meta("next_seq")) do
        {:ok, <<n::64>>} -> {:ok, n}
        :not_found -> {:ok, 1}
        {:error, reason} -> {:error, reason}
      end

    with {:ok, current} <- marker,
         :ok <-
           Store.write(
             instance,
             [{:put, :default, Keys.meta("next_seq"), <<max(current, next)::64>>}],
             sync: true
           ) do
      if next > current,
        do: Logger.warning("[ankusa] next_seq advanced to #{next} past archived segments")

      :ok
    end
  end

  # ── helpers ───────────────────────────────────────────────────────────────

  defp checkpoint_dir(config), do: Config.path(config, "store.checkpoint")

  defp remove(dir) do
    case File.rm_rf(dir) do
      {:ok, _removed} -> :ok
      {:error, reason, file} -> {:error, {:remove_failed, file, reason}}
    end
  end

  defp key({_mod, _opts, prefix}, name), do: prefix <> "backup/" <> name

  defp sha256(bin), do: :crypto.hash(:sha256, bin) |> Base.encode16(case: :lower)

  defp put(instance, {mod, opts, _prefix}, key, data) do
    case call(fn -> mod.put(instance, key, data, opts) end) do
      :ok -> :ok
      {:error, _} = error -> error
    end
  end

  # A blob store is user code (S3, GCS, a custom adapter): an error return, a
  # raise, an exit and a throw are the same failure, never a crash.
  defp call(fun) do
    case fun.() do
      :ok -> :ok
      {:ok, _} = ok -> ok
      {:error, _} = error -> error
      other -> {:error, {:bad_return, other}}
    end
  rescue
    error -> {:error, {:raised, error}}
  catch
    :exit, reason -> {:error, {:exit, reason}}
    :throw, value -> {:error, {:throw, value}}
  end
end
