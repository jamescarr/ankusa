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
      the store's format version and intake offset, and (with
      `backup.signing_key`) an HMAC-SHA256 signature over all of it; written
      after all the files.
    * `backup/LATEST` — the id, written last. A backup is the one `LATEST`
      names; anything an interrupted attempt left is ignored, then deleted.

  With `backup.secondary` every one of those objects is written to a second
  object store as well (another region or provider), one target after the
  other: the attempt succeeds only when both hold the new backup, a failed
  secondary is retried with the backoff below, and each target is purged
  right after its own success so a down secondary never makes the primary
  grow. Each object-store call is bounded by `backup.timeout_ms`.

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

  ## Restore

  At boot, `Ankusa.Store` calls `restore/3` when its directory holds no
  database: the `LATEST` backup is downloaded, its manifest checked (the
  signature against every `backup.signing_key`, the store format against what
  this release supports), every file checked against its size and sha256,
  `CURRENT` written last (a restore that fails part-way leaves no database,
  and the next boot starts it over). With signing keys configured a manifest
  that is unsigned or signed by no configured key is refused
  (`{:manifest_invalid, id, :unsigned | :bad_signature}`); a backup of a newer
  store format than this release writes is refused
  (`{:store_format_newer, found, supported}`), the same guard the store's own
  open applies. When the primary cannot give a backup (unreachable, invalid,
  failing a check, or empty) and a secondary is configured, the secondary is
  tried the same way. No backup anywhere starts an empty store; a backup
  location that cannot be read refuses the boot instead.
  `reconcile_archive/1` then squares the new store with the segments in the
  bucket. A restore (or a fresh start) leaves a `RESTORE-IN-PROGRESS` marker in
  the directory until that reconcile has succeeded, so a node that dies in
  between reconciles on its next boot (`reconcile_pending?/1`) rather than
  booting as an existing store.

  The manifest is signed, `LATEST` is not: whoever can write the bucket cannot
  inject content a restore accepts, but can still point `LATEST` at an older
  backup this node signed.

  ## Verify

  `verify/2` reads `LATEST` back from an object store without writing
  anything: the manifest (signature included) and every file it lists, one at
  a time, against their size and sha256. With `backup.verify_interval_ms` this
  process runs it for every target on that interval and emits
  `[:ankusa, :backup, :verify]`; a failure is the counter
  `ankusa_backup_verify_failed_total`.

  ## Stopping

  A clean stop (`:normal`, `:shutdown`) takes one last backup before the
  process exits, bounded by `backup.final_timeout_ms`, so the recovery point
  of a node that is drained and stopped is the stop, not the last periodic
  backup. The supervisor is told to wait that long (`child_spec/1`). The store
  closes after this process, and the edge and dispatch before it.

  ## Recovery point and freshness

  The recovery point is the last backup: hooks acked after it are lost with
  the host, except for the archive copies `reconcile_archive/1` finds.
  `age_ms/1` is what `Ankusa.Health` compares to `backup.max_age_ms`.

  Emits `[:ankusa, :backup, :stop]` per attempt, `[:ankusa, :backup, :state]`
  (`age_seconds` since the last successful backup) after it, and
  `[:ankusa, :backup, :verify]` per verified target.
  """

  use GenServer

  require Logger

  alias Ankusa.{BlobStore, Config, Store}
  alias Ankusa.Queue.Archive
  alias Ankusa.Store.{Keys, Migrate}

  # 2 adds `format_version`, `intake_offset` and the signature; 1 is still read.
  @manifest_version 2
  @readable_manifest_versions [1, 2]

  @shared_exts [".sst", ".blob"]

  # Same shape as the compactor's: interval·2^n, jittered, capped at a minute.
  @max_backoff_ms 60_000

  # What the supervisor waits for, beyond the final backup, before it kills us.
  @shutdown_margin_ms 10_000

  @id_re ~r/\A[0-9A-HJKMNP-TV-Z]{26}\z/
  @sha256_re ~r/\A[0-9a-f]{64}\z/

  @typedoc "One object store a backup is written to and read from."
  @type target :: %{
          name: :primary | :secondary,
          mod: module(),
          opts: keyword(),
          prefix: String.t(),
          timeout: pos_integer()
        }

  # ── lifecycle ─────────────────────────────────────────────────────────────

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :backup))
  end

  def child_spec(opts) do
    instance = Keyword.fetch!(opts, :instance)

    %{
      id: {__MODULE__, instance},
      start: {__MODULE__, :start_link, [opts]},
      # The final backup on a clean stop runs inside `terminate/2`.
      shutdown: Ankusa.config(instance).backup.final_timeout_ms + @shutdown_margin_ms
    }
  end

  @doc """
  Take one backup now, synchronously: the number of files uploaded (shared
  files already in the object store are not) and their bytes, over every
  target.
  """
  @spec run(atom()) ::
          {:ok, %{id: String.t(), uploaded: non_neg_integer(), bytes: non_neg_integer()}}
          | {:error, term()}
  def run(instance), do: GenServer.call(Ankusa.via(instance, :backup), :run, 300_000)

  @doc """
  Check the latest backup of one object store without writing anything: the
  `LATEST` object, its manifest (signature included, against every
  `backup.signing_key`), and every file the manifest lists, downloaded one at
  a time and compared to its size and sha256.

  `:target` is `:primary` (default) or `:secondary`. `:none` when the store
  holds no backup; otherwise `{:ok, %{id: id, files: n, bytes: b}}` or
  `{:error, reason}` with the reasons of `restore/3`
  (`{:manifest_invalid, id, _}`, `{:download_failed, key, _}`,
  `{:size_mismatch, name}`, `{:checksum_mismatch, name}`,
  `{:backup_unreachable, _}`). A `LATEST` that moved while the files were
  read (a newer backup purged the one being checked) is read again once.
  """
  @spec verify(atom(), keyword()) ::
          {:ok, %{id: String.t(), files: non_neg_integer(), bytes: non_neg_integer()}}
          | :none
          | {:error, term()}
  def verify(instance, opts \\ []) do
    config = Ankusa.config(instance)

    case find_target(config, Keyword.get(opts, :target, :primary)) do
      nil -> {:error, :no_such_target}
      target -> verify_target(instance, target, config.backup.signing_key, 2)
    end
  end

  @doc """
  Milliseconds since the last successful backup, or since this process started
  while it has not made one yet; `:unknown` when it is not running.
  """
  @spec age_ms(atom()) :: non_neg_integer() | :unknown
  def age_ms(instance) do
    case :ets.lookup(health_table(instance), :state) do
      [{:state, started_ms, last_success_ms}] ->
        max(0, System.system_time(:millisecond) - (last_success_ms || started_ms))

      [] ->
        :unknown
    end
  rescue
    ArgumentError -> :unknown
  end

  @doc "The checks `Ankusa.Config.new/1` leaves to this module."
  @spec validate_config!(Config.t()) :: :ok
  def validate_config!(%Config{backup: backup}) do
    unless is_boolean(Map.get(backup, :enabled)) do
      raise ArgumentError,
            "backup.enabled must be a boolean, got #{inspect(Map.get(backup, :enabled))}"
    end

    # The value is not printed: a malformed pair can still hold credentials.
    for {key, value} <- [blob_store: Map.get(backup, :blob_store), secondary: backup.secondary] do
      case value do
        nil -> :ok
        {mod, opts} when is_atom(mod) and is_list(opts) -> :ok
        _other -> raise ArgumentError, "backup.#{key} must be nil or {module, opts}"
      end
    end

    :ok
  end

  @impl true
  # The path comes from operator config or this node's own index, never request input.
  # sobelow_skip ["Traversal.FileModule"]
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    %Config{} = config = Ankusa.config(instance)
    interval = config.backup.interval_ms
    verify_interval = config.backup.verify_interval_ms

    # `terminate/2` takes the final backup.
    Process.flag(:trap_exit, true)

    # What a crash mid-upload left behind.
    _ = File.rm_rf(checkpoint_dir(config))
    warn_if_local(instance, config)
    schedule(interval)
    if verify_interval, do: Process.send_after(self(), :verify, verify_interval)

    state = %{
      instance: instance,
      config: config,
      targets: targets(config),
      interval: interval,
      verify_interval: verify_interval,
      verifying: nil,
      failures: 0,
      # target name => (file name => manifest entry of every shared file that
      # target's `LATEST` lists); loaded after init, so a slow object store
      # does not hold up the boot
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

    # Read by `Ankusa.Health` on `/ready`, never through this process.
    :ets.new(health_table(instance), [:named_table, :protected, read_concurrency: true])
    publish_state(state)

    {:ok, state, {:continue, :load_latest}}
  end

  # Crash reports print the state; the config and the targets carry the object
  # stores' credentials and the signing keys.
  @impl true
  def format_status(%{state: %{config: _} = state} = status),
    do: %{status | state: %{state | config: :redacted, targets: :redacted}}

  def format_status(status), do: status

  @impl true
  def handle_continue(:load_latest, state), do: {:noreply, %{state | known: load_known(state)}}

  @impl true
  def handle_call(:run, _from, state) do
    {result, state} = backup(state)
    publish_state(state)
    {:reply, reply_of(result), state}
  end

  @impl true
  def handle_info(:tick, state) do
    {_result, state} = backup(state)
    publish_state(state)
    schedule(next_delay(state))
    {:noreply, state}
  end

  def handle_info(:verify, state) do
    Process.send_after(self(), :verify, state.verify_interval)
    {:noreply, start_verify(state)}
  end

  # The verify task's answer, or its death: either way the next tick may start one.
  def handle_info({ref, _result}, %{verifying: ref} = state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])
    {:noreply, %{state | verifying: nil}}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{verifying: ref} = state),
    do: {:noreply, %{state | verifying: nil}}

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(reason, state) do
    if clean_stop?(reason), do: final_backup(state)
    :ok
  end

  defp clean_stop?(reason), do: reason in [:normal, :shutdown] or match?({:shutdown, _}, reason)

  # One last backup under `backup.final_timeout_ms`, in a task so the deadline
  # holds however the object store behaves. A backup still running when the
  # supervisor's own deadline passes is killed with this process: the periodic
  # backups are the fallback.
  defp final_backup(state) do
    timeout = state.config.backup.final_timeout_ms
    task = Task.async(fn -> backup(state) end)

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, {%{id: id, uploaded: uploaded}, _state}} ->
        Logger.info("[ankusa] final backup on stop: #{id}, #{uploaded} file(s) uploaded")

      {:ok, {%{error: reason}, _state}} ->
        Logger.warning("[ankusa] final backup on stop failed: #{inspect(reason)}")

      {:exit, reason} ->
        Logger.warning("[ankusa] final backup on stop crashed: #{inspect(reason)}")

      nil ->
        Logger.warning("[ankusa] final backup on stop did not finish within #{timeout} ms")
    end
  end

  defp reply_of(%{id: _} = ok), do: {:ok, Map.take(ok, [:id, :uploaded, :bytes])}
  defp reply_of(%{error: reason}), do: {:error, reason}

  defp next_delay(%{failures: 0, interval: interval}), do: interval

  defp next_delay(%{failures: n, interval: interval}) do
    capped = (interval * Integer.pow(2, min(n, 16))) |> min(@max_backoff_ms) |> max(interval)
    max(interval, round(capped * (0.5 + :rand.uniform() * 0.5)))
  end

  defp schedule(interval), do: Process.send_after(self(), :tick, interval)

  # The instance name is operator config: one atom per instance, never request input.
  # sobelow_skip ["DOS.BinToAtom"]
  defp health_table(instance), do: :"ankusa_backup_#{instance}"

  defp publish_state(state) do
    true =
      :ets.insert(health_table(state.instance), {:state, state.started_ms, state.last_success_ms})
  end

  defp warn_if_local(instance, config) do
    for %{mod: Ankusa.BlobStore.LocalFS, opts: opts, name: name} <- targets(config) do
      root = Keyword.get(opts, :root) || Config.path(config, "segments")

      Logger.warning(
        "[ankusa] #{name} backup store for #{inspect(instance)} is the local filesystem " <>
          "(#{root}); it does not survive losing this host"
      )
    end

    :ok
  end

  # ── targets ───────────────────────────────────────────────────────────────

  defp targets(%Config{} = config) do
    timeout = config.backup.timeout_ms

    for {name, {mod, opts, prefix}} <-
          [
            primary: BlobStore.resolve_config(config, :backup),
            secondary: BlobStore.secondary_config(config)
          ],
        do: %{name: name, mod: mod, opts: opts, prefix: prefix, timeout: timeout}
  end

  defp find_target(config, name), do: Enum.find(targets(config), &(&1.name == name))

  # The error of the secondary says so; the primary's is the plain reason.
  defp tag(%{name: :primary}, reason), do: reason
  defp tag(%{name: :secondary}, reason), do: {:secondary, reason}

  defp load_known(state) do
    Map.new(state.targets, fn target ->
      known =
        case load_latest(state.instance, target, :skip) do
          {:ok, manifest} ->
            shared_entries(manifest)

          :none ->
            %{}

          {:error, reason} ->
            # Harmless: every file is uploaded again by the next backup.
            Logger.warning(
              "[ankusa] latest #{target.name} backup not read, its files will be " <>
                "uploaded again: #{inspect(reason)}"
            )

            %{}
        end

      {target.name, known}
    end)
  end

  defp shared_entries(manifest) do
    for %{"kind" => "shared", "name" => name} = entry <- manifest["files"],
        into: %{},
        do: {name, entry}
  end

  # ── one backup ────────────────────────────────────────────────────────────

  # Returns `{reply, state}`; `reply` is `%{id:, uploaded:, bytes:}` or
  # `%{error: reason}`. It holds no process-bound state change beyond the
  # returned state, so the final backup can run it in a task.
  # The path comes from operator config or this node's own index, never request input.
  # sobelow_skip ["Traversal.FileModule"]
  defp backup(state) do
    started = System.monotonic_time()
    dir = checkpoint_dir(state.config)
    known = state.known || load_known(state)
    id = new_id()

    result =
      try do
        take(state, dir, known, id)
      rescue
        error -> {:error, {:raised, error}, known}
      catch
        :exit, reason -> {:error, {:exit, reason}, known}
        :throw, value -> {:error, {:throw, value}, known}
      after
        File.rm_rf(dir)
      end

    duration = System.monotonic_time() - started
    now = System.system_time(:millisecond)

    {reply, state} =
      case result do
        {:ok, %{id: id, uploaded: uploaded, bytes: bytes, known: known}} ->
          if state.failures > 0 or state.last_success_ms == nil do
            Logger.info(
              "[ankusa] store backed up: #{id}, #{uploaded} file(s), #{bytes} bytes uploaded"
            )
          end

          emit_stop(state, %{duration: duration, files: uploaded, bytes: bytes}, %{result: :ok})

          {%{id: id, uploaded: uploaded, bytes: bytes},
           %{state | failures: 0, known: known, last_success_ms: now, wrote?: true}}

        {:error, reason, known} ->
          log_failure(state, store_root(state), reason)

          emit_stop(state, %{duration: duration, files: 0, bytes: 0}, %{
            result: :error,
            reason: reason
          })

          {%{error: reason}, %{state | failures: state.failures + 1, known: known}}
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

  defp store_root(state), do: key(hd(state.targets), "")

  defp emit_stop(state, measurements, meta) do
    Ankusa.Telemetry.emit(
      [:backup, :stop],
      measurements,
      Map.put(meta, :instance, state.instance)
    )
  end

  # `{:ok, summary}`, or `{:error, reason, known}` where `known` is what the
  # targets that finished already hold, so a failing secondary does not make
  # the next attempt upload everything to the primary again.
  defp take(state, dir, known, id) do
    case prepare(state, dir) do
      {:ok, store_id, meta, names} -> upload_targets(state, known, id, store_id, meta, dir, names)
      {:error, reason} -> {:error, reason, known}
    end
  end

  defp prepare(state, dir) do
    # The id goes into the store before the checkpoint, so a restore of this
    # backup inherits it and carries on backing up to the same place.
    with {:ok, store_id} <- ensure_store_id(state.instance),
         :ok <- check_owners(state, store_id),
         :ok <- remove(dir),
         # Read before the checkpoint: both only grow, so what the manifest
         # records is never ahead of what the checkpoint holds.
         meta = %{
           format_version: read_format(state.instance),
           intake_offset: read_intake_offset(state.instance)
         },
         :ok <- Store.checkpoint(state.instance, dir),
         {:ok, names} <- checkpoint_files(dir) do
      {:ok, store_id, meta, names}
    end
  end

  defp check_owners(state, store_id) do
    Enum.reduce_while(state.targets, :ok, fn target, :ok ->
      case check_owner(state, target, store_id) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, tag(target, reason)}}
      end
    end)
  end

  defp upload_targets(state, known, id, store_id, meta, dir, names) do
    {shared, private} = Enum.split_with(names, &shared?/1)

    ctx = %{
      store_id: store_id,
      created_at: System.system_time(:millisecond),
      meta: meta,
      dir: dir,
      id: id,
      shared: shared,
      private: private
    }

    outcome =
      Enum.reduce_while(state.targets, {:ok, %{uploaded: 0, bytes: 0, known: known}}, fn target,
                                                                                         {:ok,
                                                                                          acc} ->
        upload_one(state, target, ctx, known, acc)
      end)

    with {:ok, acc} <- outcome, do: {:ok, Map.put(acc, :id, id)}
  end

  defp upload_one(state, target, ctx, known, acc) do
    case upload_target(state, target, ctx, Map.get(known, target.name, %{})) do
      {:ok, manifest, n, bytes} ->
        purge(state, target, ctx.id)

        {:cont,
         {:ok,
          %{
            acc
            | uploaded: acc.uploaded + n,
              bytes: acc.bytes + bytes,
              known: Map.put(acc.known, target.name, shared_entries(manifest))
          }}}

      {:error, reason} ->
        {:halt, {:error, tag(target, reason), acc.known}}
    end
  end

  # One target's whole pipeline: files, then the manifest, then `LATEST`.
  defp upload_target(state, target, ctx, known) do
    %{dir: dir, id: id, shared: shared, private: private} = ctx
    instance = state.instance

    with {:ok, shared_entries, n1, b1} <- upload_shared(instance, target, dir, shared, known),
         {:ok, private_entries, n2, b2} <- upload_private(instance, target, dir, id, private) do
      manifest =
        sign(
          %{
            "version" => @manifest_version,
            "id" => id,
            "created_at_ms" => ctx.created_at,
            "store_id" => ctx.store_id,
            "writer" => state.writer,
            "format_version" => ctx.meta.format_version,
            "intake_offset" => ctx.meta.intake_offset,
            "files" => shared_entries ++ private_entries
          },
          state.config.backup.signing_key
        )

      with :ok <-
             put(instance, target, key(target, "#{id}/manifest.json"), JSON.encode!(manifest)),
           :ok <- put(instance, target, key(target, "LATEST"), id) do
        {:ok, manifest, n1 + n2, b1 + b2}
      end
    end
  end

  # The store format at the time of the backup (`m:format_version`, decimal).
  defp read_format(instance) do
    with {:ok, bin} <- Store.get(instance, :default, Keys.format_version()),
         {n, ""} <- Integer.parse(bin) do
      n
    else
      _ -> nil
    end
  end

  # The last intake-log offset the store holds, if one was ever written.
  defp read_intake_offset(instance) do
    case Store.get(instance, :default, Keys.meta("intake_offset")) do
      {:ok, <<offset::64>>} -> offset
      _ -> nil
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
  defp check_owner(state, target, store_id) do
    case load_latest(state.instance, target, :skip) do
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
  defp upload_shared(instance, target, dir, names, known) do
    Enum.reduce_while(names, {:ok, [], 0, 0}, fn name, {:ok, entries, n, bytes} ->
      path = Path.join(dir, name)

      with %{"size" => size} = entry <- Map.get(known, name),
           {:ok, %File.Stat{size: ^size}} <- File.stat(path) do
        {:cont, {:ok, [entry | entries], n, bytes}}
      else
        _ ->
          case upload(instance, target, path, name, "shared", key(target, "sst/" <> name)) do
            {:ok, entry} -> {:cont, {:ok, [entry | entries], n + 1, bytes + entry["size"]}}
            {:error, _} = error -> {:halt, error}
          end
      end
    end)
    |> reverse_entries()
  end

  defp upload_private(instance, target, dir, id, names) do
    Enum.reduce_while(names, {:ok, [], 0, 0}, fn name, {:ok, entries, n, bytes} ->
      path = Path.join(dir, name)

      case upload(instance, target, path, name, "private", key(target, "#{id}/#{name}")) do
        {:ok, entry} -> {:cont, {:ok, [entry | entries], n + 1, bytes + entry["size"]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> reverse_entries()
  end

  defp reverse_entries({:ok, entries, n, bytes}), do: {:ok, Enum.reverse(entries), n, bytes}
  defp reverse_entries(error), do: error

  # One file in memory at a time.
  # The path comes from operator config or this node's own index, never request input.
  # sobelow_skip ["Traversal.FileModule"]
  defp upload(instance, target, path, name, kind, key) do
    with {:ok, bin} <- File.read(path),
         :ok <- put(instance, target, key, bin) do
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

  # ── signing ───────────────────────────────────────────────────────────────

  # HMAC-SHA256, hex, over the manifest without its `signature`, in a canonical
  # JSON form (keys sorted, no whitespace) so the bytes signed do not depend on
  # how the manifest was encoded. The first key signs; any key verifies.
  defp sign(manifest, []), do: manifest

  defp sign(manifest, [key | _rotated]),
    do: Map.put(manifest, "signature", hmac(key, canonical(manifest)))

  defp check_signature(_manifest, []), do: :ok

  defp check_signature(manifest, keys) do
    case Map.pop(manifest, "signature") do
      {nil, _unsigned} ->
        {:error, :unsigned}

      {signature, rest} when is_binary(signature) ->
        signed = canonical(rest)

        if Enum.any?(keys, &equal?(hmac(&1, signed), signature)),
          do: :ok,
          else: {:error, :bad_signature}

      {_other, _rest} ->
        {:error, :bad_signature}
    end
  end

  defp hmac(key, data), do: :crypto.mac(:hmac, :sha256, key, data) |> Base.encode16(case: :lower)

  defp equal?(a, b), do: byte_size(a) == byte_size(b) and :crypto.hash_equals(a, b)

  defp canonical(map) when is_map(map) do
    fields =
      map
      |> Enum.sort_by(fn {key, _value} -> key end)
      |> Enum.map_join(",", fn {key, value} ->
        JSON.encode!(to_string(key)) <> ":" <> canonical(value)
      end)

    "{" <> fields <> "}"
  end

  defp canonical(list) when is_list(list),
    do: "[" <> Enum.map_join(list, ",", &canonical/1) <> "]"

  defp canonical(other), do: JSON.encode!(other)

  # ── retention ─────────────────────────────────────────────────────────────

  # Runs right after a successful backup of `target`, so every live shared
  # file is listed by its `LATEST`. Nothing is deleted on a partial view: a
  # listing or a kept manifest that cannot be read skips the purge, and a
  # failed delete is retried by the next one (the key is still listed).
  defp purge(state, target, latest_id) do
    instance = state.instance
    root = key(target, "")

    with {:ok, keys} <- list(instance, target, root),
         by_id = group_by_id(keys, root),
         kept = kept_ids(by_id, latest_id, state.config.backup.keep),
         {:ok, referenced} <- referenced_keys(instance, target, kept) do
      stale_backups =
        for {id, id_keys} <- by_id, id not in kept, key <- id_keys, do: key

      stale_shared =
        for key <- keys,
            String.starts_with?(key, root <> "sst/"),
            not MapSet.member?(referenced, key),
            do: key

      Enum.each(stale_backups ++ stale_shared, &delete(instance, target, &1))
    else
      {:error, reason} ->
        Logger.warning("[ankusa] #{target.name} backup retention skipped: #{inspect(reason)}")
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

  defp referenced_keys(instance, target, ids) do
    Enum.reduce_while(ids, {:ok, MapSet.new()}, fn id, {:ok, acc} ->
      case load_manifest(instance, target, id, :skip) do
        {:ok, manifest} ->
          keys = for %{"kind" => "shared", "key" => key} <- manifest["files"], do: key
          {:cont, {:ok, MapSet.union(acc, MapSet.new(keys))}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  # ── verify ────────────────────────────────────────────────────────────────

  defp start_verify(%{verifying: nil} = state) do
    %{instance: instance, config: config, targets: targets, wrote?: expected?} = state

    task = Task.async(fn -> verify_targets(instance, config, targets, expected?) end)
    %{state | verifying: task.ref}
  end

  # The previous round has not finished: skip this tick, never overlap.
  defp start_verify(state), do: state

  # Every target, each reported on `[:ankusa, :backup, :verify]`. `expected?`:
  # this process wrote a backup, so a store with none has lost it.
  defp verify_targets(instance, config, targets, expected?) do
    Enum.each(targets, fn target ->
      started = System.monotonic_time()

      {result, measurements, reason} =
        case verify_target(instance, target, config.backup.signing_key, 2) do
          {:ok, %{files: files, bytes: bytes}} -> {:ok, %{files: files, bytes: bytes}, nil}
          :none when expected? -> {:error, %{files: 0, bytes: 0}, :no_backup}
          :none -> {:none, %{files: 0, bytes: 0}, nil}
          {:error, reason} -> {:error, %{files: 0, bytes: 0}, reason}
        end

      if result == :error do
        Logger.error("[ankusa] #{target.name} backup failed verification: #{inspect(reason)}")
      end

      Ankusa.Telemetry.emit(
        [:backup, :verify],
        Map.put(measurements, :duration, System.monotonic_time() - started),
        %{instance: instance, target: target.name, result: result, reason: reason}
      )
    end)
  end

  defp verify_target(instance, target, keys, attempts) do
    with {:ok, id} <- read_latest_id(instance, target),
         {:ok, manifest} <- load_manifest(instance, target, id, keys) do
      case check_files(instance, target, manifest["files"]) do
        {:ok, bytes} ->
          {:ok, %{id: id, files: length(manifest["files"]), bytes: bytes}}

        {:error, _reason} = error ->
          # A newer backup may have purged the one being read.
          if attempts > 1 and read_latest_id(instance, target) not in [{:ok, id}, :none],
            do: verify_target(instance, target, keys, attempts - 1),
            else: error
      end
    end
  end

  # Nothing is kept: one file at a time, whole (a file is at most a few tens of
  # MiB, see `Ankusa.Store`'s SST and blob caps).
  defp check_files(instance, target, files) do
    Enum.reduce_while(files, {:ok, 0}, fn file, {:ok, bytes} ->
      %{"name" => name, "key" => key, "size" => size, "sha256" => sha} = file

      with {:ok, bin} <- download(instance, target, key),
           :ok <- check(byte_size(bin) == size, {:size_mismatch, name}),
           :ok <- check(sha256(bin) == sha, {:checksum_mismatch, name}) do
        {:cont, {:ok, bytes + size}}
      else
        {:error, _} = error -> {:halt, error}
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
  `{:store_format_newer, found, supported}`, `{:download_failed, key, _}`,
  `{:size_mismatch, name}`, `{:checksum_mismatch, name}`, `{:write_failed, name, _}`,
  `{:unrecognized_store_files, names}`).

  The primary object store is tried first. With `backup.secondary`, any
  outcome of the primary but a restored store sends the restore on to the
  secondary, and an empty primary does not make the store fresh while the
  secondary holds a backup. When neither gives a backup the error is the
  primary's.

  `:format` (default: the store format this release writes) is the newest
  store format to restore: a backup of a newer one is refused.

  A directory that holds database files but no `CURRENT`, and was not left by
  an interrupted restore, is refused rather than cleared: it may be a damaged
  store someone wants to recover, and deleting it is not this function's call.

  Called by `Ankusa.Store` before it opens the database, with the config it
  was started with: the instance need not be running.
  """
  @spec restore(Config.t(), Path.t(), keyword()) :: {:ok, :restored | :fresh} | {:error, term()}
  def restore(%Config{instance: instance} = config, path, opts \\ []) do
    ctx = %{
      keys: config.backup.signing_key,
      format: Keyword.get_lazy(opts, :format, &Migrate.current_format/0)
    }

    with :ok <- check_restorable(path) do
      case targets(config) do
        [primary] ->
          restore_target(instance, primary, ctx, path)

        [primary, secondary] ->
          restore_with_fallback(instance, primary, secondary, ctx, path)
      end
    end
  end

  defp restore_with_fallback(instance, primary, secondary, ctx, path) do
    case restore_target(instance, primary, ctx, path) do
      {:ok, :restored} = restored ->
        restored

      first ->
        if first != {:ok, :fresh} do
          Logger.warning(
            "[ankusa] primary backup unusable (#{inspect(first)}); trying the secondary"
          )
        end

        case {first, restore_target(instance, secondary, ctx, path)} do
          {_, {:ok, :restored}} ->
            Logger.warning("[ankusa] store restored from the SECONDARY backup")
            {:ok, :restored}

          # Nothing in either place.
          {{:ok, :fresh}, {:ok, :fresh}} ->
            {:ok, :fresh}

          # The primary holds nothing, the secondary cannot be read: not fresh.
          {{:ok, :fresh}, {:error, _} = secondary_error} ->
            secondary_error

          # The primary failed and the secondary has nothing to offer.
          {{:error, _} = primary_error, _secondary} ->
            primary_error
        end
    end
  end

  defp restore_target(instance, target, ctx, path) do
    case load_latest(instance, target, ctx.keys) do
      {:ok, manifest} ->
        restore_files(instance, target, manifest, ctx, path)

      :none ->
        with :ok <- clear_dir(path),
             :ok <- write_local(Path.join(path, @restore_marker), "fresh") do
          Logger.info("[ankusa] no backup under #{key(target, "")}; starting with an empty store")

          {:ok, :fresh}
        end

      {:error, _} = error ->
        error
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
  # The path comes from operator config or this node's own index, never request input.
  # sobelow_skip ["Traversal.FileModule"]
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

  defp restore_files(instance, target, manifest, ctx, path) do
    marker = Path.join(path, @restore_marker)
    {current, rest} = Enum.split_with(manifest["files"], &(&1["name"] == "CURRENT"))

    with :ok <- check_format(manifest, ctx.format),
         :ok <- clear_dir(path),
         :ok <- write_local(marker, manifest["id"]),
         {:ok, bytes} <- restore_all(instance, target, rest ++ current, path) do
      # CURRENT is down, so the directory is a database now. The marker stays
      # until the archive is reconciled (`Ankusa.Store` clears it): RocksDB
      # ignores it, and a boot that finds it beside CURRENT reconciles.
      _ = Ankusa.Fsync.fsync_dir(path)

      age_s = div(System.system_time(:millisecond) - manifest["created_at_ms"], 1000)

      Logger.warning(
        "[ankusa] store at #{path} restored from #{target.name} backup #{manifest["id"]} " <>
          "(#{length(manifest["files"])} files, #{bytes} bytes, taken #{age_s} s ago)"
      )

      if ctx.keys == [] and Map.has_key?(manifest, "signature") do
        Logger.warning(
          "[ankusa] the backup manifest is signed but no backup.signing_key is configured: " <>
            "its authenticity was not checked"
        )
      end

      {:ok, :restored}
    end
  end

  # An older image must not restore (then half-open) a newer image's store: the
  # same guard the store's own open applies (`Ankusa.Store.Migrate`).
  defp check_format(%{"format_version" => found}, supported)
       when is_integer(found) and found > supported,
       do: {:error, {:store_format_newer, found, supported}}

  defp check_format(_manifest, _supported), do: :ok

  defp restore_all(instance, target, files, path) do
    Enum.reduce_while(files, {:ok, 0}, fn file, {:ok, bytes} ->
      case restore_file(instance, target, file, path) do
        :ok -> {:cont, {:ok, bytes + file["size"]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  # The path comes from operator config or this node's own index, never request input.
  # sobelow_skip ["Traversal.FileModule"]
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

  defp restore_file(instance, target, file, path) do
    %{"name" => name, "key" => key, "size" => size, "sha256" => sha} = file

    with {:ok, bin} <- download(instance, target, key),
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

  defp download(instance, target, key) do
    case get(instance, target, key) do
      {:ok, bin} when is_binary(bin) -> {:ok, bin}
      {:error, reason} -> {:error, {:download_failed, key, reason}}
    end
  end

  defp check(true, _error), do: :ok
  defp check(false, error), do: {:error, error}

  # ── reading LATEST ────────────────────────────────────────────────────────

  # The id `LATEST` names, `:none` when there is no `LATEST` at all. A store
  # that cannot be read is never `:none`.
  defp read_latest_id(instance, target) do
    case get(instance, target, key(target, "LATEST")) do
      {:ok, body} when is_binary(body) ->
        id = String.trim(body)

        if Regex.match?(@id_re, id),
          do: {:ok, id},
          else: {:error, {:manifest_invalid, id, :bad_id}}

      {:error, :not_found} ->
        :none

      {:error, reason} ->
        {:error, {:backup_unreachable, reason}}
    end
  end

  # `{:ok, manifest}` for the backup `LATEST` names, `:none` when there is no
  # `LATEST`. `keys` are the signing keys to check the manifest against
  # (`:skip`: the caller only needs its file list, e.g. retention).
  defp load_latest(instance, target, keys) do
    with {:ok, id} <- read_latest_id(instance, target),
         do: load_manifest(instance, target, id, keys)
  end

  defp load_manifest(instance, target, id, keys) do
    case get(instance, target, key(target, "#{id}/manifest.json")) do
      {:ok, bin} when is_binary(bin) ->
        with {:ok, manifest} <- decode_manifest(bin),
             :ok <- validate_manifest(manifest, id),
             :ok <- verify_signature(manifest, keys) do
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

  defp verify_signature(_manifest, :skip), do: :ok
  defp verify_signature(manifest, keys), do: check_signature(manifest, keys)

  defp decode_manifest(bin) do
    case JSON.decode(bin) do
      {:ok, %{} = manifest} -> {:ok, manifest}
      {:ok, _other} -> {:error, :not_an_object}
      {:error, reason} -> {:error, {:json, reason}}
    end
  end

  defp validate_manifest(%{"version" => version, "id" => id, "files" => files} = m, id)
       when version in @readable_manifest_versions and is_list(files) and
              is_integer(:erlang.map_get("created_at_ms", m)) do
    cond do
      not Enum.all?(files, &valid_file?/1) -> {:error, :bad_file_entry}
      not Enum.any?(files, &(&1["name"] == "CURRENT")) -> {:error, :no_current}
      not valid_format_version?(Map.get(m, "format_version")) -> {:error, :bad_format_version}
      true -> :ok
    end
  end

  defp validate_manifest(%{"version" => version}, _id)
       when version not in @readable_manifest_versions,
       do: {:error, {:unsupported_version, version}}

  defp validate_manifest(_manifest, _id), do: {:error, :malformed}

  defp valid_format_version?(nil), do: true
  defp valid_format_version?(version), do: is_integer(version) and version >= 1

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
  It is `Ankusa.Queue.Archive.reconcile/2` over this node's own segments (the
  dated layout's `<node>` directory and the legacy `seg/` prefix):

    * `m:next_seq` moves past the highest archived seq, so no new segment key
      overwrites an archived one;
    * a segment the store has no catalogue row for is catalogued from its
      `.idx` object and its header, and the archive obligations it settles are
      cleared, so the compactor does not write those hooks again under the same
      first seq.

  A segment whose index object is missing or does not decode (its writer died
  between the two PUTs) is logged and skipped. Any other read failure is an
  error: the store must not start with seqs it may hand out twice.
  """
  @spec reconcile_archive(Config.t()) :: :ok | {:error, term()}
  def reconcile_archive(%Config{} = config) do
    with {:ok, _summary} <- Archive.reconcile(config, node: :own, advance_seq: true), do: :ok
  end

  # ── helpers ───────────────────────────────────────────────────────────────

  defp checkpoint_dir(config), do: Config.path(config, "store.checkpoint")

  # The path comes from operator config or this node's own index, never request input.
  # sobelow_skip ["Traversal.FileModule"]
  defp remove(dir) do
    case File.rm_rf(dir) do
      {:ok, _removed} -> :ok
      {:error, reason, file} -> {:error, {:remove_failed, file, reason}}
    end
  end

  defp key(%{prefix: prefix}, name), do: prefix <> "backup/" <> name

  defp sha256(bin), do: :crypto.hash(:sha256, bin) |> Base.encode16(case: :lower)

  defp put(instance, target, key, data),
    do: call(fn -> target.mod.put(instance, key, data, target.opts) end, target.timeout)

  defp get(instance, target, key),
    do: call(fn -> target.mod.get(instance, key, target.opts) end, target.timeout)

  defp list(instance, target, prefix),
    do: call(fn -> target.mod.list(instance, prefix, target.opts) end, target.timeout)

  defp delete(instance, target, key),
    do: call(fn -> target.mod.delete(instance, key, target.opts) end, target.timeout)

  # A blob store is user code (S3, GCS, a custom adapter): an error return, a
  # raise, an exit, a throw and a call that outlasts `timeout` are the same
  # failure, never a crash or a hang. The bounded call runs in a process of its
  # own, monitored and not linked: the caller may be trapping exits (the store
  # restores from its `init/1`) and must not see an `EXIT` per call. The result
  # travels as the exit reason, which shares large binaries rather than copying
  # them.
  defp call(fun, :infinity), do: guarded(fun)

  defp call(fun, timeout) do
    {pid, ref} = spawn_monitor(fn -> exit({:blob_result, guarded(fun)}) end)

    receive do
      {:DOWN, ^ref, :process, ^pid, {:blob_result, result}} -> result
      {:DOWN, ^ref, :process, ^pid, reason} -> {:error, {:exit, reason}}
    after
      timeout ->
        Process.exit(pid, :kill)

        receive do
          {:DOWN, ^ref, :process, ^pid, _reason} -> {:error, :timeout}
        end
    end
  end

  defp guarded(fun) do
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
