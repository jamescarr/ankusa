defmodule Ankusa.Store do
  @moduledoc """
  The node's local store: one RocksDB database per instance, under
  `Config.path(config, "store")`.

  Holds hooks, one delivery row per hook and sink (the DLQ is the dead rows),
  the quarantine pen, API-managed sources, rate-limit overrides and the
  archive catalogue. Column families:

    * `default` — sequence, migration markers, sources, rate limits
    * `hooks` — `<<seq::64>>` → envelope
    * `deliveries` — `<<seq::64, sink::16>>` → delivery row
    * `index` — due/inflight/dead rows, archive obligations, cleared markers,
      claim-check refs
    * `archive` — segment catalogue and imported legacy locations
    * `quarantine` — quarantined envelopes

  `Ankusa.Store` is the one process that owns the database handle; every other
  caller reads or writes through the functions here, which run the RocksDB
  NIFs (on the dirty schedulers) from the calling process and look the handles
  up in ETS on every call. Durability is per call: `sync: true` means the
  write is on disk before the call returns.

  RocksDB is one WAL per store, so a `sync: false` writer's bytes are durable
  as soon as any later synced commit lands. Every write is atomic, and the
  recovery mode tolerates only a torn tail, never earlier damage.
  """

  use GenServer

  require Logger

  alias Ankusa.Config
  alias Ankusa.Store.Keys
  alias Ankusa.Store.Migrate

  @cf_names ~w(default hooks deliveries index archive quarantine)a

  @cache_bytes 64 * 1024 * 1024

  # How long a store that failed to reopen waits before trying again by itself.
  @retry_open_ms 5_000

  # `tolerate_corrupted_tail_records` drops a torn tail and fails the open on
  # any damage before it. `paranoid_checks` makes every read report corruption
  # instead of returning possibly-wrong bytes.
  @db_opts [
    create_if_missing: true,
    create_missing_column_families: true,
    paranoid_checks: true,
    wal_recovery_mode: :tolerate_corrupted_tail_records,
    max_total_wal_size: 512 * 1024 * 1024,
    db_write_buffer_size: 256 * 1024 * 1024,
    max_background_jobs: 4,
    keep_log_file_num: 5,
    max_log_file_size: 16 * 1024 * 1024
  ]

  # ── lifecycle ─────────────────────────────────────────────────────────────

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :store))
  end

  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :instance)},
      start: {__MODULE__, :start_link, [opts]},
      shutdown: 30_000
    }
  end

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    config = Keyword.fetch!(opts, :config)
    path = Config.path(config, "store")

    Process.flag(:trap_exit, true)

    retry_open_ms = Keyword.get(opts, :retry_open_ms, @retry_open_ms)

    with :ok <- Ankusa.Fsync.mkdir_p(path) do
      case :rocksdb.new_cache(:lru, @cache_bytes) do
        {:ok, cache} ->
          with {:ok, state} <- open_and_publish(instance, path, cache, config) do
            {:ok, Map.merge(state, %{retry_open_ms: retry_open_ms, retry_ref: nil})}
          end

        {:error, reason} ->
          open_failed(path, reason)
      end
    else
      {:error, reason} -> open_failed(path, reason)
    end
  end

  defp open_and_publish(instance, path, cache, config) do
    case :rocksdb.open(String.to_charlist(path), @db_opts, descriptors(cache)) do
      {:ok, db, cf_handles} ->
        cfs = @cf_names |> Enum.zip(cf_handles) |> Map.new()

        case put_sentinels(db, cfs) do
          :ok -> publish(instance, path, cache, db, cfs, config)
          {:error, reason} -> close_and_fail(path, cache, db, cfs, {:sentinels, reason})
        end

      {:error, reason} ->
        :rocksdb.release_cache(cache)
        open_failed(path, reason)
    end
  end

  defp publish(instance, path, cache, db, cfs, config) do
    table = :"ankusa_store_#{instance}"

    created =
      try do
        ^table = :ets.new(table, [:named_table, :protected, read_concurrency: true])
        true
      rescue
        ArgumentError -> false
      end

    if created do
      true = :ets.insert(table, {:handles, %{db: db, cache: cache, cfs: cfs}})

      Logger.info("[ankusa] store at #{path}. Durable to power loss on THIS host only.")

      # Handles are published, so the import can use `write/3` and `get/3`.
      case Migrate.run(config) do
        :ok ->
          {:ok, %{db: db, cache: cache, cfs: cfs, table: table, path: path}}

        {:error, reason} ->
          # A 0.3 data dir this node could not read completely and trustworthily
          # must not boot as if it were the whole story. The reason names the
          # file and byte; the artifact is untouched. Nobody else has the handles
          # yet, so the table can go after the close.
          {_, _} = close_db(db, cfs)
          :ets.delete(table)
          :rocksdb.release_cache(cache)
          {:stop, reason}
      end
    else
      close_and_fail(path, cache, db, cfs, :ets_table_exists)
    end
  end

  defp close_and_fail(path, cache, db, cfs, reason) do
    {_, _} = close_db(db, cfs)
    :rocksdb.release_cache(cache)
    open_failed(path, reason)
  end

  # Close with the column-family handles still referenced by the caller.
  #
  # The binding frees each handle's lock and condition variable when its last
  # reference goes, and tears the same handle down inside `close`. A reference
  # dropped on another thread while `close` runs races that teardown: it was
  # seen as an abort inside `:rocksdb.close/1` (the destructor notifying a
  # condition variable the cleanup had just deleted). ETS frees its copies
  # asynchronously, so a handle held only there can be dropped mid-close.
  # Returning `cfs` after the close is what keeps the caller holding it across
  # the call; drop it only afterwards.
  defp close_db(db, cfs), do: {:rocksdb.close(db), cfs}

  # The end-of-range markers `fold/6` relies on. Idempotent: every open
  # rewrites them, so a store created before a family existed gains it.
  defp put_sentinels(db, cfs) do
    {:ok, batch} = :rocksdb.batch()

    try do
      for {cf, key} <- Keys.sentinels(),
          do: :ok = :rocksdb.batch_put(batch, Map.fetch!(cfs, cf), key, <<>>)

      :rocksdb.write_batch(db, batch, sync: true)
    after
      :rocksdb.release_batch(batch)
    end
  end

  defp open_failed(path, reason) do
    Logger.error(
      "[ankusa] store at #{path} could not be opened: #{inspect(reason)}. " <>
        "Refusing to start: a store this node cannot read is never treated as empty."
    )

    {:stop, {:store_open_failed, path, reason}}
  end

  @impl true
  def handle_call(:reopen, _from, state) do
    case reopen_db(state) do
      {:ok, state} -> {:reply, :ok, state}
      {:error, reason, state} -> {:reply, {:error, reason}, state}
    end
  end

  # A store left closed by a failed reopen opens itself again. With no ingest
  # nothing else would ask, and dispatch, the pen and the admin API all answer
  # `:store_unavailable` until something does.
  @impl true
  def handle_info(:retry_open, %{db: nil} = state) do
    case reopen_db(%{state | retry_ref: nil}) do
      {:ok, state} ->
        Logger.warning("[ankusa] store at #{state.path} is open again")
        {:noreply, state}

      {:error, _reason, state} ->
        {:noreply, state}
    end
  end

  def handle_info(:retry_open, state), do: {:noreply, %{state | retry_ref: nil}}

  def handle_info(message, state) do
    Logger.warning("[ankusa] store received an unexpected message: #{inspect(message)}")
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    # First, so callers see `:store_unavailable` rather than a closing database.
    _ = :ets.delete(state.table)
    if state.db, do: {_, _} = close_db(state.db, state.cfs)
    _ = :rocksdb.release_cache(state.cache)
    :ok
  end

  defp reopen_db(state) do
    # Drop the published handles first, so callers see `:store_unavailable`
    # instead of racing the close. `state.cfs` keeps them alive until the close
    # is done (see `close_db/2`).
    :ets.delete(state.table, :handles)
    if state.db, do: {_, _} = close_db(state.db, state.cfs)

    case :rocksdb.open(String.to_charlist(state.path), @db_opts, descriptors(state.cache)) do
      {:ok, db, cf_handles} ->
        cfs = @cf_names |> Enum.zip(cf_handles) |> Map.new()
        true = :ets.insert(state.table, {:handles, %{db: db, cache: state.cache, cfs: cfs}})
        {:ok, cancel_retry(%{state | db: db, cfs: cfs})}

      {:error, reason} ->
        # Stay up and unpublished: every call answers `:store_unavailable` until
        # a later reopen works, which `schedule_retry/1` makes happen on its own.
        Logger.error("[ankusa] store at #{state.path} could not be reopened: #{inspect(reason)}")
        {:error, reason, schedule_retry(%{state | db: nil, cfs: %{}})}
    end
  end

  defp schedule_retry(%{retry_ref: nil} = state),
    do: %{state | retry_ref: Process.send_after(self(), :retry_open, state.retry_open_ms)}

  defp schedule_retry(state), do: state

  defp cancel_retry(%{retry_ref: nil} = state), do: state

  defp cancel_retry(%{retry_ref: ref} = state) do
    Process.cancel_timer(ref)
    %{state | retry_ref: nil}
  end

  @doc """
  Closes and reopens the database at the same path, then republishes the
  handles.

  RocksDB latches a background error after a failed WAL append (the disk was
  full) and keeps rejecting writes after space is freed. Reopening replays the
  WAL, drops a torn tail, and clears the latch. `Ankusa.Queue.Writer` calls
  this after a failed commit, at most once every few seconds. A failed reopen
  leaves the store unavailable, not crashed, and the store then retries by
  itself every few seconds until it opens.
  """
  @spec reopen(atom()) :: :ok | {:error, term()}
  def reopen(instance) do
    GenServer.call(Ankusa.via(instance, :store), :reopen, 60_000)
  end

  defp descriptors(cache) do
    block = [block_based_table_options: [block_cache: cache]]

    Enum.map(@cf_names, fn name ->
      base = Keyword.merge(block, cf_opts(name))
      {Atom.to_charlist(name), base}
    end)
  end

  # Compression is pointless for fixed-size rows (deliveries) and index keys
  # whose values are a few bytes; it pays only on the hook payloads and
  # quarantined bodies. Those two go through zstd into blob files, so large
  # values land outside the block cache's LSM region.
  defp cf_opts(name) when name in [:hooks, :quarantine], do: large_cf_opts(name)

  defp cf_opts(name) do
    [compression: :none, write_buffer_size: buffer_size(name)]
  end

  defp large_cf_opts(name) do
    [
      compression: :zstd,
      write_buffer_size: buffer_size(name),
      enable_blob_files: true,
      min_blob_size: 4096,
      blob_compression_type: :zstd,
      enable_blob_garbage_collection: true
    ]
  end

  defp buffer_size(:hooks), do: 64 * 1024 * 1024
  defp buffer_size(name) when name in [:deliveries, :index], do: 16 * 1024 * 1024
  defp buffer_size(_), do: 4 * 1024 * 1024

  # ── writes ────────────────────────────────────────────────────────────────

  @type op ::
          {:put, atom(), binary(), binary()}
          | {:delete, atom(), binary()}
          | {:delete_range, atom(), binary(), binary()}

  @doc """
  Writes `ops` as one atomic batch. `sync: true` returns only once the batch
  is durable. `[]` is a no-op success. Keys and values must be binaries: a
  malformed op raises, it is never reported as an unavailable store.
  """
  @spec write(atom(), [op()], keyword()) :: :ok | {:error, term()}
  def write(instance, ops, opts \\ [])
  def write(_instance, [], _opts), do: :ok

  def write(instance, ops, opts) when is_list(ops) do
    with_handles(instance, fn handles ->
      sync = Keyword.get(opts, :sync, false)

      with {:ok, batch} <- :rocksdb.batch() do
        try do
          Enum.each(ops, &add_op(batch, handles.cfs, &1))
          :rocksdb.write_batch(handles.db, batch, sync: sync)
        after
          :rocksdb.release_batch(batch)
        end
      end
    end)
  end

  defp add_op(batch, cfs, {:put, cf, k, v}) when is_binary(k) and is_binary(v),
    do: :rocksdb.batch_put(batch, Map.fetch!(cfs, cf), k, v)

  defp add_op(batch, cfs, {:delete, cf, k}) when is_binary(k),
    do: :rocksdb.batch_delete(batch, Map.fetch!(cfs, cf), k)

  defp add_op(batch, cfs, {:delete_range, cf, from, to}) when is_binary(from) and is_binary(to),
    do: :rocksdb.batch_delete_range(batch, Map.fetch!(cfs, cf), from, to)

  # ── reads ─────────────────────────────────────────────────────────────────

  @doc """
  Reads `key` from column family `cf`. Corruption is reported, never hidden.
  """
  @spec get(atom(), atom(), binary()) :: {:ok, binary()} | :not_found | {:error, term()}
  def get(instance, cf, key) when is_atom(cf) and is_binary(key) do
    with_handles(instance, fn handles ->
      :rocksdb.get(handles.db, Map.fetch!(handles.cfs, cf), key, [])
    end)
  end

  @doc """
  Reads `keys` from column family `cf`, results in key order. Any per-key
  error turns the whole call into an error.
  """
  @spec multi_get(atom(), atom(), [binary()]) ::
          {:ok, [{:ok, binary()} | :not_found]} | {:error, term()}
  def multi_get(instance, cf, keys) when is_atom(cf) and is_list(keys) do
    Enum.each(keys, fn key ->
      is_binary(key) or raise ArgumentError, "store keys must be binaries, got: #{inspect(key)}"
    end)

    with_handles(instance, fn handles ->
      results = :rocksdb.multi_get(handles.db, Map.fetch!(handles.cfs, cf), keys, [])

      case Enum.find(results, &match?({:error, _}, &1)) do
        {:error, reason} -> {:error, reason}
        nil -> {:ok, results}
      end
    end)
  end

  @doc """
  Folds a scanned key `family` (an internal key family such as `:hooks`, `:due` or `:dead`) over the
  half-open key range `{lower, upper}`. `fun.(key, value, acc)` returns
  `{:cont, acc}` or `{:halt, acc}`; `reverse: true` walks newest-first.

  The RocksDB binding reports corruption met during iteration as a clean end
  of the scan (`invalid_iterator`), so a scan is only believed when it
  *proves* it finished: forward it must reach a key at or beyond `upper` (the
  family's high sentinel always exists to stop on), reverse a key below `lower`
  or the low sentinel. A scan that goes invalid before that returns
  `{:error, {:corruption, _}}` instead of a silently shortened result. Stopping
  early with `{:halt, acc}` is the caller's choice and needs no proof.
  Sentinels are never passed to `fun`.
  """
  @spec fold(
          atom(),
          atom(),
          {binary(), binary()},
          acc,
          (binary(), binary(), acc ->
             {:cont, acc} | {:halt, acc}),
          keyword()
        ) ::
          {:ok, acc} | {:error, term()}
        when acc: term()
  def fold(instance, family, {lower, upper}, acc, fun, opts \\ [])
      when is_atom(family) and is_binary(lower) and is_binary(upper) do
    %{cf: cf, lo: lo, hi: hi} = Keys.family(family)
    reverse? = Keyword.get(opts, :reverse, false)
    spec = %{family: family, lower: lower, upper: upper, lo: lo, hi: hi, reverse?: reverse?}

    case scan_bounds(spec) do
      :empty ->
        {:ok, acc}

      {read_opts, first} ->
        with_handles(instance, fn handles ->
          case :rocksdb.iterator(handles.db, Map.fetch!(handles.cfs, cf), read_opts) do
            {:ok, itr} ->
              try do
                scan(itr, first, spec, fun, acc)
              after
                :rocksdb.iterator_close(itr)
              end

            {:error, reason} ->
              {:error, reason}
          end
        end)
    end
  end

  # Forward scans cover the sentinels' inside: from just above `lo` to `hi`
  # inclusive. Reverse scans run from just below `min(upper, hi)` down to `lo`
  # inclusive. Either way the iterator is bounded by sentinels that exist.
  defp scan_bounds(%{reverse?: false} = s) do
    lower_bound = <<s.lo::binary, 0>>
    upper_bound = <<s.hi::binary, 0>>
    start = max(s.lower, lower_bound)

    if start >= upper_bound do
      :empty
    else
      {[iterate_lower_bound: lower_bound, iterate_upper_bound: upper_bound], {:seek, start}}
    end
  end

  defp scan_bounds(%{reverse?: true} = s) do
    upper_bound = min(s.upper, s.hi)

    if upper_bound <= s.lo do
      :empty
    else
      {[iterate_lower_bound: s.lo, iterate_upper_bound: upper_bound], :last}
    end
  end

  defp scan(itr, move, s, fun, acc) do
    case :rocksdb.iterator_move(itr, move) do
      {:ok, key, value} ->
        if scan_done?(s, key) do
          {:ok, acc}
        else
          case fun.(key, value, acc) do
            {:cont, acc} -> scan(itr, if(s.reverse?, do: :prev, else: :next), s, fun, acc)
            {:halt, acc} -> {:ok, acc}
          end
        end

      {:error, :invalid_iterator} ->
        Logger.error(
          "[ankusa] scan of #{s.family} ended before its end-of-range marker: " <>
            "the store is corrupt or unreadable past this point"
        )

        {:error, {:corruption, ~c"scan ended before its end-of-range marker"}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Forward: a key at/after the end of the range, or the high sentinel itself.
  # Reverse: a key before the start of the range, or the low sentinel itself.
  defp scan_done?(%{reverse?: false} = s, key), do: key >= s.upper or key == s.hi
  defp scan_done?(%{reverse?: true} = s, key), do: key < s.lower or key == s.lo

  @doc """
  Reads a RocksDB CF property like `"rocksdb.estimate-num-keys"`,
  `"rocksdb.total-sst-files-size"` or `"rocksdb.total-blob-file-size"`,
  parsed to an integer.
  """
  @spec property(atom(), atom(), String.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def property(instance, cf, name) do
    with_handles(instance, fn handles ->
      case :rocksdb.get_property(handles.db, Map.fetch!(handles.cfs, cf), name) do
        {:ok, bin} when is_binary(bin) -> parse_property(bin)
        {:error, reason} -> {:error, reason}
      end
    end)
  end

  defp parse_property(bin) do
    case Integer.parse(String.trim(bin)) do
      {n, _} -> {:ok, n}
      :error -> {:error, {:bad_property_value, bin}}
    end
  end

  @doc """
  For each key prefix, whether `family` holds any key that starts with it, in
  the order given. One iterator serves the whole list, so probing a batch costs
  a seek per prefix instead of an iterator per prefix.

  It carries `fold/6`'s guarantee: the family's high sentinel always exists, so
  a seek that finds nothing at all means the iterator went invalid on damage,
  and the answer is `{:error, {:corruption, _}}`, never a quiet `false`.
  """
  @spec prefixes_present(atom(), atom(), [binary()]) :: {:ok, [boolean()]} | {:error, term()}
  def prefixes_present(_instance, _family, []), do: {:ok, []}

  def prefixes_present(instance, family, prefixes) when is_atom(family) and is_list(prefixes) do
    %{cf: cf, lo: lo, hi: hi} = Keys.family(family)
    read_opts = [iterate_lower_bound: <<lo::binary, 0>>, iterate_upper_bound: <<hi::binary, 0>>]

    with_handles(instance, fn handles ->
      case :rocksdb.iterator(handles.db, Map.fetch!(handles.cfs, cf), read_opts) do
        {:ok, itr} ->
          try do
            probe(itr, prefixes, [])
          after
            :rocksdb.iterator_close(itr)
          end

        {:error, reason} ->
          {:error, reason}
      end
    end)
  end

  defp probe(_itr, [], acc), do: {:ok, Enum.reverse(acc)}

  defp probe(itr, [prefix | rest], acc) when is_binary(prefix) do
    case :rocksdb.iterator_move(itr, {:seek, prefix}) do
      {:ok, key, _value} ->
        probe(itr, rest, [String.starts_with?(key, prefix) | acc])

      {:error, :invalid_iterator} ->
        Logger.error("[ankusa] probe ended before its end-of-range marker: the store is corrupt")
        {:error, {:corruption, ~c"probe ended before its end-of-range marker"}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ── handle plumbing ───────────────────────────────────────────────────────

  # Every call looks the handles up fresh: a handle is a NIF resource whose
  # owner is the Store process, so caching one in a caller would survive the
  # owner's death and raise `ArgumentError` on every later use.
  defp with_handles(instance, fun) do
    case handles(instance) do
      {:ok, handles} ->
        try do
          fun.(handles)
        rescue
          e in ArgumentError ->
            Logger.error("[ankusa] store handle is stale: #{Exception.message(e)}")
            {:error, :store_unavailable}
        end

      {:error, :store_unavailable} = error ->
        error
    end
  end

  defp handles(instance) do
    table = :"ankusa_store_#{instance}"

    try do
      case :ets.lookup(table, :handles) do
        [{:handles, handles}] -> {:ok, handles}
        [] -> unavailable(instance)
      end
    rescue
      ArgumentError -> unavailable(instance)
    end
  end

  defp unavailable(instance) do
    Logger.error("[ankusa] store for instance #{inspect(instance)} is not running")
    {:error, :store_unavailable}
  end
end
