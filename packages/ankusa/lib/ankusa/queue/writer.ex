defmodule Ankusa.Queue.Writer do
  @moduledoc """
  The only seq assigner: commits batches of hooks to the store with
  `sync: true`, so a `2xx` to the provider always means the bytes are on disk.

  One process per instance, under `Ankusa.via(instance, :queue_writer)`. The
  commit is a single atomic store batch: the hook, one pending delivery row and
  due key (with its active-tenant marker) per sink, the archive obligation when
  the `:storage` role runs, the counter deltas of all of them, the `m:next_seq`
  marker and, for a tenant seen for the first time, its ordinal (this process is
  the only one that allocates them, see `queue/tenants.ex`). A failed commit
  consumes its seqs and ordinals (gaps are allowed) but acks nothing. A hook
  whose tenant would pass `tenants.max` is refused with `{:error,
  :tenant_limit}` in its place of the reply, and nothing of it is written.
  """

  use GenServer

  require Logger

  alias Ankusa.Config
  alias Ankusa.Queue.Deliveries
  alias Ankusa.Queue.Deliveries.Counters
  alias Ankusa.Queue.Tenants
  alias Ankusa.Store
  alias Ankusa.Store.Keys

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :queue_writer))
  end

  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.fetch!(opts, :instance)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    config = Ankusa.config(instance)

    # A seq is never reused: if either read fails, refuse to start rather than
    # guess a floor. The same for tenant ordinals.
    with {:ok, marker} <- marker_seq(instance),
         {:ok, last} <- last_hook_seq(instance),
         {:ok, ord_marker} <- Tenants.next_ord(instance),
         {:ok, last_ord} <- last_tenant_ord(instance) do
      next_seq = max(marker, last + 1)
      next_ord = max(ord_marker, last_ord + 1)

      Logger.info("[ankusa] store next_seq=#{next_seq} tenants=#{next_ord - 1}")

      dedupe = config.dedupe

      {:ok,
       %{
         instance: instance,
         next_seq: next_seq,
         next_ord: next_ord,
         tenants_max: config.tenants.max,
         dedupe_max: dedupe.max_entries,
         dedupe_per_tenant: dedupe.max_entries_per_tenant,
         # tenant id => ordinal; only this process reads or writes it, and
         # only after a commit succeeded
         ords: :ets.new(:ankusa_tenant_ords, [:set, :private]),
         archive?: Config.role?(config, :storage),
         last_at: 0
       }}
    else
      {:error, reason} -> {:stop, {:queue_writer_init_failed, reason}}
    end
  end

  defp marker_seq(instance) do
    case Store.get(instance, :default, Keys.meta("next_seq")) do
      {:ok, <<n::64>>} -> {:ok, n}
      :not_found -> {:ok, 1}
      {:error, reason} -> {:error, reason}
    end
  end

  defp last_hook_seq(instance) do
    %{hi: hi} = Keys.family(:hooks)

    Store.fold(
      instance,
      :hooks,
      {Keys.hook(1), hi},
      0,
      fn key, _v, _acc -> {:halt, :binary.decode_unsigned(key)} end,
      reverse: true
    )
  end

  defp last_tenant_ord(instance) do
    %{hi: hi} = Keys.family(:tenants)

    Store.fold(
      instance,
      :tenants,
      {Keys.tenant(0), hi},
      0,
      fn <<?t, ord::32>>, _v, _acc -> {:halt, ord} end,
      reverse: true
    )
  end

  @impl true
  def handle_call({:enqueue, items, deadline, extra_ops}, {caller, _tag}, state) do
    cond do
      expired?(deadline) ->
        # Not started, so nothing to undo: no seq consumed, no store touched.
        {:reply, {:error, :deadline_exceeded}, state}

      # The caller died while its call waited here (a commit task killed from
      # outside, or taken down with its batcher). The batcher has already
      # answered those hooks 503, so committing the batch would store hooks
      # nobody acked. Callers are always local: the Registry is node-local.
      not Process.alive?(caller) ->
        {:reply, {:error, :caller_gone}, state}

      true ->
        commit_batch(items, extra_ops, state)
    end
  end

  # The archive-replay batch: one synced commit for the entries and the job's
  # cursor, so a crash cannot leave hooks without their replay marker or a
  # cursor without its hooks.
  def handle_call({:redrive, entries, extra_ops}, {caller, _tag}, state) do
    if Process.alive?(caller) do
      redrive_batch(entries, extra_ops, state)
    else
      {:reply, {:error, :caller_gone}, state}
    end
  end

  # One bounded batch of expired dedupe keys from `Ankusa.Queue.DedupeSweeper`.
  # Each key is deleted only if it still holds the expiry the sweeper saw, so a
  # refresh that landed since is never undone, and its counter goes down in the
  # same batch. Not synced: a lost batch is found again by the next sweep.
  def handle_call({:expire_dedupe, entries}, _from, state) do
    {:reply, expire_dedupe(state.instance, entries), state}
  end

  defp expired?(:infinity), do: false
  defp expired?(deadline), do: System.monotonic_time(:millisecond) >= deadline

  defp redrive_batch(entries, extra_ops, state) do
    now = max(System.system_time(:millisecond), state.last_at)
    count = length(entries)
    tenants = entries |> Enum.map(&Tenants.normalize(&1.tenant_id)) |> Enum.uniq()

    with {:ok, admission} <- admit(tenants, state),
         true <- all_admitted?(tenants, admission) do
      ops =
        entries
        |> Enum.with_index(state.next_seq)
        |> Enum.reduce([], fn {%{
                                 bin: bin,
                                 size: size,
                                 sinks: sinks,
                                 replay_id: replay_id,
                                 source_id: source_id
                               } = entry, seq},
                              ops ->
          ord = Map.fetch!(admission.ords, Tenants.normalize(entry.tenant_id))

          row_ops =
            Enum.flat_map(sinks, fn {index, mod} ->
              row =
                Deliveries.encode_row(%{
                  module: mod,
                  state: :pending,
                  attempts: 0,
                  at: now,
                  error: nil,
                  size: size,
                  source_id: source_id,
                  replay: replay_id
                })

              Deliveries.new_ops(ord, now, seq, index, size, row)
            end)

          [{:put, :hooks, Keys.hook(seq), bin} | row_ops] ++ ops
        end)

      bytes = Enum.sum(Enum.map(entries, & &1.size))

      ops =
        Deliveries.coalesce(
          [{:put, :default, Keys.meta("next_seq"), <<state.next_seq + count::64>>} | ops] ++
            admission.ops ++ extra_ops
        )

      result = commit(state.instance, ops, count, bytes)

      state = %{
        state
        | next_seq: state.next_seq + count,
          next_ord: admission.next_ord,
          last_at: now
      }

      case result do
        :ok ->
          cache_put(state, admission.cache)
          wake_dispatch(state.instance, now)
          {:reply, {:ok, count}, state}

        {:error, reason} ->
          Logger.error(
            "[ankusa] store commit of #{count} replayed hook(s) failed, nothing acked: #{inspect(reason)}"
          )

          {:reply, {:error, reason}, state}
      end
    else
      false -> {:reply, {:error, :tenant_limit}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  # `extra_ops` (a quarantine release's pen deletes and job cursor) ride in the
  # same synced batch as the hooks, and commit even when every item was a
  # duplicate.
  defp commit_batch(items, extra_ops, state) do
    # Never behind a previous batch, even if the wall clock steps back: dispatch
    # keeps its scan floor just under the stamps it has seen, and relies on
    # stamps never going down.
    now = max(System.system_time(:millisecond), state.last_at)
    tenants = items |> Enum.map(&item_tenant/1) |> Enum.uniq()

    case admit(tenants, state) do
      {:ok, admission} ->
        tagged = Enum.map(items, &{&1, Map.has_key?(admission.ords, item_tenant(&1))})

        if extra_ops != [] and Enum.any?(tagged, &(not elem(&1, 1))) do
          # The caller's ops (a quarantine release's pen deletes) cover every
          # item: refusing one would lose it, so refuse the call.
          {:reply, {:error, :tenant_limit}, state}
        else
          commit_admitted(tagged, extra_ops, admission, now, state)
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp commit_admitted(tagged, extra_ops, admission, now, state) do
    admitted = for {item, true} <- tagged, do: item

    case dedupe_check(state, admitted, now, admission.ords, extra_ops == []) do
      {:error, reason} ->
        # The dedupe read failed: no seq consumed, nothing written, nothing
        # acked. Callers see the store error, not a guessed commit.
        {:reply, {:error, reason}, state}

      {:ok, %{fresh: []} = checked} when extra_ops == [] ->
        # Every item was a duplicate (or refused): write nothing, don't wake dispatch.
        {:reply, {:ok, with_refused(tagged, checked.results)}, state}

      {:ok, %{fresh: []} = checked} ->
        # No hook to commit, but the caller's own ops must still land.
        case Store.write(state.instance, extra_ops, sync: true) do
          :ok -> {:reply, {:ok, with_refused(tagged, checked.results)}, state}
          {:error, reason} -> {:reply, {:error, reason}, state}
        end

      {:ok, checked} ->
        {ops, batch_size, bytes} =
          build_ops(checked.fresh, state.next_seq, now, state.archive?, admission.ords)

        ops = Deliveries.coalesce(checked.dedupe_ops ++ ops ++ admission.ops ++ extra_ops)

        result = commit(state.instance, ops, batch_size, bytes)

        state = %{
          state
          | next_seq: state.next_seq + length(checked.fresh),
            next_ord: admission.next_ord,
            last_at: now
        }

        case result do
          :ok ->
            cache_put(state, admission.cache)
            wake_dispatch(state.instance, now)

            committed =
              checked.fresh
              |> Enum.with_index(state.next_seq - length(checked.fresh))
              |> Enum.map(fn {{env, _bin, _mods, _ttl, _replay_id}, seq} ->
                {:committed, %{env | seq: seq}}
              end)

            results = reassemble(checked.results, committed)
            {:reply, {:ok, with_refused(tagged, results)}, state}

          {:error, reason} ->
            Logger.error(
              "[ankusa] store commit of #{length(tagged)} hook(s) failed, nothing acked: #{inspect(reason)}"
            )

            {:reply, {:error, reason}, state}
        end
    end
  end

  # ── tenant ordinals ───────────────────────────────────────────────────────

  @ord_cache_max 65_536

  defp item_tenant({env, _bin, _mods, _ttl, _replay_id}), do: Tenants.normalize(env.tenant_id)

  # Resolves the ordinals of `tenants`: the cache, then one `multi_get` of the
  # rows of the uncached ones, then new ordinals for those with none (up to
  # `tenants.max`). `ops` are the puts of the new ordinals, for the commit batch.
  defp admit(tenants, state) do
    {known, misses} =
      Enum.reduce(tenants, {%{}, []}, fn tenant, {known, misses} ->
        case :ets.lookup(state.ords, tenant) do
          [{_, ord}] -> {Map.put(known, tenant, ord), misses}
          [] -> {known, [tenant | misses]}
        end
      end)

    misses = Enum.reverse(misses)

    with {:ok, stored} <- Tenants.lookup(state.instance, misses) do
      {new, ops, next_ord, _refused} =
        Tenants.allocate(
          state.next_ord,
          Enum.reject(misses, &Map.has_key?(stored, &1)),
          state.tenants_max
        )

      cache = Map.merge(stored, new)
      {:ok, %{ords: Map.merge(known, cache), ops: ops, next_ord: next_ord, cache: cache}}
    end
  end

  defp all_admitted?(tenants, admission),
    do: Enum.all?(tenants, &Map.has_key?(admission.ords, &1))

  # Cleared whole when full: a miss is one point read.
  defp cache_put(state, cache) do
    if :ets.info(state.ords, :size) + map_size(cache) > @ord_cache_max,
      do: :ets.delete_all_objects(state.ords)

    :ets.insert(state.ords, Map.to_list(cache))
  end

  # `results` holds the replies of the admitted items in item order; the refused
  # ones get `{:error, :tenant_limit}` in their place.
  defp with_refused(tagged, results) do
    {merged, []} =
      Enum.map_reduce(tagged, results, fn
        {_item, true}, [result | rest] -> {result, rest}
        {_item, false}, rest -> {{:error, :tenant_limit}, rest}
      end)

    merged
  end

  # ── ingest dedupe ─────────────────────────────────────────────────────────

  # Checks `env.dedupe_key` for every item that also carries a ttl, against the
  # stored `?u` keys and the batch itself. Returns, in original item order,
  # `results` (a `:duplicate` per collapsed item, `{:error, :dedupe_quota}` per
  # item whose new key would pass `dedupe.max_entries` or
  # `dedupe.max_entries_per_tenant`, a `:fresh` marker per item that will
  # commit) plus the fresh items, their dedupe ops (the keys and the counter
  # deltas of the keys they add), and the count. A stored entry counts as a
  # duplicate only while `expires_at > now`. The quota is read from the
  # `dedupe` counters (one `multi_get` per batch that has a keyed item) and
  # counted up as the batch's new keys are admitted; refreshing an expired key
  # adds none. A call that carries `extra_ops` (a quarantine release) is never
  # refused: its hooks were acked long ago.
  defp dedupe_check(state, items, now, ords, enforce?) do
    instance = state.instance
    keyed = Enum.filter(items, fn {env, _bin, _mods, ttl, _replay_id} -> dedupe?(env, ttl) end)

    keys = Enum.map(keyed, fn {env, _bin, _mods, _ttl, _replay_id} -> u_key(env) end)

    with {:ok, stored_results} <- read_stored(instance, keys),
         {:ok, usage} <- read_usage(instance, keyed, ords) do
      stored =
        keyed
        |> Enum.zip(stored_results)
        |> Map.new(fn {{env, _bin, _mods, _ttl, _replay_id}, result} ->
          {u_key(env), result}
        end)

      {_seen, results, fresh, dedupe_ops, fresh_count, _usage, deltas} =
        Enum.reduce(items, {%{}, [], [], [], 0, usage, []}, fn item,
                                                               {seen, results, fresh, ops, n,
                                                                usage, deltas} ->
          {env, _bin, _mods, ttl, _replay_id} = item

          key = if dedupe?(env, ttl), do: u_key(env)

          cond do
            key != nil and Map.has_key?(seen, key) ->
              original_id = Map.fetch!(seen, key)

              {seen, [{:duplicate, %{env | id: original_id, seq: nil}} | results], fresh, ops, n,
               usage, deltas}

            key != nil ->
              case Map.fetch(stored, key) do
                {:ok, {:ok, <<expires_at::64, original_id::binary>>}} when expires_at > now ->
                  seen = Map.put(seen, key, original_id)

                  {seen, [{:duplicate, %{env | id: original_id, seq: nil}} | results], fresh, ops,
                   n, usage, deltas}

                {:ok, {:ok, <<old_at::64, _original_id::binary>>}} ->
                  # An expired key is replaced in place: the live count is unchanged.
                  seen = Map.put(seen, key, env.id)

                  new_ops = [
                    {:put, :index, key, <<now + ttl::64, env.id::binary>>},
                    {:put, :index, expiry_key(now + ttl, env), <<>>},
                    {:delete, :index, expiry_key(old_at, env)}
                  ]

                  {seen, [{:fresh, item} | results], [item | fresh], new_ops ++ ops, n + 1, usage,
                   deltas}

                {:ok, {:ok, _corrupt}} ->
                  # A value too short to hold its 8-byte expiry (the clauses above
                  # took every decodable one). It may not suppress a hook, and it
                  # is overwritten here so the key dedupes again, instead of
                  # staying broken for good. It was counted when it was written.
                  seen = Map.put(seen, key, env.id)

                  new_ops = [
                    {:put, :index, key, <<now + ttl::64, env.id::binary>>},
                    {:put, :index, expiry_key(now + ttl, env), <<>>}
                  ]

                  {seen, [{:fresh, item} | results], [item | fresh], new_ops ++ ops, n + 1, usage,
                   deltas}

                {:ok, :not_found} ->
                  ord = Map.fetch!(ords, item_tenant(item))

                  if enforce? and over_quota?(state, usage, ord) do
                    # Never stored without its key: the provider retries, and the
                    # keys expire on their own.
                    {seen, [{:error, :dedupe_quota} | results], fresh, ops, n, usage, deltas}
                  else
                    seen = Map.put(seen, key, env.id)

                    new_ops = [
                      {:put, :index, key, <<now + ttl::64, env.id::binary>>},
                      {:put, :index, expiry_key(now + ttl, env), <<>>}
                    ]

                    usage = usage |> Map.update(0, 1, &(&1 + 1)) |> Map.update(ord, 1, &(&1 + 1))

                    {seen, [{:fresh, item} | results], [item | fresh], new_ops ++ ops, n + 1,
                     usage, [{ord, :dedupe, 1} | deltas]}
                  end
              end

            true ->
              {seen, [{:fresh, item} | results], [item | fresh], ops, n + 1, usage, deltas}
          end
        end)

      {:ok,
       %{
         results: Enum.reverse(results),
         fresh: Enum.reverse(fresh),
         dedupe_ops: Counters.ops(deltas) ++ dedupe_ops,
         fresh_count: fresh_count
       }}
    end
  end

  # The live-key counts the batch's quota check starts from: the node's and each
  # keyed tenant's, in one `multi_get`. No keyed item, no read.
  defp read_usage(_instance, [], _ords), do: {:ok, %{}}

  defp read_usage(instance, keyed, ords) do
    ord_list =
      keyed |> Enum.map(&Map.fetch!(ords, item_tenant(&1))) |> Enum.uniq()

    with {:ok, counts} <- Counters.read_field(instance, [0 | ord_list], :dedupe) do
      {:ok, [0 | ord_list] |> Enum.zip(counts) |> Map.new()}
    end
  end

  defp over_quota?(state, usage, ord) do
    Map.get(usage, 0, 0) >= state.dedupe_max or
      Map.get(usage, ord, 0) >= state.dedupe_per_tenant
  end

  # `entries` come from `Ankusa.Queue.DedupeSweeper`:
  # `{:expired, expiry_key, u_key, at, ord | nil}` (`ord` is the tenant's
  # ordinal, `nil` when the tenant has none) and `{:undecodable, expiry_key}`.
  # Returns `{:ok, deleted}`, the live keys it removed.
  defp expire_dedupe(instance, entries) do
    expired = for {:expired, _expiry, _u_key, _at, _ord} = entry <- entries, do: entry

    with {:ok, stored} <- read_stored(instance, Enum.map(expired, &elem(&1, 2))) do
      {ops, deltas, node_only, deleted} =
        expired
        |> Enum.zip(stored)
        |> Enum.reduce({[], [], 0, 0}, fn
          {{:expired, expiry, u_key, at, ord}, {:ok, <<stored_at::64, _id::binary>>}},
          {ops, deltas, node_only, deleted}
          when stored_at == at ->
            ops = [{:delete, :index, expiry}, {:delete, :index, u_key} | ops]

            if ord == nil,
              do: {ops, deltas, node_only + 1, deleted + 1},
              else: {ops, [{ord, :dedupe, -1} | deltas], node_only, deleted + 1}

          # Refreshed, or already gone: only this expiry key is stale.
          {{:expired, expiry, _u_key, _at, _ord}, _other}, {ops, deltas, node_only, deleted} ->
            {[{:delete, :index, expiry} | ops], deltas, node_only, deleted}
        end)

      # A key written by a version we no longer understand is dropped rather
      # than wedging the sweep.
      undecodable = for {:undecodable, expiry} <- entries, do: {:delete, :index, expiry}

      all =
        Deliveries.coalesce(
          ops ++ undecodable ++ Counters.ops(deltas) ++ Counters.node_ops(dedupe: -node_only)
        )

      with :ok <- Store.write(instance, all, sync: false), do: {:ok, deleted}
    end
  end

  # An entry dedupes only with a key, a ttl, and a tenant the key encoding can
  # carry. Ingest guarantees the last (it validates the tenant); a direct
  # `Queue.enqueue` caller may not, and a key builder raising inside the
  # writer's batch would crash the writer, so such an entry just skips dedupe.
  defp dedupe?(env, ttl) do
    is_binary(env.dedupe_key) and is_integer(ttl) and tenant_ok?(env.tenant_id)
  end

  # `nil` is the default tenant, as everywhere else in the edge. A NUL would
  # break the expiry key's tenant/source split, so it is refused.
  defp tenant_ok?(nil), do: true
  defp tenant_ok?(tenant), do: is_binary(tenant) and :binary.match(tenant, <<0>>) == :nomatch

  defp u_key(env), do: Keys.dedupe(tenant_of(env), env.source_id, env.dedupe_key)

  defp expiry_key(at, env),
    do: Keys.dedupe_expiry(at, tenant_of(env), env.source_id, env.dedupe_key)

  defp tenant_of(env), do: env.tenant_id || "default"

  # No keys means no dedupe anywhere in the batch: skip the store read
  # entirely, so the zero-dedupe hot path never pays for it (and a
  # store-down failure still surfaces through the commit span).
  defp read_stored(_instance, []), do: {:ok, []}
  defp read_stored(instance, keys), do: Store.multi_get(instance, :index, keys)

  # `results` holds `:duplicate` replies and `:fresh` markers in item order;
  # `committed` holds the stamped envelopes of the fresh items in the same
  # order, so the markers are replaced in place.
  defp reassemble(results, committed) do
    {reassembled, []} =
      Enum.map_reduce(results, committed, fn
        {:fresh, _item}, [committed | rest] -> {committed, rest}
        {:duplicate, _} = reply, rest -> {reply, rest}
        {:error, :dedupe_quota} = reply, rest -> {reply, rest}
      end)

    reassembled
  end

  # ── messages ──────────────────────────────────────────────────────────────

  # Expired dedupe keys are found and batched by `Ankusa.Queue.DedupeSweeper`;
  # the writer only applies a batch it is handed (`{:expire_dedupe, entries}`),
  # because a refresh and a delete of one key must not race.
  @impl true
  def handle_info(_message, state), do: {:noreply, state}

  # ── batch construction ───────────────────────────────────────────────────

  defp build_ops(items, first_seq, now, archive?, ords) do
    items
    |> Enum.with_index(first_seq)
    |> Enum.reduce({[], 0, 0}, fn {{env, bin, mods, _ttl, replay_id}, seq}, {ops, count, bytes} ->
      size = byte_size(bin)
      ord = Map.fetch!(ords, Tenants.normalize(env.tenant_id))

      # A row without `:replay` is a live delivery; a released quarantine hook's
      # rows carry the job's id, like a replayed dead letter's.
      base = %{
        module: nil,
        state: :pending,
        attempts: 0,
        at: now,
        error: nil,
        size: size,
        source_id: env.source_id
      }

      base = if is_binary(replay_id), do: Map.put(base, :replay, replay_id), else: base

      row_ops =
        mods
        |> Enum.with_index()
        |> Enum.flat_map(fn {mod, i} ->
          Deliveries.new_ops(ord, now, seq, i, size, Deliveries.encode_row(%{base | module: mod}))
        end)

      archive_ops = if archive?, do: Deliveries.archive_ops(ord, seq, size), else: []

      # A hook with no obligations (no sinks, archive off) consumes a seq but
      # writes nothing.
      ops =
        if row_ops == [] and archive_ops == [] do
          ops
        else
          [{:put, :hooks, Keys.hook(seq), bin} | row_ops] ++ archive_ops ++ ops
        end

      {ops, count + 1, bytes + size}
    end)
    |> then(fn {ops, count, bytes} ->
      {[{:put, :default, Keys.meta("next_seq"), <<first_seq + count::64>>} | ops], count, bytes}
    end)
  end

  # ── commit ───────────────────────────────────────────────────────────────

  # One atomic, synced batch covers every hook. Anything but `:ok` means the
  # batch is not durable — the span then emits `[:ankusa, :commit, :exception]`
  # (never a `:stop`), so the commit duration/batch-size series keep counting
  # only real commits.
  defp commit(instance, ops, batch_size, bytes) do
    Ankusa.Telemetry.span([:commit], %{instance: instance}, fn ->
      case Store.write(instance, ops, sync: true) do
        :ok -> {:ok, %{batch_size: batch_size, bytes: bytes}, %{}}
        {:error, reason} -> throw({:store_commit_failed, reason})
      end
    end)
  catch
    :throw, {:store_commit_failed, reason} -> {:error, reason}
  end

  # The stamp tells dispatch the earliest due time this batch can have: its rows
  # became visible only now, and may be older than anything it has scanned past.
  defp wake_dispatch(instance, at) do
    case Ankusa.whereis(instance, :dispatch) do
      pid when is_pid(pid) -> send(pid, {:wake, at})
      nil -> :ok
    end
  end
end
