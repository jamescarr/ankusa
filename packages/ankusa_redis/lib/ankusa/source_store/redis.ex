defmodule Ankusa.SourceStore.Redis do
  @moduledoc """
  Writable source store shared by every node: API-managed, tenant-scoped
  sources live in Redis, and each node keeps a mirror of them in ETS.

  `Ankusa.SourceStore.Persistent` keeps the sources it is given in the node's
  own `Ankusa.Store`, so a source created through node A's admin API is a `404`
  on node B. This store keeps them in Redis instead: a source created, updated
  or deleted on any node reaches every node configured with the same
  `namespace`.

  Reads (`fetch/2`, `get/3`, `list_tenant/2`) never touch Redis: they go to the
  node's mirror (the same ETS layout `Ankusa.SourceStore.Persistent` reads),
  so ingest costs no network round trip and a Redis outage does not start
  `404`ing hooks.

  ## Options

  `config.source_store = {Ankusa.SourceStore.Redis, opts}`:

    * `:url` (required) — the Redis URL
    * `:namespace` — key prefix, default `"ankusa:sources:<instance>"`. Two
      nodes share sources **only if they use the same namespace**.
    * `:tick_ms` — the periodic version check, default 30s
    * `:sources` — seed sources from configuration, the same shape
      `Ankusa.SourceStore.Static` takes. Seeds are this node's and read-only:
      they are never written to Redis, never appear in `list_tenant/2`, and a
      Redis entry with a seed's id is skipped with a warning.
    * `:decoder` — `(source_id, spec_map -> keyword)`, as for `Persistent`:
      run on every write, and on every entry loaded from Redis (an entry that
      no longer decodes is skipped with a warning).

  ## Key layout

  | Key | Type | Contents |
  | --- | --- | --- |
  | `<namespace>:sources` | hash | field = source id, value = `{"tenant", "name", "spec"}` as JSON |
  | `<namespace>:version` | string | bumped by every write |
  | `<namespace>` | pub/sub channel | a nudge: "the version moved" |

  ## Writes and invalidation

  A write is one Lua script: the create/update existence check, the `HSET` or
  `HDEL`, and the version bump happen in one step in Redis, so two nodes
  creating the same source cannot both succeed. Last writer wins on updates,
  as with `Persistent`. After a write the node publishes the new version; every
  node that holds a different version reloads the hash (version and hash read in
  one `MULTI`). A missed broadcast is caught by the tick, which is therefore the
  worst-case staleness for a node that was disconnected during a write.

  The node subscribes, and waits for Redis to confirm it, before its first load,
  so a write that lands in between is a message in the mailbox, never lost.

  ## Failing loudly, and outages

  A node refuses to *first* boot against a Redis it cannot reach: an edge with
  an empty mirror would `404` every API-managed source. A store process that
  crashes while the node runs keeps its mirror (the table survives in
  `Ankusa.SourceStore.Redis.TableOwner`) and resumes from it without waiting on
  Redis. A Redis error on a write answers `{:error, :store_unavailable}`
  (the admin API's `503`), and nothing is written; reads keep serving the
  mirror.
  """

  use Supervisor

  @behaviour Ankusa.SourceStore

  alias Ankusa.SourceStore.Redis.{Connections, State, TableOwner}
  alias Ankusa.SourceStore.Table

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    Supervisor.start_link(__MODULE__, instance, name: Ankusa.via(instance, :source_store_sup))
  end

  @impl true
  def init(instance) do
    opts = [instance: instance]

    # The table owner outlives the connections and the state process, so a
    # restart of either finds the mirror the edge has been serving.
    Supervisor.init([{TableOwner, opts}, {Connections, opts}], strategy: :rest_for_one)
  end

  @doc false
  # Redix calls this on every connect (the `:password` MFA in `Connections`).
  @spec password(atom()) :: String.t() | nil
  def password(instance) do
    {_mod, opts} = Ankusa.config(instance).source_store
    opts |> Keyword.fetch!(:url) |> Ankusa.Redis.Options.password()
  end

  # ── reads, from the mirror ──────────────────────────────────────────────────

  @impl Ankusa.SourceStore
  defdelegate fetch(instance, source_id), to: Table

  @impl Ankusa.SourceStore
  defdelegate list(instance), to: Table

  @impl Ankusa.SourceStore
  defdelegate get(instance, tenant, name), to: Table

  @impl Ankusa.SourceStore
  defdelegate list_tenant(instance, tenant), to: Table

  # ── writes, through the state process ───────────────────────────────────────

  @impl Ankusa.SourceStore
  defdelegate put(instance, tenant, name, spec, mode), to: State

  @impl Ankusa.SourceStore
  defdelegate delete(instance, tenant, name), to: State
end

defmodule Ankusa.SourceStore.Redis.TableOwner do
  @moduledoc """
  Keeps the source mirror alive across a restart of the store's state
  process: it creates the table and is its heir, the state process takes it
  over with `hand_over/2`, and when that process dies the table comes back
  here instead of being deleted.
  """

  use GenServer

  alias Ankusa.SourceStore.Table

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, instance, name: Ankusa.via(instance, :source_table))
  end

  @doc "Give the table to `pid` (the state process)."
  @spec hand_over(atom(), pid()) :: :ok | {:error, :not_running | :owned_elsewhere}
  def hand_over(instance, pid) do
    case Ankusa.whereis(instance, :source_table) do
      nil -> {:error, :not_running}
      owner -> GenServer.call(owner, {:hand_over, pid})
    end
  end

  @impl true
  def init(instance), do: {:ok, Table.new(instance, [{:heir, self(), :returned}])}

  @impl true
  def handle_call({:hand_over, pid}, _from, table) do
    if :ets.info(table, :owner) == self() do
      true = :ets.give_away(table, pid, :sources)
      {:reply, :ok, table}
    else
      {:reply, {:error, :owned_elsewhere}, table}
    end
  end

  @impl true
  def handle_info({:"ETS-TRANSFER", _table, _from, :returned}, table), do: {:noreply, table}
end

defmodule Ankusa.SourceStore.Redis.Connections do
  @moduledoc false
  # The two Redis connections and the state process, `:rest_for_one`: the state
  # process holds a subscription that only lives as long as the pub/sub
  # connection it was made on, so that connection dying takes the state process
  # with it, and the restart subscribes again.

  use Supervisor

  alias Ankusa.SourceStore.Redis.State
  alias Ankusa.SourceStore.Table

  @default_tick_ms 30_000

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    {_mod, store_opts} = Ankusa.config(instance).source_store

    url = Keyword.fetch!(store_opts, :url)
    namespace = Keyword.get(store_opts, :namespace) || "ankusa:sources:#{instance}"
    tick_ms = Keyword.get(store_opts, :tick_ms, @default_tick_ms)

    # The children's start arguments are printed by this supervisor's reports
    # and status: they carry the password as an MFA, read back on connect, and
    # never the seeds (sink options, verifier secrets), which the state process
    # reads from the config itself.
    redis =
      Ankusa.Redis.Options.start_opts(url, {Ankusa.SourceStore.Redis, :password, [instance]})

    # A first boot connects synchronously, so an unreachable Redis fails the
    # boot instead of an edge running on an empty mirror. A restart once the
    # mirror is loaded connects in the background and the state process
    # resumes from the mirror (see `State.init/1`).
    sync? = not loaded?(instance)

    children = [
      {Redix, redis ++ [name: Ankusa.via(instance, :source_redis), sync_connect: sync?]},
      # `Redix.PubSub` ships no `child_spec/1`, so the spec is spelled out.
      %{
        id: Redix.PubSub,
        start:
          {Redix.PubSub, :start_link,
           [redis ++ [name: Ankusa.via(instance, :source_redis_pubsub), sync_connect: sync?]]}
      },
      {State, instance: instance, namespace: namespace, tick_ms: tick_ms}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp loaded?(instance), do: :ets.lookup(Table.table(instance), :meta) != []
end

defmodule Ankusa.SourceStore.Redis.State do
  @moduledoc false
  # The node-local half of the Redis source store: owns the mirror (taken over
  # from `TableOwner`), the version it was built from, and the subscription that
  # keeps it current. Registered as `Ankusa.via(instance, :source_store)`.

  use GenServer

  require Logger

  alias Ankusa.SourceStore.Redis.TableOwner
  alias Ankusa.SourceStore.Table

  # A write is at most three round trips — the script, a reload if it raced,
  # the publish — each bounded by Redix's own timeout. The call outlasts them.
  @redis_timeout 5_000
  @call_timeout 20_000

  # KEYS: sources hash, version. ARGV: source id, entry JSON, mode.
  @put_source """
  local exists = redis.call('HEXISTS', KEYS[1], ARGV[1])
  if ARGV[3] == 'create' and exists == 1 then return {'exists'} end
  if ARGV[3] == 'update' and exists == 0 then return {'not_found'} end
  local version = tonumber(redis.call('GET', KEYS[2]) or '0') + 1
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
  redis.call('SET', KEYS[2], version)
  return {'ok', version}
  """

  # KEYS: sources hash, version. ARGV: source id.
  @delete_source """
  local version = tonumber(redis.call('GET', KEYS[2]) or '0') + 1
  if redis.call('HDEL', KEYS[1], ARGV[1]) == 0 then return {'not_found'} end
  redis.call('SET', KEYS[2], version)
  return {'ok', version}
  """

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: server(Keyword.fetch!(opts, :instance)))
  end

  @spec put(atom(), String.t(), String.t(), map(), :create | :update) ::
          {:ok, Ankusa.SourceStore.stored()}
          | {:error, :invalid, String.t()}
          | {:error, :exists}
          | {:error, :not_found}
          | {:error, :store_unavailable}
  def put(instance, tenant, name, spec, mode),
    do: GenServer.call(server(instance), {:put, tenant, name, spec, mode}, @call_timeout)

  @spec delete(atom(), String.t(), String.t()) ::
          :ok
          | {:error, :not_found}
          | {:error, :invalid, String.t()}
          | {:error, :store_unavailable}
  def delete(instance, tenant, name),
    do: GenServer.call(server(instance), {:delete, tenant, name}, @call_timeout)

  # ── GenServer ───────────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    namespace = Keyword.fetch!(opts, :namespace)
    config = Ankusa.config(instance)
    {_mod, store_opts} = config.source_store

    with {:ok, table} <- take_table(instance) do
      state = %{
        instance: instance,
        namespace: namespace,
        table: table,
        decoder: Keyword.get(store_opts, :decoder),
        conn: Ankusa.via(instance, :source_redis),
        pubsub: Ankusa.via(instance, :source_redis_pubsub),
        tick_ms: Keyword.fetch!(opts, :tick_ms),
        version: 0,
        # said once per outage: the namespace lost its version key while this
        # node holds API-managed sources
        warned_missing?: false
      }

      case :ets.lookup(table, :meta) do
        [{:meta, version}] ->
          resume(%{state | version: version})

        [] ->
          # First boot: subscribe, and wait for Redis to confirm it, BEFORE
          # loading, so a write in between is a message in the mailbox.
          Table.insert_seeds(table, Keyword.get(store_opts, :sources, %{}))

          with :ok <- subscribe(state),
               {:ok, state} <- load(state) do
            # Once per boot, not on every reload: a resumed store and every
            # later sync load the same entries again.
            Ankusa.Verifier.warn_stored_shared(config, Table.stored_sources(table))
            Process.send_after(self(), :tick, state.tick_ms)
            {:ok, state}
          else
            {:error, reason} -> {:stop, reason}
          end
      end
    else
      {:error, reason} -> {:stop, {:source_table, reason}}
    end
  end

  # A crash report prints the state and the last message: a `put` carries the
  # new source's secrets.
  @impl true
  def format_status(status) do
    Map.new(status, fn
      {:message, {:put, tenant, name, _spec, mode}} ->
        {:message, {:put, tenant, name, :redacted, mode}}

      other ->
        other
    end)
  end

  @impl true
  def handle_call({:put, tenant, name, spec, mode}, _from, state) do
    source_id = Table.source_id(tenant, name)

    cond do
      Table.seed?(state.table, source_id) ->
        {:reply, read_only(source_id), state}

      not is_map(spec) ->
        {:reply, {:error, :invalid, "spec must be a JSON object"}, state}

      true ->
        case current_entry(state, tenant, name, mode) do
          {:ok, current} ->
            spec = Table.normalize(spec, current)

            with {:ok, stored, source} <- Table.build(state.decoder, tenant, name, spec),
                 :ok <- Table.check_write(state.instance, source) do
              write(state, stored, source, mode)
            else
              {:error, :invalid, message} -> {:reply, {:error, :invalid, message}, state}
            end

          {:error, reason} ->
            {:reply, {:error, :store_unavailable}, warn_unavailable(reason, state)}
        end
    end
  end

  def handle_call({:delete, tenant, name}, _from, state) do
    source_id = Table.source_id(tenant, name)

    if Table.seed?(state.table, source_id) do
      {:reply, read_only(source_id), state}
    else
      case eval(state, @delete_source, [source_id]) do
        {:ok, ["ok", version]} ->
          committed(state, version, :ok, &Table.delete_rows(&1, tenant, name))

        {:ok, ["not_found"]} ->
          {:reply, {:error, :not_found}, state}

        {:error, reason} ->
          {:reply, {:error, :store_unavailable}, warn_unavailable(reason, state)}
      end
    end
  end

  @impl true
  def handle_info(:tick, state) do
    state = sync(state)
    Process.send_after(self(), :tick, state.tick_ms)
    {:noreply, state}
  end

  # The payload is ignored: a broadcast only says the version moved, and what
  # the version is now is Redis's to say.
  def handle_info({:redix_pubsub, _pid, _ref, :message, _properties}, state),
    do: {:noreply, sync(state)}

  # Confirmed (again, after a reconnect, or for the first time on a resumed
  # store): one load catches up on whatever was missed.
  def handle_info({:redix_pubsub, _pid, _ref, :subscribed, _properties}, state),
    do: {:noreply, sync(state)}

  def handle_info({:redix_pubsub, _pid, _ref, :disconnected, properties}, state) do
    Logger.debug("[ankusa_redis] source pub/sub disconnected: #{inspect(properties)}")
    {:noreply, state}
  end

  def handle_info({:redix_pubsub, _pid, _ref, _type, _properties}, state), do: {:noreply, state}

  def handle_info({:"ETS-TRANSFER", _table, _from, :sources}, state), do: {:noreply, state}

  # ── boot ────────────────────────────────────────────────────────────────────

  defp take_table(instance) do
    with :ok <- TableOwner.hand_over(instance, self()) do
      # `give_away` sends its message before the owner replies: take it now.
      receive do
        {:"ETS-TRANSFER", table, _from, :sources} -> {:ok, table}
      after
        0 -> {:ok, Table.table(instance)}
      end
    end
  end

  defp subscribe(state) do
    case Redix.PubSub.subscribe(state.pubsub, state.namespace, self()) do
      {:ok, ref} ->
        receive do
          {:redix_pubsub, _pid, ^ref, :subscribed, _properties} -> :ok
        after
          @redis_timeout -> {:error, {:pubsub_subscribe_failed, :timeout}}
        end

      {:error, reason} ->
        {:error, {:pubsub_subscribe_failed, reason}}
    end
  end

  # A restarted state process serves the mirror its predecessor left and
  # subscribes without waiting: the confirmation, whenever Redis answers,
  # triggers the load. Nothing here blocks on Redis.
  defp resume(state) do
    case Redix.PubSub.subscribe(state.pubsub, state.namespace, self()) do
      {:ok, _ref} ->
        Logger.info(
          "[ankusa_redis] source store restarted with a loaded mirror " <>
            "(#{length(Table.stored_keys(state.table))} sources); serving it and loading " <>
            "from Redis once the subscription is confirmed"
        )

        Process.send_after(self(), :tick, state.tick_ms)
        {:ok, state}

      {:error, reason} ->
        {:stop, {:pubsub_subscribe_failed, reason}}
    end
  end

  # ── reading Redis into the mirror ───────────────────────────────────────────

  # The version and the hash are read in one MULTI/EXEC. The mirror is updated
  # in place — every loaded entry written over its row, then rows Redis no
  # longer holds deleted — so a reader never sees it empty.
  defp load(state) do
    commands = [["GET", version_key(state)], ["HGETALL", sources_key(state)]]

    with {:ok, [raw_version, flat]} <- transaction(state, commands),
         :ok <- not_emptied(state, raw_version, flat),
         {:ok, version} <- parse_version(raw_version || "0") do
      loaded =
        flat
        |> Enum.chunk_every(2)
        |> Enum.flat_map(&load_entry(state, &1))
        |> MapSet.new()

      for {tenant, name} = key <- Table.stored_keys(state.table),
          not MapSet.member?(loaded, key),
          do: Table.delete_rows(state.table, tenant, name)

      :ets.insert(state.table, {:meta, version})
      {:ok, %{state | version: version, warned_missing?: false}}
    end
  end

  defp load_entry(state, [source_id, json]) do
    with {:ok, %{"tenant" => tenant, "name" => name, "spec" => spec}}
         when is_binary(tenant) and is_binary(name) and is_map(spec) <- JSON.decode(json),
         true <- Table.load_spec(state.table, state.decoder, tenant, name, spec) do
      [{tenant, name}]
    else
      false ->
        []

      _not_an_entry ->
        Logger.warning(
          "[ankusa_redis] skipping source #{source_id}: stored entry is not an object"
        )

        []
    end
  end

  # No version key and no sources while this node holds some: a FLUSHDB, an
  # evicted namespace. Reading that as "every source was deleted" would 404
  # every hook at once, so the mirror keeps serving.
  defp not_emptied(state, nil, []) do
    if Table.stored_keys(state.table) == [], do: :ok, else: {:error, :namespace_empty}
  end

  defp not_emptied(_state, _raw_version, _flat), do: :ok

  defp parse_version(version) when is_integer(version), do: {:ok, version}

  defp parse_version(raw) when is_binary(raw) do
    case Integer.parse(raw) do
      {version, ""} -> {:ok, version}
      _not_an_integer -> {:error, {:invalid_version, raw}}
    end
  end

  defp parse_version(raw), do: {:error, {:invalid_version, raw}}

  # ── writes ──────────────────────────────────────────────────────────────────

  # The entry an update merges its secret from. Redis's copy, not the mirror's:
  # the mirror trails another node's write by a pub/sub hop, and merging
  # against a missing or older entry would store the update without the secret.
  defp current_entry(_state, _tenant, _name, :create), do: {:ok, nil}

  defp current_entry(state, tenant, name, :update) do
    case command(state, ["HGET", sources_key(state), Table.source_id(tenant, name)]) do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, json} ->
        case JSON.decode(json) do
          {:ok, %{"spec" => spec}} when is_map(spec) -> {:ok, %{spec: spec}}
          _not_an_entry -> {:ok, nil}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp write(state, stored, source, mode) do
    entry =
      JSON.encode!(%{"tenant" => stored.tenant, "name" => stored.name, "spec" => stored.spec})

    case eval(state, @put_source, [stored.source_id, entry, mode]) do
      {:ok, ["ok", version]} ->
        committed(state, version, {:ok, stored}, &Table.put_rows(&1, stored, source))

      {:ok, ["exists"]} ->
        {:reply, {:error, :exists}, state}

      {:ok, ["not_found"]} ->
        {:reply, {:error, :not_found}, state}

      {:error, reason} ->
        {:reply, {:error, :store_unavailable}, warn_unavailable(reason, state)}
    end
  end

  # Redis accepted the write. One past this node's version means nothing else
  # was written in between, and applying the change gives Redis's table;
  # otherwise another node's write is missing here too, so reload.
  defp committed(state, version, reply, apply) do
    broadcast(state, version)

    if version == state.version + 1 do
      apply.(state.table)
      :ets.insert(state.table, {:meta, version})
      {:reply, reply, %{state | version: version}}
    else
      case load(state) do
        {:ok, state} ->
          {:reply, reply, state}

        {:error, reason} ->
          # Durable in Redis; the next tick sees the versions differ.
          {:reply, reply, warn_unavailable(reason, state)}
      end
    end
  end

  defp read_only(source_id),
    do: {:error, :invalid, "source #{source_id} is seeded from configuration and is read-only"}

  defp warn_unavailable(reason, state) do
    Logger.warning("[ankusa] source store is unavailable: #{inspect(reason)}")
    state
  end

  # Best effort: the write is durable and this node is current; a failed
  # publish costs other nodes at most one tick.
  defp broadcast(state, version) do
    case command(state, ["PUBLISH", state.namespace, Integer.to_string(version)]) do
      {:ok, _receivers} -> :ok
      {:error, reason} -> Logger.warning("[ankusa] source publish failed: #{inspect(reason)}")
    end
  end

  # ── sync ────────────────────────────────────────────────────────────────────

  # Reload when Redis's version differs from this node's — `!=`, so a Redis
  # restored to an older state is followed down as well as up.
  defp sync(state) do
    case command(state, ["GET", version_key(state)]) do
      {:ok, nil} ->
        cond do
          Table.stored_keys(state.table) != [] -> keep_mirror(state)
          state.version == 0 -> state
          true -> reload(state)
        end

      {:ok, raw} ->
        case parse_version(raw) do
          {:ok, version} when version != state.version -> reload(state)
          {:ok, _same} -> state
          {:error, reason} -> warn_unavailable(reason, state)
        end

      {:error, reason} ->
        warn_unavailable(reason, state)
    end
  end

  defp reload(state) do
    case load(state) do
      {:ok, state} -> state
      {:error, :namespace_empty} -> keep_mirror(state)
      {:error, reason} -> warn_unavailable(reason, state)
    end
  end

  defp keep_mirror(%{warned_missing?: true} = state), do: state

  defp keep_mirror(state) do
    Logger.warning(
      "[ankusa_redis] sources namespace #{state.namespace} has no version key; keeping the " <>
        "last known sources (#{length(Table.stored_keys(state.table))})"
    )

    %{state | warned_missing?: true}
  end

  # ── redis ───────────────────────────────────────────────────────────────────

  defp eval(state, script, args) do
    keys = [sources_key(state), version_key(state)]
    command(state, ["EVAL", script, "2" | keys ++ Enum.map(args, &to_string/1)])
  end

  defp command(state, command) do
    case Redix.command(state.conn, command, timeout: @redis_timeout) do
      {:ok, value} -> {:ok, value}
      {:error, reason} -> {:error, {:redis_unavailable, reason}}
    end
  end

  defp transaction(state, commands) do
    case Redix.transaction_pipeline(state.conn, commands, timeout: @redis_timeout) do
      {:ok, results} -> {:ok, results}
      {:error, reason} -> {:error, {:redis_unavailable, reason}}
    end
  end

  defp sources_key(state), do: state.namespace <> ":sources"
  defp version_key(state), do: state.namespace <> ":version"

  defp server(instance), do: Ankusa.via(instance, :source_store)
end
