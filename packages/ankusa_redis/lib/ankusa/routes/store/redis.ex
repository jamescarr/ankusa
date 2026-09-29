defmodule Ankusa.Routes.Store.Redis do
  @moduledoc """
  Route definitions in Redis, shared by every edge node, with a version counter
  and pub/sub invalidation.

  Route management is the one part of Ankusa where every node must agree: node A
  accepting a webhook that node B rejects is an outage that looks like a
  provider problem. So definitions live in Redis and each node keeps a *compiled
  snapshot* of them in memory. The guard reads that snapshot — never Redis — on
  the request path: a network round trip per request, or per route change, would
  be a much worse trade than a few hundred kilobytes of duplicated route table.

  ## Key layout

  Everything hangs off the store's `namespace` (default
  `"ankusa:routes:<instance>"`):

  | Key | Type | Contents |
  | --- | --- | --- |
  | `<namespace>:routes` | hash | field = route id, value = `Ankusa.Routes.Route.to_json/1` as JSON |
  | `<namespace>:ip_rules` | string | the global rules, JSON |
  | `<namespace>:version` | string | monotonically increasing counter |
  | `<namespace>` | pub/sub channel | a nudge: "the version moved" |

  Two nodes share definitions **only if they are configured with the same
  namespace**; the instance-scoped default means two instances in one VM (tests,
  a claim-check node beside an edge node) never collide.

  ## Writes are conditional and atomic

  Every mutation is one Lua script, so Redis applies it as a single step:

    * `insert` and `replace` are conditioned on the version the caller validated
      against (`Ankusa.Routes.Store`'s contract). If Redis holds a different
      version, nothing is written and the answer is `{:error, :stale}` — after the
      node has reloaded, so the caller's retry validates against what is really
      there. `insert` also checks `max_routes` against the shared hash inside the
      same script, so two nodes racing for the last slot cannot both take it.
    * `delete` removes the route and bumps the version in one step, and answers
      `:not_found` from Redis rather than from the node's copy.
    * `put_ip_rules` writes the rules and bumps the version in one step.

  The condition is what keeps two nodes honest. Each validates a create against
  its own mirror, and a mirror lags by the length of a pub/sub round trip; without
  it, both could create the same id, or two enabled routes for one path.

  A write whose new version is not exactly one past the node's own has raced
  another node's write, so the node reloads the whole table instead of applying
  its change to a copy that is missing the other's.

  ## Invalidation

  After a write the node publishes on the namespace channel. The message is a
  nudge, not data: a node reads the version and reloads only if it *differs* from
  the one it holds. Differs, not "is newer", because a flushed or restored Redis
  has a *lower* version, and a mirror that only reloaded upward would keep
  enforcing a table Redis no longer holds. The version and the definitions are
  read in one transaction, so a reload is always a consistent snapshot, and each
  one gets a new snapshot epoch, which retires every cached decision at once
  (`Ankusa.Routes.Cache`).

  A missed broadcast is caught by a periodic tick (`routes.store` option
  `tick_ms`, default 30s) that does the same check. That is why the tick interval
  is the *worst case* staleness for a node that was disconnected while a route
  was changed.

  The node subscribes *before* it loads, so a write that lands in between is a
  message waiting in its mailbox rather than one it never hears. The supervisor
  is `:rest_for_one` with the pub/sub connection ahead of the state process, so a
  pub/sub connection that dies takes the state process with it and the restart
  subscribes again, instead of leaving a node that has silently stopped
  listening.

  A publish failure is not treated as a write failure: the definitions are
  already durable in Redis and this node's mirror is already updated, so the
  only cost is that other nodes wait for their tick.

  ## Failing loudly

  The store refuses to boot against a Redis it cannot reach — `sync_connect` on
  the connection, and a load in the state process's `init/1` — because the
  alternative is an edge that quietly denies every webhook. Once running, a Redis
  error on a write is `{:error, :store_unavailable}` (the admin API reports it as
  a `503`, and the connection reconnects on its own); reads keep being served
  from the in-memory snapshot, so a Redis outage does not start rejecting
  traffic.

  Seeding happens only on a namespace's first boot, in one atomic script: of two
  nodes booting together exactly one writes the seed and the other loads it. A
  route deleted through the API is therefore never resurrected by a restart.
  (The default ETS store, having nowhere to remember a deletion, re-seeds on every
  boot.)
  """

  use Supervisor

  @behaviour Ankusa.Routes.Store

  alias Ankusa.Routes.Store.Redis.State

  @default_tick_ms 30_000

  @doc """
  Options: `url` (required), `namespace`, and `tick_ms` — the store options of
  `config.routes.store`'s `{module, opts}` pair.
  """
  @impl Ankusa.Routes.Store
  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    config = Keyword.fetch!(opts, :config)
    {_mod, store_opts} = config.routes.store

    url = Keyword.fetch!(store_opts, :url)
    namespace = Keyword.get(store_opts, :namespace) || "ankusa:routes:#{instance}"
    tick_ms = Keyword.get(store_opts, :tick_ms, @default_tick_ms)

    # `sync_connect: true` on both connections: a store that cannot reach Redis
    # must fail the boot instead of running with nothing loaded.
    connection = [name: Ankusa.via(instance, :routes_redis), sync_connect: true]

    children = [
      {Redix, {url, connection}},
      # `Redix.PubSub` ships no `child_spec/1`, so the spec is spelled out.
      %{
        id: Redix.PubSub,
        start:
          {Redix.PubSub, :start_link,
           [url, [name: Ankusa.via(instance, :routes_redis_pubsub), sync_connect: true]]}
      },
      {State, instance: instance, config: config, namespace: namespace, tick_ms: tick_ms}
    ]

    # `:rest_for_one`: the state process depends on both connections, and holds a
    # subscription that only lives as long as the pub/sub connection it was made on.
    Supervisor.init(children, strategy: :rest_for_one)
  end

  # The store's address is the state process, so the facade calls it exactly as
  # it calls the ETS store.
  @impl Ankusa.Routes.Store
  defdelegate insert(instance, route, version), to: State

  @impl Ankusa.Routes.Store
  defdelegate replace(instance, route, version), to: State

  @impl Ankusa.Routes.Store
  defdelegate delete(instance, id), to: State

  @impl Ankusa.Routes.Store
  defdelegate put_ip_rules(instance, rules), to: State
end

defmodule Ankusa.Routes.Store.Redis.State do
  @moduledoc """
  The node-local half of the Redis store: the in-memory mirror of the
  definitions, the version it was built from, and the subscription that keeps it
  current.

  Registered as `Ankusa.via(instance, :routes_store)`, the same address the ETS
  store uses, so `Ankusa.Routes.Store`'s facade neither knows nor cares which
  store is configured.
  """

  use GenServer

  @behaviour Ankusa.Routes.Store

  require Logger

  alias Ankusa.Routes.{Route, Snapshot}

  # A write is at most three round trips — the script, a reload if it raced, the
  # publish — each bounded by Redix's own timeout. The call has to outlast all of
  # them, or its caller would give up on a write Redis is about to apply.
  @redis_timeout 5_000
  @call_timeout 20_000

  # Every script reads the version first: a wrong-typed version key aborts the
  # script before anything has been written, and a missing one reads as 1 (see
  # `stored_version/1`). Redis runs a script as one uninterrupted step.
  #
  # KEYS: routes hash, version. ARGV: expected version, id, route JSON, cap (0 for
  # none). The cap is what makes `insert` safe across nodes.
  @put_route """
  local version = tonumber(redis.call('GET', KEYS[2]) or '1')
  if version ~= tonumber(ARGV[1]) then return {'stale', version} end
  local cap = tonumber(ARGV[4])
  if cap > 0 and redis.call('HLEN', KEYS[1]) >= cap then return {'cap', version} end
  redis.call('HSET', KEYS[1], ARGV[2], ARGV[3])
  version = version + 1
  redis.call('SET', KEYS[2], version)
  return {'ok', version}
  """

  # KEYS: routes hash, version. ARGV: id.
  @delete_route """
  local version = tonumber(redis.call('GET', KEYS[2]) or '1')
  if redis.call('HDEL', KEYS[1], ARGV[1]) == 0 then return {'not_found'} end
  version = version + 1
  redis.call('SET', KEYS[2], version)
  return {'ok', version}
  """

  # KEYS: ip_rules, version. ARGV: rules JSON.
  @put_ip_rules """
  local version = tonumber(redis.call('GET', KEYS[2]) or '1') + 1
  redis.call('SET', KEYS[1], ARGV[1])
  redis.call('SET', KEYS[2], version)
  return {'ok', version}
  """

  # KEYS: routes hash, ip_rules, version. ARGV: rules JSON, then id/JSON pairs.
  # Returns 1 if it seeded, 0 if the namespace already existed: of two nodes
  # booting together, exactly one seeds.
  @seed """
  if redis.call('EXISTS', KEYS[3]) == 1 then return 0 end
  redis.call('SET', KEYS[2], ARGV[1])
  for i = 2, #ARGV, 2 do
    redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
  end
  redis.call('SET', KEYS[3], '1')
  return 1
  """

  @doc false
  @impl Ankusa.Routes.Store
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: server(opts[:instance]))
  end

  @impl Ankusa.Routes.Store
  def insert(instance, %Route{} = route, version),
    do: call(instance, {:insert, route, version})

  @impl Ankusa.Routes.Store
  def replace(instance, %Route{} = route, version),
    do: call(instance, {:replace, route, version})

  @impl Ankusa.Routes.Store
  def delete(instance, id), do: call(instance, {:delete, id})

  @impl Ankusa.Routes.Store
  def put_ip_rules(instance, rules), do: call(instance, {:put_ip_rules, rules})

  defp call(instance, message), do: GenServer.call(server(instance), message, @call_timeout)

  # ── GenServer ───────────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    config = Keyword.fetch!(opts, :config)
    namespace = Keyword.fetch!(opts, :namespace)

    state = %{
      instance: instance,
      namespace: namespace,
      max_routes: config.routes.max_routes,
      conn: Ankusa.via(instance, :routes_redis),
      pubsub: Ankusa.via(instance, :routes_redis_pubsub),
      tick_ms: Keyword.fetch!(opts, :tick_ms),
      version: 0,
      routes: %{},
      ip_rules: %{default: :allow, rules: []}
    }

    # Subscribe BEFORE loading: a write that lands between the two is then a
    # message waiting in the mailbox, not one this node never hears.
    with :ok <- subscribe(state),
         {:ok, state} <- bootstrap(state, config.routes) do
      Process.send_after(self(), :tick, state.tick_ms)
      {:ok, state}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:insert, route, version}, _from, state),
    do: write_route(state, route, version, :insert, state.max_routes)

  def handle_call({:replace, route, version}, _from, state),
    do: write_route(state, route, version, :replace, 0)

  def handle_call({:delete, id}, _from, state) do
    keys = [routes_key(state), version_key(state)]

    case eval(state, @delete_route, keys, [id]) do
      {:ok, ["ok", version]} ->
        committed(state, version, :delete, id, fn s -> %{s | routes: Map.delete(s.routes, id)} end)

      {:ok, ["not_found"]} ->
        {:reply, {:error, :not_found}, state}

      {:error, reason} ->
        unavailable(state, reason)
    end
  end

  def handle_call({:put_ip_rules, ip_rules}, _from, state) do
    keys = [ip_rules_key(state), version_key(state)]
    args = [JSON.encode!(Route.rules_json(ip_rules))]

    case eval(state, @put_ip_rules, keys, args) do
      {:ok, ["ok", version]} ->
        committed(state, version, :ip_rules, nil, fn s -> %{s | ip_rules: ip_rules} end)

      {:error, reason} ->
        unavailable(state, reason)
    end
  end

  @impl true
  def handle_info(:tick, state) do
    state = sync(state)
    Process.send_after(self(), :tick, state.tick_ms)
    {:noreply, state}
  end

  # `Redix.PubSub` messages are a five-element tuple:
  # `{:redix_pubsub, pid, subscription_ref, type, properties}`. The payload is
  # deliberately ignored: a broadcast only says the version moved, and what the
  # version is now is Redis's to say — a stale or reordered message must not be
  # able to make this node believe otherwise.
  def handle_info({:redix_pubsub, _pid, _ref, :message, _properties}, state),
    do: {:noreply, sync(state)}

  def handle_info({:redix_pubsub, _pid, _ref, :disconnected, properties}, state) do
    # The pub/sub client reconnects and re-subscribes on its own; anything
    # published while we were away is caught by the tick.
    Logger.debug("[ankusa] route pub/sub disconnected: #{inspect(properties)}")
    {:noreply, state}
  end

  def handle_info({:redix_pubsub, _pid, _ref, _type, _properties}, state), do: {:noreply, state}

  # ── boot ────────────────────────────────────────────────────────────────────

  defp subscribe(state) do
    case Redix.PubSub.subscribe(state.pubsub, state.namespace, self()) do
      {:ok, _ref} -> :ok
      {:error, reason} -> {:error, {:pubsub_subscribe_failed, reason}}
    end
  end

  # First boot of a namespace: write the config seed, atomically. Later boots
  # read what is there, so a route deleted through the API is never resurrected.
  defp bootstrap(state, routes_config) do
    with {:ok, %{routes: routes, ip_rules: rules}} <- Snapshot.initial_table(routes_config),
         {:ok, seeded?} <- seed(state, routes, rules) do
      if seeded? do
        state = %{state | version: 1, routes: routes, ip_rules: rules}
        Snapshot.publish(state)
        {:ok, state}
      else
        with {:ok, state} <- fetch_table(state) do
          Snapshot.publish(state)
          {:ok, state}
        end
      end
    end
  end

  defp seed(state, routes, rules) do
    keys = [routes_key(state), ip_rules_key(state), version_key(state)]

    args = [
      JSON.encode!(Route.rules_json(rules))
      | Enum.flat_map(routes, fn {id, route} -> [id, encode(route)] end)
    ]

    case eval(state, @seed, keys, args) do
      {:ok, 1} -> {:ok, true}
      {:ok, 0} -> {:ok, false}
      {:error, reason} -> {:error, reason}
    end
  end

  # ── reading the table ───────────────────────────────────────────────────────

  # The version and the definitions are read in one MULTI/EXEC, so what comes
  # back is a table that existed, not a version from one moment and a hash from
  # the next. Nothing is published here: callers decide what to announce.
  defp fetch_table(state) do
    commands = [
      ["GET", version_key(state)],
      ["HGETALL", routes_key(state)],
      ["GET", ip_rules_key(state)]
    ]

    with {:ok, [raw_version, raw_routes, raw_rules]} <- transaction(state, commands),
         {:ok, version} <- parse_version(raw_version || "1"),
         {:ok, routes} <- decode_routes(raw_routes),
         {:ok, rules} <- decode_rules(raw_rules) do
      {:ok, %{state | version: version, routes: routes, ip_rules: rules}}
    end
  end

  # The version key is written on first boot; a namespace without one (hand
  # written, or partially restored) is read as version 1 rather than treated as
  # empty, so the definitions that *are* there stay enforced.
  defp stored_version(state) do
    case command(state, ["GET", version_key(state)]) do
      {:ok, nil} -> {:ok, 1}
      {:ok, raw} -> parse_version(raw)
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_routes(flat) do
    flat
    |> Enum.chunk_every(2)
    |> Enum.reduce_while({:ok, %{}}, fn [_id, json], {:ok, acc} ->
      case Route.from_json(decode_json(json)) do
        {:ok, route} ->
          {:cont, {:ok, Map.put(acc, route.id, route)}}

        {:error, {:invalid, field, message}} ->
          {:halt, {:error, {:invalid_stored_route, field, message}}}
      end
    end)
  end

  defp decode_rules(nil), do: {:ok, %{default: :allow, rules: []}}

  defp decode_rules(raw) do
    case Route.parse_ip_rules(decode_json(raw)) do
      {:ok, rules} ->
        {:ok, rules}

      {:error, {:invalid, field, message}} ->
        {:error, {:invalid_stored_ip_rules, "#{field}: #{message}"}}
    end
  end

  defp decode_json(raw) do
    case JSON.decode(raw) do
      {:ok, decoded} -> decoded
      {:error, _reason} -> %{}
    end
  end

  # `GET` arrives as a string and a script's version as an integer (Redix decodes
  # RESP integers), so both shapes are accepted here.
  defp parse_version(version) when is_integer(version), do: {:ok, version}

  defp parse_version(raw) when is_binary(raw) do
    case Integer.parse(raw) do
      {version, ""} -> {:ok, version}
      _not_an_integer -> {:error, {:invalid_version, raw}}
    end
  end

  defp parse_version(raw), do: {:error, {:invalid_version, raw}}

  # ── writes ──────────────────────────────────────────────────────────────────

  defp write_route(state, route, version, action, cap) do
    keys = [routes_key(state), version_key(state)]

    case eval(state, @put_route, keys, [version, route.id, encode(route), cap]) do
      {:ok, ["ok", new_version]} ->
        committed(state, new_version, action, route.id, fn s ->
          %{s | routes: Map.put(s.routes, route.id, route)}
        end)

      {:ok, ["stale", _actual]} ->
        stale(state)

      {:ok, ["cap", _actual]} ->
        {:reply, {:error, :too_many_routes}, state}

      {:error, reason} ->
        unavailable(state, reason)
    end
  end

  # Redis accepted the write. If the new version is exactly one past this node's
  # own, nothing else was written in between and applying the change to the
  # mirror gives Redis's table. If not, another node's write is in Redis and not
  # in the mirror, and applying only ours would drop it — take Redis's table,
  # which has both.
  defp committed(state, new_version, action, route_id, update) do
    broadcast(state, new_version)

    if new_version == state.version + 1 do
      state =
        state
        |> update.()
        |> Map.put(:version, new_version)
        |> Snapshot.publish({action, route_id})

      {:reply, :ok, state}
    else
      case fetch_table(state) do
        {:ok, state} ->
          {:reply, :ok, Snapshot.publish(state, {action, route_id})}

        {:error, reason} ->
          # The write is durable; the mirror is behind, and the next tick sees the
          # versions differ and reloads.
          {:reply, :ok, warn_unavailable(reason, state)}
      end
    end
  end

  # The write lost a race. Refresh the mirror BEFORE answering, so the caller's
  # retry validates against what is really there. If Redis cannot even be read,
  # retrying would spin on the same stale table: that is `store_unavailable`.
  defp stale(state) do
    case fetch_table(state) do
      {:ok, state} ->
        {:reply, {:error, :stale}, Snapshot.publish(state, {:reloaded, nil})}

      {:error, reason} ->
        unavailable(state, reason)
    end
  end

  defp unavailable(state, reason),
    do: {:reply, {:error, :store_unavailable}, warn_unavailable(reason, state)}

  defp warn_unavailable(reason, state) do
    Logger.warning("[ankusa] route store is unavailable: #{inspect(reason)}")
    state
  end

  # Best effort: the definitions are durable and this node is already current, so
  # a failed publish costs other nodes at most one tick.
  defp broadcast(state, version) do
    case command(state, ["PUBLISH", state.namespace, Integer.to_string(version)]) do
      {:ok, _receivers} ->
        :ok

      {:error, reason} ->
        Logger.warning("[ankusa] route publish failed: #{inspect(reason)}")
    end
  end

  # ── sync ────────────────────────────────────────────────────────────────────

  # The tick and every broadcast take the same path: read the version, and reload
  # if it is not the one this node holds — `!=`, so a Redis that was flushed or
  # restored to an older state is followed down as well as up. A reload that
  # fails keeps the mirror that is serving traffic.
  defp sync(state) do
    case stored_version(state) do
      {:ok, version} when version != state.version -> reload(state)
      {:ok, _same} -> state
      {:error, reason} -> warn_unavailable(reason, state)
    end
  end

  defp reload(state) do
    case fetch_table(state) do
      {:ok, state} -> Snapshot.publish(state, {:reloaded, nil})
      {:error, reason} -> warn_unavailable(reason, state)
    end
  end

  # ── redis ───────────────────────────────────────────────────────────────────

  defp eval(state, script, keys, args) do
    command(state, [
      "EVAL",
      script,
      Integer.to_string(length(keys)) | keys ++ Enum.map(args, &to_string/1)
    ])
  end

  # Every Redis failure — a disconnected client, a wrong-type key, a timeout —
  # arrives here the same way and is reported the same way.
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

  # ── keys ────────────────────────────────────────────────────────────────────

  defp encode(route), do: JSON.encode!(Route.to_json(route))

  defp routes_key(state), do: state.namespace <> ":routes"
  defp ip_rules_key(state), do: state.namespace <> ":ip_rules"
  defp version_key(state), do: state.namespace <> ":version"

  defp server(instance), do: Ankusa.via(instance, :routes_store)
end
