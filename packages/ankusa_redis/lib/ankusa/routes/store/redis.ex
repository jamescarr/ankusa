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
  | `<namespace>` | pub/sub channel | the new version, as a string |

  Two nodes share definitions **only if they are configured with the same
  namespace**; the instance-scoped default means two instances in one VM (tests,
  a claim-check node beside an edge node) never collide.

  ## Invalidation

  Every mutation runs its Redis commands and `INCR <namespace>:version` in one
  pipeline, then publishes the new version. A node that sees a version greater
  than the one it holds reloads the definitions and republishes its snapshot —
  no delete pass, and no window where a stale definition is enforced. The
  decision cache (`Ankusa.Routes.Cache`) is keyed by that version, so a reload
  invalidates every cached decision at once.

  A missed broadcast is caught by a periodic tick (`routes.store` option
  `tick_ms`, default 30s) that reads the version and reloads if it is newer.
  That is why the tick interval is the *worst case* staleness for a node that
  was disconnected while a route was changed.

  A publish failure is not treated as a write failure: the definitions are
  already durable in Redis and this node's mirror is already updated, so the
  only cost is that other nodes wait for their tick.

  ## Failing loudly

  The store refuses to boot against a Redis it cannot reach — `sync_connect` on
  the connection, and a version read in the state process's `init/1` — because
  the alternative is an edge that quietly denies every webhook. Once running, a
  Redis error on a write is `{:error, :store_unavailable}` (the admin API reports
  it as a `503`, and the connection reconnects on its own); reads keep being
  served from the in-memory snapshot, so a Redis outage does not start rejecting
  traffic. Seeding happens only on a namespace's first boot, so a route deleted
  through the API is never resurrected by a restart.
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

    Supervisor.init(children, strategy: :one_for_one)
  end

  # The store's address is the state process, so the facade calls it exactly as
  # it calls the ETS store.
  @impl Ankusa.Routes.Store
  defdelegate insert(instance, route), to: State

  @impl Ankusa.Routes.Store
  defdelegate replace(instance, route), to: State

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

  @doc false
  @impl Ankusa.Routes.Store
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: server(opts[:instance]))
  end

  @impl Ankusa.Routes.Store
  def insert(instance, %Route{} = route), do: GenServer.call(server(instance), {:insert, route})

  @impl Ankusa.Routes.Store
  def replace(instance, %Route{} = route), do: GenServer.call(server(instance), {:replace, route})

  @impl Ankusa.Routes.Store
  def delete(instance, id), do: GenServer.call(server(instance), {:delete, id})

  @impl Ankusa.Routes.Store
  def put_ip_rules(instance, rules), do: GenServer.call(server(instance), {:put_ip_rules, rules})

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

    case bootstrap(state, config.routes) do
      {:ok, state} -> {:ok, state, {:continue, :subscribe}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_continue(:subscribe, state) do
    case Redix.PubSub.subscribe(state.pubsub, state.namespace, self()) do
      {:ok, _ref} ->
        # The first tick is scheduled here rather than in init/1 so a crash in
        # subscribe does not leave a stray timer behind.
        Process.send_after(self(), :tick, state.tick_ms)
        {:noreply, state}

      {:error, reason} ->
        {:stop, {:pubsub_subscribe_failed, reason}, state}
    end
  end

  @impl true
  def handle_call({:insert, route}, _from, state) do
    # The cap is checked in Redis, not in the mirror: every node shares the same
    # hash, so a node with a stale mirror must not be able to exceed it.
    case command(state, ["HLEN", routes_key(state)]) do
      {:ok, size} when size >= state.max_routes ->
        {:reply, {:error, :too_many_routes}, state}

      {:ok, _size} ->
        write(
          state,
          [["HSET", routes_key(state), route.id, encode(route)]],
          :insert,
          route.id,
          fn s ->
            %{s | routes: Map.put(s.routes, route.id, route)}
          end
        )

      {:error, reason} ->
        {:reply, {:error, :store_unavailable}, warn_unavailable(reason, state)}
    end
  end

  def handle_call({:replace, route}, _from, state) do
    write(
      state,
      [["HSET", routes_key(state), route.id, encode(route)]],
      :replace,
      route.id,
      fn s ->
        %{s | routes: Map.put(s.routes, route.id, route)}
      end
    )
  end

  def handle_call({:delete, id}, _from, state) do
    case command(state, ["HDEL", routes_key(state), id]) do
      {:ok, 0} ->
        {:reply, {:error, :not_found}, state}

      {:ok, _deleted} ->
        write(state, [], :delete, id, fn s -> %{s | routes: Map.delete(s.routes, id)} end)

      {:error, reason} ->
        {:reply, {:error, :store_unavailable}, warn_unavailable(reason, state)}
    end
  end

  def handle_call({:put_ip_rules, ip_rules}, _from, state) do
    write(
      state,
      [["SET", ip_rules_key(state), JSON.encode!(Route.rules_json(ip_rules))]],
      :ip_rules,
      nil,
      fn s -> %{s | ip_rules: ip_rules} end
    )
  end

  @impl true
  def handle_info(:tick, state) do
    state =
      case stored_version(state) do
        {:ok, version} when version > state.version -> reload(state)
        {:ok, _same_or_older} -> state
        {:error, reason} -> warn_unavailable(reason, state)
      end

    Process.send_after(self(), :tick, state.tick_ms)
    {:noreply, state}
  end

  # `Redix.PubSub` messages are a five-element tuple:
  # `{:redix_pubsub, pid, subscription_ref, type, properties}`.
  def handle_info({:redix_pubsub, _pid, _ref, :message, %{payload: payload}}, state) do
    case Integer.parse(payload) do
      {version, ""} when version > state.version -> {:noreply, reload(state)}
      _stale_or_unparsable -> {:noreply, state}
    end
  end

  def handle_info({:redix_pubsub, _pid, _ref, :disconnected, properties}, state) do
    # The pub/sub client reconnects and re-subscribes on its own; anything
    # published while we were away is caught by the tick.
    Logger.debug("[ankusa] route pub/sub disconnected: #{inspect(properties)}")
    {:noreply, state}
  end

  def handle_info({:redix_pubsub, _pid, _ref, _type, _properties}, state), do: {:noreply, state}

  # ── boot ────────────────────────────────────────────────────────────────────

  # First boot of a namespace: write the config seed. Later boots read what is
  # there, so a route deleted through the API is never resurrected.
  defp bootstrap(state, routes_config) do
    case command(state, ["EXISTS", version_key(state)]) do
      {:ok, 0} -> seed(state, routes_config)
      {:ok, _exists} -> load(state)
      {:error, reason} -> {:error, {:redis_unavailable, reason}}
    end
  end

  defp seed(state, routes_config) do
    case Snapshot.initial_table(routes_config) do
      {:ok, %{routes: routes, ip_rules: rules}} ->
        commands =
          Enum.map(routes, fn {_id, route} ->
            ["HSET", routes_key(state), route.id, encode(route)]
          end) ++
            [
              ["SET", ip_rules_key(state), JSON.encode!(Route.rules_json(rules))],
              ["SET", version_key(state), "1"]
            ]

        case Redix.pipeline(state.conn, commands) do
          {:ok, _results} ->
            state = %{state | version: 1, routes: routes, ip_rules: rules}
            Snapshot.publish(state)
            {:ok, state}

          {:error, reason} ->
            {:error, {:redis_unavailable, reason}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ── loading ─────────────────────────────────────────────────────────────────

  defp load(state) do
    with {:ok, version} <- stored_version(state),
         {:ok, raw_routes} <- command(state, ["HGETALL", routes_key(state)]),
         {:ok, raw_rules} <- command(state, ["GET", ip_rules_key(state)]),
         {:ok, routes} <- decode_routes(raw_routes),
         {:ok, rules} <- decode_rules(raw_rules) do
      state = %{state | version: version, routes: routes, ip_rules: rules}
      Snapshot.publish(state)
      {:ok, state}
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

  # `INCR` arrives as an integer (Redix decodes RESP integers) and `GET` as a
  # string, so both shapes are accepted here.
  defp parse_version(version) when is_integer(version), do: {:ok, version}

  defp parse_version(raw) when is_binary(raw) do
    case Integer.parse(raw) do
      {version, ""} -> {:ok, version}
      _not_an_integer -> {:error, {:invalid_version, raw}}
    end
  end

  defp parse_version(raw), do: {:error, {:invalid_version, raw}}

  defp warn_unavailable(reason, state) do
    Logger.warning("[ankusa] route store is unavailable: #{inspect(reason)}")
    state
  end

  # ── writes ──────────────────────────────────────────────────────────────────

  # One pipeline for the mutation *and* the version bump, so no reader ever sees
  # a definition whose version has not moved yet. The local mirror is only
  # updated after Redis has accepted the write: a failed write must not make this
  # node behave as if it had succeeded.
  defp write(state, commands, action, route_id, update) do
    case Redix.pipeline(state.conn, commands ++ [["INCR", version_key(state)]]) do
      {:ok, results} ->
        case parse_version(List.last(results)) do
          {:ok, version} ->
            broadcast(state, version)

            state =
              state
              |> update.()
              |> Map.put(:version, version)
              |> Snapshot.publish({action, route_id})

            {:reply, :ok, state}

          {:error, reason} ->
            {:reply, {:error, :store_unavailable}, warn_unavailable(reason, state)}
        end

      {:error, reason} ->
        {:reply, {:error, :store_unavailable}, warn_unavailable(reason, state)}
    end
  end

  # Best effort: the definitions are durable and this node is already current, so
  # a failed publish costs other nodes at most one tick.
  defp broadcast(state, version) do
    case Redix.command(state.conn, ["PUBLISH", state.namespace, Integer.to_string(version)]) do
      {:ok, _receivers} -> :ok
      {:error, reason} -> Logger.warning("[ankusa] route publish failed: #{inspect(reason)}")
    end
  end

  # ── reload ──────────────────────────────────────────────────────────────────

  # The tick and the broadcast take the same path: read the version, and reload
  # only if it moved. A reload that fails keeps the mirror that is serving
  # traffic.
  defp reload(state) do
    case load(state) do
      {:ok, state} -> Snapshot.publish(state, {:reloaded, nil})
      {:error, reason} -> warn_unavailable(reason, state)
    end
  end

  # ── keys ────────────────────────────────────────────────────────────────────

  # Every Redis failure — a disconnected client, a wrong-type key, a timeout —
  # arrives here the same way and is reported the same way.
  defp command(state, command) do
    case Redix.command(state.conn, command) do
      {:ok, value} -> {:ok, value}
      {:error, reason} -> {:error, {:redis_unavailable, reason}}
    end
  end

  defp encode(route), do: JSON.encode!(Route.to_json(route))

  defp routes_key(state), do: state.namespace <> ":routes"
  defp ip_rules_key(state), do: state.namespace <> ":ip_rules"
  defp version_key(state), do: state.namespace <> ":version"

  defp server(instance), do: Ankusa.via(instance, :routes_store)
end
