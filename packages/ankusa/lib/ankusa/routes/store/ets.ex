defmodule Ankusa.Routes.Store.ETS do
  @moduledoc """
  The default route store: definitions in this node's memory, with a hard cap
  and a config seed.

  `config.routes.seed` loads at boot, which is what makes a standalone (no
  Redis) deployment survivable across restarts: a route an operator added
  *through the API* is gone on restart, but the seed is not. The process owns
  the instance's snapshot table (`Ankusa.Routes.Snapshot`), and every mutation
  writes the rows it changes there, so the guard sees the change on the very
  next request — no TTL wait, no polling.

  Seeding happens **only at boot**, into an empty store — on every *instance*
  boot: a seed route deleted through the API returns at the next restart
  unless it is also removed from `config.routes.seed`. (Only the Redis store
  seeds once per namespace.) A store process that crashes and is restarted
  inside a running instance does not seed again: it resumes from the snapshot
  table it left, API-created routes included.

  The process registers under `Ankusa.via(instance, :routes_store)`; there is
  no global name, so two instances in one VM never collide.
  """

  use GenServer

  @behaviour Ankusa.Routes.Store

  alias Ankusa.Routes.{Route, Snapshot}

  @impl Ankusa.Routes.Store
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: server(opts))

  @impl Ankusa.Routes.Store
  def insert(instance, %Route{} = route, version),
    do: GenServer.call(server_name(instance), {:insert, route, version})

  @impl Ankusa.Routes.Store
  def replace(instance, %Route{} = route, version),
    do: GenServer.call(server_name(instance), {:replace, route, version})

  @impl Ankusa.Routes.Store
  def delete(instance, id), do: GenServer.call(server_name(instance), {:delete, id})

  @impl Ankusa.Routes.Store
  def put_ip_rules(instance, ip_rules),
    do: GenServer.call(server_name(instance), {:put_ip_rules, ip_rules})

  # ── GenServer ───────────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    %{routes: routes_config} = Keyword.fetch!(opts, :config)

    case Snapshot.adopt(instance) do
      {:ok, %{routes: routes, ip_rules: ip_rules, version: version}} ->
        # A restarted store: the table a crashed one left already is the state
        # (API-created routes included), so nothing is seeded or published.
        {:ok,
         %{
           instance: instance,
           max_routes: routes_config.max_routes,
           routes: routes,
           ip_rules: ip_rules,
           version: version
         }}

      :none ->
        case Snapshot.initial_table(routes_config) do
          {:ok, %{routes: routes, ip_rules: ip_rules}} ->
            state = %{
              instance: instance,
              max_routes: routes_config.max_routes,
              routes: routes,
              ip_rules: ip_rules,
              version: 1
            }

            :ok = Snapshot.publish(state)
            {:ok, state}

          {:error, reason} ->
            {:stop, reason}
        end
    end
  end

  @impl true
  def handle_call({operation, _route, version}, _from, %{version: current} = state)
      when operation in [:insert, :replace] and version != current do
    # The writer validated against a table that has since changed. The snapshot
    # it re-reads is already the current one — a mutation publishes it in the same
    # call — so there is nothing to refresh, only to refuse.
    {:reply, {:error, :stale}, state}
  end

  def handle_call({:insert, route, _version}, _from, state) do
    # The cap is checked against this call's own state, so concurrent inserts
    # can't both fit in the last slot.
    if map_size(state.routes) >= state.max_routes do
      {:reply, {:error, :too_many_routes}, state}
    else
      {:reply, :ok,
       state
       |> put_route(route)
       |> bump_version()
       |> Snapshot.mutate({:put, route}, {:insert, route.id})}
    end
  end

  def handle_call({:replace, route, _version}, _from, state) do
    {:reply, :ok,
     state
     |> put_route(route)
     |> bump_version()
     |> Snapshot.mutate({:put, route}, {:replace, route.id})}
  end

  def handle_call({:delete, id}, _from, state) do
    case Map.fetch(state.routes, id) do
      :error ->
        {:reply, {:error, :not_found}, state}

      {:ok, _route} ->
        state = %{state | routes: Map.delete(state.routes, id)} |> bump_version()
        {:reply, :ok, Snapshot.mutate(state, {:delete, id}, {:delete, id})}
    end
  end

  def handle_call({:put_ip_rules, ip_rules}, _from, state) do
    state = %{state | ip_rules: ip_rules} |> bump_version()
    {:reply, :ok, Snapshot.mutate(state, :ip_rules, {:ip_rules, nil})}
  end

  @impl true
  def handle_info({:drop_generation, gen}, state) do
    Snapshot.drop_generation(state.instance, gen)
    {:noreply, state}
  end

  # ── state transitions ───────────────────────────────────────────────────────

  defp put_route(state, route), do: %{state | routes: Map.put(state.routes, route.id, route)}

  defp bump_version(state), do: %{state | version: state.version + 1}

  defp server(opts), do: server_name(Keyword.fetch!(opts, :instance))

  defp server_name(instance), do: Ankusa.via(instance, :routes_store)
end
