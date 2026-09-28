defmodule Ankusa.Routes.Store.ETS do
  @moduledoc """
  The default route store: definitions in this node's memory, with a hard cap
  and a config seed.

  `config.routes.seed` loads at boot, which is what makes a standalone (no
  Redis) deployment survivable across restarts: a route an operator added
  *through the API* is gone on restart, but the seed is not. Every mutation
  rebuilds the snapshot and republishes it to `:persistent_term`, so the guard
  sees the change on the very next request — no TTL wait, no polling.

  Seeding happens **only at boot** and only into an empty store, so a route
  deleted through the API stays deleted until the next restart.

  The process registers under `Ankusa.via(instance, :routes_store)`; there is
  no global name, so two instances in one VM never collide.
  """

  use GenServer

  @behaviour Ankusa.Routes.Store

  alias Ankusa.Routes.{Route, Snapshot}

  @impl Ankusa.Routes.Store
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: server(opts))

  @impl Ankusa.Routes.Store
  def snapshot(instance), do: Snapshot.get(instance)

  @impl Ankusa.Routes.Store
  def get(instance, id), do: GenServer.call(server_name(instance), {:get, id})

  @impl Ankusa.Routes.Store
  def insert(instance, %Route{} = route),
    do: GenServer.call(server_name(instance), {:insert, route})

  @impl Ankusa.Routes.Store
  def replace(instance, %Route{} = route),
    do: GenServer.call(server_name(instance), {:replace, route})

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

    with {:ok, routes} <- seeds(routes_config),
         {:ok, ip_rules} <- global_rules(routes_config) do
      state = %{
        instance: instance,
        max_routes: routes_config.max_routes,
        routes: routes,
        ip_rules: ip_rules,
        version: 1
      }

      Snapshot.put(instance, build(state))
      {:ok, state}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    case Map.fetch(state.routes, id) do
      {:ok, route} -> {:reply, {:ok, route}, state}
      :error -> {:reply, :error, state}
    end
  end

  def handle_call({:insert, route}, _from, state) do
    # The cap is checked against this call's own state, so concurrent inserts
    # can't both fit in the last slot.
    if map_size(state.routes) >= state.max_routes do
      {:reply, {:error, :too_many_routes}, state}
    else
      {:reply, :ok, state |> put_route(route) |> changed(:insert, route.id)}
    end
  end

  def handle_call({:replace, route}, _from, state) do
    {:reply, :ok, state |> put_route(route) |> changed(:replace, route.id)}
  end

  def handle_call({:delete, id}, _from, state) do
    case Map.fetch(state.routes, id) do
      :error ->
        {:reply, {:error, :not_found}, state}

      {:ok, _route} ->
        state = %{state | routes: Map.delete(state.routes, id)}
        {:reply, :ok, changed(state, :delete, id)}
    end
  end

  def handle_call({:put_ip_rules, ip_rules}, _from, state) do
    {:reply, :ok, %{state | ip_rules: ip_rules} |> changed(:ip_rules, nil)}
  end

  # ── state transitions ───────────────────────────────────────────────────────

  defp put_route(state, route), do: %{state | routes: Map.put(state.routes, route.id, route)}

  # One version bump, one snapshot rebuild, one event — a caller never observes
  # a half-applied change.
  defp changed(state, action, route_id) do
    state = %{state | version: state.version + 1}
    Snapshot.put(state.instance, build(state))

    Ankusa.Telemetry.emit([:routes, :changed], %{}, %{
      instance: state.instance,
      action: action,
      route_id: route_id,
      version: state.version
    })

    state
  end

  defp build(state), do: Snapshot.build(state.routes, state.ip_rules, state.version)

  # ── boot ────────────────────────────────────────────────────────────────────

  # `Ankusa.Routes.validate_config!/1` already rejected an unparseable seed at
  # boot, so a failure here is a store started without validation — stop rather
  # than run with a partial table.
  defp seeds(%{seed: seed, max_routes: max_routes}) do
    if length(seed) > max_routes do
      {:error, {:seed_too_large, length(seed), max_routes}}
    else
      seed
      |> Enum.reduce_while({:ok, %{}}, fn attrs, {:ok, acc} ->
        case Route.from_attrs(attrs) do
          {:ok, route} ->
            {:cont, {:ok, Map.put(acc, route.id, route)}}

          {:error, {:invalid, field, message}} ->
            {:halt, {:error, {:invalid_seed, field, message}}}
        end
      end)
      |> case do
        {:ok, routes} when map_size(routes) == length(seed) -> {:ok, routes}
        {:ok, routes} -> {:error, {:duplicate_seed_ids, map_size(routes), length(seed)}}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp global_rules(%{ip_rules: %{default: default, rules: rules}}) do
    case Route.parse_rules(rules) do
      {:ok, parsed} -> {:ok, %{default: default, rules: parsed}}
      {:error, message} -> {:error, {:invalid_ip_rules, message}}
    end
  end

  defp server(opts), do: server_name(Keyword.fetch!(opts, :instance))

  defp server_name(instance), do: Ankusa.via(instance, :routes_store)
end
