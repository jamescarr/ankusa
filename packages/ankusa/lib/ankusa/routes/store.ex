defmodule Ankusa.Routes.Store do
  @moduledoc """
  Where route definitions live. This module is both the **behaviour** every
  store implements and the instance-scoped **facade** the context calls, the
  same split as `Ankusa.WAL` and `Ankusa.SourceStore`: the facade resolves
  `config.routes.store`'s `{module, opts}` and delegates.

  ## Two stores

    * `Ankusa.Routes.Store.ETS` — the default. Definitions are node-local and
      vanish on restart; `config.routes.seed` is how a standalone deployment
      gets a durable starting point.
    * `Ankusa.Routes.Store.Redis` (in `ankusa_redis`) — definitions live in
      Redis and a version counter plus pub/sub invalidates every node. The
      guard still reads the in-memory snapshot, never Redis.

  ## Definitions, never evictions

  A store holds definitions, so `insert/2` is capped by
  `config.routes.max_routes` and returns `{:error, :too_many_routes}` past it —
  nothing is silently evicted. An evicted definition would turn into a rejected
  webhook, and a rejected webhook is an operator's outage, not a cache miss.

  `{:error, :store_unavailable}` is the transient failure (Redis is down); the
  in-memory store never returns it.
  """

  alias Ankusa.Routes.Route

  @type error ::
          :too_many_routes
          | :not_found
          | :store_unavailable

  @callback start_link(keyword()) :: GenServer.on_start()

  @callback snapshot(atom()) :: map()

  @callback get(atom(), String.t()) :: {:ok, Route.t()} | :error

  @callback insert(atom(), Route.t()) :: :ok | {:error, error()}

  @callback replace(atom(), Route.t()) :: :ok | {:error, error()}

  @callback delete(atom(), String.t()) :: :ok | {:error, error()}

  @callback put_ip_rules(atom(), map()) :: :ok | {:error, error()}

  # ── facade ──────────────────────────────────────────────────────────────────

  @spec snapshot(atom()) :: map()
  def snapshot(instance), do: mod(instance).snapshot(instance)

  @spec get(atom(), String.t()) :: {:ok, Route.t()} | :error
  def get(instance, id), do: mod(instance).get(instance, id)

  @spec insert(atom(), Route.t()) :: :ok | {:error, error()}
  def insert(instance, %Route{} = route), do: mod(instance).insert(instance, route)

  @spec replace(atom(), Route.t()) :: :ok | {:error, error()}
  def replace(instance, %Route{} = route), do: mod(instance).replace(instance, route)

  @spec delete(atom(), String.t()) :: :ok | {:error, error()}
  def delete(instance, id), do: mod(instance).delete(instance, id)

  @spec put_ip_rules(atom(), map()) :: :ok | {:error, error()}
  def put_ip_rules(instance, ip_rules), do: mod(instance).put_ip_rules(instance, ip_rules)

  defp mod(instance) do
    %Ankusa.Config{routes: %{store: {mod, _opts}}} = Ankusa.config(instance)
    mod
  end
end
