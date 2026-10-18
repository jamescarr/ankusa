defmodule Ankusa.Routes.Store do
  @moduledoc """
  Where route definitions live. This module is both the **behaviour** every
  store implements and the instance-scoped **facade** the context calls, the
  same split as `Ankusa.SourceStore`: the facade resolves
  `config.routes.store`'s `{module, opts}` and delegates.

  ## Two stores

    * `Ankusa.Routes.Store.ETS` — the default. Definitions are node-local and
      vanish on restart; `config.routes.seed` is how a standalone deployment
      gets a durable starting point, and it is applied on every boot.
    * `Ankusa.Routes.Store.Redis` (in `ankusa_redis`) — definitions live in
      Redis and a version counter plus pub/sub invalidates every node. The
      guard still reads the in-memory snapshot, never Redis.

  ## Definitions, never evictions

  A store holds definitions, so `insert/3` is capped by
  `config.routes.max_routes` and returns `{:error, :too_many_routes}` past it —
  nothing is silently evicted. An evicted definition would turn into a rejected
  webhook, and a rejected webhook is an operator's outage, not a cache miss.

  ## Writes are version-checked

  `Ankusa.Routes` validates a write — a free id, no colliding enabled route —
  against the snapshot it just read, so a store may only apply it if that
  snapshot is still current. `insert/3` and `replace/3` therefore take the
  `version` of the snapshot the caller validated against, and answer
  `{:error, :stale}`, having changed nothing, when the table has moved on. Before
  answering `:stale` a store MUST have published the newer snapshot, so the
  caller's retry validates against what is really there.

  A store shared by several nodes checks that version in the shared store,
  atomically with the write, not against its own copy: a node whose copy lags
  would otherwise happily validate against a table another node already changed,
  and two nodes could both create the same id.

  `delete/2` and `put_ip_rules/2` carry no cross-route validation, so they take
  no version: a delete answers `{:error, :not_found}` from the store's own
  authoritative state, and replacing the global rules is a whole-value write.

  `{:error, :store_unavailable}` is the transient failure — Redis is down, or the
  store process did not answer, which the facade reports the same way. The
  in-memory store never returns it itself.
  """

  require Logger

  alias Ankusa.Routes.Route

  @type error ::
          :too_many_routes
          | :not_found
          | :stale
          | :store_unavailable

  @doc """
  Called by `Ankusa.Instance` with `[instance: instance]`; read the store's own
  options from `Ankusa.config(instance).routes.store`.
  """
  @callback start_link(keyword()) :: GenServer.on_start()

  @callback insert(atom(), Route.t(), pos_integer()) :: :ok | {:error, error()}

  @callback replace(atom(), Route.t(), pos_integer()) :: :ok | {:error, error()}

  @callback delete(atom(), String.t()) :: :ok | {:error, error()}

  @callback put_ip_rules(atom(), map()) :: :ok | {:error, error()}

  # ── facade ──────────────────────────────────────────────────────────────────

  @spec insert(atom(), Route.t(), pos_integer()) :: :ok | {:error, error()}
  def insert(instance, %Route{} = route, version),
    do: call(instance, :insert, [instance, route, version])

  @spec replace(atom(), Route.t(), pos_integer()) :: :ok | {:error, error()}
  def replace(instance, %Route{} = route, version),
    do: call(instance, :replace, [instance, route, version])

  @spec delete(atom(), String.t()) :: :ok | {:error, error()}
  def delete(instance, id), do: call(instance, :delete, [instance, id])

  @spec put_ip_rules(atom(), map()) :: :ok | {:error, error()}
  def put_ip_rules(instance, ip_rules), do: call(instance, :put_ip_rules, [instance, ip_rules])

  # A store call that exits — the process is not running, or it did not answer in
  # time — is the transient store error, not a crash of whoever asked.
  defp call(instance, function, args) do
    apply(mod(instance), function, args)
  catch
    :exit, reason ->
      Logger.warning("[ankusa] route store #{function} failed: #{inspect(reason)}")
      {:error, :store_unavailable}
  end

  defp mod(instance) do
    %Ankusa.Config{routes: %{store: {mod, _opts}}} = Ankusa.config(instance)
    mod
  end
end
