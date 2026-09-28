defmodule Ankusa.Instance do
  @moduledoc """
  Supervises one instance of the framework from a `%Ankusa.Config{}`. Only the
  children for configured roles boot, so the same code runs as one all-roles
  release on a laptop or as split edge/storage/dispatch fleets — the boundaries
  between components are durable state (the WAL), not function calls.

  Components hand work to each other through the WAL; killing the storage or
  dispatch tree never stops the edge from acking. There are no links across
  component boundaries.
  """

  use Supervisor

  require Logger

  alias Ankusa.Config

  @spec start_link(Config.t()) :: Supervisor.on_start()
  def start_link(%Config{} = config) do
    Supervisor.start_link(__MODULE__, config, name: Ankusa.via(config.instance, :instance))
  end

  def child_spec(%Config{} = config) do
    %{
      id: {__MODULE__, config.instance},
      start: {__MODULE__, :start_link, [config]},
      type: :supervisor
    }
  end

  @impl true
  def init(%Config{} = config) do
    # read-mostly config for every call site, no Application.get_env buried deep
    Ankusa.put_config(config)
    Ankusa.ClaimCheck.validate_config!(config)
    Ankusa.Routes.validate_config!(config)
    opts = [instance: config.instance, config: config]

    children =
      metrics_children(config, opts) ++
        wal_children(config, opts) ++
        routes_children(config, opts) ++
        edge_children(config, opts) ++
        routes_admin_children(config) ++
        dispatch_children(config, opts) ++
        storage_children(config, opts) ++
        claim_check_children(config, opts) ++
        admin_children(config, opts)

    Supervisor.init(children, strategy: :one_for_one)
  end

  # The admin API's Prometheus reporter, first of all: it attaches its handlers
  # synchronously (`start_async: false`), so the events every later child emits
  # while starting — boot-time dispatch of the WAL backlog, ingest the edge
  # accepts before the rest of the tree is up — are counted.
  defp metrics_children(config, opts) do
    if config.admin.enabled, do: [{Ankusa.Metrics, opts}], else: []
  end

  # The WAL only matters to roles that actually read or write it. A node
  # running only `:claim_check` needs blob-store credentials, never WAL
  # credentials — so it shouldn't open one.
  defp wal_children(config, opts) do
    if Enum.any?([:edge, :dispatch, :storage], &Config.role?(config, &1)) do
      {wal_mod, _} = config.wal
      [{wal_mod, opts}]
    else
      []
    end
  end

  defp edge_children(config, opts) do
    if Config.role?(config, :edge) do
      [
        {Ankusa.Edge.Quarantine, opts},
        {Ankusa.Edge.BatcherSupervisor, opts},
        {Bandit,
         plug: {Ankusa.Edge.Router, [instance: config.instance]}, scheme: :http, port: config.port}
      ]
    else
      []
    end
  end

  # The route definitions and the decision cache, before the ingress listener
  # accepts a request: the guard fails closed, so a store that isn't up yet
  # would reject every hook.
  defp routes_children(config, opts) do
    if routes?(config) do
      {store_mod, _store_opts} = config.routes.store
      cache_name = Ankusa.via(config.instance, :routes_cache)

      [
        {store_mod, opts},
        {Ankusa.Routes.Cache, [name: cache_name] ++ cache_opts(config)}
      ]
    else
      []
    end
  end

  # The route table is node-local, so a node that isn't the edge has nothing to
  # guard: the definitions it managed would never be read.
  defp routes?(config), do: config.routes.enabled and Config.role?(config, :edge)

  # The management API, after the ingress listener: it edits the definitions
  # that listener already enforces, so it must never be the last thing to come
  # up.
  defp routes_admin_children(config) do
    if routes?(config) do
      Logger.warning(
        "[ankusa] route management API on :#{config.routes.admin.port} is unauthenticated; " <>
          "do not expose it publicly, front it with your own proxy or network policy"
      )

      [
        Supervisor.child_spec(
          {Bandit,
           plug: {Ankusa.Routes.Router, [instance: config.instance]},
           scheme: :http,
           port: config.routes.admin.port},
          id: Ankusa.Routes.Router
        )
      ]
    else
      []
    end
  end

  # The Local adapter's `:gc_interval` is a generational sweep, not an expiry
  # timer: a generation older than the sweep interval can be dropped before its
  # entries' own `:ttl` is up, which is why `routes.cache.ttl_ms` must stay
  # under `gc_interval_ms` (enforced by `Ankusa.Routes.validate_config!/1`).
  defp cache_opts(config) do
    %{max_size: max_size, gc_interval_ms: gc_interval} = config.routes.cache

    # `telemetry: false`: no per-command Nebulex spans on the ingest hot path.
    # Our own [:ankusa, :routes, *] events are the observability surface.
    [max_size: max_size, gc_interval: gc_interval, telemetry: false]
  end

  defp claim_check_children(config, _opts) do
    if Config.role?(config, :claim_check) do
      Logger.warning(
        "[ankusa] claim-check API on :#{config.claim_check.port} performs no authentication; " <>
          "front it with your own proxy, mesh, or network policy"
      )

      [
        Supervisor.child_spec(
          {Bandit,
           plug: {Ankusa.ClaimCheck.Router, [instance: config.instance]},
           scheme: :http,
           port: config.claim_check.port},
          id: Ankusa.ClaimCheck.Router
        )
      ]
    else
      []
    end
  end

  defp dispatch_children(config, opts) do
    if Config.role?(config, :dispatch), do: [{Ankusa.Dispatch.Pipeline, opts}], else: []
  end

  defp storage_children(config, opts) do
    if Config.role?(config, :storage) do
      [{Ankusa.Storage.Compactor, opts}] ++ sweeper_children(config, opts)
    else
      []
    end
  end

  defp sweeper_children(config, opts) do
    if config.claim_check.retention_days, do: [{Ankusa.ClaimCheck.Sweeper, opts}], else: []
  end

  # The operator API, last, so it only listens once everything it reports on
  # has started.
  defp admin_children(config, _opts) do
    if config.admin.enabled do
      Logger.warning(
        "[ankusa] admin API on :#{config.admin.port} is unauthenticated; do not expose it " <>
          "publicly, front it with your own proxy or network policy"
      )

      [
        Supervisor.child_spec(
          {Bandit,
           plug: {Ankusa.Admin.Router, [instance: config.instance]},
           scheme: :http,
           port: config.admin.port},
          id: Ankusa.Admin.Router
        )
      ]
    else
      []
    end
  end
end
