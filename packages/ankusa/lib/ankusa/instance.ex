defmodule Ankusa.Instance do
  @moduledoc """
  Supervises one instance of the framework from a `%Ankusa.Config{}`. Only the
  children for configured roles boot, so the same code runs any role set. The
  boundaries between components are durable state (this node's store), not
  function calls. The store is node-local, so every role that reads or writes
  hooks has to run on the node that holds it.

  ## Failure domains

  The tree is `:rest_for_one`, and what the instance does when a part of it
  fails depends on which part:

    * **The core** — the store, the source store and the edge subtree (routes,
      queue writer, quarantine, rate limiter, batchers, ingress listener) — is
      what acks hooks. A crash there restarts whatever depends on it: the edge
      restarts with the store it writes to, and everything started after it
      restarts with the edge. A core that keeps crashing exhausts this
      supervisor's budget and the instance stops, for its parent to restart.
    * **Every other domain** — dispatch, archive/storage, store backup,
      lifecycle events, metrics, and the admin, route-admin and claim-check
      listeners — runs under `Ankusa.Instance.Isolated`. It has its own
      restart budget, and when that is exhausted it is restarted later with
      backoff instead of taking the instance down. A broken sink, a port
      someone else took or a full object store stops that domain, not the edge
      acking hooks; the hooks wait in the store and are dispatched when the
      domain returns.
    * **The registry** — a restart of `Ankusa.Registry` forgets every name an
      instance process registered. `Ankusa.Instance.RegistryWatch` notices and
      stops the instance, so whatever supervises it starts it again, every
      process registered anew.
  """

  use Supervisor

  require Logger

  alias Ankusa.Config
  alias Ankusa.Instance.Isolated

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
    Ankusa.Verifier.validate_config!(config)
    Ankusa.Verifier.warn_unverified_shared(config)
    Ankusa.Routes.validate_config!(config)
    Ankusa.Queue.validate_config!(config)
    Ankusa.Lifecycle.validate_config!(config)
    Ankusa.Edge.RateLimiter.validate_config!(config)
    Ankusa.Edge.Quarantine.validate_config!(config)
    Ankusa.Dispatch.Pipeline.validate_config!(config)
    Ankusa.Store.Backup.validate_config!(config)
    opts = [instance: config.instance, config: config]

    children =
      [{Ankusa.Instance.RegistryWatch, config.instance}] ++
        metrics_children(config, opts) ++
        store_children(config, opts) ++
        backup_children(config, opts) ++
        source_store_children(config, opts) ++
        lifecycle_children(config, opts) ++
        edge_children(config, opts) ++
        routes_admin_children(config) ++
        dispatch_children(config, opts) ++
        storage_children(config, opts) ++
        claim_check_children(config) ++
        admin_children(config)

    # `:any_significant`: the registry watcher is the only significant child.
    # Its exit takes this supervisor down, and the instance's parent restarts
    # it.
    Supervisor.init(children, strategy: :rest_for_one, auto_shutdown: :any_significant)
  end

  # One optional subtree behind its own restart budget: see
  # `Ankusa.Instance.Isolated`.
  defp isolated(config, domain, children) do
    [{Isolated, instance: config.instance, domain: domain, children: children}]
  end

  # The admin API's Prometheus reporter, first of all: it attaches its handlers
  # synchronously (`start_async: false`), so the events every later child emits
  # while starting — boot-time dispatch of the stored backlog, ingest the edge
  # accepts before the rest of the tree is up — are counted. The gauge poller
  # beside it samples state (queue depth, pen, disk) on its own interval.
  defp metrics_children(config, opts) do
    if config.admin.enabled,
      do: isolated(config, :metrics, [{Ankusa.Metrics, opts}, {Ankusa.Metrics.Gauges, opts}]),
      else: []
  end

  # The node's local store. Roles that read or write hooks, deliveries or the
  # archive need it; a writable source store needs it too, because it is where
  # the sources live. It must start before every child that reads from it: the
  # source store, the edge, dispatch and storage.
  defp store_children(config, opts) do
    if store?(config), do: [{Ankusa.Store, opts}], else: []
  end

  # The store's backup uploader, right behind the store: under `:rest_for_one`
  # a store restart restarts it too. An unreachable object store fails
  # backups, never the store or the edge.
  defp backup_children(%Config{backup: %{enabled: true}} = config, opts) do
    if store?(config), do: isolated(config, :backup, [{Ankusa.Store.Backup, opts}]), else: []
  end

  defp backup_children(_config, _opts), do: []

  @doc """
  Whether this instance runs the node-local store (`Ankusa.Store`): any of the
  `:edge`, `:dispatch` or `:storage` roles, or a persistent source store.
  """
  @spec store?(Config.t()) :: boolean()
  def store?(%Config{} = config) do
    Enum.any?([:edge, :dispatch, :storage], &Config.role?(config, &1)) or
      match?({Ankusa.SourceStore.Persistent, _}, config.source_store)
  end

  # The one process that assigns seqs and commits hooks, in the edge subtree
  # (only an `:edge` node writes hooks). Only under `wal: :disk`: under
  # `wal: :none` ingest acks on a sink's confirm and nothing is committed.
  defp writer_children(%Config{wal: :disk}, opts), do: [{Ankusa.Queue.Writer, opts}]
  defp writer_children(_config, _opts), do: []

  # A writable store (e.g. `Ankusa.SourceStore.Persistent`) must be up before the
  # edge accepts a request, since every ingest reads through it. A read-only
  # store is config-only and has no process: `function_exported?/1` on a module
  # that may not be loaded yet needs `Code.ensure_loaded/1` first.
  defp source_store_children(config, _opts) do
    {store_mod, _store_opts} = config.source_store

    if Code.ensure_loaded?(store_mod) and function_exported?(store_mod, :start_link, 1) do
      [{store_mod, config}]
    else
      []
    end
  end

  # Lifecycle events are published from memory (`Ankusa.Lifecycle.Publisher`),
  # on every node whatever its roles: the admin API that makes the changes runs
  # on any node. It starts before every listener that can make one.
  defp lifecycle_children(%Config{lifecycle: %{sinks: []}}, _opts), do: []

  defp lifecycle_children(config, opts),
    do: isolated(config, :lifecycle, [{Ankusa.Lifecycle.Publisher, opts}])

  # Everything that stands between a request and the ack, in one subtree
  # (`:rest_for_one`, so each child restarts with what it depends on):
  # a crash anywhere in it restarts the rest of it, never the store.
  defp edge_children(config, opts) do
    if Config.role?(config, :edge) do
      # The route definitions and the decision cache, before the ingress
      # listener accepts a request: the guard fails closed, so a store that
      # isn't up yet would reject every hook.
      edge =
        routes_children(config, opts) ++
          writer_children(config, opts) ++
          [
            {Ankusa.Edge.Quarantine, opts},
            # Before the batchers and the listener, so the tables exist before
            # the first request can reach `Ingest`.
            {Ankusa.Edge.RateLimiter, opts}
          ] ++
          batcher_children(config, opts) ++
          [
            {Bandit,
             plug: {Ankusa.Edge.Router, [instance: config.instance]},
             scheme: :http,
             port: config.port}
          ]

      [
        %{
          id: {:edge, config.instance},
          start:
            {Supervisor, :start_link,
             [edge, [strategy: :rest_for_one, name: Ankusa.via(config.instance, :edge)]]},
          type: :supervisor
        }
      ]
    else
      []
    end
  end

  # The batcher exists to group WAL commits. `wal: :none` has no log to group:
  # ingest publishes to the sinks in the request instead.
  defp batcher_children(%Config{wal: :none}, _opts), do: []
  defp batcher_children(_config, opts), do: [{Ankusa.Edge.BatcherSupervisor, opts}]

  defp routes_children(config, opts) do
    if routes?(config) do
      {store_mod, _store_opts} = config.routes.store
      cache_name = Ankusa.via(config.instance, :routes_cache)

      [
        # Owns the snapshot table across store restarts (see TableOwner).
        {Ankusa.Routes.TableOwner, opts},
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
        "[ankusa] route management API on " <>
          "#{listen_addr(config.routes.admin, "routes.admin.ip")} is unauthenticated; " <>
          "do not expose it publicly, front it with your own proxy or network policy"
      )

      isolated(config, :routes_admin, [
        bandit_child(
          Ankusa.Routes.Router,
          config.instance,
          config.routes.admin,
          "routes.admin.ip"
        )
      ])
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
    #
    # `gc_memory_check_interval`: the adapter enforces `max_size` in this
    # periodic check, not on each write, and its default is ten seconds. A
    # scanner adds a cache entry per new path, so check every second.
    [
      max_size: max_size,
      gc_interval: gc_interval,
      gc_memory_check_interval: :timer.seconds(1),
      telemetry: false
    ]
  end

  defp claim_check_children(config) do
    if Config.role?(config, :claim_check) do
      Logger.warning(
        "[ankusa] claim-check API on #{listen_addr(config.claim_check, "claim_check.ip")} " <>
          "performs no authentication; " <>
          "front it with your own proxy, mesh, or network policy"
      )

      isolated(config, :claim_check, [
        bandit_child(
          Ankusa.ClaimCheck.Router,
          config.instance,
          config.claim_check,
          "claim_check.ip"
        )
      ])
    else
      []
    end
  end

  defp dispatch_children(config, opts) do
    if Config.role?(config, :dispatch),
      do:
        isolated(config, :dispatch, [
          {Ankusa.Dispatch.Pipeline, opts},
          {Ankusa.Dispatch.Replayer, opts}
        ]),
      else: []
  end

  defp storage_children(config, opts) do
    if Config.role?(config, :storage) do
      isolated(
        config,
        :storage,
        [{Ankusa.Storage.Compactor, opts}] ++ sweeper_children(config, opts)
      )
    else
      []
    end
  end

  defp sweeper_children(config, opts) do
    if config.claim_check.retention_days, do: [{Ankusa.ClaimCheck.Sweeper, opts}], else: []
  end

  # The operator API, last, so it only listens once everything it reports on
  # has started.
  defp admin_children(config) do
    if config.admin.enabled do
      Logger.warning(
        "[ankusa] admin API on #{listen_addr(config.admin, "admin.ip")} is unauthenticated; " <>
          "do not expose it " <>
          "publicly, front it with your own proxy or network policy"
      )

      isolated(config, :admin, [
        bandit_child(Ankusa.Admin.Router, config.instance, config.admin, "admin.ip")
      ])
    else
      []
    end
  end

  defp bandit_child(plug_module, instance, %{port: port, ip: ip}, key) do
    Supervisor.child_spec(
      {Bandit,
       plug: {plug_module, [instance: instance]},
       scheme: :http,
       ip: Config.listen_ip!(ip, key),
       port: port},
      id: plug_module
    )
  end

  defp listen_addr(%{ip: ip, port: port}, key) do
    "#{:inet.ntoa(Config.listen_ip!(ip, key))}:#{port}"
  end
end
