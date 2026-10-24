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

    * **The core** — the claim stores' breaker table
      (`Ankusa.ClaimCheck.Stores`), the store, the source store and the edge
      subtree (routes, queue writer, quarantine, rate limiter, batchers,
      ingress listener) — is
      what acks hooks. A crash there restarts whatever depends on it: the edge
      restarts with the store it writes to, and everything started after it
      restarts with the edge. A core that keeps crashing exhausts this
      supervisor's budget and the instance stops, for its parent to restart.
    * **Every other domain** — dispatch, archive/storage, store backup,
      lifecycle events, metrics, and the admin, route-admin, claim-check and
      metrics listeners — runs under `Ankusa.Instance.Isolated`. It has its own
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
  alias Ankusa.Listener

  @spec start_link(Config.t() | atom()) :: Supervisor.on_start()
  def start_link(%Config{} = config) do
    Ankusa.put_config(config)
    start_link(config.instance)
  end

  def start_link(instance) when is_atom(instance) do
    Supervisor.start_link(__MODULE__, instance, name: Ankusa.via(instance, :instance))
  end

  @doc """
  The child spec for the instance `config` describes. Building it stores the
  config (`Ankusa.put_config/1`), so the parent's spec, its supervisor reports
  and `:sys.get_status/1` name only the instance, and every (re)start reads the
  config back. Build it only for an instance you are about to start: it
  replaces that name's stored config.
  """
  def child_spec(%Config{} = config) do
    Ankusa.put_config(config)

    %{
      id: {__MODULE__, config.instance},
      start: {__MODULE__, :start_link, [config.instance]},
      type: :supervisor
    }
  end

  @impl true
  def init(instance) when is_atom(instance) do
    # Every child reads the config back by name (`Ankusa.config/1`): a
    # supervisor prints its children's start arguments.
    config = Ankusa.config(instance)
    Ankusa.Dispatch.Pipeline.validate_config!(config)
    Ankusa.ClaimCheck.validate_config!(config)
    Ankusa.Verifier.validate_config!(config)
    Ankusa.Verifier.validate_shared!(config)
    Ankusa.Routes.validate_config!(config)
    Ankusa.Queue.validate_config!(config)
    Ankusa.Ordering.validate_config!(config)
    Ankusa.Lifecycle.validate_config!(config)
    Ankusa.Edge.RateLimiter.validate_config!(config)
    Ankusa.Edge.Quarantine.validate_config!(config)
    Ankusa.Store.Backup.validate_config!(config)
    Ankusa.BlobStore.validate_config!(config)
    Ankusa.HttpClient.Egress.validate_config!(config)
    Ankusa.Intake.validate_config!(config)
    if config.wal == :dual, do: Ankusa.Intake.Pending.create(instance)
    opts = [instance: config.instance]

    children =
      [{Ankusa.Instance.RegistryWatch, config.instance}] ++
        metrics_children(config, opts) ++
        auth_children(config) ++
        Ankusa.BlobStore.Pool.child_specs(config) ++
        claim_store_children(opts) ++
        intake_children(config, opts) ++
        store_children(config, opts) ++
        erase_children(config, opts) ++
        backup_children(config, opts) ++
        [{Ankusa.Sink.Http.Pool, opts}] ++
        source_store_children(config, opts) ++
        lifecycle_children(config, opts) ++
        edge_children(config, opts) ++
        routes_admin_children(config) ++
        dispatch_children(config, opts) ++
        storage_children(config, opts) ++
        claim_check_children(config, opts) ++
        admin_children(config) ++
        metrics_listener_children(config)

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

  # The signing keys of every control plane that runs `auth.mode: :jwt`
  # (`Ankusa.Admin.Auth.JWT.Keys`), before the listeners that read them. It
  # fetches a JWKS after it is up, so a slow identity provider never delays boot.
  defp auth_children(config) do
    if Enum.any?(control_plane_auth(config), &(&1.mode == :jwt)),
      do: isolated(config, :auth, [{Ankusa.Admin.Auth.JWT.Keys, [instance: config.instance]}]),
      else: []
  end

  defp control_plane_auth(config) do
    [config.admin.auth, config.routes.admin.auth, config.claim_check.auth]
  end

  # The Prometheus reporter behind `/metrics` (admin API or metrics listener),
  # first of all: it attaches its handlers
  # synchronously (`start_async: false`), so the events every later child emits
  # while starting — boot-time dispatch of the stored backlog, ingest the edge
  # accepts before the rest of the tree is up — are counted. The gauge poller
  # beside it samples state (queue depth, pen, disk) on its own interval.
  defp metrics_children(config, opts) do
    if config.admin.enabled or config.metrics.enabled,
      do: isolated(config, :metrics, [{Ankusa.Metrics, opts}, {Ankusa.Metrics.Gauges, opts}]),
      else: []
  end

  # The claim stores' write breakers (`Ankusa.ClaimCheck.Stores`): an ETS
  # table every claim writer shares — dispatch, the edge's direct-mode
  # publish — so it starts before them. One small process, in every role.
  defp claim_store_children(opts), do: [{Ankusa.ClaimCheck.Stores, opts}]

  # The node's local store. Roles that read or write hooks, deliveries or the
  # archive need it; a writable source store needs it too, because it is where
  # the sources live. It must start before every child that reads from it: the
  # source store, the edge, dispatch and storage.
  defp store_children(config, opts) do
    if store?(config), do: [{Ankusa.Store, opts}, {Ankusa.Audit, opts}], else: []
  end

  # Tenant erases (`Ankusa.Tenant`): right behind the store and the audit trail
  # they write to and ahead of every listener, because its start closes the
  # tenants whose erase a restart interrupted, before the edge can take one of
  # their hooks. Its own domain, so a bug in a job never restarts the edge.
  defp erase_children(config, opts) do
    if store?(config),
      do: isolated(config, :tenant_erase, [{Ankusa.Tenant.Eraser, opts}]),
      else: []
  end

  # The store's backup uploader, right behind the store: under `:rest_for_one`
  # a store restart restarts it too. An unreachable object store fails
  # backups, never the store or the edge.
  defp backup_children(%Config{backup: %{enabled: true}} = config, opts) do
    if store?(config), do: isolated(config, :backup, [{Ankusa.Store.Backup, opts}]), else: []
  end

  defp backup_children(_config, _opts), do: []

  @doc """
  Whether this instance runs the node-local store (`Ankusa.Store`) and the audit
  trail written into it (`Ankusa.Audit`): any of the `:edge`, `:dispatch` or
  `:storage` roles, the admin API (every change it makes is audited), or a
  source store that accepts writes.
  """
  @spec store?(Config.t()) :: boolean()
  def store?(%Config{} = config) do
    Enum.any?([:edge, :dispatch, :storage], &Config.role?(config, &1)) or config.admin.enabled or
      writable_source_store?(config)
  end

  defp writable_source_store?(%Config{source_store: {mod, _opts}}) do
    Code.ensure_loaded?(mod) and
      (function_exported?(mod, :put, 6) or function_exported?(mod, :put, 5))
  end

  # The one process that assigns seqs and commits hooks, in the edge subtree
  # (only an `:edge` node writes hooks). Under `wal: :disk` and `wal: :dual` (which
  # adds the intake log); under `wal: :none` ingest acks on a sink's confirm and
  # nothing is committed.
  defp writer_children(%Config{wal: wal}, opts) when wal in [:disk, :dual],
    do: [{Ankusa.Queue.Writer, opts}]

  defp writer_children(_config, _opts), do: []

  # `wal: :dual` (ADR 0002): the adapter's own processes (its client, never the
  # sink's) start before the store, so boot recovery can reach the log.
  defp intake_children(%Config{wal: :dual} = config, opts),
    do: [Ankusa.Intake.module(config).child_spec(opts)]

  defp intake_children(_config, _opts), do: []

  # Boot recovery (it replays the log into the store and republishes the local
  # tail; the node does not serve until it is done), the supervisor of the
  # writer's publish tasks, and the reconciler that completes carried hooks: all
  # before the writer, which uses them.
  defp intake_edge_children(%Config{wal: :dual} = config, opts) do
    [
      {Ankusa.Intake.Recovery, opts},
      {Task.Supervisor, name: Ankusa.via(config.instance, :intake_tasks)},
      {Ankusa.Intake.Reconciler, opts}
    ]
  end

  defp intake_edge_children(_config, _opts), do: []

  # Expires the dedupe keys the writer inserts (`Ankusa.Queue.DedupeSweeper`).
  defp dedupe_sweeper_children(%Config{wal: wal}, opts) when wal in [:disk, :dual],
    do: [{Ankusa.Queue.DedupeSweeper, opts}]

  defp dedupe_sweeper_children(_config, _opts), do: []

  # A writable store (e.g. `Ankusa.SourceStore.Persistent`) must be up before the
  # edge accepts a request, since every ingest reads through it. A read-only
  # store is config-only and has no process: `function_exported?/1` on a module
  # that may not be loaded yet needs `Code.ensure_loaded/1` first.
  defp source_store_children(config, opts) do
    {store_mod, _store_opts} = config.source_store

    if Code.ensure_loaded?(store_mod) and function_exported?(store_mod, :start_link, 1) do
      [{store_mod, opts}]
    else
      []
    end
  end

  # Lifecycle events are read from the audit trail (`Ankusa.Lifecycle.Publisher`
  # is the cursor reader over it), on every node that has the store the trail is
  # in: the admin API that makes the changes runs on any node, and a node with no
  # store audits, and so announces, nothing. It starts behind the store and the
  # source store, whose state it settles pending rows against.
  defp lifecycle_children(%Config{lifecycle: %{sinks: []}}, _opts), do: []

  defp lifecycle_children(config, opts) do
    if store?(config),
      do: isolated(config, :lifecycle, [{Ankusa.Lifecycle.Publisher, opts}]),
      else: []
  end

  # Everything that stands between a request and the ack, in one subtree
  # (`:rest_for_one`, so each child restarts with what it depends on):
  # a crash anywhere in it restarts the rest of it, never the store.
  defp edge_children(config, opts) do
    if Config.role?(config, :edge) do
      # The route definitions and the decision cache, before the ingress
      # listener accepts a request: the guard fails closed, so a store that
      # isn't up yet would reject every hook.
      # Last, so a crash of the sweeper restarts nothing that serves, and a
      # restart of the writer (which it calls) restarts the sweeper.
      edge =
        routes_children(config, opts) ++
          intake_edge_children(config, opts) ++
          writer_children(config, opts) ++
          fleet_children(config, opts) ++
          [
            {Ankusa.Edge.Quarantine, opts},
            # Before the batchers and the listener, so the tables exist before
            # the first request can reach `Ingest`.
            {Ankusa.Edge.RateLimiter, opts}
          ] ++
          batcher_children(config, opts) ++
          [
            {Bandit,
             Listener.bandit_options(config, config.listener.tls) ++
               [plug: {Ankusa.Edge.Router, [instance: config.instance]}, port: config.port]}
          ] ++
          dedupe_sweeper_children(config, opts)

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

  # The shared store behind `rate_limits.scope: :fleet` / `dedupe.scope: :fleet`
  # and the process that guards it (breaker, settle queue): before the
  # listener, so the tables exist before the first request reaches `Ingest`. A
  # store that cannot be reached at boot does not stop the node (see
  # `Ankusa.RateLimiter.Store`).
  defp fleet_children(%Config{fleet: %{store: {store, _opts}}} = config, opts) do
    if :fleet in [config.rate_limits.scope, config.dedupe.scope],
      do: [{store, opts}, {Ankusa.Edge.Fleet, opts}],
      else: []
  end

  defp fleet_children(_config, _opts), do: []

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
      warn_unauthenticated(
        config.routes.admin,
        "[ankusa] route management API on " <>
          "#{listen_addr(config.routes.admin, "routes.admin.ip")} has no authentication " <>
          "(routes.admin.auth.mode: none); set routes.admin.auth before exposing it beyond this host"
      )

      isolated(config, :routes_admin, [
        bandit_child(
          Ankusa.Routes.Router,
          config,
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

  # The gateway's read cache and reader start before its listener, in the same
  # subtree. `Isolated` restarts children one for one, so a reader crash
  # leaves the listener up: until it is back, reads go straight to the store.
  defp claim_check_children(config, opts) do
    if Config.role?(config, :claim_check) do
      warn_unauthenticated(
        config.claim_check,
        "[ankusa] claim-check API on #{listen_addr(config.claim_check, "claim_check.ip")} " <>
          "has no authentication (claim_check.auth.mode: none); " <>
          "set claim_check.auth before exposing it beyond this host"
      )

      cache =
        if config.claim_check.read_cache_bytes > 0,
          do: [{Ankusa.ClaimCheck.Cache, opts}],
          else: []

      isolated(
        config,
        :claim_check,
        cache ++
          [
            {Ankusa.ClaimCheck.Reader, opts},
            bandit_child(
              Ankusa.ClaimCheck.Router,
              config,
              config.claim_check,
              "claim_check.ip"
            )
          ]
      )
    else
      []
    end
  end

  defp dispatch_children(config, opts) do
    if Config.role?(config, :dispatch),
      do:
        isolated(
          config,
          :dispatch,
          [
            {Ankusa.Dispatch.Pipeline, opts},
            {Ankusa.Dispatch.Replayer, opts},
            {Ankusa.Queue.DlqReaper, opts}
          ] ++ if(Config.role?(config, :storage), do: [], else: sweeper_children(config, opts))
        ),
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

  # The claim sweeper: one per node, in the `:storage` subtree when the node has
  # that role, else in the `:dispatch` one (see `Ankusa.ClaimCheck.Sweeper.run?/1`).
  defp sweeper_children(config, opts) do
    if Ankusa.ClaimCheck.Sweeper.run?(config), do: [{Ankusa.ClaimCheck.Sweeper, opts}], else: []
  end

  # The operator API, last, so it only listens once everything it reports on
  # has started.
  defp admin_children(config) do
    if config.admin.enabled do
      warn_unauthenticated(
        config.admin,
        "[ankusa] admin API on #{listen_addr(config.admin, "admin.ip")} has no authentication " <>
          "(admin.auth.mode: none); set admin.auth before exposing it beyond this host"
      )

      isolated(config, :admin, [
        bandit_child(Ankusa.Admin.Router, config, config.admin, "admin.ip")
      ])
    else
      []
    end
  end

  defp bandit_child(plug_module, %Config{} = config, %{port: port, ip: ip, tls: tls}, key) do
    Supervisor.child_spec(
      {Bandit,
       Listener.bandit_options(config, tls) ++
         [
           plug: {plug_module, [instance: config.instance]},
           ip: Config.listen_ip!(ip, key),
           port: port
         ]},
      id: plug_module
    )
  end

  # The metrics listener, after the admin API: it only serves what the reporter
  # (started first, see `metrics_children/2`) has collected.
  defp metrics_listener_children(%Config{metrics: %{enabled: true} = metrics} = config) do
    isolated(config, :metrics_listener, [
      bandit_child(Ankusa.Metrics.Router, config, metrics, "metrics.ip")
    ])
  end

  defp metrics_listener_children(_config), do: []

  # A listener with no authentication of its own warns at boot. One that
  # authenticates (`auth.mode`) does not, and neither does one that demands a
  # client certificate from your CA.
  defp warn_unauthenticated(%{auth: %{mode: mode}}, _message) when mode != :none, do: :ok
  defp warn_unauthenticated(%{tls: %{verify: :verify_peer}}, _message), do: :ok
  defp warn_unauthenticated(_section, message), do: Logger.warning(message)

  defp listen_addr(%{ip: ip, port: port}, key) do
    "#{:inet.ntoa(Config.listen_ip!(ip, key))}:#{port}"
  end
end
