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
    opts = [instance: config.instance, config: config]

    children =
      metrics_children(config, opts) ++
        wal_children(config, opts) ++
        edge_children(config, opts) ++
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
  # credentials (e.g. a Postgres connection) — so it shouldn't open one.
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
