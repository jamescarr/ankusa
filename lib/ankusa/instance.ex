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

    {wal_mod, _} = config.wal
    opts = [instance: config.instance, config: config]

    children =
      [{wal_mod, opts}] ++
        edge_children(config, opts) ++
        dispatch_children(config, opts) ++
        storage_children(config, opts)

    Supervisor.init(children, strategy: :one_for_one)
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

  defp dispatch_children(config, opts) do
    if Config.role?(config, :dispatch), do: [{Ankusa.Dispatch.Pipeline, opts}], else: []
  end

  defp storage_children(config, opts) do
    if Config.role?(config, :storage), do: [{Ankusa.Storage.Compactor, opts}], else: []
  end
end
