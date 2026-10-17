defmodule ControlPanel.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      ControlPanelWeb.Telemetry,
      {Phoenix.PubSub, name: ControlPanel.PubSub},
      {Task.Supervisor, name: ControlPanel.TaskSupervisor},
      # Registers the connection process as `:nats` and reconnects it in the
      # background; every user of it must cope with the process being absent.
      {Gnat.ConnectionSupervisor,
       %{
         name: :nats,
         backoff_period: 2_000,
         connection_settings: [
           %{
             host: to_charlist(Application.fetch_env!(:control_panel, :nats_host)),
             port: Application.fetch_env!(:control_panel, :nats_port),
             no_responders: true
           }
         ]
       }},
      # The load generator's own HTTP pool: sized to its in-flight cap, so a
      # slow ingest (the delay fault) cannot starve it of connections.
      {Finch, name: ControlPanel.Finch, pools: %{default: [size: 300]}},
      ControlPanel.Generator,
      ControlPanel.Cluster,
      ControlPanel.IngestMetrics,
      ControlPanel.Stream,
      ControlPanelWeb.Endpoint
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: ControlPanel.Supervisor)
  end

  @impl true
  def config_change(changed, _new, removed) do
    ControlPanelWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
