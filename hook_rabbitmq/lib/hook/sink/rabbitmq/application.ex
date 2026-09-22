defmodule Hook.Sink.RabbitMQ.Application do
  @moduledoc """
  Boots a bare `DynamicSupervisor` that owns one `Hook.Sink.RabbitMQ.Connection`
  per instance, started on demand by `Hook.Sink.RabbitMQ.deliver/3`.

  Self-contained: `hook` core has no idea this package exists, no changes to
  `Hook.Instance` were needed. Connections register through the same
  `Hook.Registry` every other instance-scoped process in the framework uses
  (`Hook.via/2`), so they're crash-isolated from the dispatch pipeline that
  calls them — a lost RabbitMQ connection never takes down ingest or dispatch,
  it just makes `deliver/3` return `{:error, :not_connected}`, which the
  source's `Hook.RetryPolicy` already handles.
  """

  use Application

  @impl true
  def start(_type, _args) do
    children = [{DynamicSupervisor, name: Hook.Sink.RabbitMQ.Supervisor, strategy: :one_for_one}]
    Supervisor.start_link(children, strategy: :one_for_one, name: Hook.Sink.RabbitMQ.TopSup)
  end
end
