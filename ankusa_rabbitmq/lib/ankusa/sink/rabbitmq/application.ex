defmodule Ankusa.Sink.RabbitMQ.Application do
  @moduledoc """
  Boots a bare `DynamicSupervisor` that owns one `Ankusa.Sink.RabbitMQ.Connection`
  per instance, started on demand by `Ankusa.Sink.RabbitMQ.deliver/3`.

  Self-contained: `ankusa` core has no idea this package exists, no changes to
  `Ankusa.Instance` were needed. Connections register through the same
  `Ankusa.Registry` every other instance-scoped process in the framework uses
  (`Ankusa.via/2`), so they're crash-isolated from the dispatch pipeline that
  calls them — a lost RabbitMQ connection never takes down ingest or dispatch,
  it just makes `deliver/3` return `{:error, :not_connected}`, which the
  source's `Ankusa.RetryPolicy` already handles.
  """

  use Application

  @impl true
  def start(_type, _args) do
    children = [{DynamicSupervisor, name: Ankusa.Sink.RabbitMQ.Supervisor, strategy: :one_for_one}]
    Supervisor.start_link(children, strategy: :one_for_one, name: Ankusa.Sink.RabbitMQ.TopSup)
  end
end
