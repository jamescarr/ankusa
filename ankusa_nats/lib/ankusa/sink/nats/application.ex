defmodule Ankusa.Sink.NATS.Application do
  @moduledoc """
  Boots a bare `DynamicSupervisor` that owns one gnat connection per
  `{instance, connection}`, started on demand by `Ankusa.Sink.NATS`'s
  `deliver/3`. `ankusa` core doesn't know this package exists.

  A disconnected or never-connected server never takes down ingest or
  dispatch: `deliver/3` returns `{:error, :not_connected}` and the source's
  `Ankusa.RetryPolicy` handles it.
  """

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {DynamicSupervisor, name: Ankusa.Sink.NATS.Supervisor, strategy: :one_for_one}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Ankusa.Sink.NATS.TopSup)
  end
end
