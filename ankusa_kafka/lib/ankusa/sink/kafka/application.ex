defmodule Ankusa.Sink.Kafka.Application do
  @moduledoc """
  Boots a bare `DynamicSupervisor` that owns the brod clients
  `Ankusa.Sink.Kafka.deliver/3` starts on demand. `ankusa` core doesn't know
  this package exists. A crashed or disconnected client never takes down
  ingest or dispatch; `deliver/3` returns `{:error, _}` and the source's
  `Ankusa.RetryPolicy` handles it.
  """

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {DynamicSupervisor, name: Ankusa.Sink.Kafka.Supervisor, strategy: :one_for_one}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Ankusa.Sink.Kafka.TopSup)
  end
end
