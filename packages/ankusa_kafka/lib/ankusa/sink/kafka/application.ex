defmodule Ankusa.Sink.Kafka.Application do
  @moduledoc """
  Starts the connection tree (`Ankusa.Sink.Connections`) for the brod clients
  `Ankusa.Sink.Kafka` opens on demand from `publish/4`. `ankusa` core doesn't
  know this package exists. A crashed or disconnected client never takes down
  ingest or dispatch; `deliver/3` returns `{:error, _}` and the source's
  `Ankusa.RetryPolicy` handles it.
  """

  use Application

  @impl true
  def start(_type, _args), do: Ankusa.Sink.Connections.start_link(Ankusa.Sink.Kafka)
end
