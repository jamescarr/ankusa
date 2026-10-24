defmodule Ankusa.Sink.NATS.Application do
  @moduledoc """
  Starts the connection tree (`Ankusa.Sink.Connections`) for the gnat
  connections `Ankusa.Sink.NATS` opens on demand from `publish/4`, one per
  `{instance, connection}`. `ankusa` core doesn't know this package exists.

  A disconnected or never-connected server never takes down ingest or
  dispatch: `deliver/3` returns `{:error, :not_connected}` and the source's
  `Ankusa.RetryPolicy` handles it.
  """

  use Application

  @impl true
  def start(_type, _args), do: Ankusa.Sink.Connections.start_link(Ankusa.Sink.NATS)
end
