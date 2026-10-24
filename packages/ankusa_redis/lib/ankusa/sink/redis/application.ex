defmodule Ankusa.Sink.Redis.Application do
  @moduledoc """
  Starts the connection tree (`Ankusa.Sink.Connections`) for the Redix
  connections `Ankusa.Sink.Redis` opens on demand from `publish/4`, one per
  `{instance, url}`. `ankusa` core doesn't know this package exists.

  A disconnected or never-connected server never takes down ingest or
  dispatch: `deliver/3` returns `{:error, {:connection, reason}}` and the
  source's `Ankusa.RetryPolicy` handles it.
  """

  use Application

  @impl true
  def start(_type, _args), do: Ankusa.Sink.Connections.start_link(Ankusa.Sink.Redis)
end
