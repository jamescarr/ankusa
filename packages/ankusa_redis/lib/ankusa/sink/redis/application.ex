defmodule Ankusa.Sink.Redis.Application do
  @moduledoc """
  Boots a bare `DynamicSupervisor` that owns one Redix connection per
  `{instance, url}`, started on demand by `Ankusa.Sink.Redis`'s `deliver/3`.
  `ankusa` core doesn't know this package exists.

  A disconnected or never-connected server never takes down ingest or
  dispatch: `deliver/3` returns `{:error, {:connection, reason}}` and the
  source's `Ankusa.RetryPolicy` handles it.
  """

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {DynamicSupervisor, name: Ankusa.Sink.Redis.Supervisor, strategy: :one_for_one}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Ankusa.Sink.Redis.TopSup)
  end
end
