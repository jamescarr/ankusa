defmodule AnkusaExample.Ingest.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Ankusa.Instance, :default}
    ]

    opts = [strategy: :one_for_one, name: AnkusaExample.Ingest.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
