defmodule AnkusaExample.Consumer.Application do
  @moduledoc """
  The Oban side of the end-to-end proof: this is the only application in the
  repo that knows what Oban is. Everything it receives arrives as a plain
  HTTP `POST /deliveries` from `Ankusa.Sink.Http` — no Ankusa dependency, no
  coupling, just the handoff contract.
  """

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      AnkusaExample.Consumer.Repo,
      {Oban, Application.fetch_env!(:ankusa_example_consumer, Oban)},
      {Bandit,
       plug: AnkusaExample.Consumer.Router,
       port: Application.fetch_env!(:ankusa_example_consumer, :port)}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: __MODULE__)
  end
end
