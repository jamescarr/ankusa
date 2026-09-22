defmodule AnkusaRabbitmq.MixProject do
  use Mix.Project

  # Same forced-split pattern as ankusa_postgres: `ankusa` core stays free of the
  # `:amqp` dependency (and everything it pulls in — amqp_client, rabbit_common
  # NIFs) — this package exists only for deployments that opt into a RabbitMQ
  # sink.
  def project do
    [
      app: :ankusa_rabbitmq,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Ankusa.Sink adapter publishing delivered hooks to a RabbitMQ exchange.",
      package: [licenses: ["Apache-2.0"]]
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Ankusa.Sink.RabbitMQ.Application, []}
    ]
  end

  defp deps do
    [
      ankusa_dep(),
      {:amqp, "~> 4.0"}
    ]
  end

  # See ankusa_postgres/mix.exs for why this exists and why it must be a
  # single conditional entry rather than duplicate entries with :only.
  defp ankusa_dep do
    if Mix.env() in [:dev, :test] do
      {:ankusa, path: ".."}
    else
      {:ankusa, "~> 0.1"}
    end
  end
end
