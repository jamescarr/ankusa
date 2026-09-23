defmodule AnkusaKafka.MixProject do
  use Mix.Project

  # Same forced-split pattern as ankusa_rabbitmq: `ankusa` core stays free of
  # `:brod` and the `crc32cer` NIF it pulls in (which needs CMake to build) —
  # this package exists only for deployments that opt into a Kafka sink.
  def project do
    [
      app: :ankusa_kafka,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Ankusa.Sink adapter producing delivered hooks to a Kafka topic.",
      package: [licenses: ["Apache-2.0"]]
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Ankusa.Sink.Kafka.Application, []}
    ]
  end

  defp deps do
    [
      ankusa_dep(),
      {:brod, "~> 4.6"}
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
