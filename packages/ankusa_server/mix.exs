defmodule AnkusaServer.MixProject do
  use Mix.Project

  # Read by the release tooling: `sed -n 's/^  @version "\(.*\)"$/\1/p'`.
  # Independent of core's version — this project is its own artifact (a Docker
  # image), tagged `ankusa_server-vX.Y.Z`, and is never published to Hex.
  @version "0.3.0"

  def project do
    [
      app: :ankusa_server,
      version: @version,
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      releases: releases()
    ]
  end

  def application do
    [extra_applications: [:logger], mod: {AnkusaServer.Application, []}]
  end

  # One image with every adapter: the operator picks the WAL and sink they need
  # in YAML, and nothing has to be rebuilt. `override: true` on core for the
  # same reason as examples/kafka-sqs-consumer/ingest_app: the adapter packages
  # resolve Hex `:ankusa` when they are built as nested dependencies, and this
  # project must always run the core in this checkout.
  defp deps do
    [
      {:ankusa, path: "../ankusa", override: true},
      # Core's own requirement is the Hex release outside dev/test, and this
      # project builds in :prod; same reason as the override above.
      {:async_api_spex, path: "../async_api_spex", override: true},
      {:ankusa_rabbitmq, path: "../ankusa_rabbitmq"},
      {:ankusa_kafka, path: "../ankusa_kafka"},
      {:ankusa_nats, path: "../ankusa_nats"},
      {:ankusa_redis, path: "../ankusa_redis"},
      {:yaml_elixir, "~> 2.12"}
    ]
  end

  defp releases do
    [
      ankusa: [
        include_executables_for: [:unix],
        applications: [ankusa_server: :permanent]
      ]
    ]
  end
end
