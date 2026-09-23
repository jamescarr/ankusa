defmodule AnkusaExample.Ingest.MixProject do
  use Mix.Project

  # Not a published package — the deployable wrapper for this example. Not
  # shared with examples/rabbitmq-consumer: a shared wrapper would compile both
  # `amqp` and `brod` into both images.
  def project do
    [
      app: :ankusa_example_ingest,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: true,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {AnkusaExample.Ingest.Application, []}
    ]
  end

  defp deps do
    [
      # `override: true` for the same reason as examples/rabbitmq-consumer:
      # ankusa_kafka's deps() picks the Hex `:ankusa` when built as a nested
      # dependency, which conflicts with this path entry.
      {:ankusa, path: "../../..", override: true},
      {:ankusa_kafka, path: "../../../ankusa_kafka"}
    ]
  end
end
