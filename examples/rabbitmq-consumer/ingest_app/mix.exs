defmodule AnkusaExample.Ingest.MixProject do
  use Mix.Project

  # Not a published package — the deployable wrapper for this example. Depends
  # on `ankusa` (core) and `ankusa_rabbitmq` (the sink) via path deps, exactly
  # like a real deployment would depend on them via Hex once published.
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
      {:ankusa, path: "../../.."},
      {:ankusa_rabbitmq, path: "../../../ankusa_rabbitmq"}
    ]
  end
end
