defmodule AnkusaExample.Ingest.MixProject do
  use Mix.Project

  def project do
    [
      app: :ankusa_example_ingest,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
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
      {:ankusa, path: "../../..", override: true},
      {:ankusa_kafka, path: "../../../ankusa_kafka"}
    ]
  end
end
