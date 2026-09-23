defmodule AnkusaKafka.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/jamescarr/webhook_ingest_ex"

  def project do
    [
      app: :ankusa_kafka,
      version: @version,
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Kafka sink adapter for Ankusa webhook ingestion framework",
      package: package(),
      docs: docs()
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
      # Path dep in dev/test (mono-repo), Hex dep when published
      if Mix.env() in [:dev, :test] do
        {:ankusa, path: "..", runtime: false}
      else
        {:ankusa, "~> 0.1"}
      end,
      {:brod, "~> 4.6.3"},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: @source_url,
      extras: ["README.md", "CHANGELOG.md"]
    ]
  end
end
