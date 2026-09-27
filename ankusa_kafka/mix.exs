defmodule AnkusaKafka.MixProject do
  use Mix.Project

  @version "0.2.0"
  @source_url "https://github.com/jamescarr/ankusa"

  # Same forced-split pattern as ankusa_rabbitmq: `ankusa` core stays free of
  # `:brod` and the `crc32cer` NIF it pulls in (which needs CMake to build) —
  # this package exists only for deployments that opt into a Kafka sink.
  def project do
    [
      app: :ankusa_kafka,
      version: @version,
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Ankusa.Sink adapter producing delivered hooks to a Kafka topic.",
      package: package(),
      source_url: @source_url,
      homepage_url: @source_url,
      docs: docs()
    ]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "https://hexdocs.pm/ankusa_kafka/changelog.html"
      },
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md"],
      source_ref: "ankusa_kafka-v#{@version}",
      source_url_pattern:
        "#{@source_url}/blob/ankusa_kafka-v#{@version}/ankusa_kafka/%{path}#L%{line}",
      deps: [ankusa: "https://hexdocs.pm/ankusa"]
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
      {:brod, "~> 4.6"},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false}
    ]
  end

  # Path dep for local monorepo development/test; the Hex-published version
  # is what a consumer installing from Hex.pm actually resolves — Hex
  # rejects packages with path/git deps, so this "poncho project" split is
  # required for this package to be publishable at all. Mix rejects two
  # entries for the same app regardless of :only, so this has to be a
  # single conditional entry, not a duplicate-with-disjoint-:only pair.
  defp ankusa_dep do
    if Mix.env() in [:dev, :test] do
      {:ankusa, path: ".."}
    else
      {:ankusa, "~> 0.2"}
    end
  end
end
