defmodule AnkusaNats.MixProject do
  use Mix.Project

  @version "0.3.0"
  @source_url "https://github.com/jamescarr/ankusa"

  # Same forced-split pattern as ankusa_rabbitmq and ankusa_kafka: `ankusa` core
  # stays free of `:gnat` — this package exists only for deployments that opt
  # into a NATS JetStream sink.
  def project do
    [
      app: :ankusa_nats,
      version: @version,
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Ankusa.Sink adapter publishing delivered hooks to a NATS JetStream subject.",
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
        "Changelog" => "https://hexdocs.pm/ankusa_nats/changelog.html"
      },
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md"],
      source_ref: "ankusa_nats-v#{@version}",
      source_url_pattern:
        "#{@source_url}/blob/ankusa_nats-v#{@version}/packages/ankusa_nats/%{path}#L%{line}",
      deps: [ankusa: "https://hexdocs.pm/ankusa"]
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Ankusa.Sink.NATS.Application, []}
    ]
  end

  defp deps do
    [
      ankusa_dep(),
      {:gnat, "~> 1.17"},
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
      {:ankusa, path: "../ankusa"}
    else
      {:ankusa, "~> 0.2"}
    end
  end
end
