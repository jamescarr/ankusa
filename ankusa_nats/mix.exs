defmodule AnkusaNats.MixProject do
  use Mix.Project

  @version "0.1.0"
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
        "#{@source_url}/blob/ankusa_nats-v#{@version}/ankusa_nats/%{path}#L%{line}",
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
