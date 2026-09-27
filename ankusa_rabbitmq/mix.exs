defmodule AnkusaRabbitmq.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/jamescarr/ankusa"

  # Forced split: `ankusa` core stays free of the
  # `:amqp` dependency (and everything it pulls in — amqp_client, rabbit_common
  # NIFs) — this package exists only for deployments that opt into a RabbitMQ
  # sink.
  def project do
    [
      app: :ankusa_rabbitmq,
      version: @version,
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Ankusa.Sink adapter publishing delivered hooks to a RabbitMQ exchange.",
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
        "Changelog" => "https://hexdocs.pm/ankusa_rabbitmq/changelog.html"
      },
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md"],
      source_ref: "ankusa_rabbitmq-v#{@version}",
      source_url_pattern:
        "#{@source_url}/blob/ankusa_rabbitmq-v#{@version}/ankusa_rabbitmq/%{path}#L%{line}",
      deps: [ankusa: "https://hexdocs.pm/ankusa"]
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
      {:amqp, "~> 4.0"},
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
      {:ankusa, "~> 0.1"}
    end
  end
end
