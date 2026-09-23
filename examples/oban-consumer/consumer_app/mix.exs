defmodule AnkusaExample.Consumer.MixProject do
  use Mix.Project

  # Not a published package — the deployable Oban worker fleet for this
  # example. Zero Ankusa dependencies: the only thing it knows about the
  # ingest side is the plain HTTP handoff contract (`POST /deliveries`),
  # exactly the surface `Ankusa.Sink.Http` calls. This is also the only
  # place in the whole repo allowed to depend on `:oban`.
  def project do
    [
      app: :ankusa_example_consumer,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: true,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      releases: releases()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {AnkusaExample.Consumer.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:bandit, "~> 1.12"},
      {:plug, "~> 1.18"},
      {:ecto_sql, "~> 3.14"},
      {:postgrex, "~> 0.19"},
      {:oban, "~> 2.24"},
      # ecto_sql's Postgres adapter calls Jason directly for map-typed
      # column DDL defaults (Oban's migration hits this path) — it isn't
      # pulled in transitively, so it must be declared here.
      {:jason, "~> 1.4"}
    ]
  end

  defp releases do
    [
      consumer: [
        include_executables_for: [:unix]
      ]
    ]
  end
end
