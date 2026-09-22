defmodule AnkusaPostgres.MixProject do
  use Mix.Project

  # Separate mix project, not a dependency of `ankusa`'s own mix.exs. This is the
  # forced split point from the packaging decision: `ankusa` core stays
  # zero-external-dep (compiles for the laptop/standalone user without ever
  # touching postgrex); this package exists only for deployments that opt into
  # a shared Postgres WAL. Path-dep on `ankusa` for development; becomes a normal
  # Hex dependency once both are published.
  def project do
    [
      app: :ankusa_postgres,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Shared, multi-node Ankusa.WAL adapter backed by Postgres.",
      package: [licenses: ["Apache-2.0"]]
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      ankusa_dep(),
      {:postgrex, "~> 0.19"}
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
