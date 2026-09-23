defmodule AnkusaPostgres.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/jamescarr/ankusa"

  # Separate mix project, not a dependency of `ankusa`'s own mix.exs. This is the
  # forced split point from the packaging decision: `ankusa` core stays
  # zero-external-dep (compiles for the laptop/standalone user without ever
  # touching postgrex); this package exists only for deployments that opt into
  # a shared Postgres WAL. Path-dep on `ankusa` for development; becomes a normal
  # Hex dependency once both are published.
  def project do
    [
      app: :ankusa_postgres,
      version: @version,
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Shared, multi-node Ankusa.WAL adapter backed by Postgres.",
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
        "Changelog" => "https://hexdocs.pm/ankusa_postgres/changelog.html"
      },
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md"],
      source_ref: "ankusa_postgres-v#{@version}",
      source_url_pattern:
        "#{@source_url}/blob/ankusa_postgres-v#{@version}/ankusa_postgres/%{path}#L%{line}",
      deps: [ankusa: "https://hexdocs.pm/ankusa"]
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      ankusa_dep(),
      {:postgrex, "~> 0.19"},
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
