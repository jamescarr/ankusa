defmodule AnkusaRa.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/jamescarr/ankusa"

  # Separate mix project, not a dependency of `ankusa`'s own mix.exs. Same
  # forced split point as `ankusa_postgres`: `ankusa` core stays free of the
  # `:ra` dependency (and of the distributed-Erlang machinery a shared WAL
  # needs), so a laptop deployment never compiles it. This package exists only
  # for deployments that opt into a replicated WAL. Path-dep on `ankusa` for
  # development; becomes a normal Hex dependency once both are published.
  def project do
    [
      app: :ankusa_ra,
      version: @version,
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      description: "Shared, multi-node Ankusa.WAL adapter backed by a Ra (Raft) log.",
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
        "Changelog" => "https://hexdocs.pm/ankusa_ra/changelog.html"
      },
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md"],
      source_ref: "ankusa_ra-v#{@version}",
      source_url_pattern: "#{@source_url}/blob/ankusa_ra-v#{@version}/ankusa_ra/%{path}#L%{line}",
      deps: [ankusa: "https://hexdocs.pm/ankusa"]
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      ankusa_dep(),
      # Raft. The whole point of this adapter: a replicated, majority-acked
      # log with leader election, snapshots and membership changes, which is
      # exactly the state machine a shared WAL needs and exactly the part
      # that is not worth hand-rolling.
      {:ra, "~> 3.2"},
      # Only `mix ankusa.wal.migrate` uses this: the task reads the old
      # Postgres WAL's cursors and dedup ledger to move them into a Ra cluster.
      # A migration tool that cannot speak to the thing it migrates from is not
      # a migration tool, so the dependency lives here rather than being
      # assumed to be present.
      {:postgrex, "~> 0.19"},
      {:stream_data, "~> 1.1", only: :test},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false}
    ]
  end

  # Path dep for local monorepo development/test; the Hex-published version is
  # what a consumer installing from Hex.pm actually resolves — Hex rejects
  # packages with path/git deps, so this "poncho project" split is required for
  # this package to be publishable at all. Mix rejects two entries for the same
  # app regardless of :only, so this has to be a single conditional entry.
  defp ankusa_dep do
    if Mix.env() in [:dev, :test] do
      {:ankusa, path: ".."}
    else
      {:ankusa, "~> 0.1"}
    end
  end
end
