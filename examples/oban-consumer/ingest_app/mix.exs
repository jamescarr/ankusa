defmodule AnkusaExample.Ingest.MixProject do
  use Mix.Project

  # Not a published package — the deployable wrapper for this example. Depends
  # on `ankusa` (core) and `ankusa_postgres` (the WAL) via path deps, exactly
  # like a real deployment would depend on them via Hex once published.
  def project do
    [
      app: :ankusa_example_ingest,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: true,
      deps: deps(),
      releases: releases()
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
      # `override: true`: ankusa_postgres's own deps() picks its Hex entry for
      # `:ankusa` when Mix evaluates it as a nested dependency (deps build
      # under :prod by default, regardless of this project's own Mix.env())
      # — that conflicts with our direct path entry below. override tells
      # Mix to use ours everywhere in the tree, which is exactly what a
      # monorepo example wiring both path-dependent packages together needs.
      {:ankusa, path: "../../..", override: true},
      {:ankusa_postgres, path: "../../../ankusa_postgres"}
    ]
  end

  defp releases do
    [
      ingest: [
        include_executables_for: [:unix]
      ]
    ]
  end
end
