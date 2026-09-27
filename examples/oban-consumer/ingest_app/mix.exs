defmodule AnkusaExample.Ingest.MixProject do
  use Mix.Project

  # Not a published package — the deployable wrapper for this example. Depends
  # on `ankusa` (core) via a path dep, exactly like a real deployment would
  # depend on the Hex-published package.
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
      # path dep on core; `override: true` keeps it authoritative over any
      # Hex resolution
      {:ankusa, path: "../../..", override: true}
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
