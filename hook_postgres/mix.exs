defmodule HookPostgres.MixProject do
  use Mix.Project

  # Separate mix project, not a dependency of `hook`'s own mix.exs. This is the
  # forced split point from the packaging decision: `hook` core stays
  # zero-external-dep (compiles for the laptop/standalone user without ever
  # touching postgrex); this package exists only for deployments that opt into
  # a shared Postgres WAL. Path-dep on `hook` for development; becomes a normal
  # Hex dependency once both are published.
  def project do
    [
      app: :hook_postgres,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:hook, path: ".."},
      {:postgrex, "~> 0.19"}
    ]
  end
end
