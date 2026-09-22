defmodule HookExample.Ingest.MixProject do
  use Mix.Project

  # Not a published package — the deployable wrapper for this example. Depends
  # on `hook` (core) and `hook_rabbitmq` (the sink) via path deps, exactly
  # like a real deployment would depend on them via Hex once published.
  def project do
    [
      app: :hook_example_ingest,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: true,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {HookExample.Ingest.Application, []}
    ]
  end

  defp deps do
    [
      {:hook, path: "../../.."},
      {:hook_rabbitmq, path: "../../../hook_rabbitmq"}
    ]
  end
end
