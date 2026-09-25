defmodule AnkusaChaosSink.MixProject do
  use Mix.Project

  # The chaos harness's consumer endpoint: records what was actually delivered,
  # so "missing deliveries: 0" is a statement about the consumer, not about the
  # WAL's own bookkeeping. Deliberately its own tiny project — it is a test
  # fixture, not part of the framework.
  def project do
    [
      app: :ankusa_chaos_sink,
      version: "0.1.0",
      elixir: "~> 1.20",
      deps: deps(),
      releases: [sink: [include_executables_for: [:unix]]]
    ]
  end

  def application do
    [extra_applications: [:logger, :crypto], mod: {AnkusaChaosSink, []}]
  end

  defp deps do
    [
      {:bandit, "~> 1.12"},
      {:plug, "~> 1.18"},
      {:postgrex, "~> 0.19"}
    ]
  end
end
