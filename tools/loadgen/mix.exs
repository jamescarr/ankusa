defmodule Loadgen.MixProject do
  use Mix.Project

  def project do
    [
      app: :loadgen,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: false,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:req, "~> 0.7"},
      {:postgrex, "~> 0.19"}
    ]
  end
end
