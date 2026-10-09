defmodule AnkusaSdk.MixProject do
  use Mix.Project

  @version "0.4.0"
  @source_url "https://github.com/jamescarr/ankusa"

  # A pure HTTP client on purpose: the SDK talks to a deployment's listeners
  # and delivers hooks to the app's own Plug pipeline, so it must be loadable
  # next to `ankusa` core (or entirely without it) and drag in no broker
  # clients — a consumer brings its own Broadway/AMQP/brod/gnat/Redix.
  def project do
    [
      app: :ankusa_sdk,
      version: @version,
      elixir: "~> 1.20",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description:
        "Elixir client for Ankusa: receive HTTP-sink deliveries, decode queue " <>
          "messages, redeem claim checks, and drive the routes/admin/sources APIs.",
      package: package(),
      source_url: @source_url,
      homepage_url: @source_url,
      docs: docs()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "https://hexdocs.pm/ankusa_sdk/changelog.html"
      },
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md"],
      source_ref: "sdk-elixir-v#{@version}",
      source_url_pattern:
        "#{@source_url}/blob/sdk-elixir-v#{@version}/packages/sdk-elixir/%{path}#L%{line}"
    ]
  end

  def application do
    # No `mod`: nothing to supervise. Req's pools are started by its own
    # application, and the receiver runs inside the app's web server.
    [extra_applications: [:logger, :crypto]]
  end

  defp deps do
    [
      {:req, "~> 0.7"},
      {:plug, "~> 1.18"},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false}
    ]
  end
end
