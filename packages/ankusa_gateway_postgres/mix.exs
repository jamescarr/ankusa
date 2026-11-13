defmodule AnkusaGatewayPostgres.MixProject do
  use Mix.Project

  @version "0.0.0"
  @source_url "https://github.com/jamescarr/ankusa"

  # The gateway's Postgres adapter: the `table` authorizer's source in a
  # customer's own tables, its join expressed as one SQL query. Its own package
  # so the gateway library (and every deployment that uses a permissions file
  # or an HTTP endpoint instead) never pulls `:postgrex`.
  def project do
    [
      app: :ankusa_gateway_postgres,
      version: @version,
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description:
        "Ankusa gateway Postgres adapter: table permissions from a customer's own database.",
      package: package(),
      source_url: @source_url,
      homepage_url: @source_url,
      docs: docs(),
      test_coverage: [summary: [threshold: 80]],
      # Every PLT lives under _build, which CI caches (keyed on the toolchain pins
      # and mix.lock), so a run rebuilds a PLT only when one of them changed.
      dialyzer: [plt_core_path: "_build/plts", plt_local_path: "_build/plts"]
    ]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "https://hexdocs.pm/ankusa_gateway_postgres/changelog.html"
      },
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md"],
      source_ref: "ankusa_gateway_postgres-v#{@version}",
      source_url_pattern:
        "#{@source_url}/blob/ankusa_gateway_postgres-v#{@version}/packages/ankusa_gateway_postgres/%{path}#L%{line}",
      deps: [
        ankusa: "https://hexdocs.pm/ankusa",
        ankusa_gateway: "https://hexdocs.pm/ankusa_gateway"
      ]
    ]
  end

  # No application callback: the source starts its pool as an authorizer child.
  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    ankusa_deps() ++
      [
        {:postgrex, "~> 0.22"},
        {:ex_doc, "~> 0.40", only: :dev, runtime: false},
        {:credo, "~> 1.7", only: :dev, runtime: false},
        {:dialyxir, "~> 1.4", only: :dev, runtime: false}
      ]
  end

  # Path deps for local development/test; Hex requirements otherwise, where a
  # consumer resolves both from Hex (Hex rejects path deps). One conditional
  # entry per app: Mix rejects two for the same app regardless of `:only`.
  defp ankusa_deps do
    if Mix.env() in [:dev, :test] do
      [
        {:ankusa_gateway, path: "../ankusa_gateway"},
        {:ankusa, path: "../ankusa", override: true},
        # Core's own requirement on this is the Hex release, which Mix resolves
        # when it loads `../ankusa` as a dependency (deps load under :prod), so
        # the checkout is pinned here the same way `:ankusa` is.
        {:async_api_spex, path: "../async_api_spex", override: true}
      ]
    else
      # Publish core and `ankusa_gateway` first: both are referenced from Hex.
      [{:ankusa, "~> 0.8"}, {:ankusa_gateway, "~> 0.1"}]
    end
  end
end
