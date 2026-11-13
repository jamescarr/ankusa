defmodule AnkusaGateway.MixProject do
  use Mix.Project

  @version "0.0.0"
  @source_url "https://github.com/jamescarr/ankusa"

  # Its own package (ADR 0005): the claim-check gateway and the permissioned
  # object facade, carved out of core so a gateway can be deployed and released
  # on its own. It depends on core for the claim read engine, the blob store
  # adapters, the listener and the JWT verifier; core knows nothing of it.
  def project do
    [
      app: :ankusa_gateway,
      version: @version,
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description:
        "Ankusa gateway: serves claim-check refs, and fronts object stores with pluggable " <>
          "authentication and authorization.",
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
        "Changelog" => "https://hexdocs.pm/ankusa_gateway/changelog.html"
      },
      files: ~w(lib priv .formatter.exs mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md"],
      source_ref: "ankusa_gateway-v#{@version}",
      source_url_pattern:
        "#{@source_url}/blob/ankusa_gateway-v#{@version}/packages/ankusa_gateway/%{path}#L%{line}",
      deps: [ankusa: "https://hexdocs.pm/ankusa"]
    ]
  end

  # No application callback: an embedder starts `{Ankusa.Gateway, config}`
  # under its own supervisor, after `{Ankusa.Instance, config}`.
  def application do
    [extra_applications: [:logger, :crypto]]
  end

  defp deps do
    ankusa_deps() ++
      [
        {:plug, "~> 1.18"},
        {:bandit, "~> 1.14 and >= 1.14.1"},
        {:jose, "~> 1.11 and >= 1.11.13"},
        {:nebulex, "~> 3.0"},
        {:nebulex_local, "~> 3.0"},
        {:telemetry_metrics, "~> 1.2"},
        {:yaml_elixir, "~> 2.12", only: :test},
        {:stream_data, "~> 1.1", only: :test},
        {:ex_doc, "~> 0.40", only: :dev, runtime: false},
        {:credo, "~> 1.7", only: :dev, runtime: false},
        {:dialyxir, "~> 1.4", only: :dev, runtime: false},
        {:sobelow, "~> 0.14", only: :dev, runtime: false}
      ]
  end

  # Path dep for local monorepo development/test; the Hex-published version is
  # what a consumer installing from Hex.pm resolves (Hex rejects packages with
  # path deps). One conditional entry, not a duplicate-with-disjoint-:only pair:
  # Mix rejects two entries for the same app.
  defp ankusa_deps do
    if Mix.env() in [:dev, :test] do
      [
        {:ankusa, path: "../ankusa"},
        # Core's own requirement on this is the Hex release, which Mix resolves
        # when it loads `../ankusa` as a dependency (deps load under :prod), so
        # the checkout is pinned here the same way `:ankusa` is.
        {:async_api_spex, path: "../async_api_spex", override: true}
      ]
    else
      # `~> 0.8`: the first core release with the gateway's seams
      # (`Ankusa.Admin.Auth.JWT.verify/5`, named key realms, `metrics.extra`,
      # `Ankusa.Admin.Redact.closed_view/2`) and without the `:claim_check` role
      # and `Ankusa.ClaimCheck.Router`. Publish core first.
      [{:ankusa, "~> 0.8"}]
    end
  end
end
