defmodule AnkusaOidc.MixProject do
  use Mix.Project

  @version "0.0.0"
  @source_url "https://github.com/jamescarr/ankusa"

  # Forced split: `ankusa` core stays free of `:jose`, and only the deployments
  # that turn single sign-on on (`auth.enabled`) need a JWT verifier. This
  # package is not a sink: it is the `Ankusa.Auth.Verifier` for OpenID Connect
  # bearer tokens.
  def project do
    [
      app: :ankusa_oidc,
      version: @version,
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description:
        "Ankusa.Auth.Verifier for OpenID Connect: verifies bearer JWTs against the identity provider's JWKS.",
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
        "Changelog" => "https://hexdocs.pm/ankusa_oidc/changelog.html"
      },
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md"],
      source_ref: "ankusa_oidc-v#{@version}",
      source_url_pattern:
        "#{@source_url}/blob/ankusa_oidc-v#{@version}/packages/ankusa_oidc/%{path}#L%{line}",
      deps: [ankusa: "https://hexdocs.pm/ankusa"]
    ]
  end

  # No application callback: the instance supervises the verifier
  # (`Ankusa.Auth.OIDC.start_link/1`), one per instance.
  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    ankusa_deps() ++
      [
        {:jose, "~> 1.11"},
        {:req, "~> 0.7"},
        {:ex_doc, "~> 0.40", only: :dev, runtime: false}
      ]
  end

  # Path dep for local monorepo development/test; the Hex-published version
  # is what a consumer installing from Hex.pm actually resolves — Hex
  # rejects packages with path/git deps, so this "poncho project" split is
  # required for this package to be publishable at all. Mix rejects two
  # entries for the same app regardless of :only, so this has to be a
  # single conditional entry, not a duplicate-with-disjoint-:only pair.
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
      [{:ankusa, "~> 0.5"}]
    end
  end
end
