defmodule AnkusaRedis.MixProject do
  use Mix.Project

  @version "0.3.0"
  @source_url "https://github.com/jamescarr/ankusa"

  # Forced split: `ankusa` core stays free of `:redix`, and every deployment
  # that needs neither the multi-node route store nor the pub/sub sink never
  # compiles it. This package carries both `:redix` users — the route store
  # and `Ankusa.Sink.Redis` — because they share the one dependency. (The
  # decision cache is core's own `nebulex_local`, because every deployment
  # wants that.)
  def project do
    [
      app: :ankusa_redis,
      version: @version,
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description:
        "Ankusa Redis adapters: a route store shared across edge nodes, and a pub/sub sink.",
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
        "Changelog" => "https://hexdocs.pm/ankusa_redis/changelog.html"
      },
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md"],
      source_ref: "ankusa_redis-v#{@version}",
      source_url_pattern:
        "#{@source_url}/blob/ankusa_redis-v#{@version}/packages/ankusa_redis/%{path}#L%{line}",
      deps: [ankusa: "https://hexdocs.pm/ankusa"]
    ]
  end

  def application do
    [extra_applications: [:logger], mod: {Ankusa.Sink.Redis.Application, []}]
  end

  defp deps do
    ankusa_deps() ++
      [
        {:redix, "~> 1.5"},
        {:ex_doc, "~> 0.40", only: :dev, runtime: false}
      ]
  end

  # Path dep for local monorepo development/test; the Hex-published version
  # is what a consumer installing from Hex.pm actually resolves — Hex
  # rejects packages with path/git deps, so this "poncho project" split is
  # required for this package to be publishable at all. Mix rejects two
  # entries for the same app regardless of :only, so this has to be a
  # single conditional entry, not a duplicate-with-disjoint-:only pair.
  #
  # The requirement is a compatibility fence, not a nicety: core 0.2.x calls the
  # store as `insert(instance, route)` and `replace(instance, route)`, while this
  # store implements the version-checked `insert/3` and `replace/3` that core has
  # from the 0.3 line on (see `Ankusa.Routes.Store`). Against a 0.2.x core every
  # route write would fail, so Hex must never pair them.
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
      [{:ankusa, "~> 0.3"}]
    end
  end
end
