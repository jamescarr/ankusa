defmodule Ankusa.MixProject do
  use Mix.Project

  @version "0.5.0"
  @source_url "https://github.com/jamescarr/ankusa"

  def project do
    [
      app: :ankusa,
      version: @version,
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      description: description(),
      package: package(),
      source_url: @source_url,
      homepage_url: @source_url,
      docs: docs()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      # :crypto backs the verifiers, UUIDv7, WAL CRCs, and the base64/hex in the
      # sinks; :xmerl parses S3's ListObjectsV2 XML. HTTP is `Req` (which brings
      # its own Finch/Mint TLS stack), so `:inets` is no longer needed.
      extra_applications: [:logger, :crypto, :xmerl],
      mod: {Ankusa.Application, []}
    ]
  end

  defp description do
    "A loosely coupled, high-throughput webhook ingestion framework: durable " <>
      "WAL, pluggable verification/storage/delivery, multi-tenant " <>
      "catch-URL routing."
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url, "Changelog" => "https://hexdocs.pm/ankusa/changelog.html"},
      files: ~w(lib priv .formatter.exs mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "elixir",
      source_ref: "ankusa-v#{@version}",
      source_url_pattern:
        "#{@source_url}/blob/ankusa-v#{@version}/packages/ankusa/%{path}#L%{line}",
      extras: [
        "../../docs/elixir.md",
        "../../README.md",
        "../../docs/quickstart.md",
        "../../docs/configuration.md",
        "../../docs/deployment.md",
        "../../docs/architecture.md",
        "../../docs/delivery.md",
        "../../docs/integrations.md",
        "../../docs/storage.md",
        "../../docs/claim-check.md",
        "../../docs/multi-tenancy.md",
        "../../docs/asyncapi.md",
        "../../docs/packaging.md",
        "../../docs/testing.md",
        "../../docs/releasing.md",
        "CHANGELOG.md"
      ],
      groups_for_extras: [
        Guides: [
          "../../docs/elixir.md",
          "../../docs/quickstart.md",
          "../../docs/configuration.md",
          "../../docs/deployment.md",
          "../../docs/architecture.md",
          "../../docs/delivery.md",
          "../../docs/integrations.md",
          "../../docs/storage.md",
          "../../docs/claim-check.md",
          "../../docs/multi-tenancy.md",
          "../../docs/asyncapi.md"
        ],
        Contributing: [
          "../../docs/packaging.md",
          "../../docs/testing.md",
          "../../docs/releasing.md"
        ]
      ],
      groups_for_modules: [
        "Internals (no stability guarantee)": [
          Ankusa.Edge.Router,
          Ankusa.Edge.Ingest,
          Ankusa.Edge.Batcher,
          Ankusa.Edge.BatcherSupervisor,
          Ankusa.Edge.Quarantine,
          Ankusa.Storage.Compactor,
          Ankusa.Store,
          Ankusa.Queue.Writer,
          Ankusa.Dispatch.Pipeline,
          Ankusa.ClaimCheck.Router,
          Ankusa.ClaimCheck.Sweeper,
          Ankusa.Http,
          Ankusa.HttpClient,
          Ankusa.Instance.Isolated,
          Ankusa.Instance.RegistryWatch,
          Ankusa.UUIDv7
        ]
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      async_api_spex_dep(),
      {:bandit, "~> 1.12.5"},
      {:plug, "~> 1.18"},
      # CIDR parsing and range membership for the route-management IP rules and
      # trusted proxies (`Ankusa.Routes`, `Ankusa.Net.ClientIP`). `Ankusa.Net`
      # keeps the :inet-tuple boundary and the IPv4-mapped normalization on top;
      # the prefix bit math is the package's.
      {:cidr, "~> 1.2"},
      # HTTP client for the S3/GCS blob stores, the claim-check Remote adapter,
      # Sink.Http and Sink.SQS. Replaces hand-rolled :httpc plumbing (and starts its own
      # Finch pool, so embedders configure nothing).
      {:req, "~> 0.7"},
      # AWS Signature V4 — the signing implementation behind the official
      # aws-elixir SDK. Replaces ~80 lines of hand-rolled canonical-request /
      # string-to-sign / HMAC-chain code; signs Ankusa.BlobStore.S3 and Ankusa.Sink.SQS.
      {:aws_signature, "~> 0.4"},
      # Built-in Prometheus mapping of Ankusa.Telemetry (Ankusa.Metrics).
      # Every deployment wants metrics, so they live in core rather than an
      # adapter package.
      {:telemetry_metrics, "~> 1.2"},
      {:telemetry_metrics_prometheus_core, "~> 1.2"},
      # Periodic gauge measurements for `Ankusa.Metrics` (`Ankusa.Metrics.Gauges`:
      # queue depth, oldest-due age, pen and disk usage).
      {:telemetry_poller, "~> 1.1"},
      # The route-management decision cache (`Ankusa.Routes.Cache`). Every
      # deployment that turns routes on wants a decision cache, so it lives in
      # core rather than an adapter package — the Redis *definitions* store is
      # the separate `ankusa_redis` package, since only deployments sharing
      # route definitions across nodes need a network store.
      {:nebulex, "~> 3.0"},
      {:nebulex_local, "~> 3.0"},
      # The node's local store (hooks, deliveries, archive catalogue, quarantine
      # pen, sources, rate limits). Native build needs cmake >= 3.12, a C++20
      # compiler and, on Linux, kernel headers. 3.1.2 is the first Hex build
      # that compiles outside a git checkout.
      {:rocksdb, "~> 3.1 and >= 3.1.2"},
      # The OpenAPI contract test reads priv/openapi/*.yaml and drives the
      # listeners from the examples in it, so the document and the code cannot
      # drift apart.
      {:yaml_elixir, "~> 2.12", only: :test},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false}
    ]
  end

  # The AsyncAPI document builder behind `GET /asyncapi.json`
  # (`Ankusa.AsyncApi`). Same poncho split as the adapters' `ankusa_dep/0`: the
  # monorepo checkout in dev/test, the Hex release for anyone consuming this
  # package from Hex.pm (Hex rejects path deps). A project that path-depends on
  # core and builds in :prod, like `ankusa_server`, pins the checkout itself.
  defp async_api_spex_dep do
    if Mix.env() in [:dev, :test] do
      {:async_api_spex, path: "../async_api_spex"}
    else
      {:async_api_spex, "~> 0.1"}
    end
  end
end
