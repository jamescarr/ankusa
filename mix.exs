defmodule Ankusa.MixProject do
  use Mix.Project

  @version "0.2.0"
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
      "WAL, pluggable verification/dedup/storage/delivery, multi-tenant " <>
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
      extras: [
        "docs/elixir.md",
        "README.md",
        "docs/quickstart.md",
        "docs/configuration.md",
        "docs/deployment.md",
        "docs/architecture.md",
        "docs/delivery.md",
        "docs/integrations.md",
        "docs/storage.md",
        "docs/claim-check.md",
        "docs/multi-tenancy.md",
        "docs/packaging.md",
        "docs/testing.md",
        "docs/releasing.md",
        "CHANGELOG.md"
      ],
      groups_for_extras: [
        Guides: [
          "docs/elixir.md",
          "docs/quickstart.md",
          "docs/configuration.md",
          "docs/deployment.md",
          "docs/architecture.md",
          "docs/delivery.md",
          "docs/integrations.md",
          "docs/storage.md",
          "docs/claim-check.md",
          "docs/multi-tenancy.md"
        ],
        Contributing: ["docs/packaging.md", "docs/testing.md", "docs/releasing.md"]
      ],
      groups_for_modules: [
        "Internals (no stability guarantee)": [
          Ankusa.Edge.Router,
          Ankusa.Edge.Ingest,
          Ankusa.Edge.Batcher,
          Ankusa.Edge.BatcherSupervisor,
          Ankusa.Edge.Quarantine,
          Ankusa.Storage.Compactor,
          Ankusa.Storage.Index,
          Ankusa.Dispatch.Pipeline,
          Ankusa.Dispatch.DLQ,
          Ankusa.ClaimCheck.Router,
          Ankusa.ClaimCheck.Sweeper,
          Ankusa.DurableLog,
          Ankusa.Http,
          Ankusa.HttpClient,
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
      {:bandit, "~> 1.12.5"},
      {:plug, "~> 1.18"},
      # HTTP client for the S3/GCS blob stores, the claim-check Remote adapter,
      # and Sink.Http. Replaces hand-rolled :httpc plumbing (and starts its own
      # Finch pool, so embedders configure nothing).
      {:req, "~> 0.7"},
      # AWS Signature V4 — the signing implementation behind the official
      # aws-elixir SDK. Replaces ~80 lines of hand-rolled canonical-request /
      # string-to-sign / HMAC-chain code in Ankusa.BlobStore.S3.
      {:aws_signature, "~> 0.4"},
      # Built-in Prometheus mapping of Ankusa.Telemetry (Ankusa.Metrics).
      # Every deployment wants metrics, so they live in core rather than an
      # adapter package.
      {:telemetry_metrics, "~> 1.2"},
      {:telemetry_metrics_prometheus_core, "~> 1.2"},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false}
    ]
  end
end
