defmodule Ankusa.MixProject do
  use Mix.Project

  def project do
    [
      app: :ankusa,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      description: description(),
      package: package()
    ]
  end

  defp description do
    "A loosely coupled, high-throughput webhook ingestion framework: durable " <>
      "WAL, pluggable verification/dedup/storage/delivery, multi-tenant " <>
      "catch-URL routing."
  end

  defp package do
    [
      licenses: ["Apache-2.0"]
      # links: %{"GitHub" => "https://github.com/<org>/ankusa"}  # set before first publish
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
      {:aws_signature, "~> 0.4"}
    ]
  end
end
