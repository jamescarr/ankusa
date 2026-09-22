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
      # :inets/:ssl back the Ankusa.Sink.Http forwarder and the S3/GCS
      # BlobStore adapters (:httpc); :crypto backs the verifiers, UUIDv7,
      # WAL CRCs, and S3 SigV4 signing; :xmerl parses S3 ListObjectsV2 XML.
      extra_applications: [:logger, :crypto, :inets, :ssl, :xmerl],
      mod: {Ankusa.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:bandit, "~> 1.12.5"},
      {:plug, "~> 1.18"}
    ]
  end
end
