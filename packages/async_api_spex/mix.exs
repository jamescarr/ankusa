defmodule AsyncApiSpex.MixProject do
  use Mix.Project

  @version "0.2.0"
  @source_url "https://github.com/jamescarr/ankusa"

  def project do
    [
      app: :async_api_spex,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description:
        "Declarative AsyncAPI 3.0 documents: structs, use macros, a Plug, and a mix task.",
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
        "Changelog" => "https://hexdocs.pm/async_api_spex/changelog.html"
      },
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md"],
      source_ref: "async_api_spex-v#{@version}",
      source_url_pattern:
        "#{@source_url}/blob/async_api_spex-v#{@version}/packages/async_api_spex/%{path}#L%{line}"
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:plug, "~> 1.14", optional: true},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false}
    ]
  end
end
