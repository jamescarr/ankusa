defmodule AsyncApiSpex do
  @moduledoc """
  Declarative AsyncAPI 3.0 documents for Elixir applications.

  Build an `AsyncApiSpex.Document` from structs, optionally declaring reusable
  schemas with `use AsyncApiSpex.Schema` and messages with
  `use AsyncApiSpex.Message`, then:

    * `resolve/1` extracts those modules into `components` and replaces their
      uses with references;
    * `to_map/1` produces a JSON-encodable map;
    * `encode!/1` produces JSON;
    * `validate/1` checks the document and reports errors by JSON path.

  Serve a document from a Plug application with `AsyncApiSpex.Plug.RenderSpec`,
  or write it to a file with `mix async_api_spex.gen`.
  """

  alias AsyncApiSpex.Document

  @doc """
  Returns the media type of an AsyncAPI document, `"application/asyncapi+json"`.
  """
  @spec content_type() :: String.t()
  def content_type, do: "application/asyncapi+json"

  @doc """
  Extracts schema and message modules into components, returning a new document.

  See `AsyncApiSpex.Resolver.resolve/1`. Idempotent.
  """
  @spec resolve(Document.t()) :: Document.t()
  defdelegate resolve(document), to: AsyncApiSpex.Resolver

  @doc """
  Encodes a document as a JSON-encodable map. Resolves it first.

  See `AsyncApiSpex.Encoder.to_map/1`.
  """
  @spec to_map(Document.t()) :: map()
  defdelegate to_map(document), to: AsyncApiSpex.Encoder

  @doc """
  Encodes a document as a JSON string. Resolves it first.

  See `AsyncApiSpex.Encoder.encode!/1`.
  """
  @spec encode!(Document.t()) :: binary()
  defdelegate encode!(document), to: AsyncApiSpex.Encoder

  @doc """
  Validates a document, resolving it first.

  Returns `:ok` or `{:error, messages}`, where each message names the JSON path
  of the problem. See `AsyncApiSpex.Validator.validate/1`.
  """
  @spec validate(Document.t()) :: :ok | {:error, [String.t()]}
  defdelegate validate(document), to: AsyncApiSpex.Validator
end
