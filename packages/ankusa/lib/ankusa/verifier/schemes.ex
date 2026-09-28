defmodule Ankusa.Verifier.Schemes do
  @moduledoc ~S"""
  Named presets for `Ankusa.Verifier.Hmac` — the wire details of well-known
  providers as data, so adding a provider is editing a table, not writing a
  module.

  Any body-HMAC provider not listed here is expressed inline as a
  `%Ankusa.Verifier.Hmac.Scheme{}` (see `validate!/1` for the invariants).
  Providers that are not body-HMAC (Twilio's URL + sorted-form-param SHA1,
  PayPal's RSA over a fetched certificate) are not expressible by this engine
  and stay bespoke `Ankusa.Verifier` modules.
  """

  alias Ankusa.Verifier.Hmac.Scheme

  @presets %{
    github: %Scheme{
      signature_header: "X-Hub-Signature-256",
      parse: :whole,
      sig_prefix: "sha256=",
      signed: "{body}",
      hash: :sha256,
      encoding: :hex
    },
    shopify: %Scheme{
      signature_header: "X-Shopify-Hmac-SHA256",
      parse: :whole,
      signed: "{body}",
      hash: :sha256,
      encoding: :base64
    },
    slack: %Scheme{
      signature_header: "X-Slack-Signature",
      parse: :whole,
      sig_prefix: "v0=",
      signed: "v0:{ts}:{body}",
      hash: :sha256,
      encoding: :hex,
      timestamp: {:header, "X-Slack-Request-Timestamp"}
    },
    stripe: %Scheme{
      signature_header: "Stripe-Signature",
      parse: :csv_pairs,
      sig_key: "v1",
      signed: "{ts}.{body}",
      hash: :sha256,
      encoding: :hex,
      timestamp: {:sig, "t"}
    },
    standard_webhooks: %Scheme{
      signature_header: "webhook-signature",
      parse: :space_versions,
      version: "v1,",
      signed: "{header:webhook-id}.{header:webhook-timestamp}.{body}",
      hash: :sha256,
      encoding: :base64,
      secret_decode: :whsec_base64,
      timestamp: {:header, "webhook-timestamp"}
    }
  }

  @parses [:whole, :csv_pairs, :space_versions]
  @hashes [:sha256, :sha512, :sha1]
  @encodings [:hex, :base64]
  @secret_decodes [:raw, :whsec_base64]

  @doc "Every named preset, keyed by scheme name."
  @spec all() :: %{atom() => %Scheme{}}
  def all, do: @presets

  @doc "Look up a named preset."
  @spec fetch(atom()) :: {:ok, %Scheme{}} | :error
  def fetch(name) when is_atom(name) do
    Map.fetch(@presets, name)
  end

  def fetch(_), do: :error

  @doc """
  Validate a scheme descriptor, raising `ArgumentError` naming the offending
  field on the first violation.
  """
  @spec validate!(%Scheme{}) :: %Scheme{}
  def validate!(%Scheme{} = scheme) do
    unless is_binary(scheme.signature_header) and scheme.signature_header != "" do
      raise ArgumentError, "scheme.signature_header must be a non-empty string"
    end

    unless scheme.parse in @parses do
      raise ArgumentError,
            "scheme.parse must be one of #{inspect(@parses)}, got: #{inspect(scheme.parse)}"
    end

    unless scheme.hash in @hashes do
      raise ArgumentError,
            "scheme.hash must be one of #{inspect(@hashes)}, got: #{inspect(scheme.hash)}"
    end

    unless scheme.encoding in @encodings do
      raise ArgumentError,
            "scheme.encoding must be one of #{inspect(@encodings)}, got: #{inspect(scheme.encoding)}"
    end

    unless scheme.secret_decode in @secret_decodes do
      raise ArgumentError,
            "scheme.secret_decode must be one of #{inspect(@secret_decodes)}, " <>
              "got: #{inspect(scheme.secret_decode)}"
    end

    unless valid_timestamp?(scheme.timestamp) do
      raise ArgumentError,
            "scheme.timestamp must be nil, {:header, name}, or {:sig, field}, " <>
              "got: #{inspect(scheme.timestamp)}"
    end

    unless valid_signed?(scheme.signed) do
      raise ArgumentError,
            "scheme.signed contains an unknown token, got: #{inspect(scheme.signed)}"
    end

    if scheme.parse == :csv_pairs and is_nil(scheme.sig_key) do
      raise ArgumentError, "scheme.sig_key is required when scheme.parse is :csv_pairs"
    end

    if match?({:sig, _}, scheme.timestamp) and scheme.parse != :csv_pairs do
      raise ArgumentError, "scheme.timestamp as {:sig, field} requires scheme.parse :csv_pairs"
    end

    if String.contains?(scheme.signed, "{ts}") and is_nil(scheme.timestamp) do
      raise ArgumentError, "scheme.signed uses {ts} but scheme.timestamp is nil"
    end

    scheme
  end

  defp valid_timestamp?(nil), do: true
  defp valid_timestamp?({:header, name}) when is_binary(name) and name != "", do: true
  defp valid_timestamp?({:sig, field}) when is_binary(field) and field != "", do: true
  defp valid_timestamp?(_), do: false

  defp valid_signed?(signed) when is_binary(signed) do
    tokens =
      ~r/\{([^}]*)\}/
      |> Regex.scan(signed, capture: :all_but_first)
      |> List.flatten()

    Enum.all?(tokens, fn
      "body" -> true
      "ts" -> true
      "header:" <> _ -> true
      _ -> false
    end)
  end

  defp valid_signed?(_), do: false
end
