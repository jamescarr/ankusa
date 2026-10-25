defmodule Ankusa.Verifier.Schemes do
  @moduledoc ~S"""
  Named presets for `Ankusa.Verifier.Hmac` and `Ankusa.Verifier.Asymmetric` —
  the wire details of well-known providers as data, so adding a provider is
  editing a table, not writing a module.

  Any body-HMAC provider not listed here is expressed inline as a
  `%Ankusa.Verifier.Hmac.Scheme{}`, any public-key one as a
  `%Ankusa.Verifier.Asymmetric.Scheme{}` (see `validate!/1` for the
  invariants). Providers that are not a signature over a request-derived
  string (Twilio's URL + sorted-form-param SHA1, PayPal's RSA over a fetched
  certificate) are not expressible by these engines and stay bespoke
  `Ankusa.Verifier` modules.
  """

  alias Ankusa.Verifier.Asymmetric
  alias Ankusa.Verifier.Hmac.Scheme
  alias Ankusa.Verifier.Template

  @asymmetric_presets %{
    # Standard Webhooks `v1a`: Ed25519 over `<webhook-id>.<webhook-timestamp>.<body>`,
    # base64, the verifier holding the provider's `whpk_` public key.
    standard_webhooks_asym: %Asymmetric.Scheme{
      signature_header: "webhook-signature",
      parse: :space_versions,
      version: "v1a,",
      signed: "{header:webhook-id}.{header:webhook-timestamp}.{body}",
      algorithm: :ed25519,
      encoding: :base64,
      timestamp: {:header, "webhook-timestamp"}
    }
  }

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
  @algorithms [:ed25519, :rsa_sha256]

  # The presets' `signed` templates, compiled once at build time: `verify/2`
  # fills them per request (`Ankusa.Verifier.Template.signed_iodata/3`) and
  # never parses one.
  @compiled Map.new(@presets, fn {name, scheme} -> {name, Template.compile(scheme.signed)} end)
  @asymmetric_compiled Map.new(@asymmetric_presets, fn {name, scheme} ->
                         {name, Template.compile(scheme.signed)}
                       end)

  @doc "Every named `Ankusa.Verifier.Hmac` preset, keyed by scheme name."
  @spec all() :: %{atom() => Scheme.t()}
  def all, do: @presets

  @doc "Look up a named `Ankusa.Verifier.Hmac` preset."
  @spec fetch(atom()) :: {:ok, Scheme.t()} | :error
  def fetch(name) when is_atom(name) do
    Map.fetch(@presets, name)
  end

  def fetch(_), do: :error

  @doc "A named `Ankusa.Verifier.Hmac` preset with its `signed` template compiled."
  @spec fetch_compiled(atom()) :: {:ok, Scheme.t(), Template.compiled()} | :error
  def fetch_compiled(name) when is_atom(name) do
    case @presets do
      %{^name => scheme} -> {:ok, scheme, Map.fetch!(@compiled, name)}
      _ -> :error
    end
  end

  def fetch_compiled(_), do: :error

  @doc "Every named `Ankusa.Verifier.Asymmetric` preset, keyed by scheme name."
  @spec asymmetric_all() :: %{atom() => Asymmetric.Scheme.t()}
  def asymmetric_all, do: @asymmetric_presets

  @doc "Look up a named `Ankusa.Verifier.Asymmetric` preset."
  @spec fetch_asymmetric(atom()) :: {:ok, Asymmetric.Scheme.t()} | :error
  def fetch_asymmetric(name) when is_atom(name) do
    Map.fetch(@asymmetric_presets, name)
  end

  def fetch_asymmetric(_), do: :error

  @doc "A named `Ankusa.Verifier.Asymmetric` preset with its `signed` template compiled."
  @spec fetch_asymmetric_compiled(atom()) ::
          {:ok, Asymmetric.Scheme.t(), Template.compiled()} | :error
  def fetch_asymmetric_compiled(name) when is_atom(name) do
    case @asymmetric_presets do
      %{^name => scheme} -> {:ok, scheme, Map.fetch!(@asymmetric_compiled, name)}
      _ -> :error
    end
  end

  def fetch_asymmetric_compiled(_), do: :error

  @doc """
  Validate a scheme descriptor (`Ankusa.Verifier.Hmac.Scheme` or
  `Ankusa.Verifier.Asymmetric.Scheme`), raising `ArgumentError` naming the
  offending field on the first violation.
  """
  @spec validate!(Scheme.t()) :: Scheme.t()
  @spec validate!(Asymmetric.Scheme.t()) :: Asymmetric.Scheme.t()
  def validate!(%Scheme{} = scheme) do
    validate_header_and_parse!(scheme)

    unless scheme.hash in @hashes do
      raise ArgumentError,
            "scheme.hash must be one of #{inspect(@hashes)}, got: #{inspect(scheme.hash)}"
    end

    validate_encoding!(scheme)

    unless scheme.secret_decode in @secret_decodes do
      raise ArgumentError,
            "scheme.secret_decode must be one of #{inspect(@secret_decodes)}, " <>
              "got: #{inspect(scheme.secret_decode)}"
    end

    validate_template!(scheme)
  end

  def validate!(%Asymmetric.Scheme{} = scheme) do
    validate_header_and_parse!(scheme)

    unless scheme.algorithm in @algorithms do
      raise ArgumentError,
            "scheme.algorithm must be one of #{inspect(@algorithms)}, " <>
              "got: #{inspect(scheme.algorithm)}"
    end

    validate_encoding!(scheme)
    validate_template!(scheme)
  end

  defp validate_header_and_parse!(scheme) do
    unless is_binary(scheme.signature_header) and scheme.signature_header != "" do
      raise ArgumentError, "scheme.signature_header must be a non-empty string"
    end

    unless scheme.parse in @parses do
      raise ArgumentError,
            "scheme.parse must be one of #{inspect(@parses)}, got: #{inspect(scheme.parse)}"
    end
  end

  defp validate_encoding!(scheme) do
    unless scheme.encoding in @encodings do
      raise ArgumentError,
            "scheme.encoding must be one of #{inspect(@encodings)}, got: #{inspect(scheme.encoding)}"
    end
  end

  # The checks over what `Ankusa.Verifier.Template` reads, common to every scheme.
  defp validate_template!(scheme) do
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
