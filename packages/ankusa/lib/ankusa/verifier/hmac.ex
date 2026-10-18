defmodule Ankusa.Verifier.Hmac do
  @moduledoc ~S"""
  Configurable HMAC signature verifier, driven by a
  `%Ankusa.Verifier.Hmac.Scheme{}` descriptor instead of one module per provider.

  `opts` must carry `:scheme` — either a named preset atom
  (`:stripe | :github | :standard_webhooks | :shopify | :slack`, resolved through
  `Ankusa.Verifier.Schemes`) or an inline `%Scheme{}` describing any body-HMAC
  provider. `:secret` and `:tolerance` carry the key and replay window, exactly
  as with the old bespoke verifiers.

  `:secret` is a binary or a list of binaries (current first); every key is
  tried, so a rotation window is `secret: [new, old]` — hooks signed with
  either verify while the provider switches over, and none land in quarantine.
  An empty key would verify whatever anyone signs with it, so a missing or
  empty secret, an empty list, or an element that is empty or does not decode
  fails every hook with `:bad_secret`.

  `:tolerance` is the replay window in seconds, 0 to
  `Ankusa.Verifier.max_tolerance_seconds/0` (3600; default 300). A window wider
  than an hour is not a window, so anything else fails every hook with
  `:bad_tolerance` and refuses the boot of a source declared in config.

  `hash: :sha1` on an inline scheme is allowed, for providers that still sign
  with it, but it is no longer collision resistant: `weak_hash?/1` reports it,
  boot logs a warning, and every such verification counts toward
  `ankusa_verifier_weak_hash_total`.

  Providers that are not body-HMAC — Twilio (SHA1 over the full request URL plus
  sorted form params) and PayPal (RSA over a certificate fetched from a provider
  URL) — are not expressible here and stay bespoke `Ankusa.Verifier` modules.
  """

  @behaviour Ankusa.Verifier

  alias Ankusa.Envelope
  alias Ankusa.Verifier
  alias Ankusa.Verifier.Hmac.Scheme
  alias Ankusa.Verifier.Template

  defmodule Scheme do
    @moduledoc """
    Declarative description of one provider's HMAC signature scheme.

    The one field with no sensible default is `signature_header`: which header
    carries the signature(s). Everything else defaults to the most common shape
    (raw SHA-256 hex over the body).
    """

    @enforce_keys [:signature_header]
    defstruct [
      :signature_header,
      parse: :whole,
      sig_prefix: nil,
      sig_key: nil,
      version: nil,
      signed: "{body}",
      hash: :sha256,
      encoding: :hex,
      secret_decode: :raw,
      timestamp: nil
    ]

    @type parse :: :whole | :csv_pairs | :space_versions
    @type hash :: :sha256 | :sha512 | :sha1
    @type encoding :: :hex | :base64
    @type secret_decode :: :raw | :whsec_base64
    @type timestamp :: nil | {:header, String.t()} | {:sig, String.t()}

    @type t :: %__MODULE__{
            signature_header: String.t(),
            parse: parse(),
            sig_prefix: String.t() | nil,
            sig_key: String.t() | nil,
            version: String.t() | nil,
            signed: String.t(),
            hash: hash(),
            encoding: encoding(),
            secret_decode: secret_decode(),
            timestamp: timestamp()
          }
  end

  @impl true
  @spec verify(Envelope.t(), keyword()) :: :ok | {:error, term()}
  def verify(%Envelope{} = env, opts) do
    with {:ok, scheme} <- resolve_scheme(opts),
         :ok <- Verifier.check_tolerance(opts),
         {:ok, candidates, sig_ts} <- Template.candidates(env, scheme),
         {:ok, ts} <- Template.timestamp(env, scheme, sig_ts, opts),
         {:ok, signed} <- Template.signed_bytes(env, scheme, ts),
         {:ok, keys} <- secret_keys(opts, scheme) do
      matched? =
        Enum.any?(keys, fn key ->
          expected =
            encode(:crypto.mac(:hmac, crypto_hash(scheme.hash), key, signed), scheme.encoding)

          Enum.any?(candidates, &Verifier.secure_compare(&1, expected))
        end)

      if matched? do
        :ok
      else
        {:error, :no_match}
      end
    end
  end

  @doc "Human name of the scheme for telemetry: the preset name, or `\"custom\"`."
  @impl true
  @spec scheme_name(keyword()) :: String.t()
  def scheme_name(opts) do
    case opts[:scheme] do
      atom when is_atom(atom) -> Atom.to_string(atom)
      %Scheme{} -> "custom"
      _ -> "custom"
    end
  end

  @doc """
  Check `opts` the way `verify/2` would use them, without a request: the
  scheme resolves, `:tolerance` (when set) is an integer from 0 to
  `Ankusa.Verifier.max_tolerance_seconds/0`, and every secret decodes to a
  non-empty key. `:ok`, or the error `verify/2` would return for every hook.
  Used at boot for sources declared in config
  (`Ankusa.Verifier.validate_config!/1`).
  """
  @impl true
  @spec validate_opts(keyword()) :: :ok | {:error, :bad_secret | :bad_scheme | :bad_tolerance}
  def validate_opts(opts) do
    with {:ok, scheme} <- resolve_scheme(opts),
         :ok <- Verifier.check_tolerance(opts),
         {:ok, _keys} <- secret_keys(opts, scheme) do
      :ok
    end
  end

  @doc """
  Whether `opts` verify with SHA-1, a hash that is no longer collision
  resistant. No named preset uses it (`verifier_test.exs` asserts that), so
  only an inline `%Scheme{hash: :sha1}` is weak. It works, because some
  providers still sign that way; `Ankusa.Verifier.validate_config!/1` logs a
  warning at boot and every verification increments
  `ankusa_verifier_weak_hash_total`.
  """
  @spec weak_hash?(keyword()) :: boolean()
  def weak_hash?(opts), do: match?(%Scheme{hash: :sha1}, opts[:scheme])

  defp resolve_scheme(opts) do
    case opts[:scheme] do
      nil ->
        {:error, :bad_scheme}

      atom when is_atom(atom) ->
        case Ankusa.Verifier.Schemes.fetch(atom) do
          {:ok, scheme} -> {:ok, scheme}
          :error -> {:error, :bad_scheme}
        end

      %Scheme{} = scheme ->
        try do
          {:ok, Ankusa.Verifier.Schemes.validate!(scheme)}
        rescue
          ArgumentError -> {:error, :bad_scheme}
        end

      _ ->
        {:error, :bad_scheme}
    end
  end

  # An empty HMAC key verifies whatever anyone signs with it, and anyone can
  # sign with it, so the verifier fails closed: a missing or empty `:secret`,
  # an empty list, or any element that is not a binary or decodes to nothing is
  # `:bad_secret`. One unusable key in a list is a config mistake, not a
  # narrower rotation window.
  defp secret_keys(opts, %Scheme{secret_decode: decode}) do
    keys = opts |> Keyword.get(:secret) |> List.wrap() |> Enum.map(&decode_secret(&1, decode))

    if keys != [] and Enum.all?(keys, &match?({:ok, _}, &1)),
      do: {:ok, Enum.map(keys, fn {:ok, key} -> key end)},
      else: {:error, :bad_secret}
  end

  defp decode_secret(secret, decode) when is_binary(secret) do
    case raw_key(secret, decode) do
      {:ok, key} when key != "" -> {:ok, key}
      _empty_or_undecodable -> :error
    end
  end

  defp decode_secret(_secret, _decode), do: :error

  @doc """
  The HMAC key a configured secret stands for: the string itself (`:raw`), or
  the base64 after an optional `whsec_` prefix (`:whsec_base64`, Standard
  Webhooks). `:error` when the base64 does not decode.
  """
  @spec raw_key(String.t(), :raw | :whsec_base64) :: {:ok, binary()} | :error
  def raw_key(secret, :raw), do: {:ok, secret}

  def raw_key(secret, :whsec_base64),
    do: secret |> String.replace_prefix("whsec_", "") |> Base.decode64()

  # The scheme says `:sha1` (the name providers and `docs/configuration.md` use);
  # `:crypto.mac/4` knows SHA-1 as `:sha` and raises `:badarg` on `:sha1`.
  defp crypto_hash(:sha1), do: :sha
  defp crypto_hash(hash), do: hash

  defp encode(mac, :hex), do: Base.encode16(mac, case: :lower)
  defp encode(mac, :base64), do: Base.encode64(mac)
end
