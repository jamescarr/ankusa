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

  Providers that are not body-HMAC — Twilio (SHA1 over the full request URL plus
  sorted form params) and PayPal (RSA over a certificate fetched from a provider
  URL) — are not expressible here and stay bespoke `Ankusa.Verifier` modules.
  """

  @behaviour Ankusa.Verifier

  alias Ankusa.Envelope
  alias Ankusa.Verifier
  alias Ankusa.Verifier.Hmac.Scheme

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
         {:ok, candidates, sig_ts} <- candidates(env, scheme),
         {:ok, ts} <- timestamp(env, scheme, sig_ts, opts),
         {:ok, signed} <- signed_bytes(env, scheme, ts),
         {:ok, keys} <- secret_keys(opts, scheme) do
      matched? =
        Enum.any?(keys, fn key ->
          expected = encode(:crypto.mac(:hmac, scheme.hash, key, signed), scheme.encoding)
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
        {:ok, Ankusa.Verifier.Schemes.validate!(scheme)}

      _ ->
        {:error, :bad_scheme}
    end
  end

  defp candidates(env, %Scheme{} = scheme) do
    case Envelope.header(env, scheme.signature_header) do
      nil ->
        {:error, :missing_signature}

      header ->
        case scheme.parse do
          :whole -> whole(header, scheme.sig_prefix)
          :csv_pairs -> csv_pairs(header, scheme)
          :space_versions -> space_versions(header, scheme)
        end
    end
  end

  defp whole(header, nil), do: {:ok, [header], nil}

  defp whole(header, prefix) do
    if String.starts_with?(header, prefix) do
      {:ok, [String.replace_prefix(header, prefix, "")], nil}
    else
      {:error, :malformed_signature}
    end
  end

  defp csv_pairs(header, %Scheme{sig_key: sig_key, timestamp: timestamp}) do
    parts =
      header
      |> String.split(",", trim: true)
      |> Enum.map(&String.split(&1, "=", parts: 2))

    sigs = for [^sig_key, v] <- parts, do: v

    sig_ts =
      case timestamp do
        {:sig, field} ->
          Enum.find_value(parts, fn
            [^field, v] -> v
            _ -> nil
          end)

        _ ->
          nil
      end

    if sigs != [] do
      {:ok, sigs, sig_ts}
    else
      {:error, :malformed_signature}
    end
  end

  defp space_versions(header, %Scheme{version: version}) do
    sigs =
      header
      |> String.split(" ", trim: true)
      |> Enum.flat_map(fn token ->
        case version do
          nil ->
            [token]

          v ->
            if String.starts_with?(token, v) do
              [String.replace_prefix(token, v, "")]
            else
              []
            end
        end
      end)

    if sigs != [] do
      {:ok, sigs, nil}
    else
      {:error, :malformed_signature}
    end
  end

  defp timestamp(_env, %Scheme{timestamp: nil}, _sig_ts, _opts), do: {:ok, nil}

  defp timestamp(env, %Scheme{timestamp: {:header, name}}, _sig_ts, opts) do
    case Envelope.header(env, name) do
      nil -> {:error, :missing_signature}
      "" -> {:error, :missing_signature}
      ts -> check_timestamp(ts, opts)
    end
  end

  defp timestamp(_env, %Scheme{timestamp: {:sig, _field}}, sig_ts, opts) do
    case sig_ts do
      nil -> {:error, :missing_signature}
      ts -> check_timestamp(ts, opts)
    end
  end

  defp check_timestamp(ts, opts) do
    case Verifier.check_timestamp(ts, opts) do
      :ok -> {:ok, ts}
      {:error, _} = err -> err
    end
  end

  defp signed_bytes(env, %Scheme{signed: template}, ts) do
    with :ok <- header_values?(env, template),
         :ok <- ts_value?(template, ts) do
      {:ok,
       Regex.replace(~r/\{(body|ts|header:[^}]*)\}/, template, fn _, token ->
         case token do
           "body" -> env.body
           "ts" -> ts
           "header:" <> name -> Envelope.header(env, name)
         end
       end)}
    end
  end

  defp header_values?(env, template) do
    ~r/\{header:([^}]*)\}/
    |> Regex.scan(template, capture: :all_but_first)
    |> Enum.reduce_while(:ok, fn [name], :ok ->
      case Envelope.header(env, name) do
        nil -> {:halt, {:error, :missing_signature}}
        _ -> {:cont, :ok}
      end
    end)
  end

  defp ts_value?(template, ts) do
    if is_nil(ts) and String.contains?(template, "{ts}") do
      {:error, :missing_signature}
    else
      :ok
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

  defp raw_key(secret, :raw), do: {:ok, secret}

  defp raw_key(secret, :whsec_base64),
    do: secret |> String.replace_prefix("whsec_", "") |> Base.decode64()

  defp encode(mac, :hex), do: Base.encode16(mac, case: :lower)
  defp encode(mac, :base64), do: Base.encode64(mac)
end
