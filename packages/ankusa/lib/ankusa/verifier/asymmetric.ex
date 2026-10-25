defmodule Ankusa.Verifier.Asymmetric do
  @moduledoc ~S"""
  Public-key signature verifier: the provider signs with a private key only it
  holds, and Ankusa holds nothing secret, only the provider's *public* key. A
  leak of the node's config cannot be used to forge a hook, which is the point
  over `Ankusa.Verifier.Hmac`.

  `opts`:

    * `:scheme` — a preset atom (`:standard_webhooks_asym`, resolved through
      `Ankusa.Verifier.Schemes.fetch_asymmetric/1`) or an inline
      `%Ankusa.Verifier.Asymmetric.Scheme{}`. The wire half (which header, how it
      parses, what is signed, which timestamp) is `Ankusa.Verifier.Template`,
      the same engine `Hmac` runs on.
    * `:public_key` — the provider's key, or a list of them (current first;
      every key is tried, so a rotation is `public_key: [new, old]`). A key is
      one of
        * `whpk_<base64>`: the Standard Webhooks form, the 32 raw bytes of an
          Ed25519 public key;
        * the bare base64 of those 32 bytes;
        * a PEM `PUBLIC KEY` (SubjectPublicKeyInfo) or, for RSA, `RSA PUBLIC KEY`
          (PKCS#1) block;
        * for RSA, the bare base64 of the DER of either.
      A missing key, an empty list, more than 8 keys, or any element that
      does not decode to a key of the scheme's algorithm fails every hook with
      `:bad_key`, and a source declared in config refuses to boot.
    * `:tolerance` — the replay window in seconds, as `Ankusa.Verifier.Hmac`.

  Algorithms (`Scheme.algorithm`): `:ed25519` (RFC 8032, the signature is 64
  bytes) and `:rsa_sha256` (RSASSA-PKCS1-v1_5 over SHA-256; a modulus under 2048
  bits or an exponent under 65537 is `:bad_key`). Signatures are base64 or hex
  (`Scheme.encoding`).

  The request path stays cheap for a forged flood: a candidate whose decoded
  length is not the key's signature length is dropped before any public-key
  operation, and at most 8 candidates are tried.
  """

  @behaviour Ankusa.Verifier

  alias Ankusa.Envelope
  alias Ankusa.Verifier
  alias Ankusa.Verifier.Asymmetric.Scheme
  alias Ankusa.Verifier.Template

  # Both bound the public-key operations one unauthenticated request can cause:
  # keys × candidates, at roughly 50-100 µs each.
  @max_keys 8
  @max_candidates 8
  @min_rsa_bits 2048
  @min_rsa_exponent 65_537

  # DER of an Ed25519 SubjectPublicKeyInfo up to the 32-byte key.
  @ed25519_spki <<0x30, 0x2A, 0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70, 0x03, 0x21, 0x00>>

  defmodule Scheme do
    @moduledoc """
    Declarative description of one provider's public-key signature scheme: the
    wire fields of `Ankusa.Verifier.Hmac.Scheme` (`signature_header`, `parse`,
    `sig_prefix`, `sig_key`, `version`, `signed`, `timestamp`; see
    `Ankusa.Verifier.Template`) with `algorithm` and `encoding` in place of
    `hash`, `encoding` and `secret_decode`.
    """

    @enforce_keys [:signature_header]
    defstruct [
      :signature_header,
      parse: :whole,
      sig_prefix: nil,
      sig_key: nil,
      version: nil,
      signed: "{body}",
      algorithm: :ed25519,
      encoding: :base64,
      timestamp: nil
    ]

    @type algorithm :: :ed25519 | :rsa_sha256
    @type t :: %__MODULE__{
            signature_header: String.t(),
            parse: :whole | :csv_pairs | :space_versions,
            sig_prefix: String.t() | nil,
            sig_key: String.t() | nil,
            version: String.t() | nil,
            signed: String.t(),
            algorithm: algorithm(),
            encoding: :hex | :base64,
            timestamp: nil | {:header, String.t()} | {:sig, String.t()}
          }
  end

  @typep key ::
           {:ed25519, binary()}
           | {:rsa, e :: pos_integer(), n :: pos_integer(), bytes :: pos_integer()}

  @impl true
  @spec verify(Envelope.t(), keyword()) :: :ok | {:error, term()}
  def verify(%Envelope{} = env, opts) do
    with {:ok, scheme, template} <- resolve_scheme(opts),
         :ok <- Verifier.check_tolerance(opts),
         {:ok, candidates, sig_ts} <- Template.candidates(env, scheme),
         {:ok, ts} <- Template.timestamp(env, scheme, sig_ts, opts),
         {:ok, signed} <- Template.signed_iodata(env, template, ts),
         {:ok, keys} <- public_keys(opts, scheme) do
      # One flat message for every key and candidate: the primitive would flatten
      # an iolist again on each call.
      signed = Template.to_binary(signed)
      sigs = decode_candidates(candidates, scheme.encoding, Enum.map(keys, &sig_size/1))

      matched? =
        Enum.any?(keys, fn key ->
          Enum.any?(sigs, &(byte_size(&1) == sig_size(key) and verify_one(key, signed, &1)))
        end)

      if matched?, do: :ok, else: {:error, :no_match}
    end
  end

  @doc "Human name of the scheme for telemetry: the preset name, or `\"custom\"`."
  @impl true
  @spec scheme_name(keyword()) :: String.t()
  def scheme_name(opts) do
    case opts[:scheme] do
      atom when is_atom(atom) and not is_nil(atom) -> Atom.to_string(atom)
      _ -> "custom"
    end
  end

  @doc """
  Check `opts` the way `verify/2` would use them, without a request: the scheme
  resolves, `:tolerance` is in range and every public key decodes. `:ok`, or
  the error `verify/2` would return for every hook.
  """
  @impl true
  @spec validate_opts(keyword()) :: :ok | {:error, :bad_scheme | :bad_tolerance | :bad_key}
  def validate_opts(opts) do
    with {:ok, scheme, _template} <- resolve_scheme(opts),
         :ok <- Verifier.check_tolerance(opts),
         {:ok, _keys} <- public_keys(opts, scheme) do
      :ok
    end
  end

  # The scheme and its `signed` template compiled: a preset's was compiled when
  # `Ankusa.Verifier.Schemes` was built, an inline scheme's is compiled here.
  defp resolve_scheme(opts) do
    case opts[:scheme] do
      atom when is_atom(atom) and not is_nil(atom) ->
        case Ankusa.Verifier.Schemes.fetch_asymmetric_compiled(atom) do
          {:ok, scheme, template} -> {:ok, scheme, template}
          :error -> {:error, :bad_scheme}
        end

      %Scheme{} = scheme ->
        try do
          scheme = Ankusa.Verifier.Schemes.validate!(scheme)
          {:ok, scheme, Template.compile(scheme.signed)}
        rescue
          ArgumentError -> {:error, :bad_scheme}
        end

      _ ->
        {:error, :bad_scheme}
    end
  end

  # An unusable key is `:bad_key` for every hook, never a narrower rotation
  # list: one bad element in the list is a config mistake.
  defp public_keys(opts, %Scheme{algorithm: algorithm}) do
    keys = opts |> Keyword.get(:public_key) |> List.wrap()

    decoded =
      if length(keys) <= @max_keys, do: Enum.map(keys, &decode_key(&1, algorithm)), else: []

    if decoded != [] and Enum.all?(decoded, &match?({:ok, _}, &1)),
      do: {:ok, Enum.map(decoded, fn {:ok, key} -> key end)},
      else: {:error, :bad_key}
  end

  @spec decode_key(term(), Scheme.algorithm()) :: {:ok, key()} | :error
  defp decode_key(key, algorithm) when is_binary(key) do
    key = String.trim(key)

    if String.starts_with?(key, "-----BEGIN"),
      do: pem_key(key, algorithm),
      else: bare_key(key, algorithm)
  rescue
    _ -> :error
  catch
    _, _ -> :error
  end

  defp decode_key(_key, _algorithm), do: :error

  defp bare_key("whpk_" <> b64, :ed25519), do: raw_ed25519(b64)
  defp bare_key("whpk_" <> _, _other), do: :error

  defp bare_key(b64, :ed25519), do: raw_ed25519(b64)

  defp bare_key(b64, :rsa_sha256) do
    with {:ok, der} <- Base.decode64(b64) do
      rsa_key(der, :SubjectPublicKeyInfo) |> or_else(fn -> rsa_key(der, :RSAPublicKey) end)
    end
  end

  defp or_else(:error, fun), do: fun.()
  defp or_else(ok, _fun), do: ok

  defp raw_ed25519(b64) do
    case Base.decode64(b64) do
      {:ok, <<_::binary-size(32)>> = pub} -> {:ok, {:ed25519, pub}}
      _ -> :error
    end
  end

  defp pem_key(pem, algorithm) do
    case :public_key.pem_decode(pem) do
      [{:SubjectPublicKeyInfo, der, :not_encrypted}] when algorithm == :ed25519 ->
        ed25519_spki(der)

      [{:SubjectPublicKeyInfo, der, :not_encrypted}] ->
        rsa_key(der, :SubjectPublicKeyInfo)

      [{:RSAPublicKey, der, :not_encrypted}] when algorithm == :rsa_sha256 ->
        rsa_key(der, :RSAPublicKey)

      _ ->
        :error
    end
  end

  defp ed25519_spki(<<@ed25519_spki::binary, pub::binary-size(32)>>), do: {:ok, {:ed25519, pub}}
  defp ed25519_spki(_der), do: :error

  defp rsa_key(der, type) do
    case :public_key.pem_entry_decode({type, der, :not_encrypted}) do
      {:RSAPublicKey, n, e} when is_integer(n) and is_integer(e) -> rsa_checked(n, e)
      _ -> :error
    end
  rescue
    _ -> :error
  catch
    _, _ -> :error
  end

  defp rsa_checked(n, e) do
    bytes = byte_size(:binary.encode_unsigned(n))

    if bytes * 8 >= @min_rsa_bits and e >= @min_rsa_exponent and rem(e, 2) == 1,
      do: {:ok, {:rsa, e, n, bytes}},
      else: :error
  end

  # Candidates that decode to a length no configured key signs with are dropped
  # first, for free, and only then are the first `@max_candidates` kept: junk of
  # the wrong length can never push the genuine signature out of the window.
  defp decode_candidates(candidates, encoding, sizes) do
    candidates
    |> Enum.flat_map(fn candidate ->
      case decode_signature(candidate, encoding) do
        {:ok, sig} -> if byte_size(sig) in sizes, do: [sig], else: []
        :error -> []
      end
    end)
    |> Enum.take(@max_candidates)
  end

  defp decode_signature(candidate, :base64) do
    case Base.decode64(candidate) do
      {:ok, _} = ok -> ok
      :error -> Base.decode64(candidate, padding: false)
    end
  end

  defp decode_signature(candidate, :hex), do: Base.decode16(candidate, case: :mixed)

  defp sig_size({:ed25519, _pub}), do: 64
  defp sig_size({:rsa, _e, _n, bytes}), do: bytes

  # `:crypto.verify/5` returns `false` for a bad signature and raises only for a
  # key it cannot load, which `decode_key/2` has already ruled out; a raise here
  # still must not take the request down, so it is a non-match.
  defp verify_one({:ed25519, pub}, signed, sig) do
    :crypto.verify(:eddsa, :none, signed, sig, [pub, :ed25519])
  rescue
    _ -> false
  end

  defp verify_one({:rsa, e, n, _bytes}, signed, sig) do
    :crypto.verify(:rsa, :sha256, signed, sig, [e, n])
  rescue
    _ -> false
  end
end
