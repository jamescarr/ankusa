defmodule Ankusa.Verifier.ClientCert do
  @moduledoc """
  The sender is whoever holds the client certificate the edge listener's mutual
  TLS handshake accepted (`tls.verify: :verify_peer`, `Ankusa.Listener`), for
  providers and internal systems that authenticate by certificate rather than by
  signing the body. Like `Ankusa.Verifier.Token` it says who is calling, not
  what the body is.

  `opts`, at least one of:

    * `:fingerprints` — SHA-256 fingerprints of the DER certificates that may
      post to this source: 64 hex characters, any case, optional `:`
      separators (what `openssl x509 -fingerprint -sha256` prints). A pin
      holds whatever the listener's CA bundle says.
    * `:subjects` — identities a certificate may carry, in the form
      `Ankusa.Admin.Auth.MTLS.identities/1` produces: `dns:<name>`,
      `email:<address>`, `uri:<uri>`, `ip:<address>` (subject alternative
      names) or `cn:<common name>`. A subject pin trusts the listener's CA to
      have issued that identity only to the party meant: scope the listener's
      `cacertfile` to a CA that does, or pin fingerprints instead.

  A certificate matching either list is accepted. Neither list set, a
  malformed fingerprint or a subject without a known prefix is `:bad_pin` for
  every hook, and a source declared in config refuses to boot (also when the
  edge listener has no `verify: :verify_peer`: without it no certificate ever
  arrives, see `Ankusa.Verifier.validate_config!/1`).

  The certificate arrives as `opts[:peer_cert]`, the DER the TLS handshake
  accepted (`Plug.Conn.get_peer_data/1`'s `:ssl_cert`), put there by
  `Ankusa.Edge.Router` for a source with this verifier and nobody else. No
  certificate (a plain-HTTP listener, or a re-verification from the quarantine
  pen, which does not keep one) is `:missing_signature`; a certificate that
  cannot be parsed, with `:subjects` set and no fingerprint match, is
  `:malformed_signature`; a valid certificate nobody pinned is `:no_match`. Use
  `on_verify_failure: reject` for a certificate source: a quarantined hook
  cannot be released by replay.
  """

  @behaviour Ankusa.Verifier

  alias Ankusa.Admin.Auth.MTLS
  alias Ankusa.Envelope

  @identity_prefixes ~w(dns: email: uri: ip: cn:)

  @impl true
  @spec verify(Envelope.t(), keyword()) :: :ok | {:error, term()}
  def verify(%Envelope{}, opts) do
    with {:ok, fingerprints, subjects} <- pins(opts) do
      case opts[:peer_cert] do
        der when is_binary(der) and der != "" -> check(der, fingerprints, subjects)
        _ -> {:error, :missing_signature}
      end
    end
  end

  @doc "Human name of the scheme for telemetry."
  @impl true
  @spec scheme_name(keyword()) :: String.t()
  def scheme_name(_opts), do: "client_cert"

  @doc """
  Check `opts` the way `verify/2` would use them, without a request: at least
  one pin, every fingerprint 64 hex characters, every subject a known identity
  form. `:ok`, or `{:error, :bad_pin}`.
  """
  @impl true
  @spec validate_opts(keyword()) :: :ok | {:error, :bad_pin}
  def validate_opts(opts) do
    with {:ok, _fingerprints, _subjects} <- pins(opts), do: :ok
  end

  @doc "The normalised form of a fingerprint (lowercase hex, no separators), or `:error`."
  @spec normalize_fingerprint(term()) :: {:ok, String.t()} | :error
  def normalize_fingerprint(fingerprint) when is_binary(fingerprint) do
    hex = fingerprint |> String.replace(":", "") |> String.downcase(:ascii)

    if byte_size(hex) == 64 and hex =~ ~r/\A[0-9a-f]+\z/, do: {:ok, hex}, else: :error
  end

  def normalize_fingerprint(_other), do: :error

  defp check(der, fingerprints, subjects) do
    cond do
      Base.encode16(:crypto.hash(:sha256, der), case: :lower) in fingerprints ->
        :ok

      subjects == [] ->
        {:error, :no_match}

      true ->
        case MTLS.identities(der) do
          [] ->
            {:error, :malformed_signature}

          identities ->
            if Enum.any?(identities, &(&1 in subjects)), do: :ok, else: {:error, :no_match}
        end
    end
  end

  defp pins(opts) do
    fingerprints =
      opts |> Keyword.get(:fingerprints, []) |> List.wrap() |> Enum.map(&normalize_fingerprint/1)

    subjects = opts |> Keyword.get(:subjects, []) |> List.wrap()

    if (fingerprints != [] or subjects != []) and
         Enum.all?(fingerprints, &match?({:ok, _}, &1)) and Enum.all?(subjects, &identity?/1),
       do: {:ok, Enum.map(fingerprints, fn {:ok, hex} -> hex end), subjects},
       else: {:error, :bad_pin}
  end

  defp identity?(subject) when is_binary(subject) do
    Enum.any?(
      @identity_prefixes,
      &(String.starts_with?(subject, &1) and byte_size(subject) > byte_size(&1))
    )
  end

  defp identity?(_other), do: false
end
