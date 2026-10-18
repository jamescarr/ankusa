defmodule Ankusa.Verifier do
  @moduledoc """
  Signature and timestamp verification. HMAC is microseconds, so it runs inline
  on the hot path, before the ack.

  Adapters implement `verify/2` over the raw envelope (the exact bytes matter).
  `opts` carry the per-source secret and any tolerance window
  (`Ankusa.Verifier.check_timestamp/2`).

  The verifiers in core:

    * `Ankusa.Verifier.None` — accepts everything;
    * `Ankusa.Verifier.Hmac` — a shared-secret signature over the request;
    * `Ankusa.Verifier.Asymmetric` — a public-key (Ed25519, RSA) signature over
      the request; the node holds no secret;
    * `Ankusa.Verifier.Token` — a static bearer, basic, header or query
      credential;
    * `Ankusa.Verifier.ClientCert` — the client certificate of the edge
      listener's mutual-TLS handshake.

  The edge puts request context some verifiers need into `opts`, for those
  verifiers only: `:peer_cert` (the DER client certificate, or `nil`) for
  `ClientCert` and `:query` (the raw query string) for a `Token` in the query.

  ## Failure vocabulary

  Adapters return `{:error, reason}`, which the source's `on_verify_failure`
  policy turns into `:reject`, `:quarantine`, or `:accept_flag`. The reasons are
  deliberately shared so telemetry and dashboards can count them without
  knowing which provider produced them:

    * `:missing_signature` — a required header, credential or certificate is
      absent
    * `:malformed_signature` — present, but not parseable
    * `:no_match` — nothing matched the expected digest, key, token or pin
    * `:timestamp_out_of_tolerance` — outside the replay window
    * `:bad_secret` — the configured secret can't be used as a key
      (`Verifier.Hmac`, `Verifier.Token`)
    * `:bad_key` — a configured public key can't be used (`Verifier.Asymmetric`)
    * `:bad_pin` — a certificate source pins nothing usable
      (`Verifier.ClientCert`)
    * `:bad_scheme` — an unknown or missing scheme name (`Verifier.Hmac`,
      `Verifier.Asymmetric`), or an unknown `:location` (`Verifier.Token`)
    * `:bad_tolerance` — `:tolerance` is not an integer from 0 to
      `max_tolerance_seconds/0` (`Verifier.Hmac`, `Verifier.Asymmetric`)
  """

  require Logger

  alias Ankusa.Envelope

  @default_tolerance_seconds 300
  @max_tolerance_seconds 3600

  @doc """
  Verify an envelope. Return `:ok` to accept, or `{:error, reason}` to trigger
  the source's `on_verify_failure` policy (`:reject | :quarantine | :accept_flag`).
  """
  @callback verify(Envelope.t(), opts :: keyword()) :: :ok | {:error, term()}

  @doc """
  Check `opts` without a request: `:ok`, or the `{:error, reason}` `verify/2`
  would return for every hook. Optional — `validate_config!/1` runs it, at boot,
  for every source declared in config whose verifier implements it, and the
  image's loader runs it for every source an admin API write submits.
  """
  @callback validate_opts(opts :: keyword()) :: :ok | {:error, atom()}

  @doc """
  Human name of the scheme for telemetry; `nil` when the verifier has no named
  scheme. Optional — verifiers that only implement `verify/2` are attributed by
  their module name instead.
  """
  @callback scheme_name(opts :: keyword()) :: String.t() | nil

  @optional_callbacks scheme_name: 1, validate_opts: 1

  @doc """
  The widest replay window, in seconds, a verifier accepts: an hour. Beyond that
  a signed hook stays replayable long enough that the window protects nothing.
  """
  @spec max_tolerance_seconds() :: pos_integer()
  def max_tolerance_seconds, do: @max_tolerance_seconds

  @doc """
  Whether the verifier `{mod, opts}` checks signatures with a weak hash (SHA-1).
  Only `Ankusa.Verifier.Hmac` can; see `Ankusa.Verifier.Hmac.weak_hash?/1`.
  """
  @spec weak_hash?(module(), keyword()) :: boolean()
  def weak_hash?(Ankusa.Verifier.Hmac, opts), do: Ankusa.Verifier.Hmac.weak_hash?(opts)
  def weak_hash?(_mod, _opts), do: false

  @doc "Constant-time compare of two binaries of equal length."
  @spec secure_compare(binary(), binary()) :: boolean()
  def secure_compare(a, b) when is_binary(a) and is_binary(b) do
    byte_size(a) == byte_size(b) and :crypto.hash_equals(a, b)
  end

  @doc """
  The scheme name a verification is attributed to: `mod.scheme_name(opts)`
  when the verifier implements it, else the module name.
  """
  @spec scheme_name(module(), keyword()) :: String.t() | nil
  def scheme_name(mod, opts) do
    # `function_exported?/3` is false for a module nothing has loaded yet, which
    # would label the first verification with the module name instead of the
    # scheme.
    if Code.ensure_loaded?(mod) and function_exported?(mod, :scheme_name, 1),
      do: mod.scheme_name(opts),
      else: inspect(mod)
  end

  @doc """
  Whether `opts[:tolerance]` is a usable replay window: absent (the default
  window), or an integer from 0 to `max_tolerance_seconds/0`. A window wider
  than an hour is not a window, it is the absence of one. Shared by every
  verifier that checks a timestamp.
  """
  @spec check_tolerance(keyword()) :: :ok | {:error, :bad_tolerance}
  def check_tolerance(opts) do
    case opts[:tolerance] do
      nil -> :ok
      t when is_integer(t) and t >= 0 and t <= @max_tolerance_seconds -> :ok
      _ -> {:error, :bad_tolerance}
    end
  end

  @doc """
  Refuse to boot with a source declared in config whose verifier would fail
  every hook, which `on_verify_failure` then quarantines, flags or rejects —
  silently, at runtime. Every verifier that implements `validate_opts/1` is
  asked: an `Ankusa.Verifier.Hmac` or `Ankusa.Verifier.Token` secret that is
  missing, empty or undecodable, an `Ankusa.Verifier.Asymmetric` public key that
  is not a key, an `Ankusa.Verifier.ClientCert` that pins nothing, an unknown
  scheme, or a `:tolerance` outside `0..max_tolerance_seconds/0`. A
  `ClientCert` source also needs `listener.tls` with `verify: :verify_peer`:
  no other listener ever receives a client certificate. Sources a writable store
  holds are checked when they are written. Raises `ArgumentError` naming the
  source. A source that verifies with SHA-1
  (`Ankusa.Verifier.Hmac.weak_hash?/1`) boots, with one warning.
  """
  @spec validate_config!(Ankusa.Config.t()) :: :ok
  def validate_config!(%Ankusa.Config{} = config) do
    config
    |> Ankusa.Queue.static_sources()
    |> Enum.each(fn
      {id, %Ankusa.Source{verifier: {mod, opts}}} -> validate_source!(config, id, mod, opts)
      _other -> :ok
    end)
  end

  defp validate_source!(config, id, mod, opts) do
    if Code.ensure_loaded?(mod) and function_exported?(mod, :validate_opts, 1) do
      case mod.validate_opts(opts) do
        :ok ->
          warn_weak_hash(id, mod, opts)
          check_listener!(config, id, mod)

        {:error, reason} ->
          raise ArgumentError, "source #{id}: " <> invalid_message(reason, opts)
      end
    end

    :ok
  end

  defp invalid_message(:bad_scheme, _opts), do: "verifier scheme is missing or unknown"
  defp invalid_message(:bad_secret, _opts), do: "verifier secret is missing or undecodable"

  defp invalid_message(:bad_key, _opts),
    do: "verifier public_key is missing or is not a key of the scheme's algorithm"

  defp invalid_message(:bad_pin, _opts),
    do:
      "verifier pins no certificate: set fingerprints (SHA-256, 64 hex characters) " <>
        "or subjects (dns:, email:, uri:, ip: or cn: identities)"

  defp invalid_message(:bad_tolerance, opts) do
    "verifier tolerance must be an integer from 0 to " <>
      "#{@max_tolerance_seconds} seconds, got: #{inspect(opts[:tolerance])}"
  end

  defp invalid_message(reason, _opts), do: "verifier options are invalid: #{inspect(reason)}"

  defp check_listener!(config, id, Ankusa.Verifier.ClientCert) do
    unless match?(%{verify: :verify_peer}, config.listener.tls) do
      raise ArgumentError,
            "source #{id}: client_cert verification needs listener.tls with verify: " <>
              "verify_peer, and cacertfile: no client certificate reaches a listener that " <>
              "does not ask for one"
    end
  end

  defp check_listener!(_config, _id, _mod), do: :ok

  defp warn_weak_hash(id, mod, opts) do
    if weak_hash?(mod, opts) do
      Logger.warning(
        "[ankusa] source #{id} verifies with SHA-1, which is no longer collision " <>
          "resistant: use sha256 or sha512 if the provider can sign with it. Every " <>
          "verification counts toward ankusa_verifier_weak_hash_total."
      )
    end

    :ok
  end

  @doc """
  Whether `source` may run under `config`: `{:error, message}` when it is shared
  (`tenant_id` `"default"`), has no verifier (`Ankusa.Verifier.None`), does not
  set `trust_url_tenant: true`, and the edge takes the tenant from the request
  (any `Ankusa.RouteResolver` but `Ankusa.RouteResolver.Path`). Such a source
  would file hooks under whatever tenant the request names, and spend that
  tenant's rate limit, with nothing to say the sender may. The verdict depends
  on the config alone, never on the node's roles, so every node reading one
  config agrees.
  """
  @spec check_shared(Ankusa.Config.t(), Ankusa.Source.t()) :: :ok | {:error, String.t()}
  def check_shared(%Ankusa.Config{} = config, %Ankusa.Source{} = source) do
    if refused?(config, source) do
      {:error,
       "source #{source.id} is shared (tenant_id \"default\") and has no verifier, so it " <>
         "would file hooks under whatever tenant the request names: give it the provider's " <>
         "verifier, a tenant of its own, or trust_url_tenant: true to accept that " <>
         "(docs/multi-tenancy.md#tenant-scoping-what-tenant_id-actually-does)"}
    else
      :ok
    end
  end

  @doc """
  Refuse to boot with a source declared in config that `check_shared/2`
  refuses. Sources a writable store holds are checked when they are written
  (and a stored one written before the check existed is named by
  `warn_stored_shared/2`). Raises `ArgumentError` naming the source.
  """
  @spec validate_shared!(Ankusa.Config.t()) :: :ok
  def validate_shared!(%Ankusa.Config{} = config) do
    config
    |> Ankusa.Queue.static_sources()
    |> Enum.each(fn {_id, source} ->
      with {:error, message} <- check_shared(config, source) do
        raise ArgumentError, message
      end
    end)
  end

  @doc """
  Log one warning per stored (API-managed) source that `check_shared/2`
  refuses. Such a source was written before writes were checked: it keeps
  serving as it did, and only a write that still lacks `trust_url_tenant: true`
  is refused.
  """
  @spec warn_stored_shared(Ankusa.Config.t(), [Ankusa.Source.t()]) :: :ok
  def warn_stored_shared(%Ankusa.Config{} = config, sources) do
    for source <- sources, refused?(config, source) do
      Logger.warning(
        "[ankusa] stored source #{source.id} is shared (tenant_id \"default\") and has no " <>
          "verifier: it still files hooks under whatever tenant the request names, but a " <>
          "write without trust_url_tenant: true is refused now. Save it with the provider's " <>
          "verifier, or with trust_url_tenant: true " <>
          "(docs/multi-tenancy.md#tenant-scoping-what-tenant_id-actually-does)."
      )
    end

    :ok
  end

  defp refused?(config, source) do
    match?(
      %Ankusa.Source{
        tenant_id: "default",
        verifier: {Ankusa.Verifier.None, _},
        trust_url_tenant: false
      },
      source
    ) and not match?({Ankusa.RouteResolver.Path, _}, config.route_resolver)
  end

  @doc """
  Check a Unix-seconds timestamp from a signature header against a tolerance
  window.

  Every verifier that carries a signed timestamp needs this, so the window
  semantics are defined once instead of per adapter. `opts` may set `:tolerance`
  in seconds (default #{@default_tolerance_seconds}, i.e. ±5 minutes), and
  `:now` (Unix seconds) to judge the window against a fixed instant instead of
  the clock: `Ankusa.Dispatch.Replayer` sets it to the hook's receive time when
  it re-verifies a quarantined hook, so a release hours later holds the hook to
  the window it arrived in, not one it can no longer meet.
  """
  @spec check_timestamp(String.t(), keyword()) :: :ok | {:error, atom()}
  def check_timestamp(ts, opts) do
    tolerance = Keyword.get(opts, :tolerance, @default_tolerance_seconds)
    now = Keyword.get_lazy(opts, :now, fn -> System.system_time(:second) end)

    case Integer.parse(ts) do
      {ts_int, _} ->
        if abs(now - ts_int) <= tolerance do
          :ok
        else
          {:error, :timestamp_out_of_tolerance}
        end

      :error ->
        {:error, :malformed_signature}
    end
  end
end
