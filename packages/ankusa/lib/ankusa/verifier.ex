defmodule Ankusa.Verifier do
  @moduledoc """
  Signature and timestamp verification. HMAC is microseconds, so it runs inline
  on the hot path, before the ack.

  Adapters implement `verify/2` over the raw envelope (the exact bytes matter).
  `opts` carry the per-source secret and any tolerance window
  (`Ankusa.Verifier.check_timestamp/2`).

  ## Failure vocabulary

  Adapters return `{:error, reason}`, which the source's `on_verify_failure`
  policy turns into `:reject`, `:quarantine`, or `:accept_flag`. The reasons are
  deliberately shared so telemetry and dashboards can count them without
  knowing which provider produced them:

    * `:missing_signature` — a required header is absent
    * `:malformed_signature` — present, but not parseable
    * `:no_match` — nothing matched the expected digest
    * `:timestamp_out_of_tolerance` — outside the replay window
    * `:bad_secret` — the configured secret can't be used as a key
    * `:bad_scheme` — an unknown or missing scheme name (`Verifier.Hmac`)
  """

  require Logger

  alias Ankusa.Envelope

  @default_tolerance_seconds 300

  @doc """
  Verify an envelope. Return `:ok` to accept, or `{:error, reason}` to trigger
  the source's `on_verify_failure` policy (`:reject | :quarantine | :accept_flag`).
  """
  @callback verify(Envelope.t(), opts :: keyword()) :: :ok | {:error, term()}

  @doc """
  Human name of the scheme for telemetry; `nil` when the verifier has no named
  scheme. Optional — verifiers that only implement `verify/2` are attributed by
  their module name instead.
  """
  @callback scheme_name(opts :: keyword()) :: String.t() | nil

  @optional_callbacks scheme_name: 1

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
  Refuse to boot with a source declared in config whose `Ankusa.Verifier.Hmac`
  secret is missing, empty or undecodable (or whose scheme is unknown): such a
  source would fail every hook with `:bad_secret`, which `on_verify_failure`
  then quarantines, flags or rejects — silently, at runtime. Sources a
  writable store holds are checked when they are written. Raises
  `ArgumentError` naming the source.
  """
  @spec validate_config!(Ankusa.Config.t()) :: :ok
  def validate_config!(%Ankusa.Config{} = config) do
    config
    |> Ankusa.Queue.static_sources()
    |> Enum.each(fn
      {id, %Ankusa.Source{verifier: {Ankusa.Verifier.Hmac, opts}}} ->
        case Ankusa.Verifier.Hmac.validate_opts(opts) do
          :ok ->
            :ok

          {:error, :bad_scheme} ->
            raise ArgumentError, "source #{id}: verifier scheme is missing or unknown"

          {:error, :bad_secret} ->
            raise ArgumentError, "source #{id}: verifier secret is missing or undecodable"
        end

      _other ->
        :ok
    end)
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
