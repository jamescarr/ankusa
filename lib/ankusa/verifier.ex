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
  Check a Unix-seconds timestamp from a signature header against a tolerance
  window.

  Every verifier that carries a signed timestamp needs this, so the window
  semantics are defined once instead of per adapter. `opts` may set `:tolerance`
  in seconds (default #{@default_tolerance_seconds}, i.e. ±5 minutes).
  """
  @spec check_timestamp(String.t(), keyword()) :: :ok | {:error, atom()}
  def check_timestamp(ts, opts) do
    tolerance = Keyword.get(opts, :tolerance, @default_tolerance_seconds)

    case Integer.parse(ts) do
      {ts_int, _} ->
        if abs(System.system_time(:second) - ts_int) <= tolerance do
          :ok
        else
          {:error, :timestamp_out_of_tolerance}
        end

      :error ->
        {:error, :malformed_signature}
    end
  end
end
