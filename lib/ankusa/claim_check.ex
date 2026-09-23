defmodule Ankusa.ClaimCheck do
  @moduledoc """
  Claim Check gateway: check bytes in, get a `Ankusa.ClaimCheck.Ticket` back;
  present the ticket, get the bytes back. Every producer and consumer in the
  system — Elixir or not — shares this one contract; the storage engine and
  the network topology (in-process `BlobStore` access vs. an HTTP hop to a
  `:claim_check`-role node) stay hidden behind it.

  This module is both the behaviour and the instance-scoped facade, mirroring
  `Ankusa.BlobStore`. **The facade is smart; adapters are dumb transport.**
  `check_in/4`/`redeem/3` build/validate the ticket, enforce the size cap,
  verify end-to-end integrity, and emit telemetry — every adapter only moves
  bytes.

  Adapters:

    * `Ankusa.ClaimCheck.Direct` — calls the instance's configured `BlobStore`
      in-process. The right choice for any Ankusa node that already holds
      blob-store credentials, regardless of fleet size.
    * `Ankusa.ClaimCheck.Remote` — HTTP client against a `:claim_check`-role
      node. For callers that must not hold blob-store credentials (a non-BEAM
      consumer, or an Ankusa node deliberately isolated from them).

  Permanent errors (`:not_found`, `:integrity_mismatch`, `:invalid_tenant`,
  `:invalid_id`, `:too_large`, `:forbidden`, `:unsupported_ticket_version`)
  mean retrying won't help — callers should dead-letter. Transient errors
  (`{:unavailable, reason}`) should be retried. `:unauthorized` is a
  misconfiguration; treat it as permanent and alert.
  """

  alias Ankusa.ClaimCheck.Ticket
  alias Ankusa.{Config, Telemetry}

  @type meta :: %{
          required(:tenant_id) => String.t(),
          required(:id) => String.t(),
          optional(:content_type) => String.t() | nil
        }

  @type reason ::
          :invalid_tenant
          | :invalid_id
          | :too_large
          | :not_found
          | :integrity_mismatch
          | :unauthorized
          | :forbidden
          | :unsupported_ticket_version
          | {:unavailable, term()}

  @doc "Adapter callback: durably store the checked-in bytes under this ticket."
  @callback store(instance :: atom(), Ticket.t(), data :: iodata(), opts :: keyword()) ::
              :ok | {:error, reason()}

  @doc "Adapter callback: fetch the raw bytes for this ticket."
  @callback fetch(instance :: atom(), Ticket.t(), opts :: keyword()) ::
              {:ok, binary()} | {:error, reason()}

  @doc """
  Check bytes in. Builds and validates the ticket, enforces
  `claim_check.max_bytes`, stores via the configured (or `:adapter`-overridden)
  adapter, and returns the ticket only once the adapter reports a durable
  write.

  opts:

    * `:adapter`         — `{module, opts}`, overrides `config.claim_check.adapter`
    * `:expect_sha256`   — fail with `:integrity_mismatch` unless the computed
                            digest matches (used by `ClaimCheck.Router` to
                            verify an `x-ankusa-sha256` header against the body
                            it actually received)
  """
  @spec check_in(atom(), iodata(), meta(), keyword()) :: {:ok, Ticket.t()} | {:error, reason()}
  def check_in(instance, data, meta, opts \\ []) do
    %Config{claim_check: cc} = Ankusa.config(instance)
    {mod, adapter_opts} = Keyword.get(opts, :adapter, cc.adapter)
    bin = IO.iodata_to_binary(data)
    started = System.monotonic_time()

    result =
      with {:ok, ticket} <- Ticket.new(meta, bin),
           :ok <- check_size(ticket, cc.max_bytes),
           :ok <- check_expected_sha(ticket, opts),
           :ok <- mod.store(instance, ticket, bin, adapter_opts) do
        {:ok, ticket}
      end

    emit(:check_in, started, byte_size(bin), meta, mod, result)
    result
  end

  @doc """
  Redeem a ticket for its bytes. Fetches via the configured (or
  `:adapter`-overridden) adapter, then verifies `size` and `sha256`
  end-to-end — integrity is never trusted from the adapter or a remote
  server, only checked here at the redeemer.
  """
  @spec redeem(atom(), Ticket.t(), keyword()) :: {:ok, binary()} | {:error, reason()}
  def redeem(instance, %Ticket{} = ticket, opts \\ []) do
    %Config{claim_check: cc} = Ankusa.config(instance)
    {mod, adapter_opts} = Keyword.get(opts, :adapter, cc.adapter)
    started = System.monotonic_time()

    result =
      with {:ok, bin} <- mod.fetch(instance, ticket, adapter_opts),
           :ok <- verify_integrity(ticket, bin) do
        {:ok, bin}
      end

    emit(
      :redeem,
      started,
      ticket.size,
      %{tenant_id: ticket.tenant_id, id: ticket.id},
      mod,
      result
    )

    result
  end

  @doc """
  Validate `config.claim_check` at boot. Raises (fails boot fast) rather than
  surfacing a misconfiguration as a runtime 503/401 storm:

    * a `:claim_check`-role node configured with the `Remote` adapter (it
      would proxy to itself)
    * a `:claim_check`-role node with no `api_tokens` (never an open blob proxy)
    * `claim_check.max_bytes < max_body_bytes` on a `:dispatch` node (would
      dead-letter hooks the edge legitimately accepted)
    * `claim_check.retention_days` set with a non-`LocalFS` claim store (the
      sweeper only ever covers `LocalFS`; S3/GCS need a bucket lifecycle rule)
  """
  @spec validate_config!(Config.t()) :: :ok
  def validate_config!(%Config{claim_check: cc} = config) do
    if Config.role?(config, :claim_check) do
      case cc.adapter do
        {Ankusa.ClaimCheck.Remote, _} ->
          raise ArgumentError,
                "claim_check.adapter must not be Ankusa.ClaimCheck.Remote on a :claim_check-role node " <>
                  "(it would proxy the API to itself) — use Ankusa.ClaimCheck.Direct"

        _ ->
          :ok
      end

      if cc.api_tokens == %{} do
        raise ArgumentError,
              "claim_check.api_tokens is empty on a :claim_check-role node — refusing to boot an " <>
                "unauthenticated blob proxy. Configure at least one bearer token."
      end
    end

    if Config.role?(config, :dispatch) and cc.max_bytes < config.max_body_bytes do
      raise ArgumentError,
            "claim_check.max_bytes (#{cc.max_bytes}) is smaller than max_body_bytes " <>
              "(#{config.max_body_bytes}) — a dispatch node would dead-letter hooks the edge " <>
              "already accepted"
    end

    if cc.retention_days != nil do
      case cc.adapter do
        {Ankusa.ClaimCheck.Direct, direct_opts} ->
          blob_store = Keyword.get(direct_opts, :blob_store, config.storage.blob_store)

          case blob_store do
            {Ankusa.BlobStore.LocalFS, _} ->
              :ok

            {other, _} ->
              raise ArgumentError,
                    "claim_check.retention_days is set but the claim store is #{inspect(other)} — " <>
                      "the LocalFS sweeper doesn't cover it. Use a bucket lifecycle rule on the " <>
                      "claims/ prefix instead, and leave retention_days nil."
          end

        {other, _} ->
          raise ArgumentError,
                "claim_check.retention_days is set but claim_check.adapter is #{inspect(other)}, " <>
                  "not Direct — the sweeper only runs against a directly-held BlobStore"
      end
    end

    :ok
  end

  defp check_size(%Ticket{size: size}, max_bytes) when size > max_bytes, do: {:error, :too_large}
  defp check_size(_ticket, _max_bytes), do: :ok

  defp check_expected_sha(%Ticket{sha256: sha256}, opts) do
    case Keyword.get(opts, :expect_sha256) do
      nil -> :ok
      ^sha256 -> :ok
      _mismatch -> {:error, :integrity_mismatch}
    end
  end

  defp verify_integrity(%Ticket{} = ticket, bin) do
    cond do
      byte_size(bin) != ticket.size ->
        {:error, :integrity_mismatch}

      Base.encode16(:crypto.hash(:sha256, bin), case: :lower) != ticket.sha256 ->
        {:error, :integrity_mismatch}

      true ->
        :ok
    end
  end

  defp emit(op, started, size, meta, adapter, result) do
    duration = System.monotonic_time() - started

    Telemetry.emit(
      [:claim_check, op],
      %{duration: duration, size: size},
      Map.merge(meta, %{adapter: adapter, result: result_tag(result)})
    )
  end

  defp result_tag({:ok, _}), do: :ok
  defp result_tag({:error, reason}), do: reason
end
