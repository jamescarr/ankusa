defmodule Ankusa.ClaimCheck do
  @moduledoc """
  Claim check: store payloads too large to ride inline in a queue message, and
  hand each one back by reference.

  Ankusa's dispatch pipeline is the only writer. It writes directly to the
  instance's `Ankusa.BlobStore`, packing every claim a WAL read batch needs for
  one tenant into one object (`Ankusa.ClaimCheck.Pack`), so one `PUT` covers
  many claims. Each claim gets an `Ankusa.ClaimCheck.Ref`: a single URN naming
  the object, the claim's byte range inside it, and its sha256.

  Readers either call `redeem/2` in-process, fetch
  `GET /v1/claims/<tenant>/<object_id>/<offset>/<length>` from a
  `:claim_check`-role node (`Ankusa.ClaimCheck.Router`), or read the objects
  straight from the bucket. This module does no authentication or
  authorization; whatever fronts the gateway decides who may read what.

  Objects are written once under a freshly minted id and never rewritten, so
  every read is safe to cache forever.

  Permanent errors (`:not_found`, `:integrity_mismatch`, `:invalid_ref`,
  `:invalid_tenant`, `:invalid_id`, `:invalid_range`) mean retrying won't help —
  callers should dead-letter. `{:unavailable, reason}` is transient.
  """

  alias Ankusa.ClaimCheck.{Pack, Ref}
  alias Ankusa.{BlobStore, Config, Telemetry, UUIDv7}

  # ZIP without ZIP64 caps an archive at 65,535 entries (one is the manifest)
  # and 4 GiB. Packs split well inside both regardless of pack_max_bytes.
  @max_pack_claims 65_000
  @max_pack_bytes 0xFFFFFFFF - 16 * 1024 * 1024

  @type item :: %{
          required(:id) => String.t(),
          required(:body) => binary(),
          optional(:tenant_id) => String.t(),
          optional(:content_type) => String.t() | nil,
          optional(:received_at) => integer() | nil
        }

  @type reason ::
          :invalid_ref
          | :invalid_tenant
          | :invalid_id
          | :invalid_range
          | :not_found
          | :integrity_mismatch
          | {:unavailable, term()}

  # Uploads are network-bound, so more than the scheduler count is fine; the
  # cap exists so one huge batch can't open an unbounded number of sockets.
  @upload_concurrency 16

  # A manifest row per claim: its id, digest, JSON keys, and numbers. The
  # content type is added on top. Only used to size packs against the cap.
  @manifest_row_bytes 200

  @doc """
  Store `items` for one tenant as a single pack object, with one `PUT`.

  Returns a ref per item id once the write is durable. opts:

    * `:object_id` — the pack's UUIDv7; default a freshly minted one. A
      one-claim pack can reuse its envelope id so a retried check-in rewrites
      the same object instead of orphaning one.
  """
  @spec check_in(atom(), String.t(), [item()], keyword()) ::
          {:ok, %{String.t() => Ref.t()}} | {:error, reason()}
  def check_in(instance, tenant_id, [_ | _] = items, opts \\ []) do
    object_id = Keyword.get_lazy(opts, :object_id, &UUIDv7.generate/0)
    started = System.monotonic_time()

    result =
      with :ok <- Ref.validate_tenant(tenant_id),
           :ok <- Ref.validate_object_id(object_id) do
        claims = Enum.map(items, &claim/1)
        {data, placements} = Pack.build(claims)
        key = Ref.object_key(tenant_id, object_id)

        case put(instance, key, data) do
          :ok ->
            {:ok, refs(tenant_id, object_id, claims, placements)}

          {:error, reason} ->
            {:error, {:unavailable, reason}}
        end
      end

    Telemetry.emit(
      [:claim_check, :check_in],
      %{
        duration: System.monotonic_time() - started,
        size: Enum.reduce(items, 0, &(byte_size(&1.body) + &2)),
        claims: length(items)
      },
      %{
        instance: instance,
        tenant_id: tenant_id,
        object_id: object_id,
        result: result_tag(result)
      }
    )

    result
  end

  @doc """
  Store many items, possibly for many tenants: group them by tenant, split each
  group into packs of at most `claim_check.pack_max_bytes`, and upload the
  packs concurrently. A body larger than the cap gets a pack of its own.

  Returns a result per item id. A failed pack fails only its own items.
  """
  @spec check_in_batch(atom(), [item()]) :: %{String.t() => {:ok, Ref.t()} | {:error, reason()}}
  def check_in_batch(_instance, []), do: %{}

  def check_in_batch(instance, items) do
    %Config{claim_check: %{pack_max_bytes: cap}} = Ankusa.config(instance)

    items
    |> Enum.group_by(& &1.tenant_id)
    |> Enum.flat_map(fn {tenant_id, group} ->
      group |> split(cap) |> Enum.map(&{tenant_id, &1})
    end)
    |> Task.async_stream(
      fn {tenant_id, pack} -> {pack, check_in(instance, tenant_id, pack)} end,
      max_concurrency: @upload_concurrency,
      timeout: :infinity
    )
    |> Enum.reduce(%{}, fn {:ok, {pack, result}}, acc ->
      Enum.reduce(pack, acc, fn item, acc ->
        Map.put(acc, item.id, item_result(result, item.id))
      end)
    end)
  end

  @doc """
  Redeem a ref (a `%Ref{}` or its URN) for its bytes: one ranged read, then an
  end-to-end sha256 check. Integrity is never trusted from the store.
  """
  @spec redeem(atom(), Ref.t() | String.t()) :: {:ok, binary()} | {:error, reason()}
  def redeem(instance, urn) when is_binary(urn) do
    with {:ok, ref} <- Ref.parse(urn), do: redeem(instance, ref)
  end

  def redeem(instance, %Ref{} = ref) do
    started = System.monotonic_time()

    result =
      with {:ok, bin} <- read(instance, ref.tenant_id, ref.object_id, ref.offset, ref.length) do
        if sha256(bin) == ref.sha256, do: {:ok, bin}, else: {:error, :integrity_mismatch}
      end

    Telemetry.emit(
      [:claim_check, :redeem],
      %{duration: System.monotonic_time() - started, size: ref.length},
      %{
        instance: instance,
        tenant_id: ref.tenant_id,
        object_id: ref.object_id,
        result: result_tag(result)
      }
    )

    result
  end

  @doc """
  Read `length` bytes at `offset` in an object, with no digest check: the
  gateway's byte transport. A range that runs past the end of the object is
  `:invalid_range`; a missing object is `:not_found`.
  """
  @spec read(atom(), String.t(), String.t(), non_neg_integer(), pos_integer()) ::
          {:ok, binary()} | {:error, reason()}
  def read(instance, tenant_id, object_id, offset, length) do
    with :ok <- Ref.validate_tenant(tenant_id),
         :ok <- Ref.validate_object_id(object_id) do
      key = Ref.object_key(tenant_id, object_id)

      case BlobStore.get_range(instance, key, offset, length) do
        {:ok, bin} when byte_size(bin) == length -> {:ok, bin}
        {:ok, _short} -> {:error, :invalid_range}
        {:error, :not_found} -> {:error, :not_found}
        # LocalFS reads past the end as :eof; S3 and GCS answer 416.
        {:error, :eof} -> {:error, :invalid_range}
        {:error, {:status, 416, _body}} -> {:error, :invalid_range}
        {:error, reason} -> {:error, {:unavailable, reason}}
      end
    end
  end

  @doc """
  Validate `config.claim_check` at boot. Raises rather than surfacing a
  misconfiguration at runtime:

    * `claim_check.pack_max_bytes` must be a positive integer
    * `claim_check.retention_days` set with a non-`LocalFS` blob store (the
      sweeper only ever covers `LocalFS`; S3/GCS need a bucket lifecycle rule)
    * `max_body_bytes` of 4 GiB or more: a pack can't hold a body that large
  """
  @spec validate_config!(Config.t()) :: :ok
  def validate_config!(%Config{claim_check: cc} = config) do
    unless is_integer(cc.pack_max_bytes) and cc.pack_max_bytes > 0 do
      raise ArgumentError,
            "claim_check.pack_max_bytes must be a positive integer, got #{inspect(cc.pack_max_bytes)}"
    end

    if config.max_body_bytes >= 0xFFFFFFFF do
      raise ArgumentError,
            "max_body_bytes must be under 4 GiB: a claim pack can't hold a larger body"
    end

    if cc.retention_days != nil do
      case config.storage.blob_store do
        {Ankusa.BlobStore.LocalFS, _} ->
          :ok

        {other, _} ->
          raise ArgumentError,
                "claim_check.retention_days is set but the blob store is #{inspect(other)} — " <>
                  "the LocalFS sweeper doesn't cover it. Use a bucket lifecycle rule on the " <>
                  "claims/ prefix instead, and leave retention_days nil."
      end
    end

    :ok
  end

  # ── internals ─────────────────────────────────────────────────────────────

  defp claim(item) do
    %{
      id: item.id,
      body: item.body,
      sha256: sha256(item.body),
      content_type: Map.get(item, :content_type),
      received_at: Map.get(item, :received_at)
    }
  end

  defp refs(tenant_id, object_id, claims, placements) do
    claims
    |> Enum.zip(placements)
    |> Map.new(fn {claim, placement} ->
      {claim.id,
       %Ref{
         tenant_id: tenant_id,
         object_id: object_id,
         offset: placement.offset,
         length: placement.length,
         sha256: claim.sha256
       }}
    end)
  end

  # Greedy, order-preserving: start a new pack when the next item would push
  # this one past the cap (or past what a ZIP can hold). A single oversized
  # item still gets a pack of its own.
  defp split(items, cap) do
    cap = min(cap, @max_pack_bytes)

    {packs, current, _size, _count} =
      Enum.reduce(items, {[], [], 0, 0}, fn item, {packs, current, size, count} ->
        cost = cost(item)

        if current != [] and (size + cost > cap or count == @max_pack_claims) do
          {[Enum.reverse(current) | packs], [item], cost, 1}
        else
          {packs, [item | current], size + cost, count + 1}
        end
      end)

    Enum.reverse([Enum.reverse(current) | packs])
  end

  defp cost(item) do
    content_type = Map.get(item, :content_type) || ""

    byte_size(item.body) + Pack.entry_overhead(item.id) + @manifest_row_bytes +
      byte_size(content_type)
  end

  # A blob store is external code: a crash in its write is an unavailable
  # store, not a crashed caller.
  defp put(instance, key, data) do
    BlobStore.put(instance, key, data)
  rescue
    error -> {:error, error}
  end

  defp item_result({:ok, refs}, id), do: {:ok, Map.fetch!(refs, id)}
  defp item_result({:error, reason}, _id), do: {:error, reason}

  defp sha256(bin), do: Base.encode16(:crypto.hash(:sha256, bin), case: :lower)

  defp result_tag({:ok, _}), do: :ok
  defp result_tag({:error, reason}), do: reason
end
