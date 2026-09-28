defmodule Ankusa.ClaimCheck do
  @moduledoc """
  Claim check: store payloads too large to ride inline in a queue message, and
  hand each one back by reference.

  Ankusa's dispatch pipeline is the only writer. It writes directly to the
  instance's `Ankusa.BlobStore`, packing every claim a WAL read batch needs for
  one tenant into one object (`Ankusa.ClaimCheck.Pack`), so one `PUT` covers
  many claims. Each claim gets an `Ankusa.ClaimCheck.Ref` (a URN naming its
  tenant and claim id) and the sha256 of its bytes, which travel together as
  a `t:claim/0`.

  Readers either call `redeem/3` in-process, fetch
  `GET /v1/claims/<tenant>/<claim_id>` from a `:claim_check`-role node
  (`Ankusa.ClaimCheck.Router`), or read the objects straight from the bucket.
  This module does no authentication or authorization; whatever fronts the
  gateway decides who may read what.

  Objects are written once under a freshly minted id and never rewritten, so
  every read is safe to cache forever.

  Permanent errors (`:not_found`, `:integrity_mismatch`, `:invalid_ref`,
  `:invalid_tenant`, `:invalid_id`) mean retrying won't help —
  callers should dead-letter. `{:unavailable, reason}` is transient.
  """

  alias Ankusa.ClaimCheck.{Pack, Ref}
  alias Ankusa.{BlobStore, Config, Telemetry}

  # ZIP without ZIP64 caps an archive at 65,535 entries (two are the index and
  # the manifest) and 4 GiB; a claim id's 16 position bits cap it at 65,536. Packs split well inside both regardless of pack_max_bytes.
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
          | :not_found
          | :integrity_mismatch
          | {:unavailable, term()}

  @typedoc "A checked-in claim: its ref, and the lowercase hex sha256 of its bytes."
  @type claim :: %{ref: Ref.t(), sha256: String.t()}

  # Uploads are network-bound, so more than the scheduler count is fine; the
  # cap exists so one huge batch can't open an unbounded number of sockets.
  @upload_concurrency 16

  # A manifest row per claim: its ids, digest, JSON keys, and numbers. The
  # content type is added on top. Only used to size packs against the cap.
  @manifest_row_bytes 200

  @doc """
  Store `items` for one tenant as a single pack object, with one `PUT`.

  Returns a `t:claim/0` per item id once the write is durable. opts:

    * `:pack_id` — the pack's id (`Ankusa.ClaimCheck.Ref.pack_id/2`); default
      a freshly minted one. A one-claim pack can derive it from its envelope
      so a retried check-in rewrites the same object instead of orphaning one.
  """
  @spec check_in(atom(), String.t(), [item()], keyword()) ::
          {:ok, %{String.t() => claim()}} | {:error, reason()}
  def check_in(instance, tenant_id, [_ | _] = items, opts \\ []) do
    pack_id = Keyword.get_lazy(opts, :pack_id, &Ref.new_pack_id/0)
    started = System.monotonic_time()

    result =
      with :ok <- Ref.validate_tenant(tenant_id),
           :ok <- Ref.validate_pack_id(pack_id) do
        claims =
          items
          |> Enum.with_index()
          |> Enum.map(fn {item, index} -> claim(item, Ref.claim_id(pack_id, index)) end)

        {data, _placements} = Pack.build(claims)
        key = Ref.object_key(tenant_id, pack_id)

        case put(instance, key, data) do
          :ok ->
            {:ok, Map.new(claims, &{&1.id, checked_in(tenant_id, &1)})}

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
        pack_id: pack_id,
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
  @spec check_in_batch(atom(), [item()]) :: %{String.t() => {:ok, claim()} | {:error, reason()}}
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
  Redeem a ref (a `%Ref{}` or its URN) for its bytes and check them against
  `sha256`, the lowercase hex digest the queue message carried with the ref.
  Integrity is never trusted from the store.
  """
  @spec redeem(atom(), Ref.t() | String.t(), String.t()) :: {:ok, binary()} | {:error, reason()}
  def redeem(instance, urn, sha256) when is_binary(urn) do
    with {:ok, ref} <- Ref.parse(urn), do: redeem(instance, ref, sha256)
  end

  def redeem(instance, %Ref{} = ref, sha256) do
    started = System.monotonic_time()

    result =
      with {:ok, bin} <- read(instance, ref.tenant_id, ref.claim_id) do
        if sha256(bin) == sha256, do: {:ok, bin}, else: {:error, :integrity_mismatch}
      end

    size =
      case result do
        {:ok, bin} -> byte_size(bin)
        {:error, _} -> 0
      end

    Telemetry.emit(
      [:claim_check, :redeem],
      %{duration: System.monotonic_time() - started, size: size},
      %{
        instance: instance,
        tenant_id: ref.tenant_id,
        claim_id: ref.claim_id,
        result: result_tag(result)
      }
    )

    result
  end

  @doc """
  Read a claim's bytes, with no digest check: the gateway's byte transport.
  Two ranged reads: the pack's index up to the claim's row, then the claim.
  A claim id past the end of its pack's index, or a missing pack, is
  `:not_found`.
  """
  @spec read(atom(), String.t(), String.t()) :: {:ok, binary()} | {:error, reason()}
  def read(instance, tenant_id, claim_id) do
    with :ok <- Ref.validate_tenant(tenant_id),
         {:ok, pack_id, index} <- Ref.locate(claim_id) do
      key = Ref.object_key(tenant_id, pack_id)

      with {:ok, prefix} <- get_range(instance, key, 0, Pack.index_prefix_bytes(index)),
           {:ok, offset, length} <- locate(prefix, index),
           {:ok, bin} when byte_size(bin) == length <- get_range(instance, key, offset, length) do
        {:ok, bin}
      else
        {:ok, _short} -> {:error, :not_found}
        {:error, reason} -> {:error, reason}
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

  defp claim(item, claim_id) do
    %{
      claim_id: claim_id,
      id: item.id,
      body: item.body,
      sha256: sha256(item.body),
      content_type: Map.get(item, :content_type),
      received_at: Map.get(item, :received_at)
    }
  end

  defp checked_in(tenant_id, claim),
    do: %{ref: %Ref{tenant_id: tenant_id, claim_id: claim.claim_id}, sha256: claim.sha256}

  defp locate(prefix, index) do
    case Pack.locate(prefix, index) do
      {:ok, offset, length} -> {:ok, offset, length}
      :error -> {:error, :not_found}
    end
  end

  # A read that starts past the end of an object comes back empty (LocalFS
  # answers :eof; S3 and GCS answer 416), and one that runs past the end comes
  # back short; the caller decides what a short read means.
  defp get_range(_instance, _key, _offset, 0), do: {:ok, <<>>}

  defp get_range(instance, key, offset, length) do
    case BlobStore.get_range(instance, key, offset, length) do
      {:ok, bin} -> {:ok, bin}
      {:error, :not_found} -> {:error, :not_found}
      {:error, :eof} -> {:ok, <<>>}
      {:error, {:status, 416, _body}} -> {:ok, <<>>}
      {:error, reason} -> {:error, {:unavailable, reason}}
    end
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

    byte_size(item.body) + Pack.entry_overhead() + byte_size(item.id) + @manifest_row_bytes +
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
