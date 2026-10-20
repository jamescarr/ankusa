defmodule Ankusa.ClaimCheck do
  @moduledoc """
  Claim check: store payloads too large to ride inline in a queue message, and
  hand each one back by reference.

  Ankusa's dispatch pipeline is the only writer. It writes directly to the
  instance's claim stores (`Ankusa.ClaimCheck.Stores`: one or more object
  stores, with failover between them), packing every claim a WAL read batch
  needs for one tenant into one object (`Ankusa.ClaimCheck.Pack`), so one
  `PUT` covers many claims. Each claim gets an `Ankusa.ClaimCheck.Ref` (a URN
  naming its tenant and claim id — and, with `claim_check.ref_signing_keys`,
  an expiry and an HMAC over both) and the sha256 of its bytes, which travel
  together as a `t:claim/0`.

  Readers either call `redeem/3` in-process, fetch
  `GET /v1/claims/<tenant>/<claim_id>` from a `:claim_check`-role node
  (`Ankusa.ClaimCheck.Router`), or read the objects straight from the bucket.
  The gateway authenticates and authorizes every request (`claim_check.auth`,
  `claim_check.authorize`) and holds the ref to `verify_ref/2`; `read/4`,
  `locate/4` and `read_bytes/5` below do neither, so code calling them has
  already decided who may read what.

  Objects are written once under a freshly minted id and never rewritten, so
  every read is safe to cache forever.

  The gateway checks what it serves against the pack's manifest
  (`claim_check.integrity_check: :manifest`, the default): see `digest/3`.

  Permanent errors (`:not_found`, `:integrity_mismatch`, `:invalid_ref`,
  `:invalid_tenant`, `:invalid_id`, `:unsigned_ref`, `:bad_signature`,
  `:ref_expired`) mean retrying won't help — callers should dead-letter.
  `{:unavailable, reason}` is transient, and so is `:forbidden` (the object
  store refused this node's credential: a configuration fault to fix, not a
  reason to drop the claim).
  """

  require Logger

  alias Ankusa.ClaimCheck.{Cache, Pack, Reader, Ref, Stores}
  alias Ankusa.{Config, Telemetry}

  # ZIP without ZIP64 caps an archive at 65,534 entries (two are the index and
  # the manifest) and 4 GiB; a claim id's 16 position bits cap it at 65,536.
  # Packs split well inside both regardless of pack_max_bytes.
  @max_pack_claims 65_000
  @max_pack_bytes 0xFFFFFFFF - 16 * 1024 * 1024

  # How long a signed ref is good for when neither `claim_check.ref_ttl_ms` nor
  # a retention window says.
  @default_ref_ttl_ms 30 * 24 * 3_600_000

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
          | :unsigned_ref
          | :bad_signature
          | :ref_expired
          | :forbidden
          | {:unavailable, term()}

  @typedoc "A checked-in claim: its ref, and the lowercase hex sha256 of its bytes."
  @type claim :: %{ref: Ref.t(), sha256: String.t()}

  # Uploads are network-bound, so more than the scheduler count is fine; the
  # cap exists so one huge batch can't open an unbounded number of sockets.
  @upload_concurrency 16

  # A manifest row per claim: its ids, digest, JSON keys, and numbers. The
  # content type is added on top. Only used to size packs against the cap.
  @manifest_row_bytes 200

  # A pack's index is read this far on a miss, whatever the claim asked for:
  # the first 1 024 claims' rows (8 231 bytes), which is every claim of every
  # pack dispatch builds at `dispatch.batch` 128, so one read and one cache
  # entry locate them all.
  @index_read_bytes Pack.index_prefix_bytes(1023)

  # A pack id is 48 bits of time and 64 random bits, so a claim store already
  # holding an object under a new one is a reused id, not luck. `check_in/4`
  # mints a fresh id this many times before it gives up.
  @max_pack_attempts 3

  @doc """
  Store `items` for one tenant as a single pack object, with one conditional
  `PUT` (`Ankusa.BlobStore.put_if_absent/4`): a claim store that already holds
  an object under the pack's key refuses, and `check_in/4` writes the pack
  under a **fresh pack id** (at most 3 attempts), so a reused or colliding id
  can never overwrite claims that are already referenced. The refs it returns
  name the id the pack was written under.

  Returns a `t:claim/0` per item id once the write is durable in one of the
  claim stores (`Ankusa.ClaimCheck.Stores.write/6`). The whole call, every
  store tried, every attempt and multipart part, and every pack-id retry,
  shares one deadline (`Ankusa.ClaimCheck.Stores.write_deadline/1`, the largest
  claim store `op_deadline_ms`): past it the call returns an error rather than
  keep a dispatch task and its connections busy. A store that throttles with a
  `Retry-After` that does not fit is `{:error, {:unavailable, {:throttled,
  seconds}}}` at once, for dispatch's own backoff to schedule. opts:

    * `:pack_id` — the first pack id tried (`Ankusa.ClaimCheck.Ref.pack_id/2`);
      default a freshly minted one (`Ref.new_pack_id/0`: now plus 64 random bits).
  """
  @spec check_in(atom(), String.t(), [item()], keyword()) ::
          {:ok, %{String.t() => claim()}} | {:error, reason()}
  def check_in(instance, tenant_id, [_ | _] = items, opts \\ []) do
    first_id = Keyword.get_lazy(opts, :pack_id, &Ref.new_pack_id/0)
    started = System.monotonic_time()

    {result, pack_id} =
      with :ok <- Ref.validate_tenant(tenant_id),
           :ok <- Ref.validate_pack_id(first_id) do
        config = Ankusa.config(instance)
        deadline = Stores.write_deadline(config)
        write_pack(instance, config, tenant_id, items, first_id, deadline, @max_pack_attempts)
      else
        {:error, _reason} = error -> {error, first_id}
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
        result: result_tag(result),
        store: store(result)
      }
    )

    case result do
      {:ok, claims, _store} -> {:ok, claims}
      {:error, _reason} = error -> error
    end
  end

  # One pack written under `pack_id`; on a collision, again under a fresh id.
  # Returns the result and the id it ended on.
  defp write_pack(instance, config, tenant_id, items, pack_id, deadline, attempts_left) do
    expires_at = ref_expires_at(config)

    claims =
      items
      |> Enum.with_index()
      |> Enum.map(fn {item, index} -> claim(item, Ref.claim_id(pack_id, index)) end)

    {data, _placements} = Pack.build(claims)
    key = Ref.object_key(tenant_id, pack_id, config.claim_check.key_layout)

    case Stores.write(instance, config, pack_id, key, data, deadline: deadline) do
      {:ok, store} ->
        {{:ok, Map.new(claims, &{&1.id, checked_in(config, expires_at, tenant_id, &1)}), store},
         pack_id}

      {:error, :already_exists} ->
        Telemetry.emit([:claim_check, :pack_collision], %{}, %{
          instance: instance,
          tenant_id: tenant_id,
          pack_id: pack_id
        })

        cond do
          attempts_left <= 1 ->
            {{:error, {:unavailable, :pack_id_collision}}, pack_id}

          System.monotonic_time(:millisecond) >= deadline ->
            {{:error, {:unavailable, :deadline}}, pack_id}

          true ->
            write_pack(
              instance,
              config,
              tenant_id,
              items,
              Ref.new_pack_id(),
              deadline,
              attempts_left - 1
            )
        end

      {:error, _reason} = error ->
        {error, pack_id}
    end
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
  Check a ref against this node's policy, as the gateway does before it reads
  anything:

    * a v2 ref must carry a signature some `claim_check.ref_signing_keys` entry
      produced (`{:error, :bad_signature}` otherwise) and must not be past its
      expiry (`{:error, :ref_expired}`)
    * a v1 ref is `{:error, :unsigned_ref}` unless `claim_check.accept_unsigned_refs`
      is true (the default only while `ref_signing_keys` is empty)
  """
  @spec verify_ref(atom() | Config.t(), Ref.t()) ::
          :ok | {:error, :unsigned_ref | :bad_signature | :ref_expired}
  def verify_ref(%Config{claim_check: cc}, %Ref{signature: nil}) do
    if accept_unsigned?(cc), do: :ok, else: {:error, :unsigned_ref}
  end

  def verify_ref(%Config{claim_check: cc}, %Ref{} = ref),
    do: Ref.verify(ref, cc.ref_signing_keys, System.system_time(:second))

  def verify_ref(instance, %Ref{} = ref), do: verify_ref(Ankusa.config(instance), ref)

  defp accept_unsigned?(%{accept_unsigned_refs: nil, ref_signing_keys: keys}), do: keys == []
  defp accept_unsigned?(%{accept_unsigned_refs: accept}), do: accept

  @doc """
  Redeem a ref (a `%Ref{}` or its URN) for its bytes and check them against
  `sha256`, the lowercase hex digest the queue message carried with the ref.
  Integrity is never trusted from the store. The ref is held to the node's
  policy first (`verify_ref/2`): a forged, expired or (when refused) unsigned
  ref reads nothing.
  """
  @spec redeem(atom(), Ref.t() | String.t(), String.t()) :: {:ok, binary()} | {:error, reason()}
  def redeem(instance, urn, sha256) when is_binary(urn) do
    with {:ok, ref} <- Ref.parse(urn), do: redeem(instance, ref, sha256)
  end

  def redeem(instance, %Ref{} = ref, sha256) do
    started = System.monotonic_time()

    result =
      with :ok <- verify_ref(instance, ref),
           {:ok, bin} <- read(instance, ref.tenant_id, ref.claim_id) do
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

  @typedoc """
  A located claim: its tenant, pack and index in the pack, the index of the
  claim store the pack was found in, its object key there, and the claim's
  offset and length in it; and how many claims the pack holds (`nil` if its
  index does not say) and the offset where its last claim ends, when the index
  bytes read have that row (`nil` otherwise).
  """
  @type location :: %{
          tenant_id: String.t(),
          pack_id: String.t(),
          index: non_neg_integer(),
          store: non_neg_integer(),
          key: String.t(),
          offset: non_neg_integer(),
          length: non_neg_integer(),
          claims: pos_integer() | nil,
          claims_end: non_neg_integer() | nil
        }

  @doc """
  The `:deadline` of a read starting now: `claim_check.read_deadline_ms` from
  now, as an absolute `System.monotonic_time(:millisecond)`.
  """
  @spec deadline(atom() | Config.t()) :: integer()
  def deadline(%Config{claim_check: %{read_deadline_ms: ms}}),
    do: System.monotonic_time(:millisecond) + ms

  def deadline(instance), do: deadline(Ankusa.config(instance))

  @doc """
  Read a claim's bytes, with no digest check: `locate/4`, then `read_bytes/5`
  over the whole claim, both under one `:deadline` (default `deadline/1`). A
  claim id past the end of its pack's index, a missing pack, or a pack that
  ends before the claim does is `:not_found`.
  """
  @spec read(atom(), String.t(), String.t(), keyword()) :: {:ok, binary()} | {:error, reason()}
  def read(instance, tenant_id, claim_id, opts \\ []) do
    opts = Keyword.put_new_lazy(opts, :deadline, fn -> deadline(instance) end)

    with {:ok, location} <- locate(instance, tenant_id, claim_id, opts) do
      read_bytes(instance, location, 0, location.length, opts)
    end
  end

  @doc """
  Find a claim without reading it: its pack's index, from
  `Ankusa.ClaimCheck.Cache`, else one ranged read of the index's first
  8 231 bytes (the first 1 024 claims' rows), or up to the claim's row when
  that is further, searching every claim store under both key layouts
  (`Ankusa.ClaimCheck.Stores.read_range/6`). A claim id past the end of the
  index, or a pack no store has, is `:not_found`; a pack not found while some
  store could not answer is that store's error.

  On a `:claim_check` node the read goes through `Ankusa.ClaimCheck.Reader`,
  and gives up with `{:unavailable, :deadline}` at `opts[:deadline]` (default
  `deadline/1`); elsewhere it goes straight to the store.
  """
  @spec locate(atom(), String.t(), String.t(), keyword()) ::
          {:ok, location()} | {:error, reason()}
  def locate(instance, tenant_id, claim_id, opts \\ []) do
    with :ok <- Ref.validate_tenant(tenant_id),
         {:ok, pack_id, index} <- Ref.locate(claim_id) do
      config = Ankusa.config(instance)
      deadline = Keyword.get_lazy(opts, :deadline, fn -> deadline(config) end)
      locate_in(instance, config, tenant_id, pack_id, index, deadline)
    end
  end

  # `read` is how far the cached (or fetched) index was asked for: when that
  # covers this claim's row, a shorter prefix means the pack ends before it.
  # A fetch this caller joined may have asked for less than it needs (only
  # for a claim past row 1 023): it fetches again.
  defp locate_in(instance, config, tenant_id, pack_id, index, deadline) do
    needed = Pack.index_prefix_bytes(index)

    case Cache.pack(instance, tenant_id, pack_id) do
      {:ok, %{read: read} = pack} when read >= needed ->
        Reader.emit_read(instance, :index, :cache, {:ok, pack}, pack.store, 0)
        located(pack, tenant_id, pack_id, index)

      _miss_or_short ->
        length = max(needed, @index_read_bytes)

        case fetch_index(instance, config, tenant_id, pack_id, length, deadline) do
          {:ok, %{read: read} = pack} when read >= needed ->
            located(pack, tenant_id, pack_id, index)

          {:ok, _shorter} ->
            locate_in(instance, config, tenant_id, pack_id, index, deadline)

          {:error, _reason} = error ->
            error
        end
    end
  end

  # The cache is filled inside the fetch, so a read that outlives its
  # caller's deadline still leaves the index for the retry. The closure reads
  # the config itself (a `persistent_term` read) rather than closing over it:
  # it is copied into the reader and its task, and queued there, and the
  # config carries every sink's and store's credentials.
  defp fetch_index(instance, config, tenant_id, pack_id, length, deadline) do
    keys = Ref.object_keys(tenant_id, pack_id, config.claim_check.key_layout)

    fun = fn ->
      case Stores.read_range(instance, Ankusa.config(instance), pack_id, keys, 0, length) do
        {:ok, prefix, store, key} ->
          pack = %{store: store, key: key, prefix: prefix, read: length}
          Cache.put_pack(instance, tenant_id, pack_id, pack)
          {:ok, pack}

        {:error, _reason} = error ->
          error
      end
    end

    run(instance, {:index, tenant_id, pack_id}, Stores.home(config, pack_id), fun, deadline)
  end

  defp located(pack, tenant_id, pack_id, index) do
    case Pack.locate(pack.prefix, index) do
      {:ok, offset, length} ->
        {claims, claims_end} = extent(pack.prefix)

        {:ok,
         %{
           tenant_id: tenant_id,
           pack_id: pack_id,
           index: index,
           store: pack.store,
           key: pack.key,
           offset: offset,
           length: length,
           claims: claims,
           claims_end: claims_end
         }}

      :error ->
        {:error, :not_found}
    end
  end

  # How many claims the pack holds, and where its last one ends (where the
  # manifest's entry starts) when the index bytes read reach the last row.
  defp extent(prefix) do
    case Pack.count(prefix) do
      {:ok, claims} -> {claims, last_claim_end(prefix, claims)}
      :error -> {nil, nil}
    end
  end

  defp last_claim_end(prefix, claims) do
    case Pack.locate(prefix, claims - 1) do
      {:ok, offset, length} -> offset + length
      :error -> nil
    end
  end

  @doc """
  `length` bytes of a located claim, starting `first` bytes into it: the
  whole claim from `Ankusa.ClaimCheck.Cache` when it is there, else one ranged
  read, through `Ankusa.ClaimCheck.Reader` on a `:claim_check` node (see
  `locate/4` for `opts[:deadline]`). A whole claim of at most
  `claim_check.read_cache_max_entry_bytes` is cached; a range never is. The
  caller keeps the range inside the claim's `length`; a pack that ends before
  the range does is `:not_found`, and forgets the cached pack (it was swept).
  """
  @spec read_bytes(atom(), location(), non_neg_integer(), non_neg_integer(), keyword()) ::
          {:ok, binary()} | {:error, reason()}
  def read_bytes(instance, location, first, length, opts \\ [])

  def read_bytes(_instance, _location, _first, 0, _opts), do: {:ok, <<>>}

  def read_bytes(instance, location, first, length, opts) do
    %{tenant_id: tenant_id, pack_id: pack_id, index: index} = location
    whole? = first == 0 and length == location.length

    case if(whole?, do: Cache.body(instance, tenant_id, pack_id, index), else: :error) do
      {:ok, bin} ->
        Reader.emit_read(instance, :bytes, :cache, {:ok, bin}, location.store, 0)
        {:ok, bin}

      :error ->
        fetch_bytes(instance, location, first, length, whole?, opts)
    end
  end

  defp fetch_bytes(instance, location, first, length, whole?, opts) do
    %{tenant_id: tenant_id, pack_id: pack_id, index: index} = location
    config = Ankusa.config(instance)
    deadline = Keyword.get_lazy(opts, :deadline, fn -> deadline(config) end)
    cache? = whole? and length <= config.claim_check.read_cache_max_entry_bytes

    key =
      if whole?,
        do: {:bytes, tenant_id, pack_id, index},
        else: {:range, tenant_id, pack_id, index, first, length}

    %{store: store, key: object_key, offset: offset} = location

    fun = fn ->
      case Stores.get_range(
             instance,
             Ankusa.config(instance),
             store,
             object_key,
             offset + first,
             length
           ) do
        {:ok, bin} when byte_size(bin) == length ->
          if cache?, do: Cache.put_body(instance, tenant_id, pack_id, index, bin)
          {:ok, bin}

        {:ok, _short} ->
          {:error, :not_found}

        {:error, _reason} = error ->
          error
      end
    end

    case run(instance, key, location.store, fun, deadline) do
      {:error, :not_found} = error ->
        Cache.forget_pack(instance, tenant_id, pack_id)
        error

      result ->
        result
    end
  end

  # The manifest's rows are about 250 bytes of JSON each: one ranged read of
  # the entry's header and this much more usually has all of it.
  @manifest_row_estimate 320
  @manifest_first_read_floor 2048
  @manifest_first_read_cap 4 * 1024 * 1024

  @doc """
  The lowercase hex sha256 of a located claim, as its pack's manifest records
  it (`claim_check.integrity_check: :manifest`, the gateway's check).

  The pack's digests come from `Ankusa.ClaimCheck.Cache` (inside the pack's
  entry, so under `claim_check.read_cache_bytes` and aged with it), else from
  one ranged read of the manifest where the last claim ends (a second one for
  a manifest longer than the first guessed), shared by every concurrent caller
  through `Ankusa.ClaimCheck.Reader` like any other read. A manifest that does
  not parse, or does not list exactly this pack's claims, is
  `{:error, :integrity_mismatch}`; a pack that ends before its manifest does is
  `:not_found`. `opts[:deadline]` is as for `locate/4`.
  """
  @spec digest(atom(), location(), keyword()) :: {:ok, String.t()} | {:error, reason()}
  def digest(instance, location, opts \\ []) do
    %{tenant_id: tenant_id, pack_id: pack_id, index: index} = location

    case Cache.digests(instance, tenant_id, pack_id) do
      {:ok, digests} ->
        Reader.emit_read(instance, :manifest, :cache, {:ok, digests}, location.store, 0)
        digest_at(digests, index)

      :error ->
        config = Ankusa.config(instance)
        deadline = Keyword.get_lazy(opts, :deadline, fn -> deadline(config) end)

        with {:ok, digests} <- fetch_digests(instance, config, location, deadline) do
          digest_at(digests, index)
        end
    end
  end

  defp digest_at(digests, index) do
    case Pack.digest(digests, index) do
      {:ok, digest} -> {:ok, Base.encode16(digest, case: :lower)}
      :error -> {:error, :integrity_mismatch}
    end
  end

  defp fetch_digests(_instance, _config, %{claims: nil}, _deadline),
    do: {:error, :integrity_mismatch}

  defp fetch_digests(instance, config, location, deadline) do
    %{tenant_id: tenant_id, pack_id: pack_id, claims: claims} = location

    with {:ok, claims_end} <- claims_end(instance, config, location, deadline) do
      fun = fn -> read_manifest(instance, location, claims_end, claims) end

      case run(instance, {:manifest, tenant_id, pack_id}, location.store, fun, deadline) do
        {:error, :not_found} = error ->
          Cache.forget_pack(instance, tenant_id, pack_id)
          error

        result ->
          result
      end
    end
  end

  # Where the last claim ends, which is where the manifest's entry starts: from
  # the located claim when its index read reached the last row, else the last
  # row's own location (a pack of more than 1 024 claims).
  defp claims_end(_instance, _config, %{claims_end: claims_end}, _deadline)
       when is_integer(claims_end),
       do: {:ok, claims_end}

  defp claims_end(instance, config, location, deadline) do
    %{tenant_id: tenant_id, pack_id: pack_id, claims: claims} = location

    case locate_in(instance, config, tenant_id, pack_id, claims - 1, deadline) do
      {:ok, %{claims_end: claims_end}} when is_integer(claims_end) -> {:ok, claims_end}
      {:ok, _no_end} -> {:error, :integrity_mismatch}
      {:error, _reason} = error -> error
    end
  end

  # Runs in the reader's task, so it fills the cache even if its callers gave
  # up. It reads the config itself, for the reason `fetch_index/6` does.
  defp read_manifest(instance, location, start, claims) do
    %{tenant_id: tenant_id, pack_id: pack_id, store: store, key: key} = location
    config = Ankusa.config(instance)
    header = Pack.manifest_header_bytes()

    first =
      header +
        min(
          max(claims * @manifest_row_estimate, @manifest_first_read_floor),
          @manifest_first_read_cap
        )

    with {:ok, bin} <- Stores.get_range(instance, config, store, key, start, first),
         {:ok, size} <- manifest_size(bin, header),
         {:ok, json} <- manifest_json(instance, config, location, start, bin, header, size),
         {:ok, digests} <- manifest_digests(json, pack_id, claims) do
      Cache.put_digests(instance, tenant_id, pack_id, digests)
      {:ok, digests}
    end
  end

  # Too few bytes for the header: the pack ends before its manifest.
  defp manifest_size(bin, header) when byte_size(bin) < header, do: {:error, :not_found}

  defp manifest_size(bin, _header) do
    case Pack.manifest_size(bin) do
      {:ok, size} -> {:ok, size}
      :error -> {:error, :integrity_mismatch}
    end
  end

  defp manifest_json(_instance, _config, _location, _start, bin, header, size)
       when byte_size(bin) >= header + size,
       do: {:ok, binary_part(bin, header, size)}

  defp manifest_json(instance, config, location, start, _bin, header, size) do
    case Stores.get_range(instance, config, location.store, location.key, start + header, size) do
      {:ok, json} when byte_size(json) == size -> {:ok, json}
      {:ok, _short} -> {:error, :not_found}
      {:error, _reason} = error -> error
    end
  end

  defp manifest_digests(json, pack_id, claims) do
    case Pack.digests(json, pack_id, claims) do
      {:ok, digests} -> {:ok, digests}
      :error -> {:error, :integrity_mismatch}
    end
  end

  # Through the reader when it runs (a `:claim_check` node), else straight to
  # the store: no limit, no coalescing, no deadline but the store's timeout.
  defp run(instance, key, store, fun, deadline) do
    case Ankusa.whereis(instance, :claim_reader) do
      nil ->
        started = System.monotonic_time()
        result = fun.()
        duration = System.monotonic_time() - started
        Reader.emit_read(instance, elem(key, 0), :direct, result, store, duration)
        result

      _pid ->
        Reader.fetch(instance, key, store, fun, deadline)
    end
  end

  @doc """
  Validate `config.claim_check` at boot. Raises rather than surfacing a
  misconfiguration at runtime:

    * `claim_check.pack_max_bytes` must be a positive integer
    * `claim_check.retention_days` must be `nil` or an integer >= 1, and
      `claim_check.sweep_interval_ms` an integer >= 1
    * `claim_check.blob_stores` must be `nil` or a non-empty list of
      `{module, opts}`; `claim_check.placement` `:ordered` or `:hash`;
      `claim_check.key_layout` `:flat` or `:sharded`
    * `claim_check.retention_days` set with any non-`LocalFS` claim store (the
      sweeper only ever covers `LocalFS`; S3/GCS need a bucket lifecycle rule)
    * `claim_check.bucket_expiry_days` declared with an object-store claim store
      shorter than the longest a claim can be needed: the retry horizon of the
      default policy (21 days) + `queue.dlq.retention_days` + `claim_check.redeem_grace_ms`.
      A bucket lifecycle rule cannot see claim leases, so the declared age is
      the real cutoff; undeclared, boot warns with the minimum
    * `max_body_bytes` of 4 GiB or more: a pack can't hold a body that large
    * `claim_check.ref_signing_keys` must be a list of strings of at least 16
      bytes, and not empty while `claim_check.accept_unsigned_refs` is `false`
    * `claim_check.authorize` must be `nil` or a `{module, fun, args}` the
      module exports with arity `length(args) + 1`

  A `:claim_check`-only node with a `LocalFS` claim store boots with a
  warning: the gateway reads its own disk, and other nodes' claims are only
  there if that directory is shared with them.
  """
  @spec validate_config!(Config.t()) :: :ok
  def validate_config!(%Config{claim_check: cc} = config) do
    unless is_integer(cc.pack_max_bytes) and cc.pack_max_bytes > 0 do
      raise ArgumentError,
            "claim_check.pack_max_bytes must be a positive integer, got #{inspect(cc.pack_max_bytes)}"
    end

    unless cc.retention_days == nil or (is_integer(cc.retention_days) and cc.retention_days >= 1) do
      raise ArgumentError,
            "claim_check.retention_days must be nil or an integer >= 1, got #{inspect(cc.retention_days)}"
    end

    unless is_integer(cc.sweep_interval_ms) and cc.sweep_interval_ms >= 1 do
      raise ArgumentError,
            "claim_check.sweep_interval_ms must be a positive integer, got #{inspect(cc.sweep_interval_ms)}"
    end

    if config.max_body_bytes >= 0xFFFFFFFF do
      raise ArgumentError,
            "max_body_bytes must be under 4 GiB: a claim pack can't hold a larger body"
    end

    unless cc.blob_stores == nil or valid_stores?(cc.blob_stores) do
      raise ArgumentError,
            "claim_check.blob_stores must be nil or a non-empty list of {module, opts}, " <>
              "got #{inspect(cc.blob_stores)}"
    end

    unless cc.placement in [:ordered, :hash] do
      raise ArgumentError,
            "claim_check.placement must be :ordered or :hash, got #{inspect(cc.placement)}"
    end

    unless cc.key_layout in [:flat, :sharded] do
      raise ArgumentError,
            "claim_check.key_layout must be :flat or :sharded, got #{inspect(cc.key_layout)}"
    end

    modules = for {_index, mod, _opts} <- Stores.list(config), do: mod

    case Enum.find(modules, &(&1 != Ankusa.BlobStore.LocalFS)) do
      store when store != nil and cc.retention_days != nil ->
        raise ArgumentError,
              "claim_check.retention_days is set but the claim store is #{inspect(store)} — " <>
                "the LocalFS sweeper doesn't cover it. Use a bucket lifecycle rule on the " <>
                "claims/ prefix instead, and leave retention_days nil."

      _ ->
        :ok
    end

    validate_bucket_expiry!(config, modules)

    if Ankusa.BlobStore.LocalFS in modules and Config.role?(config, :claim_check) and
         not Enum.any?([:edge, :dispatch, :storage], &Config.role?(config, &1)) do
      Logger.warning(
        "[ankusa] the claim gateway reads claims from local disk (LocalFS); other nodes' " <>
          "claims are not here unless this directory is shared with them. Point " <>
          "claim_check.blob_stores at the stores the dispatch nodes write to."
      )
    end

    :ok = validate_refs!(cc)
    validate_authorize!(cc)
  end

  # The retry horizon of the default exponential policy: 84 attempts, each at
  # most 6 h apart (`Ankusa.RetryPolicy.Exponential`). A custom policy's horizon
  # is the operator's to add to `bucket_expiry_days`.
  @default_retry_horizon_days 21

  # A bucket lifecycle rule expires claim objects by age and cannot see claim
  # leases, so for object-store claims the declared bucket age is the real
  # cutoff: it has to outlast a claim's longest need, or a hook that is still
  # retrying, dead-lettered or just delivered loses its body.
  defp validate_bucket_expiry!(config, modules) do
    %Config{claim_check: cc} = config

    if Enum.any?(modules, &(&1 != Ankusa.BlobStore.LocalFS)) do
      grace_days = div(cc.redeem_grace_ms + 86_399_999, 86_400_000)
      needed = @default_retry_horizon_days + config.queue.dlq.retention_days + grace_days

      cond do
        cc.bucket_expiry_days == nil and Config.role?(config, :dispatch) ->
          Logger.warning(
            "[ankusa] claim packs are in an object store and claim_check.bucket_expiry_days " <>
              "is not declared: a bucket lifecycle rule cannot see claim leases, so it must " <>
              "expire claims/ objects no earlier than #{needed} days (retry horizon " <>
              "#{@default_retry_horizon_days} + queue.dlq.retention_days " <>
              "#{config.queue.dlq.retention_days} + redeem grace #{grace_days})"
          )

        cc.bucket_expiry_days != nil and cc.bucket_expiry_days < needed ->
          raise ArgumentError,
                "claim_check.bucket_expiry_days is #{cc.bucket_expiry_days} but a claim can be " <>
                  "needed for #{needed} days (retry horizon #{@default_retry_horizon_days} + " <>
                  "queue.dlq.retention_days #{config.queue.dlq.retention_days} + redeem grace " <>
                  "#{grace_days}): raise the bucket rule and this key, or lower the others"

        true ->
          :ok
      end
    end
  end

  defp valid_stores?([_ | _] = stores),
    do: Enum.all?(stores, &match?({mod, opts} when is_atom(mod) and is_list(opts), &1))

  defp valid_stores?(_), do: false

  @min_signing_key_bytes 16

  defp validate_refs!(cc) do
    keys = cc.ref_signing_keys

    unless is_list(keys) and
             Enum.all?(keys, &(is_binary(&1) and byte_size(&1) >= @min_signing_key_bytes)) do
      raise ArgumentError,
            "claim_check.ref_signing_keys must be a list of strings of at least " <>
              "#{@min_signing_key_bytes} bytes, [current | previous], got " <>
              inspect(Enum.map(List.wrap(keys), &redacted_key/1))
    end

    if cc.accept_unsigned_refs == false and keys == [] do
      raise ArgumentError,
            "claim_check.accept_unsigned_refs is false but claim_check.ref_signing_keys is " <>
              "empty: no ref could be minted or accepted"
    end

    :ok
  end

  defp redacted_key(key) when is_binary(key), do: "<#{byte_size(key)} bytes>"
  defp redacted_key(_other), do: "<not a string>"

  defp validate_authorize!(%{authorize: nil}), do: :ok

  defp validate_authorize!(%{authorize: {mod, fun, args}})
       when is_atom(mod) and is_atom(fun) and is_list(args) do
    if Code.ensure_loaded?(mod) and function_exported?(mod, fun, length(args) + 1) do
      :ok
    else
      raise ArgumentError,
            "claim_check.authorize #{inspect(mod)}.#{fun}/#{length(args) + 1} is not defined"
    end
  end

  defp validate_authorize!(%{authorize: other}) do
    raise ArgumentError,
          "claim_check.authorize must be nil or {module, fun, args}, got #{inspect(other)}"
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

  # A v2 ref when `claim_check.ref_signing_keys` has a key (signed with the
  # first, expiring at `expires_at`), else a v1 ref.
  defp checked_in(config, expires_at, tenant_id, claim) do
    ref = %Ref{tenant_id: tenant_id, claim_id: claim.claim_id}

    case config.claim_check.ref_signing_keys do
      [] -> %{ref: ref, sha256: claim.sha256}
      [key | _previous] -> %{ref: Ref.sign(ref, key, expires_at), sha256: claim.sha256}
    end
  end

  # The Unix second a ref minted now stops being served: `ref_ttl_ms` from now,
  # or the retention window, or 30 days when claims are never swept. Rounded up
  # to the second, so a ref never expires before its TTL.
  defp ref_expires_at(%Config{claim_check: cc}) do
    ttl_ms =
      cc.ref_ttl_ms ||
        if(cc.retention_days, do: cc.retention_days * 86_400_000, else: @default_ref_ttl_ms)

    div(System.system_time(:millisecond) + ttl_ms + 999, 1000)
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

  defp item_result({:ok, refs}, id), do: {:ok, Map.fetch!(refs, id)}
  defp item_result({:error, reason}, _id), do: {:error, reason}

  @doc false
  @spec sha256(binary()) :: String.t()
  def sha256(bin), do: Base.encode16(:crypto.hash(:sha256, bin), case: :lower)

  defp result_tag({:ok, _bin}), do: :ok
  defp result_tag({:ok, _claims, _store}), do: :ok
  defp result_tag({:error, reason}), do: reason

  defp store({:ok, _claims, store}), do: store
  defp store({:error, _reason}), do: nil
end
