defmodule Ankusa.ClaimCheck.Ref do
  @moduledoc """
  A claim-check reference: one string that names a claim's bytes.

      urn:ankusa:claim:v1:<tenant>:<claim_id>
      urn:ankusa:claim:v2:<tenant>:<claim_id>:<exp_unix>:<signature>

    * `tenant` — `[A-Za-z0-9_-]{1,64}`; the same string in the URN, the gateway
      path, and the storage key, with no encoding anywhere
    * `claim_id` — a canonical (uppercase) `Ankusa.ULID`. Its first 48 bits
      are the pack's creation time and pick the pack's `dt=` folder; its next
      5 bits (the id's eleventh character) pick the pack's `shard=` folder
      under the `:sharded` layout (`object_key/3`); its last 16 bits are the
      claim's position in the pack. The pack's own id is the claim id with
      those 16 bits zeroed, so the ref locates the pack without an index
      lookup.
    * `exp_unix` (v2) — the Unix second after which the gateway refuses the
      ref (`410`), decimal, no leading zero.
    * `signature` (v2) — the unpadded base64url (43 characters) HMAC-SHA256,
      under one of `claim_check.ref_signing_keys`, of the URN up to and
      excluding the last colon: `urn:ankusa:claim:v2:<tenant>:<claim_id>:<exp_unix>`.

  A v1 ref is an unexpiring bearer string; v2 binds the tenant, the claim and
  an expiry to a key only the operator holds, so a leaked queue message stops
  granting reads when it expires and a guessed claim id grants nothing. This
  module parses and verifies the grammar; `Ankusa.ClaimCheck.verify_ref/2`
  applies the node's policy (which keys, whether a v1 ref is still accepted).

  The digest isn't part of the ref: a queue message carries it next to the
  ref, as `sha256`, and the reader checks it.

  A ref maps to a gateway path by dropping the prefix:
  `GET /v1/claims/<tenant>/<claim_id>` for v1, and the same path with
  `?exp=<exp_unix>&sig=<signature>` for v2 — see `path/1`.
  """

  alias Ankusa.{ULID, Verifier}

  @enforce_keys [:tenant_id, :claim_id]
  defstruct [:tenant_id, :claim_id, :expires_at, :signature]

  @type t :: %__MODULE__{
          tenant_id: String.t(),
          claim_id: String.t(),
          expires_at: pos_integer() | nil,
          signature: String.t() | nil
        }

  @typedoc """
  How a pack's storage key is laid out: `:flat` is
  `claims/tenant=<t>/dt=<d>/<pack>`, `:sharded` adds `shard=<c>/` between the
  tenant and the date — see `object_key/3`.
  """
  @type layout :: :flat | :sharded

  @prefix "urn:ankusa:claim:v1:"
  @prefix_v2 "urn:ankusa:claim:v2:"
  @claims_prefix "claims/"
  # Seconds since the epoch, canonical decimal: no sign, no leading zero, and
  # at most 12 digits (year 33658), so every SDK can hold it in an int.
  @exp_regex ~r/\A[1-9][0-9]{0,11}\z/
  # 32 bytes, unpadded base64url.
  @signature_regex ~r/\A[A-Za-z0-9_-]{43}\z/

  @doc "The fixed storage prefix every claim object lives under."
  @spec claims_prefix() :: String.t()
  def claims_prefix, do: @claims_prefix

  @doc """
  Parse a URN. Rejects anything that isn't exactly the v1 or the v2 grammar.
  The signature of a v2 ref is checked for shape only; `verify/3` checks it.
  """
  @spec parse(String.t()) :: {:ok, t()} | {:error, :invalid_ref}
  def parse(@prefix <> rest) do
    with [tenant, claim_id] <- String.split(rest, ":"),
         :ok <- validate_tenant(tenant),
         {:ok, _pack_id, _index} <- locate(claim_id) do
      {:ok, %__MODULE__{tenant_id: tenant, claim_id: claim_id}}
    else
      _ -> {:error, :invalid_ref}
    end
  end

  def parse(@prefix_v2 <> rest) do
    with [tenant, claim_id, exp, signature] <- String.split(rest, ":"),
         {:ok, _ref} = parsed <- signed(tenant, claim_id, exp, signature) do
      parsed
    else
      _ -> {:error, :invalid_ref}
    end
  end

  def parse(_), do: {:error, :invalid_ref}

  @doc """
  A v2 ref from its four parts as strings (the gateway's path segments and its
  `exp` and `sig` query parameters), checked for shape only. `{:error,
  :invalid_tenant | :invalid_id}` for a bad tenant or claim id, `{:error,
  :bad_signature}` for an `exp` or `sig` that is not in the grammar.
  """
  @spec signed(term(), term(), term(), term()) ::
          {:ok, t()} | {:error, :invalid_tenant | :invalid_id | :bad_signature}
  def signed(tenant, claim_id, exp, signature) do
    with :ok <- validate_tenant(tenant),
         {:ok, _pack_id, _index} <- locate(claim_id),
         true <- is_binary(exp) and Regex.match?(@exp_regex, exp),
         true <- is_binary(signature) and Regex.match?(@signature_regex, signature) do
      {:ok,
       %__MODULE__{
         tenant_id: tenant,
         claim_id: claim_id,
         expires_at: String.to_integer(exp),
         signature: signature
       }}
    else
      false -> {:error, :bad_signature}
      {:error, _reason} = error -> error
    end
  end

  @doc "The URN form: v2 when the ref is signed, else v1."
  @spec to_string(t()) :: String.t()
  def to_string(%__MODULE__{tenant_id: tenant_id, claim_id: claim_id, signature: nil}),
    do: @prefix <> tenant_id <> ":" <> claim_id

  def to_string(%__MODULE__{} = ref),
    do: signing_input(ref) <> ":" <> ref.signature

  @doc """
  The gateway path that serves this ref's bytes: with `?exp=<exp_unix>&sig=<signature>`
  when the ref is signed. Neither value needs encoding.
  """
  @spec path(t()) :: String.t()
  def path(%__MODULE__{tenant_id: tenant_id, claim_id: claim_id, signature: nil}),
    do: "/v1/claims/#{tenant_id}/#{claim_id}"

  def path(%__MODULE__{tenant_id: tenant_id, claim_id: claim_id} = ref),
    do: "/v1/claims/#{tenant_id}/#{claim_id}?exp=#{ref.expires_at}&sig=#{ref.signature}"

  @doc "`ref` as a v2 ref that expires at the Unix second `expires_at`, signed with `key`."
  @spec sign(t(), binary(), pos_integer()) :: t()
  def sign(%__MODULE__{} = ref, key, expires_at)
      when is_binary(key) and is_integer(expires_at) and expires_at > 0 do
    ref = %{ref | expires_at: expires_at, signature: nil}
    %{ref | signature: mac(key, ref)}
  end

  @doc """
  Check a ref against the signing keys, current first. A ref signed with any of
  them is `:ok`; `now` is a Unix second.

    * `{:error, :unsigned_ref}` — a v1 ref: it carries nothing to check
    * `{:error, :bad_signature}` — no key produced this signature; checked
      before the expiry, so a forged expiry is never reported as a real one
    * `{:error, :ref_expired}` — signed, and `now` is past `expires_at`
  """
  @spec verify(t(), [binary()], integer()) ::
          :ok | {:error, :unsigned_ref | :bad_signature | :ref_expired}
  def verify(%__MODULE__{signature: nil}, _keys, _now), do: {:error, :unsigned_ref}

  def verify(%__MODULE__{} = ref, keys, now) when is_list(keys) do
    # Every key is tried, so the time taken doesn't say which one matched.
    matched? =
      Enum.reduce(keys, false, fn key, acc ->
        Verifier.secure_compare(mac(key, ref), ref.signature) or acc
      end)

    cond do
      not matched? -> {:error, :bad_signature}
      now > ref.expires_at -> {:error, :ref_expired}
      true -> :ok
    end
  end

  defp signing_input(%__MODULE__{tenant_id: tenant_id, claim_id: claim_id, expires_at: exp}),
    do: @prefix_v2 <> tenant_id <> ":" <> claim_id <> ":" <> Integer.to_string(exp)

  defp mac(key, ref) do
    :hmac |> :crypto.mac(:sha256, key, signing_input(ref)) |> Base.url_encode64(padding: false)
  end

  @doc "The storage key, under `layout`, of the pack this ref points into."
  @spec key(t(), layout()) :: String.t()
  def key(%__MODULE__{tenant_id: tenant_id, claim_id: claim_id}, layout) do
    {:ok, pack_id, _index} = locate(claim_id)
    object_key(tenant_id, pack_id, layout)
  end

  @doc "A fresh pack id, stamped with the current time."
  @spec new_pack_id() :: String.t()
  def new_pack_id,
    do: pack_id(System.system_time(:millisecond), :crypto.strong_rand_bytes(8))

  @doc """
  The pack id for a Unix millisecond timestamp and 64 bits of entropy: the
  timestamp, the entropy, and 16 zero bits that each claim's id fills with
  its position in the pack.
  """
  @spec pack_id(non_neg_integer(), <<_::64>>) :: String.t()
  def pack_id(ms, <<entropy::64>>) when is_integer(ms) and ms >= 0,
    do: ULID.encode(<<ms::48, entropy::64, 0::16>>)

  @doc "The id of the claim at `index` in pack `pack_id`."
  @spec claim_id(String.t(), non_neg_integer()) :: String.t()
  def claim_id(pack_id, index) when index in 0..0xFFFF do
    {:ok, <<pack::112, 0::16>>} = ULID.decode(pack_id)
    ULID.encode(<<pack::112, index::16>>)
  end

  @doc "Split a claim id into its pack's id and its position in that pack."
  @spec locate(term()) :: {:ok, String.t(), non_neg_integer()} | {:error, :invalid_id}
  def locate(claim_id) do
    case ULID.decode(claim_id) do
      {:ok, <<pack::112, index::16>>} -> {:ok, ULID.encode(<<pack::112, 0::16>>), index}
      :error -> {:error, :invalid_id}
    end
  end

  @doc "`:ok` if `pack_id` is a ULID whose position bits are all zero."
  @spec validate_pack_id(term()) :: :ok | {:error, :invalid_id}
  def validate_pack_id(pack_id) do
    case ULID.decode(pack_id) do
      {:ok, <<_::112, 0::16>>} -> :ok
      _ -> {:error, :invalid_id}
    end
  end

  @doc """
  The storage key for a pack: Hive-style partition folders, so Spark,
  BigQuery, and Athena discover `tenant` and `dt` as columns. `dt` is the UTC
  date of the pack id's timestamp, so the key is a pure function of the ref.
  Only call this with a validated tenant and pack id.

    * `:flat` — `claims/tenant=<t>/dt=<yyyy-mm-dd>/<pack_id>`.
    * `:sharded` — `claims/tenant=<t>/shard=<c>/dt=<yyyy-mm-dd>/<pack_id>`,
      `c` being `shard/1` of the pack id: one of 32 prefixes per tenant, so a
      busy tenant's writes spread over 32 object-store partitions instead of
      landing on the tail of one. `shard=` sits above `dt=` so a partition
      formed on a shard stays warm across days, and below `tenant=` so one
      tenant is still one prefix to delete.
  """
  @spec object_key(String.t(), String.t(), layout()) :: String.t()
  def object_key(tenant_id, pack_id, :flat) do
    "#{@claims_prefix}tenant=#{tenant_id}/dt=#{date(pack_id)}/#{pack_id}"
  end

  def object_key(tenant_id, pack_id, :sharded) do
    "#{@claims_prefix}tenant=#{tenant_id}/shard=#{shard(pack_id)}/dt=#{date(pack_id)}/#{pack_id}"
  end

  @doc """
  Both storage keys a pack may live under, `preferred`'s first: a reader tries
  the configured layout, then the other one, so a pack written before the
  layout changed is still found.
  """
  @spec object_keys(String.t(), String.t(), layout()) :: [String.t()]
  def object_keys(tenant_id, pack_id, :sharded),
    do: [object_key(tenant_id, pack_id, :sharded), object_key(tenant_id, pack_id, :flat)]

  def object_keys(tenant_id, pack_id, :flat),
    do: [object_key(tenant_id, pack_id, :flat), object_key(tenant_id, pack_id, :sharded)]

  @doc """
  The pack's shard under the `:sharded` layout: the id's eleventh character,
  i.e. the first five of its 64 random bits (the first ten characters are the
  48-bit timestamp), one of the 32 Crockford characters. Every claim id of a
  pack shares it, so a reader computes the key from a claim id as is.
  """
  @spec shard(String.t()) :: String.t()
  def shard(id), do: String.at(id, 10)

  defp date(pack_id) do
    {:ok, bits} = ULID.decode(pack_id)

    bits
    |> ULID.timestamp_ms()
    |> DateTime.from_unix!(:millisecond)
    |> DateTime.to_date()
    |> Date.to_iso8601()
  end

  @doc "`:ok` if `tenant_id` is a valid tenant name."
  @spec validate_tenant(term()) :: :ok | {:error, :invalid_tenant}
  def validate_tenant(tenant_id) when is_binary(tenant_id) do
    if byte_size(tenant_id) in 1..64 and tenant_chars?(tenant_id),
      do: :ok,
      else: {:error, :invalid_tenant}
  end

  def validate_tenant(_), do: {:error, :invalid_tenant}

  # `[A-Za-z0-9_-]` byte by byte: the name of a tenant is checked on every hook, and
  # a regular expression run costs more than the rest of the lookup.
  defp tenant_chars?(<<>>), do: true

  defp tenant_chars?(<<c, rest::binary>>)
       when c in ?a..?z or c in ?A..?Z or c in ?0..?9 or c == ?_ or c == ?-,
       do: tenant_chars?(rest)

  defp tenant_chars?(_), do: false

  @doc "Is `tenant_id` a valid tenant name?"
  @spec valid_tenant?(term()) :: boolean()
  def valid_tenant?(tenant_id), do: validate_tenant(tenant_id) == :ok

  defimpl String.Chars do
    def to_string(ref), do: Ankusa.ClaimCheck.Ref.to_string(ref)
  end
end
