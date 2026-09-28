defmodule Ankusa.ClaimCheck.Ref do
  @moduledoc """
  A claim-check reference: one string that names a claim's bytes and grants no
  access to them.

      urn:ankusa:claim:v1:<tenant>:<claim_id>

    * `tenant` — `[A-Za-z0-9_-]{1,64}`; the same string in the URN, the gateway
      path, and the storage key, with no encoding anywhere
    * `claim_id` — a canonical (uppercase) `Ankusa.ULID`. Its first 48 bits
      are the pack's creation time and pick the pack's `dt=` folder; its last
      16 bits are the claim's position in the pack. The pack's own id is the
      claim id with those 16 bits zeroed, so the ref locates the pack
      without an index lookup.

  The digest isn't part of the ref: a queue message carries it next to the
  ref, as `sha256`, and the reader checks it.

  A ref maps to a gateway path by dropping the prefix:
  `GET /v1/claims/<tenant>/<claim_id>` — see `path/1`.
  """

  alias Ankusa.ULID

  @enforce_keys [:tenant_id, :claim_id]
  defstruct [:tenant_id, :claim_id]

  @type t :: %__MODULE__{tenant_id: String.t(), claim_id: String.t()}

  @prefix "urn:ankusa:claim:v1:"
  @claims_prefix "claims/"
  @tenant_regex ~r/\A[A-Za-z0-9_-]{1,64}\z/

  @doc "The fixed storage prefix every claim object lives under."
  @spec claims_prefix() :: String.t()
  def claims_prefix, do: @claims_prefix

  @doc "Parse a URN. Rejects anything that isn't exactly the v1 grammar."
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

  def parse(_), do: {:error, :invalid_ref}

  @doc "The URN form."
  @spec to_string(t()) :: String.t()
  def to_string(%__MODULE__{tenant_id: tenant_id, claim_id: claim_id}),
    do: @prefix <> tenant_id <> ":" <> claim_id

  @doc "The gateway path that serves this ref's bytes."
  @spec path(t()) :: String.t()
  def path(%__MODULE__{tenant_id: tenant_id, claim_id: claim_id}),
    do: "/v1/claims/#{tenant_id}/#{claim_id}"

  @doc "The storage key of the pack this ref points into."
  @spec key(t()) :: String.t()
  def key(%__MODULE__{tenant_id: tenant_id, claim_id: claim_id}) do
    {:ok, pack_id, _index} = locate(claim_id)
    object_key(tenant_id, pack_id)
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
  """
  @spec object_key(String.t(), String.t()) :: String.t()
  def object_key(tenant_id, pack_id) do
    {:ok, bits} = ULID.decode(pack_id)

    date =
      bits
      |> ULID.timestamp_ms()
      |> DateTime.from_unix!(:millisecond)
      |> DateTime.to_date()
      |> Date.to_iso8601()

    "#{@claims_prefix}tenant=#{tenant_id}/dt=#{date}/#{pack_id}"
  end

  @doc "`:ok` if `tenant_id` is a valid tenant name."
  @spec validate_tenant(term()) :: :ok | {:error, :invalid_tenant}
  def validate_tenant(tenant_id) when is_binary(tenant_id) do
    if Regex.match?(@tenant_regex, tenant_id), do: :ok, else: {:error, :invalid_tenant}
  end

  def validate_tenant(_), do: {:error, :invalid_tenant}

  @doc "Is `tenant_id` a valid tenant name?"
  @spec valid_tenant?(term()) :: boolean()
  def valid_tenant?(tenant_id), do: validate_tenant(tenant_id) == :ok

  defimpl String.Chars do
    def to_string(ref), do: Ankusa.ClaimCheck.Ref.to_string(ref)
  end
end
