defmodule Ankusa.ClaimCheck.Ref do
  @moduledoc """
  A claim-check reference: one string that names a claim's bytes and grants no
  access to them.

      urn:ankusa:claim:v1:<tenant>:<object_id>:<offset>:<length>:sha256-<hex>

    * `tenant` — `[A-Za-z0-9_-]{1,64}`; the same string in the URN, the gateway
      path, and the storage key, with no encoding anywhere
    * `object_id` — the lowercase UUIDv7 of the pack object holding the claim;
      its timestamp picks the object's `dt=` folder
    * `offset`, `length` — where the claim's bytes sit inside that object
      (decimal, no leading zeros, `length >= 1`)
    * `sha256` — lowercase hex digest of the claim's bytes. The reader checks
      it; it is never sent to the gateway

  A ref maps to a gateway path by dropping the prefix and the digest:
  `GET /v1/claims/<tenant>/<object_id>/<offset>/<length>` — see `path/1`.
  """

  @enforce_keys [:tenant_id, :object_id, :offset, :length, :sha256]
  defstruct [:tenant_id, :object_id, :offset, :length, :sha256]

  @type t :: %__MODULE__{
          tenant_id: String.t(),
          object_id: String.t(),
          offset: non_neg_integer(),
          length: pos_integer(),
          sha256: String.t()
        }

  @prefix "urn:ankusa:claim:v1:"
  @claims_prefix "claims/"
  @tenant_regex ~r/\A[A-Za-z0-9_-]{1,64}\z/
  @object_id_regex ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/
  @sha256_regex ~r/\A[0-9a-f]{64}\z/
  # 12 digits keeps every offset and length far below anything a pack can
  # reach, and gives a front proxy's route regex a fixed bound to match.
  @offset_regex ~r/\A(0|[1-9][0-9]{0,11})\z/
  @length_regex ~r/\A[1-9][0-9]{0,11}\z/

  @doc "The fixed storage prefix every claim object lives under."
  @spec claims_prefix() :: String.t()
  def claims_prefix, do: @claims_prefix

  @doc "Parse a URN. Rejects anything that isn't exactly the v1 grammar."
  @spec parse(String.t()) :: {:ok, t()} | {:error, :invalid_ref}
  def parse(@prefix <> rest) do
    with [tenant, object_id, offset, length, "sha256-" <> sha256] <- String.split(rest, ":"),
         :ok <- validate_tenant(tenant),
         :ok <- validate_object_id(object_id),
         {:ok, offset, length} <- parse_range(offset, length),
         true <- Regex.match?(@sha256_regex, sha256) do
      {:ok,
       %__MODULE__{
         tenant_id: tenant,
         object_id: object_id,
         offset: offset,
         length: length,
         sha256: sha256
       }}
    else
      _ -> {:error, :invalid_ref}
    end
  end

  def parse(_), do: {:error, :invalid_ref}

  @doc "The URN form."
  @spec to_string(t()) :: String.t()
  def to_string(%__MODULE__{} = ref) do
    @prefix <>
      Enum.join(
        [
          ref.tenant_id,
          ref.object_id,
          Integer.to_string(ref.offset),
          Integer.to_string(ref.length),
          "sha256-" <> ref.sha256
        ],
        ":"
      )
  end

  @doc "The gateway path that serves this ref's bytes."
  @spec path(t()) :: String.t()
  def path(%__MODULE__{} = ref) do
    "/v1/claims/#{ref.tenant_id}/#{ref.object_id}/#{ref.offset}/#{ref.length}"
  end

  @doc "The storage key of the object this ref points into."
  @spec key(t()) :: String.t()
  def key(%__MODULE__{tenant_id: tenant_id, object_id: object_id}),
    do: object_key(tenant_id, object_id)

  @doc """
  The storage key for an object: Hive-style partition folders, so Spark,
  BigQuery, and Athena discover `tenant` and `dt` as columns. `dt` is the UTC
  date of the object id's UUIDv7 timestamp, so the key is a pure function of
  the ref. Only call this with a validated tenant and object id.
  """
  @spec object_key(String.t(), String.t()) :: String.t()
  def object_key(tenant_id, object_id) do
    {:ok, ms} = Ankusa.UUIDv7.timestamp_ms(object_id)
    date = ms |> DateTime.from_unix!(:millisecond) |> DateTime.to_date() |> Date.to_iso8601()
    "#{@claims_prefix}tenant=#{tenant_id}/dt=#{date}/#{object_id}"
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

  @doc "`:ok` if `object_id` is a lowercase UUIDv7."
  @spec validate_object_id(term()) :: :ok | {:error, :invalid_id}
  def validate_object_id(object_id) when is_binary(object_id) do
    if Regex.match?(@object_id_regex, object_id), do: :ok, else: {:error, :invalid_id}
  end

  def validate_object_id(_), do: {:error, :invalid_id}

  @doc "Parse a decimal offset and length in the canonical form the grammar allows."
  @spec parse_range(String.t(), String.t()) ::
          {:ok, non_neg_integer(), pos_integer()} | {:error, :invalid_range}
  def parse_range(offset, length) when is_binary(offset) and is_binary(length) do
    if Regex.match?(@offset_regex, offset) and Regex.match?(@length_regex, length) do
      {:ok, String.to_integer(offset), String.to_integer(length)}
    else
      {:error, :invalid_range}
    end
  end

  def parse_range(_, _), do: {:error, :invalid_range}

  defimpl String.Chars do
    def to_string(ref), do: Ankusa.ClaimCheck.Ref.to_string(ref)
  end
end
