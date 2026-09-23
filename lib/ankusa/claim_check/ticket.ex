defmodule Ankusa.ClaimCheck.Ticket do
  @moduledoc """
  The canonical, versioned claim-check value that crosses every boundary —
  process, HTTP, and non-BEAM consumers alike. A ticket never carries the
  storage key: the key is *derived* from `tenant_id` and `id` (see `key/1`),
  so presenting a ticket can never steer a read at an arbitrary object (a
  compaction segment, or another tenant's claim).

  Wire shape (JSON):

      {"v": 1, "tenant_id": "acme", "id": "0199a1c2-...-7...",
       "size": 3145728, "sha256": "9f86d0...", "content_type": "application/json"}
  """

  @enforce_keys [:tenant_id, :id, :size, :sha256]
  defstruct [:tenant_id, :id, :size, :sha256, :content_type, v: 1]

  @type t :: %__MODULE__{
          v: 1,
          tenant_id: String.t(),
          id: String.t(),
          size: non_neg_integer(),
          sha256: String.t(),
          content_type: String.t() | nil
        }

  @max_tenant_bytes 256
  @uuid_v7_regex ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/

  @doc """
  Build a ticket from claim metadata and the checked-in bytes. Computes
  `size` and `sha256` from `data` — never trust a caller-supplied digest for
  the ticket that gets handed back.
  """
  @spec new(
          %{
            required(:tenant_id) => String.t(),
            required(:id) => String.t(),
            optional(:content_type) => String.t() | nil
          },
          iodata()
        ) ::
          {:ok, t()} | {:error, atom()}
  def new(%{tenant_id: tenant_id, id: id} = meta, data) do
    with :ok <- validate_tenant(tenant_id),
         :ok <- validate_id(id) do
      bin = IO.iodata_to_binary(data)

      {:ok,
       %__MODULE__{
         tenant_id: tenant_id,
         id: id,
         size: byte_size(bin),
         sha256: Base.encode16(:crypto.hash(:sha256, bin), case: :lower),
         content_type: Map.get(meta, :content_type)
       }}
    end
  end

  @doc "The derived, traversal-safe storage key: `claims/<enc(tenant_id)>/<id>`."
  @spec key(t()) :: String.t()
  def key(%__MODULE__{tenant_id: tenant_id, id: id}),
    do: "claims/#{encode_tenant(tenant_id)}/#{id}"

  @doc "Serialize to the wire map (JSON-encodable)."
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = t) do
    %{
      "v" => t.v,
      "tenant_id" => t.tenant_id,
      "id" => t.id,
      "size" => t.size,
      "sha256" => t.sha256,
      "content_type" => t.content_type
    }
  end

  @doc "Inverse of `to_map/1`; validates the format indicator and every field."
  @spec from_map(map()) :: {:ok, t()} | {:error, atom()}
  def from_map(%{"v" => 1} = map) do
    with {:ok, tenant_id} <- fetch_string(map, "tenant_id"),
         :ok <- validate_tenant(tenant_id),
         {:ok, id} <- fetch_string(map, "id"),
         :ok <- validate_id(id),
         {:ok, size} <- fetch_non_neg_integer(map, "size"),
         {:ok, sha256} <- fetch_sha256(map, "sha256") do
      {:ok,
       %__MODULE__{
         v: 1,
         tenant_id: tenant_id,
         id: id,
         size: size,
         sha256: sha256,
         content_type: Map.get(map, "content_type")
       }}
    end
  end

  def from_map(%{"v" => _other}), do: {:error, :unsupported_ticket_version}
  def from_map(_map), do: {:error, :unsupported_ticket_version}

  @doc false
  @spec validate_tenant(term()) :: :ok | {:error, :invalid_tenant}
  def validate_tenant(tenant_id)
      when is_binary(tenant_id) and byte_size(tenant_id) > 0 and
             byte_size(tenant_id) <= @max_tenant_bytes do
    if String.valid?(tenant_id), do: :ok, else: {:error, :invalid_tenant}
  end

  def validate_tenant(_), do: {:error, :invalid_tenant}

  @doc false
  @spec validate_id(term()) :: :ok | {:error, :invalid_id}
  def validate_id(id) when is_binary(id) do
    if Regex.match?(@uuid_v7_regex, id), do: :ok, else: {:error, :invalid_id}
  end

  def validate_id(_), do: {:error, :invalid_id}

  # Percent-encode every byte outside [A-Za-z0-9_-] (including `.`, so `.`
  # and `..` can never appear in the derived key) — injective, traversal-proof
  # on `BlobStore.LocalFS`, and passes sane tenant ids through unchanged.
  defp encode_tenant(tenant_id) do
    tenant_id
    |> :binary.bin_to_list()
    |> Enum.map(&encode_byte/1)
    |> IO.iodata_to_binary()
  end

  defp encode_byte(b)
       when b in ?A..?Z or b in ?a..?z or b in ?0..?9 or b == ?_ or b == ?-,
       do: <<b>>

  defp encode_byte(b), do: "%" <> Base.encode16(<<b>>, case: :upper)

  defp fetch_string(map, key) do
    case Map.get(map, key) do
      v when is_binary(v) -> {:ok, v}
      _ -> {:error, :unsupported_ticket_version}
    end
  end

  defp fetch_non_neg_integer(map, key) do
    case Map.get(map, key) do
      v when is_integer(v) and v >= 0 -> {:ok, v}
      _ -> {:error, :unsupported_ticket_version}
    end
  end

  @sha256_hex_regex ~r/\A[0-9a-f]{64}\z/

  defp fetch_sha256(map, key) do
    case Map.get(map, key) do
      v when is_binary(v) ->
        if Regex.match?(@sha256_hex_regex, v),
          do: {:ok, v},
          else: {:error, :unsupported_ticket_version}

      _ ->
        {:error, :unsupported_ticket_version}
    end
  end
end
