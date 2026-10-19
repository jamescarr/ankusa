defmodule Ankusa.Queue.Tenants do
  @moduledoc false

  # Dense tenant ordinals (ADR 0001, section 2).
  #
  # `tenants` column family: `<<?t, ord::32>>` -> tenant id and `<<?n, tenant
  # id>>` -> `<<ord::32>>`; `m:next_ord` (default family) is the next ordinal to
  # hand out. Ordinal 0 is the node aggregate; real ordinals start at 1, are
  # assigned in first-sight order and are never reused.
  #
  # Only `Ankusa.Queue.Writer` allocates (and the format 2 -> 3 migration, which
  # runs before anything serves): `allocate/2` is pure and returns the puts that
  # must ride in the same synced batch as the rows using the new ordinals.
  # Readers go through `ord/2` and `name/2`.

  alias Ankusa.Store
  alias Ankusa.Store.Keys

  @default "default"

  @doc """
  The tenant a hook belongs to: `nil`, an empty or a non-binary tenant id is the
  `"default"` tenant, as in `Envelope.idempotency_key/1`.
  """
  @spec normalize(term()) :: String.t()
  def normalize(tenant) when is_binary(tenant) and tenant != "", do: tenant
  def normalize(_tenant), do: @default

  @doc "The ordinal of `tenant_id`, or `:not_found` when the tenant has no rows (never had any)."
  @spec ord(atom(), String.t()) :: {:ok, pos_integer()} | :not_found | {:error, term()}
  def ord(instance, tenant_id) do
    case Store.get(instance, :tenants, Keys.tenant_ord(normalize(tenant_id))) do
      {:ok, <<ord::32>>} -> {:ok, ord}
      {:ok, _bad} -> {:error, {:corrupt_value, :tenant_ords}}
      other -> other
    end
  end

  @doc "The tenant id of ordinal `ord`, or `:not_found` (a tombstone has an empty id)."
  @spec name(atom(), non_neg_integer()) :: {:ok, String.t()} | :not_found | {:error, term()}
  def name(instance, ord) do
    case Store.get(instance, :tenants, Keys.tenant(ord)) do
      {:ok, <<>>} -> :not_found
      other -> other
    end
  end

  @doc "How many ordinals have been handed out (`m:next_ord - 1`)."
  @spec count(atom()) :: {:ok, non_neg_integer()} | {:error, term()}
  def count(instance) do
    with {:ok, next} <- next_ord(instance), do: {:ok, next - 1}
  end

  @doc "The next ordinal to hand out."
  @spec next_ord(atom()) :: {:ok, pos_integer()} | {:error, term()}
  def next_ord(instance) do
    case Store.get(instance, :default, Keys.meta("next_ord")) do
      {:ok, <<next::32>>} -> {:ok, next}
      :not_found -> {:ok, 1}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Hands out ordinals to `tenants` (distinct tenant ids with no ordinal yet), in
  order, starting at `next` and stopping once `max` ordinals exist. Returns
  `{ords, ops, next, refused}`: the new `tenant => ord` pairs, the puts of their
  rows and of `m:next_ord`, the next ordinal after them, and the tenants left
  without one. `max` is `nil` for no limit (the migration never strands rows).
  """
  @spec allocate(pos_integer(), [String.t()], pos_integer() | nil) ::
          {%{String.t() => pos_integer()}, [Store.op()], pos_integer(), [String.t()]}
  def allocate(next, tenants, max) do
    {ords, next, refused} =
      Enum.reduce(tenants, {%{}, next, []}, fn tenant, {ords, next, refused} ->
        if max != nil and next - 1 >= max,
          do: {ords, next, [tenant | refused]},
          else: {Map.put(ords, tenant, next), next + 1, refused}
      end)

    ops =
      case map_size(ords) do
        0 ->
          []

        _ ->
          Enum.flat_map(ords, fn {tenant, ord} ->
            [
              {:put, :tenants, Keys.tenant(ord), tenant},
              {:put, :tenants, Keys.tenant_ord(tenant), <<ord::32>>}
            ]
          end) ++ [{:put, :default, Keys.meta("next_ord"), <<next::32>>}]
      end

    {ords, ops, next, Enum.reverse(refused)}
  end

  @doc """
  The ordinals of `tenants` that already have a row, read in one `multi_get`:
  `{:ok, %{tenant => ord}}`.
  """
  @spec lookup(atom(), [String.t()]) :: {:ok, %{String.t() => pos_integer()}} | {:error, term()}
  def lookup(_instance, []), do: {:ok, %{}}

  def lookup(instance, tenants) do
    with {:ok, results} <-
           Store.multi_get(instance, :tenants, Enum.map(tenants, &Keys.tenant_ord/1)) do
      {:ok,
       for(
         {tenant, {:ok, <<ord::32>>}} <- Enum.zip(tenants, results),
         into: %{},
         do: {tenant, ord}
       )}
    end
  end
end
