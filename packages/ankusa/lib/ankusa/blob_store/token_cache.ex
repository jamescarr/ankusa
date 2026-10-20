defmodule Ankusa.BlobStore.TokenCache do
  @moduledoc false
  # A node-wide cache of short-lived credentials for the object-store token
  # providers (`Ankusa.BlobStore.GCS.MetadataServer`,
  # `Ankusa.BlobStore.OCI.InstancePrincipal`): a value, and the time it expires.
  # A hit is an ETS read; a miss, or a value inside the refresh window, fetches
  # again; `invalidate/1` (a `401`) drops the entry so the next call fetches.
  #
  # The table is owned by `Ankusa.Application`, so the cache outlives the request
  # and task processes that fill it.

  @table :ankusa_blob_store_tokens

  @doc false
  @spec create_table() :: :ok
  def create_table do
    :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
    :ok
  end

  @doc """
  The cached value for `key` when it has more than `window_s` seconds left, else
  whatever `fetch` returns (`{:ok, value, expires_at}` caches; anything else is
  returned as is and caches nothing). `expires_at` is epoch seconds.
  """
  @spec fetch(term(), non_neg_integer(), (-> {:ok, term(), integer()} | term())) ::
          {:ok, term()} | term()
  def fetch(key, window_s, fetch) do
    now = System.system_time(:second)

    case lookup(key) do
      {:ok, value, expires_at} when expires_at - now > window_s ->
        {:ok, value}

      _miss ->
        case fetch.() do
          {:ok, value, expires_at} ->
            :ets.insert(@table, {key, value, expires_at})
            {:ok, value}

          other ->
            other
        end
    end
  end

  @doc "Drop `key`'s cached value."
  @spec invalidate(term()) :: :ok
  def invalidate(key) do
    :ets.delete(@table, key)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc false
  @spec clear() :: :ok
  def clear do
    if :ets.whereis(@table) != :undefined, do: :ets.delete_all_objects(@table)
    :ok
  end

  defp lookup(key) do
    case :ets.lookup(@table, key) do
      [{^key, value, expires_at}] -> {:ok, value, expires_at}
      [] -> :miss
    end
  rescue
    ArgumentError -> :miss
  end
end
