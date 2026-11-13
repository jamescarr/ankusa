defmodule Ankusa.Gateway.ACL.Store do
  @moduledoc """
  ACL documents (`Ankusa.Gateway.ACL`) and object owners, read and written in
  the space's own store.

  Reads go through the `:acl` cache (`Ankusa.Gateway.Cache`) for
  `gateway.sharing.acl_cache_ttl_ms`, and a miss through one
  `Ankusa.Gateway.Caller` named `:acl` (at most 32 store reads in flight,
  1024 queued, a breaker after 5 failures in a row): many requests for one
  document make one store read. A missing document or object is cached too.
  That TTL is the cross-node bound of a revocation and of a re-created
  object's old head; this node drops its own entries on every write
  (`invalidate/3`).

  `update/5` is compare-and-swap on the document's version: up to 5 rounds of
  read, `fun`, conditional write, then `{:error, :conflict}`. Nothing is ever
  lost to a concurrent update: a lost round re-reads and re-applies `fun`.
  """

  alias Ankusa.Gateway.{ACL, Cache, Caller, Config}

  @rounds 5
  @max_doc_bytes 1_048_576

  @type space :: Ankusa.Gateway.Spaces.space()
  @type head :: %{owner: String.t() | nil, incarnation: String.t() | nil}

  @doc "The `:acl` caller's child spec (started by the supervisor with sharing on)."
  @spec caller_spec(atom()) :: Supervisor.child_spec()
  def caller_spec(instance) do
    Caller.child_spec(
      instance: instance,
      name: :acl,
      label: "acl",
      max_inflight: 32,
      queue_max: 1024,
      partition_queue_max: 1024,
      batch: nil,
      breaker: %{failures: 5, open_ms: 5_000},
      run: &run(instance, &1)
    )
  end

  @doc "The document of `{kind, key}` in `space` (`nil`: none), cached."
  @spec fetch(atom(), space(), ACL.kind(), String.t(), integer()) ::
          {:ok, ACL.doc() | nil} | {:error, {:unavailable, term()}}
  def fetch(instance, space, kind, key, deadline) do
    cached(instance, {:doc, space.name, kind, key}, space, {:doc, space, kind, key}, deadline)
  end

  @doc "The object's `ankusa-owner` and `ankusa-incarnation` (`nil`: no object), cached."
  @spec head(atom(), space(), String.t(), integer()) ::
          {:ok, head() | nil} | {:error, {:unavailable, term()}}
  def head(instance, space, key, deadline) do
    cached(instance, {:head, space.name, key}, space, {:head, space, key}, deadline)
  end

  @doc """
  What the cache holds for `fetch/5` or `head/4`, without a store read:
  `{:ok, value}` or `:miss`.
  """
  @spec peek(atom(), {:doc, space(), ACL.kind(), String.t()} | {:head, space(), String.t()}) ::
          {:ok, term()} | :miss
  def peek(instance, item) do
    case Cache.fetch(instance, :acl, cache_key(item)) do
      {:ok, :none} -> {:ok, nil}
      {:ok, {_tag, value}} -> {:ok, value}
      :error -> :miss
    end
  end

  defp cached(instance, cache_key, space, item, deadline) do
    case peek(instance, item) do
      {:ok, value} -> {:ok, value}
      :miss -> Caller.call(instance, :acl, cache_key, space.name, item, deadline)
    end
  end

  defp cache_key({:doc, space, kind, key}), do: {:doc, space.name, kind, key}
  defp cache_key({:head, space, key}), do: {:head, space.name, key}

  # The caller's run: one store read, cached before it returns.
  defp run(instance, item) do
    gateway = Ankusa.Gateway.config(instance)
    result = read(instance, gateway, item)

    case result do
      {:ok, value} -> remember(instance, gateway, cache_key(item), value)
      {:error, _} -> :ok
    end

    result
  end

  defp read(instance, gateway, {:doc, space, kind, key}) do
    {module, _opts} = space.store
    opts = Config.store_opts(gateway, space) ++ [max_bytes: @max_doc_bytes]

    case module.get_object(instance, ACL.doc_key(space, {kind, key}), opts) do
      {:ok, _meta, bin} ->
        case ACL.decode(bin) do
          {:ok, doc} -> {:ok, doc}
          {:error, :invalid_acl} -> {:error, {:unavailable, :invalid_acl}}
        end

      {:error, :not_found} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, {:unavailable, reason}}
    end
  end

  defp read(instance, gateway, {:head, space, key}) do
    {module, _opts} = space.store
    object_key = space.prefix <> key

    case module.head_object(instance, object_key, Config.store_opts(gateway, space)) do
      {:ok, meta} -> {:ok, head_of(meta)}
      {:error, :not_found} -> {:ok, nil}
      {:error, reason} -> {:error, {:unavailable, reason}}
    end
  end

  defp head_of(%{metadata: metadata}),
    do: %{owner: metadata["ankusa-owner"], incarnation: metadata["ankusa-incarnation"]}

  defp remember(instance, gateway, cache_key, value) do
    tag = elem(cache_key, 0)
    stored = if value == nil, do: :none, else: {tag, value}
    Cache.put(instance, :acl, cache_key, stored, gateway.sharing.acl_cache_ttl_ms)
  end

  @doc "Drop this node's cached head and object document of `key` (after a write or a delete)."
  @spec invalidate(atom(), space(), String.t()) :: :ok
  def invalidate(instance, space, key) do
    Cache.delete(instance, :acl, {:head, space.name, key})
    Cache.delete(instance, :acl, {:doc, space.name, :object, key})
  end

  @doc "Delete the object's document (best effort, after the object is deleted)."
  @spec delete_object_doc(atom(), space(), String.t()) :: :ok | {:error, term()}
  def delete_object_doc(instance, space, key) do
    {module, _opts} = space.store
    gateway = Ankusa.Gateway.config(instance)

    module.delete_object(
      instance,
      ACL.doc_key(space, {:object, key}),
      Config.store_opts(gateway, space)
    )
  end

  @doc """
  Read, change and write the document of `{kind, key}`. `fun` gets the live
  document, or `nil` when there is none or it belongs to another incarnation
  of the object, and returns `{:ok, doc}` to write (its `incarnation` is set to
  the object's current one) or `{:error, reason}` to stop with that error. An
  object that does not exist is `{:error, :not_found}`.
  """
  @spec update(atom(), space(), ACL.kind(), String.t(), (ACL.doc() | nil ->
                                                           {:ok, ACL.doc()} | {:error, term()})) ::
          {:ok, ACL.doc()} | {:error, :not_found | :conflict | {:unavailable, term()} | term()}
  def update(instance, space, kind, key, fun) do
    gateway = Ankusa.Gateway.config(instance)

    with {:ok, incarnation} <- incarnation(instance, gateway, space, kind, key) do
      result = rounds(instance, gateway, space, {kind, key}, incarnation, fun, @rounds)
      Cache.delete(instance, :acl, {:doc, space.name, kind, key})
      result
    end
  end

  defp incarnation(_instance, _gateway, _space, :prefix, _key), do: {:ok, nil}

  defp incarnation(instance, gateway, space, :object, key) do
    {module, _opts} = space.store

    case module.head_object(instance, space.prefix <> key, Config.store_opts(gateway, space)) do
      {:ok, meta} -> {:ok, head_of(meta).incarnation}
      {:error, :not_found} -> {:error, :not_found}
      {:error, reason} -> {:error, {:unavailable, reason}}
    end
  end

  defp rounds(_instance, _gateway, _space, _resource, _incarnation, _fun, 0),
    do: {:error, :conflict}

  defp rounds(instance, gateway, space, resource, incarnation, fun, left) do
    {module, _opts} = space.store
    doc_key = ACL.doc_key(space, resource)
    opts = Config.store_opts(gateway, space)

    with {:ok, current, condition} <- current(instance, module, doc_key, opts, incarnation),
         {:ok, doc} <- fun.(current) do
      doc = %{doc | incarnation: incarnation, updated_at: System.os_time(:second)}
      put_opts = opts ++ [content_type: "application/json", if: condition]

      case module.put_object(instance, doc_key, ACL.encode(doc), put_opts) do
        {:ok, _version} ->
          {:ok, doc}

        {:error, reason} when reason in [:version_mismatch, :already_exists] ->
          rounds(instance, gateway, space, resource, incarnation, fun, left - 1)

        {:error, reason} ->
          {:error, {:unavailable, reason}}
      end
    end
  end

  # The live document (or nil) and the condition its replacement is written under.
  defp current(instance, module, doc_key, opts, incarnation) do
    case module.get_object(instance, doc_key, opts ++ [max_bytes: @max_doc_bytes]) do
      {:ok, meta, bin} ->
        doc =
          case ACL.decode(bin) do
            {:ok, %{incarnation: ^incarnation} = doc} -> doc
            # Another incarnation's grants, or a damaged document: replaced.
            _dead -> nil
          end

        {:ok, doc, {:version, meta.version}}

      {:error, :not_found} ->
        {:ok, nil, :absent}

      {:error, reason} ->
        {:error, {:unavailable, reason}}
    end
  end
end
