defmodule Ankusa.BlobStore do
  @moduledoc """
  Object storage: `PUT` whole immutable objects, range `GET` one record,
  `DELETE` for retention, `LIST` by prefix. The default is the local
  filesystem; S3, GCS, Azure Blob and OCI are drop-in adapters.

  This module is both the behaviour and the instance-scoped facade. Every call
  names a scope:

    * `:segments` — compacted hook segments (`Ankusa.Storage`): the
      `storage.blob_store`, with every new key under `storage.key_prefix`.
      Nodes that share one bucket give themselves distinct prefixes
      (`"node-a/"`), because a segment key (`seg/<first>-<last>`) is only
      unique per node.
    * `:backup` — store backups (`Ankusa.Store.Backup`): the
      `backup.blob_store` when set, else the segment store, **with**
      `storage.key_prefix`. A backup belongs to one node's store, like its
      segments.

  Claim packs do not go through this facade: `Ankusa.ClaimCheck.Stores`
  writes and reads them across `claim_check.blob_stores`, never prefixed.

  The prefix is applied once, when a new object is named (`object_key/3`);
  the full key is what the archive catalogue records, and `put`/`get`/
  `get_range`/`delete` take full keys verbatim. Changing `storage.key_prefix`
  therefore moves only new segments, never strands written ones.

  ## Encryption at rest

  The object-store adapters take an `:sse` option (`Ankusa.BlobStore.SSE`) that
  asks the provider to encrypt every object a `put/4` writes, with a KMS key or
  the provider's own. It is per store, so it covers the segment store, the
  backup store and every claim store as each is configured.

  ## Transport

  The HTTP adapters share their transport options (`Ankusa.BlobStore.Options`):
  one deadline per operation, jittered retries that honour `Retry-After`
  (`Ankusa.BlobStore.Retry`), a breaker per bucket (`Ankusa.BlobStore.Breaker`),
  multipart uploads above a size, integrity checksums
  (`Ankusa.BlobStore.Checksum`) and a connection pool per endpoint
  (`Ankusa.BlobStore.Pool`).
  """

  alias Ankusa.Config

  @type scope :: :segments | :backup

  @callback put(instance :: atom(), key :: String.t(), data :: iodata(), opts :: keyword()) ::
              :ok | {:error, term()}

  @doc """
  `put/4`, but only if no object exists under `key`: `{:error, :already_exists}`
  when one does, with that object left exactly as it was. The adapters use the
  store's own precondition (S3 and Azure `If-None-Match: *`, GCS
  `ifGenerationMatch=0`, OCI `if-none-match: *`), so the check and the write are
  one atomic request. `Ankusa.ClaimCheck` writes every pack with it: a reused
  or colliding pack id then refuses instead of overwriting claims that are
  already referenced.

  An object that exists with exactly the bytes being written is `:ok`
  (`same_object/5`): a retried request whose first attempt landed is not a
  collision with another writer.
  """
  @callback put_if_absent(
              instance :: atom(),
              key :: String.t(),
              data :: iodata(),
              opts :: keyword()
            ) :: :ok | {:error, :already_exists} | {:error, term()}
  @callback get(instance :: atom(), key :: String.t(), opts :: keyword()) ::
              {:ok, binary()} | {:error, term()}
  @callback get_range(
              instance :: atom(),
              key :: String.t(),
              offset :: non_neg_integer(),
              length :: pos_integer(),
              opts :: keyword()
            ) :: {:ok, binary()} | {:error, term()}
  @callback delete(instance :: atom(), key :: String.t(), opts :: keyword()) :: :ok

  @doc """
  Every key under `prefix`, sorted, across all of the store's result pages. A
  page that fails fails the call: a partial listing is never presented as the
  whole.
  """
  @callback list(instance :: atom(), prefix :: String.t(), opts :: keyword()) ::
              {:ok, [String.t()]} | {:error, term()}

  @doc "The `{module, opts}` a scope resolves to on `instance`."
  @spec store(atom(), scope()) :: {module(), keyword()}
  def store(instance, scope) do
    {mod, opts, _prefix} = resolve(instance, scope)
    {mod, opts}
  end

  @doc """
  The full key a new object in `scope` is written under: `key` behind
  `storage.key_prefix`. The compactor names each segment with it and records
  the result in the archive catalogue, so a later prefix change only moves new
  segments; the ones already written are read back under the key they were
  written with.
  """
  @spec object_key(atom(), scope(), String.t()) :: String.t()
  def object_key(instance, scope, key) do
    {_mod, _opts, prefix} = resolve(instance, scope)
    prefix <> key
  end

  @doc """
  Settle an `:already_exists`: `:ok` when `key` already holds exactly `data` (a
  conditional `PUT` that landed, whose answer was lost and which was then sent
  again, finds its own object), else `{:error, :already_exists}`. A store that
  cannot be read back is `{:error, :already_exists}` too: the caller mints a new
  key rather than assume.
  """
  @spec same_object(module(), atom(), String.t(), iodata(), keyword()) ::
          :ok | {:error, :already_exists}
  def same_object(mod, instance, key, data, opts) do
    expected = IO.iodata_to_binary(data)

    case mod.get(instance, key, opts) do
      {:ok, ^expected} -> :ok
      _other -> {:error, :already_exists}
    end
  end

  @spec put(atom(), scope(), String.t(), iodata()) :: :ok | {:error, term()}
  def put(instance, scope, key, data) do
    {mod, opts, _prefix} = resolve(instance, scope)
    mod.put(instance, key, data, opts)
  end

  @spec put_if_absent(atom(), scope(), String.t(), iodata()) ::
          :ok | {:error, :already_exists} | {:error, term()}
  def put_if_absent(instance, scope, key, data) do
    {mod, opts, _prefix} = resolve(instance, scope)
    mod.put_if_absent(instance, key, data, opts)
  end

  @spec get(atom(), scope(), String.t()) :: {:ok, binary()} | {:error, term()}
  def get(instance, scope, key) do
    {mod, opts, _prefix} = resolve(instance, scope)
    mod.get(instance, key, opts)
  end

  @spec get_range(atom(), scope(), String.t(), non_neg_integer(), pos_integer()) ::
          {:ok, binary()} | {:error, term()}
  def get_range(instance, scope, key, offset, length) do
    {mod, opts, _prefix} = resolve(instance, scope)
    mod.get_range(instance, key, offset, length, opts)
  end

  @spec delete(atom(), scope(), String.t()) :: :ok
  def delete(instance, scope, key) do
    {mod, opts, _prefix} = resolve(instance, scope)
    mod.delete(instance, key, opts)
  end

  @doc "Every key of this node's objects in `scope` under `prefix` (behind `storage.key_prefix` for `:segments` and `:backup`), as full keys."
  @spec list(atom(), scope(), String.t()) :: {:ok, [String.t()]} | {:error, term()}
  def list(instance, scope, prefix) do
    {mod, opts, key_prefix} = resolve(instance, scope)
    mod.list(instance, key_prefix <> prefix, opts)
  end

  defp resolve(instance, scope), do: resolve_config(Ankusa.config(instance), scope)

  @doc false
  # For callers that hold a config rather than a running instance.
  @spec resolve_config(Config.t(), scope()) :: {module(), keyword(), String.t()}
  def resolve_config(%Config{storage: storage}, :segments) do
    {mod, opts} = storage.blob_store
    {mod, opts, Map.get(storage, :key_prefix, "")}
  end

  def resolve_config(%Config{backup: backup, storage: storage}, :backup) do
    {mod, opts} = Map.get(backup, :blob_store) || storage.blob_store
    {mod, opts, Map.get(storage, :key_prefix, "")}
  end

  @doc """
  The second object store backups are also written to (`backup.secondary`),
  under the same `storage.key_prefix`; `nil` when none is configured.
  """
  @spec secondary_config(Config.t()) :: {module(), keyword(), String.t()} | nil
  def secondary_config(%Config{backup: backup, storage: storage}) do
    case Map.get(backup, :secondary) do
      {mod, opts} -> {mod, opts, Map.get(storage, :key_prefix, "")}
      nil -> nil
    end
  end

  @doc """
  Check the `:sse` option and the transport options (`Ankusa.BlobStore.Options`)
  of every blob store the config names (`storage.blob_store`,
  `backup.blob_store`, `backup.secondary`, `claim_check.blob_stores`).
  """
  @spec validate_config!(Config.t()) :: :ok
  def validate_config!(%Config{storage: storage, backup: backup, claim_check: claim_check}) do
    claim_stores =
      claim_check.blob_stores
      |> List.wrap()
      |> Enum.with_index(fn store, index -> {"claim_check.blob_stores[#{index}]", store} end)

    [
      {"storage.blob_store", storage.blob_store},
      {"backup.blob_store", backup.blob_store},
      {"backup.secondary", Map.get(backup, :secondary)}
    ]
    |> Kernel.++(claim_stores)
    |> Enum.each(fn
      {where, {mod, opts}} when is_atom(mod) ->
        Ankusa.BlobStore.SSE.validate!(mod, opts, where)
        Ankusa.BlobStore.Options.validate!(mod, opts, where)

      _unset_or_unknown ->
        :ok
    end)
  end
end
