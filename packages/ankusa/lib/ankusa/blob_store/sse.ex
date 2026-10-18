defmodule Ankusa.BlobStore.SSE do
  @moduledoc """
  Server-side encryption for the objects an `Ankusa.BlobStore` writes: the
  `:sse` option of `Ankusa.BlobStore.S3`, `GCS`, `Azure` and `OCI`.

      sse: %{type: :aes256}
      sse: %{type: :kms, key_id: "arn:aws:kms:us-east-1:111122223333:key/…"}

  It is sent on **every** `put/4` of the store it is set on: segments, backups
  (`backup/…`) and claim packs alike, because they all reach the object store
  through the adapter's `put/4` with that store's options. Reads and deletes
  carry nothing: the provider decrypts transparently.

  | Adapter | `type: :aes256` | `type: :kms` |
  | --- | --- | --- |
  | `S3` | `x-amz-server-side-encryption: AES256` | `x-amz-server-side-encryption: aws:kms`, plus `x-amz-server-side-encryption-aws-kms-key-id` when `key_id` is set (without it S3 uses the bucket's default KMS key) |
  | `GCS` | nothing: GCS encrypts every object | `x-goog-encryption-kms-key-name`, and the `kmsKeyName` upload parameter; `key_id` is the key's resource name and is required |
  | `Azure` | nothing: Blob Storage encrypts every blob | `x-ms-encryption-scope`; `key_id` is the encryption scope's name and is required |
  | `OCI` | nothing: Object Storage encrypts every object | `opc-sse-kms-key-id`; `key_id` is the Vault key's OCID and is required |

  `type: :aes256` on GCS, Azure and OCI is accepted and sends nothing, so one
  config can say "encrypted" for every provider; only `:kms` changes the key.
  The S3 headers are signed with the rest of the request. A local store
  (`Ankusa.BlobStore.LocalFS`) refuses the option: its files rest on the
  node's volume.

  A misspelled option is a boot error (`validate!/3`), not an unencrypted
  object.
  """

  @adapters %{
    Ankusa.BlobStore.S3 => :s3,
    Ankusa.BlobStore.GCS => :gcs,
    Ankusa.BlobStore.Azure => :azure,
    Ankusa.BlobStore.OCI => :oci
  }

  @type adapter :: :s3 | :gcs | :azure | :oci

  @doc "The request headers `opts[:sse]` asks `adapter` to send on a `PUT`."
  @spec headers(keyword(), adapter()) :: [{String.t(), String.t()}]
  def headers(opts, adapter) do
    case fetch(opts, adapter) do
      nil -> []
      sse -> for_adapter(adapter, sse)
    end
  end

  @doc "The query parameters `opts[:sse]` adds to a GCS upload (`kmsKeyName`)."
  @spec query(keyword(), adapter()) :: [{String.t(), String.t()}]
  def query(opts, :gcs) do
    case fetch(opts, :gcs) do
      %{type: :kms, key_id: key_id} -> [{"kmsKeyName", key_id}]
      _none_or_aes -> []
    end
  end

  def query(_opts, _adapter), do: []

  # Validated on every use as well as at boot (`validate!/3`): an object is
  # never written unencrypted because an option was spelled wrong.
  defp fetch(opts, adapter) do
    case Keyword.get(opts, :sse) do
      nil ->
        nil

      sse ->
        {:ok, normalized} = normalize_or_raise(sse)
        check_adapter!(adapter, normalized, "sse")
        normalized
    end
  end

  defp normalize_or_raise(sse) do
    case normalize(sse) do
      {:ok, _normalized} = ok -> ok
      {:error, message} -> raise ArgumentError, "sse #{message}"
    end
  end

  defp for_adapter(:s3, %{type: :aes256}), do: [{"x-amz-server-side-encryption", "AES256"}]

  defp for_adapter(:s3, %{type: :kms, key_id: key_id}) do
    [{"x-amz-server-side-encryption", "aws:kms"}] ++
      if key_id, do: [{"x-amz-server-side-encryption-aws-kms-key-id", key_id}], else: []
  end

  defp for_adapter(:gcs, %{type: :kms, key_id: key_id}),
    do: [{"x-goog-encryption-kms-key-name", key_id}]

  defp for_adapter(:azure, %{type: :kms, key_id: key_id}),
    do: [{"x-ms-encryption-scope", key_id}]

  defp for_adapter(:oci, %{type: :kms, key_id: key_id}), do: [{"opc-sse-kms-key-id", key_id}]
  defp for_adapter(_adapter, %{type: :aes256}), do: []

  @doc """
  Check a blob store's `:sse` option and raise `ArgumentError` naming `where`
  (`"storage.blob_store"`) when it cannot do what it says. Stores without the
  option, and modules this package does not know, pass.
  """
  @spec validate!(module(), term(), String.t()) :: :ok
  def validate!(module, opts, where) do
    with true <- Keyword.keyword?(opts),
         {:ok, sse} <- Keyword.fetch(opts, :sse),
         true <- sse != nil do
      check!(module, sse, where)
    else
      _no_sse -> :ok
    end
  end

  defp check!(Ankusa.BlobStore.LocalFS, _sse, where) do
    raise ArgumentError,
          "#{where}: sse is not supported by Ankusa.BlobStore.LocalFS; " <>
            "local files rest on the node's volume (use an encrypted volume)"
  end

  # A module this package does not know owns its options, `:sse` included.
  defp check!(module, sse, where) when is_map_key(@adapters, module) do
    case normalize(sse) do
      {:ok, normalized} -> check_adapter!(Map.fetch!(@adapters, module), normalized, where)
      {:error, message} -> raise ArgumentError, "#{where}: sse #{message}"
    end
  end

  defp check!(_module, _sse, _where), do: :ok

  defp check_adapter!(adapter, %{type: :kms, key_id: nil}, where)
       when adapter in [:gcs, :azure, :oci] do
    raise ArgumentError, "#{where}: sse type :kms needs a key_id on #{adapter}"
  end

  defp check_adapter!(_adapter, _sse, _where), do: :ok

  defp normalize(sse) when is_list(sse) do
    if Keyword.keyword?(sse), do: sse |> Map.new() |> normalize(), else: bad()
  end

  defp normalize(%{} = sse) do
    type = Map.get(sse, :type)
    key_id = Map.get(sse, :key_id)
    extra = Map.keys(sse) -- [:type, :key_id]

    cond do
      extra != [] ->
        {:error, "has unknown keys #{inspect(extra)}; expected :type and :key_id"}

      type not in [:aes256, :kms] ->
        {:error, "type must be :aes256 or :kms, got #{inspect(type)}"}

      not (is_nil(key_id) or (is_binary(key_id) and key_id != "")) ->
        {:error, "key_id must be a non-empty string"}

      type == :aes256 and key_id != nil ->
        {:error, "key_id is only for type :kms"}

      true ->
        {:ok, %{type: type, key_id: key_id}}
    end
  end

  defp normalize(_other), do: bad()

  defp bad, do: {:error, "must be a map like %{type: :aes256} or %{type: :kms, key_id: \"…\"}"}
end
