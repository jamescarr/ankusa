defmodule Ankusa.BlobStore.Checksum do
  @moduledoc """
  The integrity headers the HTTP object stores take on a `PUT`, and the check of
  the digest they send back on a `GET`.

  | Adapter | On `PUT` | Verified on `GET` |
  | --- | --- | --- |
  | `S3` | `x-amz-checksum-sha256` (and `Content-MD5` on each multipart part) | `x-amz-checksum-sha256`, requested with `x-amz-checksum-mode: ENABLED` |
  | `GCS` | `x-goog-hash: md5=…` | `x-goog-hash` |
  | `Azure` | `Content-MD5` and `x-ms-blob-content-md5` | `Content-MD5` |
  | `OCI` | `Content-MD5` (and `x-content-sha256`, which the signature covers) | `opc-content-md5` |

  The store validates the digest it was sent, so bytes damaged on the way up are
  refused with a `4xx`; a digest the store sends back is compared with the bytes
  received, so bytes damaged on the way down are `{:error, :integrity}`. A
  response without the header (the store never kept one, a composite multipart
  digest, an emulator) is not checked: absence is not corruption.
  """

  @doc "Base64 SHA-256 of `data`."
  @spec sha256_base64(iodata()) :: String.t()
  def sha256_base64(data), do: Base.encode64(:crypto.hash(:sha256, data))

  @doc "Base64 MD5 of `data`."
  @spec md5_base64(iodata()) :: String.t()
  def md5_base64(data), do: Base.encode64(:crypto.hash(:md5, data))

  @doc """
  `:ok` unless `expected` (the base64 digest a response header carried) is not
  the `algorithm` (`:sha256` or `:md5`) digest of `body`. A `nil` or empty
  `expected` is `:ok`.
  """
  @spec verify(:sha256 | :md5, iodata(), String.t() | nil) :: :ok | {:error, :integrity}
  def verify(_algorithm, _body, nil), do: :ok
  def verify(_algorithm, _body, ""), do: :ok

  def verify(algorithm, body, expected) do
    actual = Base.encode64(:crypto.hash(algorithm, body))
    if actual == String.trim(expected), do: :ok, else: {:error, :integrity}
  end

  @doc """
  Check a `GET` response's `body` against the digest header `names` the store
  puts on it (the first of `names` that is present); `:sha256` or `:md5`.
  `{:ok, body}` or `{:error, :integrity}`.
  """
  @spec check_response(%{String.t() => [String.t()]}, binary(), :sha256 | :md5, [String.t()]) ::
          {:ok, binary()} | {:error, :integrity}
  def check_response(headers, body, algorithm, names) do
    expected = Enum.find_value(names, &first(headers, &1))

    # An S3 composite checksum (`<digest>-<parts>`) is not a digest of the bytes.
    if is_binary(expected) and String.contains?(expected, "-") and algorithm == :sha256 do
      {:ok, body}
    else
      with :ok <- verify(algorithm, body, expected), do: {:ok, body}
    end
  end

  @doc "The md5 a `x-goog-hash` header (`crc32c=…,md5=…`, possibly several headers) carries."
  @spec goog_md5(%{String.t() => [String.t()]}) :: String.t() | nil
  def goog_md5(headers) do
    headers
    |> Map.get("x-goog-hash", [])
    |> Enum.flat_map(&String.split(&1, ","))
    |> Enum.find_value(fn part ->
      case String.split(String.trim(part), "=", parts: 2) do
        ["md5", digest] -> digest
        _other -> nil
      end
    end)
  end

  @doc "The first value of response header `name`, or `nil`."
  @spec first(%{String.t() => [String.t()]}, String.t()) :: String.t() | nil
  def first(headers, name) do
    case Map.get(headers, name) do
      [value | _] -> value
      value when is_binary(value) -> value
      _none -> nil
    end
  end
end
