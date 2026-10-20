defmodule Ankusa.BlobStore.Azure do
  @moduledoc """
  `Ankusa.BlobStore` backed by Azure Blob Storage, via
  [`Req`](https://hex.pm/packages/req). Works against real Azure or the
  [Azurite](https://learn.microsoft.com/azure/storage/common/storage-use-azurite)
  emulator.

  This adapter deliberately carries **no credential dependency** — the same
  stance as `Ankusa.BlobStore.GCS`. It accepts a credential you already have,
  in either of the two forms Azure supports without the account key:

    * `:sas_token` — a Shared Access Signature query string (no leading `?`),
      generated out-of-band with the Azure CLI, SDK, or portal. The adapter
      appends it to every request and never sees the account key.
    * `:token_provider` — `{module, fun, args}`, applied per request, must
      return `{:ok, bearer_token} | :error`. For a service running on Azure,
      use the built-in `Ankusa.BlobStore.Azure.ManagedIdentity` (system- or
      user-assigned) rather than wiring your own Entra ID token source. On a
      `401` the adapter calls the module's `invalidate/n` (same arguments) when
      it exports one, and sends the request once more with the fresh token.

  Shared Key signing (the `Authorization: SharedKey ...` HMAC) is deliberately
  not implemented — it is the kind of security-sensitive canonicalization this
  project does not hand-roll, and a SAS token is the least-privilege credential
  Azure recommends anyway.

  opts:

    * `:account_name`   — required, e.g. `"myaccount"`
    * `:container`      — required; Azure's name for the bucket
    * `:sas_token`      — optional; a SAS query string (no leading `?`)
    * `:token_provider` — optional; `{module, fun, args}` returning a bearer token
    * `:endpoint`       — default `"https://\#{account_name}.blob.core.windows.net"`;
                           point at `"http://127.0.0.1:10000/\#{account_name}"` for Azurite
    * `:timeout_ms`     — default `10_000`, for both connect and response of
                           one request that carries at most 1 MiB
    * `:upload_timeout_ms`, `:op_deadline_ms`, `:multipart_threshold_bytes`,
      `:multipart_part_bytes`, `:breaker_*`, `:pool_*` — one deadline per
                           operation, a block-list upload above a size, a
                           breaker per container and a connection pool per
                           endpoint. See `Ankusa.BlobStore.Options`
    * `:req_options`    — transport options for the HTTP client, e.g. a custom
                           Finch pool, a proxy, or `plug:` for `Req.Test` in
                           tests. See `Ankusa.HttpClient` for the accepted keys
    * `:retries`        — default `2`: extra attempts after a transport error
                           or a `408`/`429`/`5xx`. See `Ankusa.BlobStore.Retry`
    * `:sse`            — `%{type: :kms, key_id: encryption_scope}`: every blob is
                           written under that encryption scope
                           (`x-ms-encryption-scope`). Blob Storage encrypts
                           everything at rest anyway; this picks the key. See
                           `Ankusa.BlobStore.SSE`

  `put_if_absent/4` sends `If-None-Match: *` (a `409`/`412` is `:already_exists`);
  every `PUT` carries `Content-MD5` (validated by Azure) and
  `x-ms-blob-content-md5` (stored), and a `GET` verifies the `Content-MD5` it is
  sent (`Ankusa.BlobStore.Checksum`); a body over `multipart_threshold_bytes` is
  Put Block per `multipart_part_bytes` and a Put Block List.

  ## Local dev

      config :ankusa,
        storage: %{
          blob_store:
            {Ankusa.BlobStore.Azure,
             account_name: "devstoreaccount1",
             container: "ankusa-segments-dev",
             endpoint: "http://127.0.0.1:10000/devstoreaccount1",
             sas_token: System.get_env("AZURE_BLOB_SAS")}
        }

  `docker compose -f docker-compose.integration.yml up -d azurite azure-bootstrap` brings up
  the emulator and creates the container.
  """

  @behaviour Ankusa.BlobStore

  # See Ankusa.BlobStore.S3 for why this false-positive suppression exists.
  @compile {:no_warn_undefined, [:xmerl_scan, :xmerl_xpath]}

  alias Ankusa.BlobStore
  alias Ankusa.BlobStore.{Checksum, HTTP, Options, SSE}

  @api_version "2024-11-04"

  @impl true
  def put(instance, key, data, opts), do: write(instance, key, data, opts, false)

  @impl true
  def put_if_absent(instance, key, data, opts) do
    case write(instance, key, data, opts, true) do
      {:error, :already_exists} -> BlobStore.same_object(__MODULE__, instance, key, data, opts)
      other -> other
    end
  end

  @impl true
  def get(instance, key, opts) do
    opts = Options.with_deadline(opts)

    case request(instance, opts, :small, :get, blob_url(opts, key), nil, []) do
      {:ok, status, headers, body} when status in 200..299 ->
        Checksum.check_response(headers, body, :md5, ["content-md5"])

      other ->
        HTTP.reply(other)
    end
  end

  @impl true
  def get_range(instance, key, offset, length, opts) do
    opts = Options.with_deadline(opts)
    range = "bytes=#{offset}-#{offset + length - 1}"

    HTTP.reply(
      request(instance, opts, :small, :get, blob_url(opts, key), nil, [{"range", range}])
    )
  end

  @impl true
  def delete(instance, key, opts) do
    opts = Options.with_deadline(opts)
    _ = request(instance, opts, :small, :delete, blob_url(opts, key), nil, [])
    :ok
  end

  # Every page (`NextMarker`), each under a deadline of its own, and a failed
  # page fails the listing.
  @impl true
  def list(instance, prefix, opts), do: list_pages(instance, opts, prefix, nil, [])

  defp list_pages(instance, opts, prefix, marker, acc) do
    query =
      [{"restype", "container"}, {"comp", "list"}, {"prefix", prefix}] ++
        if(marker, do: [{"marker", marker}], else: [])

    url = endpoint(opts) <> "/" <> container(opts) <> query_suffix(query, opts)
    page_opts = Options.with_deadline(opts)

    with {:ok, body} <- HTTP.reply(request(instance, page_opts, :small, :get, url, nil, [])),
         {:ok, keys, next} <- parse_list_page(body) do
      acc = [keys | acc]

      case next do
        nil -> {:ok, acc |> Enum.reverse() |> Enum.concat() |> Enum.sort()}
        marker -> list_pages(instance, opts, prefix, marker, acc)
      end
    end
  end

  # ── writes ────────────────────────────────────────────────────────────────

  defp write(instance, key, data, opts, conditional?) do
    opts = Options.with_deadline(opts)
    size = IO.iodata_length(data)

    if size > Options.get(opts, :multipart_threshold_bytes),
      do: block_list(instance, opts, key, IO.iodata_to_binary(data), conditional?),
      else: single(instance, opts, key, data, size, conditional?)
  end

  defp single(instance, opts, key, data, size, conditional?) do
    md5 = Checksum.md5_base64(data)

    headers =
      SSE.headers(opts, :azure) ++
        [
          {"x-ms-blob-type", "BlockBlob"},
          {"content-type", "application/octet-stream"},
          {"content-md5", md5},
          {"x-ms-blob-content-md5", md5}
        ] ++ precondition(conditional?)

    instance
    |> request(opts, Options.kind(size), :put, blob_url(opts, key), data, headers)
    |> HTTP.write_reply(exists_statuses(conditional?))
  end

  # `If-None-Match: *` on an existing blob is a 409 `BlobAlreadyExists` (or a 412
  # from a proxy or an emulator).
  defp precondition(true), do: [{"if-none-match", "*"}]
  defp precondition(false), do: []
  defp exists_statuses(true), do: [409, 412]
  defp exists_statuses(false), do: []

  # Put Block for each `multipart_part_bytes` (its own `Content-MD5`, an
  # `upload_timeout_ms` per block inside the operation's one deadline), then one
  # Put Block List that commits them with the whole blob's md5. Uncommitted
  # blocks expire on their own, so a failure needs no cleanup.
  defp block_list(instance, opts, key, data, conditional?) do
    blocks =
      data
      |> HTTP.chunks(Options.get(opts, :multipart_part_bytes))
      |> Enum.with_index()
      |> Enum.map(fn {chunk, index} -> {block_id(index), chunk} end)

    with :ok <- put_blocks(instance, opts, key, blocks) do
      ids = Enum.map(blocks, &elem(&1, 0))
      commit(instance, opts, key, ids, Checksum.md5_base64(data), conditional?)
    end
  end

  # Every id of a blob must have the same length.
  defp block_id(index), do: Base.encode64(String.pad_leading(Integer.to_string(index), 8, "0"))

  defp put_blocks(instance, opts, key, blocks) do
    Enum.reduce_while(blocks, :ok, fn {id, chunk}, :ok ->
      url = blob_url(opts, key, [{"comp", "block"}, {"blockid", id}])

      result =
        request(instance, opts, :bulk, :put, url, chunk, [
          {"content-md5", Checksum.md5_base64(chunk)}
        ])

      case HTTP.write_reply(result, []) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp commit(instance, opts, key, ids, md5, conditional?) do
    xml = [
      ~s(<?xml version="1.0" encoding="utf-8"?><BlockList>),
      for(id <- ids, do: ["<Latest>", id, "</Latest>"]),
      "</BlockList>"
    ]

    headers =
      SSE.headers(opts, :azure) ++
        [
          {"content-type", "application/xml"},
          {"x-ms-blob-content-type", "application/octet-stream"},
          {"x-ms-blob-content-md5", md5}
        ] ++ precondition(conditional?)

    url = blob_url(opts, key, [{"comp", "blocklist"}])

    instance
    |> request(opts, :small, :put, url, xml, headers)
    |> HTTP.write_reply(exists_statuses(conditional?))
  end

  # ── requests ──────────────────────────────────────────────────────────────

  # The token is fetched per attempt (a retry carries a fresh one), and a `401`
  # drops the provider's cached token and sends once more.
  defp request(instance, opts, kind, method, url, body, headers) do
    HTTP.with_refresh(Keyword.get(opts, :token_provider), fn ->
      HTTP.request(instance, opts, {endpoint(opts), container(opts)}, kind, fn ->
        {method, url, auth_headers(opts) ++ version_header(opts) ++ headers, body}
      end)
    end)
  end

  # A SAS already encodes the API version it was minted for; sending
  # `x-ms-version` alongside can only contradict it. The bearer-token path has
  # no `sv=`, so it needs an explicit version.
  defp version_header(opts) do
    if Keyword.has_key?(opts, :sas_token), do: [], else: [{"x-ms-version", @api_version}]
  end

  defp auth_headers(opts) do
    case Keyword.get(opts, :token_provider) do
      {mod, fun, args} ->
        case apply(mod, fun, args) do
          {:ok, token} -> [{"authorization", "Bearer #{token}"}]
          :error -> []
        end

      nil ->
        []
    end
  end

  # ── URL building ──────────────────────────────────────────────────────────

  # Azure blob names are UTF-8, `%`-encoded per RFC 3986 with `/` kept as a
  # path separator — the same rule as `Ankusa.BlobStore.S3`.
  defp encode_path(key) do
    unreserved = &URI.char_unreserved?/1

    key
    |> String.split("/")
    |> Enum.map_join("/", &URI.encode(&1, unreserved))
  end

  defp blob_url(opts, key, query \\ []) do
    base = endpoint(opts) <> "/" <> container(opts) <> "/" <> encode_path(key)

    case query do
      [] -> base <> sas_suffix(opts)
      query -> base <> query_suffix(query, opts)
    end
  end

  defp query_suffix(query, opts) do
    "?" <> URI.encode_query(query, :rfc3986) <> sas_suffix(opts, "&")
  end

  defp sas_suffix(opts, joiner \\ "?") do
    case Keyword.get(opts, :sas_token) do
      nil -> ""
      token -> joiner <> token
    end
  end

  defp container(opts), do: Keyword.fetch!(opts, :container)

  @doc false
  @spec endpoint(keyword()) :: String.t()
  def endpoint(opts) do
    Keyword.get(
      opts,
      :endpoint,
      "https://#{Keyword.fetch!(opts, :account_name)}.blob.core.windows.net"
    )
  end

  # ── ListBlobs XML (stdlib :xmerl, the same approach as S3's ListObjectsV2) ──

  # A 200 body is not guaranteed to be ListBlobs XML — a proxy error page or an
  # emulator quirk will do it. `:xmerl_scan` *exits* on a malformed document;
  # that is an error for the listing, never a crash of the caller.
  defp parse_list_page(xml_body) do
    {doc, _rest} = :xmerl_scan.string(:binary.bin_to_list(xml_body), quiet: true)

    keys =
      ~c"//Blob/Name/text()"
      |> :xmerl_xpath.string(doc)
      |> Enum.map(fn {:xmlText, _parents, _pos, _lang, value, _type} -> List.to_string(value) end)

    next =
      case :xmerl_xpath.string(~c"//NextMarker/text()", doc) do
        [{:xmlText, _parents, _pos, _lang, value, _type} | _] when value != [] ->
          List.to_string(value)

        _ ->
          nil
      end

    {:ok, keys, next}
  catch
    :exit, _not_xml -> {:error, :list_unreadable}
  end
end
