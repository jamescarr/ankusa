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

  ## Object operations

  `Ankusa.BlobStore`'s optional object callbacks map onto blobs directly: the
  version is the blob's `ETag`, without its surrounding quotes; `:if` rides as
  `If-None-Match: *` (`:absent`) or `If-Match: "<version>"`, and a losing
  condition is Azure's `412` (or its `409 BlobAlreadyExists`), which the reply
  mapping the HTTP stores share reads by condition; metadata is
  `x-ms-meta-<name>`. `put_object/4` sends the object's `Content-Type` (on a Put
  Blob as `Content-Type`, on a block list as `x-ms-blob-content-type`); above the
  threshold, the content type, the metadata **and** the condition all ride the
  Put Block List — the one request that commits — and the new version is that
  response's `ETag`. `list_page/3` is `comp=list` with `maxresults`, `delimiter`
  and `marker`.

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
  @content_type "application/octet-stream"

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

  # ── object operations ─────────────────────────────────────────────────────

  @impl true
  def head_object(instance, key, opts) do
    opts = Options.with_deadline(opts)

    instance
    |> request(opts, :small, :head, blob_url(opts, key), nil, [])
    |> HTTP.object_reply(nil)
    |> object_meta_reply()
  end

  @impl true
  def get_object(instance, key, opts) do
    opts = Options.with_deadline(opts)
    condition = Keyword.get(opts, :if)
    range = Keyword.get(opts, :range)
    opts = cap_response(opts, Keyword.get(opts, :max_bytes))

    headers = List.wrap(range && HTTP.range_header(range)) ++ condition_headers(condition)

    case instance
         |> request(opts, :small, :get, blob_url(opts, key), nil, headers)
         |> HTTP.object_reply(condition) do
      {:ok, _status, _headers, {:truncated, _read}} ->
        {:error, :too_large}

      {:ok, _status, headers, body} ->
        with {:ok, body} <- verify_body(headers, body, range),
             do: {:ok, object_meta(headers), body}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def put_object(instance, key, data, opts) do
    opts = Options.with_deadline(opts)
    condition = Keyword.get(opts, :if)

    result =
      put_blob(
        instance,
        opts,
        key,
        data,
        Keyword.get(opts, :content_type, @content_type),
        Keyword.get(opts, :metadata, %{}),
        condition
      )

    result |> HTTP.object_reply(condition) |> version_reply()
  end

  @impl true
  def delete_object(instance, key, opts) do
    opts = Options.with_deadline(opts)

    case instance
         |> request(opts, :small, :delete, blob_url(opts, key), nil, [])
         |> HTTP.object_reply(nil) do
      {:ok, _status, _headers, _body} -> :ok
      # a key that is already gone is the outcome the caller asked for
      {:error, :not_found} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def list_page(instance, prefix, opts) do
    opts = Options.with_deadline(opts)

    query =
      [{"restype", "container"}, {"comp", "list"}, {"prefix", prefix}] ++
        [{"maxresults", Integer.to_string(Keyword.get(opts, :limit, 1000))}] ++
        delimiter_query(Keyword.get(opts, :delimiter)) ++ cursor_query(Keyword.get(opts, :cursor))

    url = endpoint(opts) <> "/" <> container(opts) <> query_suffix(query, opts)

    with {:ok, body} <- HTTP.reply(request(instance, opts, :small, :get, url, nil, [])) do
      parse_objects_page(body)
    end
  end

  defp object_meta_reply({:ok, _status, headers, _body}), do: {:ok, object_meta(headers)}
  defp object_meta_reply({:error, reason}), do: {:error, reason}

  defp object_meta(headers) do
    %{
      size: HTTP.object_size(headers) || 0,
      version: HTTP.version(HTTP.header(headers, "etag")) || "",
      content_type: HTTP.header(headers, "content-type"),
      last_modified: HTTP.http_date(HTTP.header(headers, "last-modified")),
      metadata: HTTP.metadata(headers, "x-ms-meta-")
    }
  end

  # A ranged read is a window, so the whole-blob digest the store sends back
  # does not describe it; only a whole read is checked.
  defp verify_body(_headers, body, range) when not is_nil(range), do: {:ok, body}

  defp verify_body(headers, body, nil),
    do: Checksum.check_response(headers, body, :md5, ["content-md5"])

  defp version_reply({:ok, _status, headers, _body}) do
    case HTTP.version(HTTP.header(headers, "etag")) do
      nil -> {:error, :no_version}
      version -> {:ok, version}
    end
  end

  defp version_reply({:error, reason}), do: {:error, reason}

  defp cap_response(opts, nil), do: opts

  defp cap_response(opts, max_bytes) do
    Keyword.update(
      opts,
      :req_options,
      [max_response_bytes: max_bytes],
      &Keyword.put(&1, :max_response_bytes, max_bytes)
    )
  end

  defp delimiter_query(nil), do: []
  defp delimiter_query(delimiter), do: [{"delimiter", delimiter}]

  defp cursor_query(nil), do: []
  defp cursor_query(cursor), do: [{"marker", cursor}]

  # ── writes ────────────────────────────────────────────────────────────────

  defp write(instance, key, data, opts, conditional?) do
    opts = Options.with_deadline(opts)
    condition = if conditional?, do: :absent

    result = put_blob(instance, opts, key, data, @content_type, %{}, condition)
    HTTP.write_reply(result, exists_statuses(conditional?))
  end

  # The blob's content type and metadata ride the request that writes it: the
  # Put Blob itself, or the Put Block List that commits a larger one (the only
  # request that carries them for a block list).
  defp put_blob(instance, opts, key, data, content_type, metadata, condition) do
    size = IO.iodata_length(data)

    if size > Options.get(opts, :multipart_threshold_bytes),
      do:
        block_list(
          instance,
          opts,
          key,
          IO.iodata_to_binary(data),
          {content_type, metadata, condition}
        ),
      else: single(instance, opts, key, data, size, {content_type, metadata, condition})
  end

  defp single(instance, opts, key, data, size, {content_type, metadata, condition}) do
    md5 = Checksum.md5_base64(data)

    headers =
      SSE.headers(opts, :azure) ++
        [
          {"x-ms-blob-type", "BlockBlob"},
          {"content-type", content_type},
          {"content-md5", md5},
          {"x-ms-blob-content-md5", md5}
        ] ++ metadata_headers(metadata) ++ condition_headers(condition)

    request(instance, opts, Options.kind(size), :put, blob_url(opts, key), data, headers)
  end

  defp metadata_headers(metadata) do
    metadata
    |> Enum.sort()
    |> Enum.map(fn {name, value} -> {"x-ms-meta-" <> String.downcase(name), value} end)
  end

  # `If-None-Match: *` on an existing blob is a 409 `BlobAlreadyExists` (or a 412
  # from a proxy or an emulator).
  defp condition_headers(nil), do: []
  defp condition_headers(:absent), do: [{"if-none-match", "*"}]
  defp condition_headers({:version, version}), do: [{"if-match", entity_tag(version)}]

  defp entity_tag(version), do: ~s("#{String.trim(version, "\"")}")

  defp exists_statuses(true), do: [409, 412]
  defp exists_statuses(false), do: []

  # Put Block for each `multipart_part_bytes` (its own `Content-MD5`, an
  # `upload_timeout_ms` per block inside the operation's one deadline), then one
  # Put Block List that commits them with the whole blob's md5. Uncommitted
  # blocks expire on their own, so a failure needs no cleanup.
  defp block_list(instance, opts, key, data, {content_type, metadata, condition}) do
    blocks =
      data
      |> HTTP.chunks(Options.get(opts, :multipart_part_bytes))
      |> Enum.with_index()
      |> Enum.map(fn {chunk, index} -> {block_id(index), chunk} end)

    with :ok <- put_blocks(instance, opts, key, blocks) do
      ids = Enum.map(blocks, &elem(&1, 0))

      commit(
        instance,
        opts,
        key,
        ids,
        Checksum.md5_base64(data),
        {content_type, metadata, condition}
      )
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

  defp commit(instance, opts, key, ids, md5, {content_type, metadata, condition}) do
    xml = [
      ~s(<?xml version="1.0" encoding="utf-8"?><BlockList>),
      for(id <- ids, do: ["<Latest>", id, "</Latest>"]),
      "</BlockList>"
    ]

    headers =
      SSE.headers(opts, :azure) ++
        [
          {"content-type", "application/xml"},
          {"x-ms-blob-content-type", content_type},
          {"x-ms-blob-content-md5", md5}
        ] ++ metadata_headers(metadata) ++ condition_headers(condition)

    url = blob_url(opts, key, [{"comp", "blocklist"}])

    request(instance, opts, :small, :put, url, xml, headers)
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
    doc = scan!(xml_body)
    {:ok, texts(doc, ~c"//Blob/Name/text()"), next_marker(doc)}
  catch
    :exit, _not_xml -> {:error, :list_unreadable}
  end

  # One page of keys with sizes, the common prefixes a `delimiter` grouped, and
  # the marker that continues it.
  defp parse_objects_page(xml_body) do
    doc = scan!(xml_body)

    objects =
      doc
      |> xpath(~c"//Blobs/Blob")
      |> Enum.map(fn blob ->
        %{
          key: blob |> texts(~c"Name/text()") |> List.first(""),
          size: blob |> texts(~c"Properties/Content-Length/text()") |> size_of()
        }
      end)

    {:ok,
     %{
       objects: objects,
       prefixes: texts(doc, ~c"//Blobs/BlobPrefix/Name/text()"),
       next: next_marker(doc)
     }}
  catch
    :exit, _not_xml -> {:error, :list_unreadable}
  end

  defp scan!(xml_body) do
    {doc, _rest} = :xmerl_scan.string(:binary.bin_to_list(xml_body), quiet: true)
    doc
  end

  defp xpath(node, path), do: :xmerl_xpath.string(path, node)

  defp texts(node, path) do
    node
    |> xpath(path)
    |> Enum.map(fn {:xmlText, _parents, _pos, _lang, value, _type} -> List.to_string(value) end)
  end

  defp size_of([size | _]) do
    case Integer.parse(size) do
      {value, ""} -> value
      _not_a_size -> 0
    end
  end

  defp size_of([]), do: 0

  defp next_marker(doc) do
    case texts(doc, ~c"//NextMarker/text()") do
      [marker | _] when marker != "" -> marker
      _none -> nil
    end
  end
end
