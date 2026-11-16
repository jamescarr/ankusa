defmodule Ankusa.BlobStore.GCS do
  @moduledoc """
  `Ankusa.BlobStore` backed by Google Cloud Storage's JSON API, via
  [`Req`](https://hex.pm/packages/req). Works against real GCS or the
  `floci-gcp` emulator in `docker-compose.integration.yml`.

  This adapter deliberately carries **no OAuth2 dependency**: `:token_provider`
  is a callback you point at whatever your deployment already uses (Goth, ADC),
  rather than a bundled OAuth2 client. On GCE/GKE the built-in
  `Ankusa.BlobStore.GCS.MetadataServer` needs no configuration.

  opts:

    * `:bucket`         — required
    * `:endpoint`       — default `"https://storage.googleapis.com"`; point
                           at `http://localhost:4588` for floci-gcp
    * `:token_provider` — `{module, fun, args}`, applied per request, must
                           return `{:ok, bearer_token} | :error`. Omit for the
                           emulator (unauthenticated). **Required against real
                           GCS**: `{Ankusa.BlobStore.GCS.MetadataServer, :token, []}`
                           on GCE/GKE, or your own source (Goth, ADC). On a
                           `401` the adapter calls the module's `invalidate/n`
                           (same arguments) when it exports one and sends the
                           request once more with the fresh token
    * `:timeout_ms`     — default `10_000`, for both connect and response of
                           one request that carries at most 1 MiB
    * `:upload_timeout_ms`, `:op_deadline_ms`, `:multipart_threshold_bytes`,
      `:multipart_part_bytes`, `:breaker_*`, `:pool_*` — one deadline per
                           operation, resumable upload above a size, a breaker
                           per bucket and a connection pool per endpoint. See
                           `Ankusa.BlobStore.Options`
    * `:req_options`    — transport options for the HTTP client, e.g. a custom
                           Finch pool, a proxy, or `plug:` for `Req.Test` in
                           tests. See `Ankusa.HttpClient` for the accepted keys
    * `:retries`        — default `2`: extra attempts after a transport error
                           or a `408`/`429`/`5xx`, each with a fresh token from
                           `:token_provider`. See `Ankusa.BlobStore.Retry`
    * `:sse`            — `%{type: :kms, key_id: key_resource_name}`: every upload
                           names the Cloud KMS key that encrypts the object
                           (`x-goog-encryption-kms-key-name` and `kmsKeyName`).
                           GCS encrypts everything at rest anyway; this picks
                           the key. See `Ankusa.BlobStore.SSE`

  `put_if_absent/4` is `ifGenerationMatch=0` (a `412` is `:already_exists`);
  every upload carries `x-goog-hash: md5=…` and a `GET` verifies the
  `x-goog-hash` md5 it is sent (`Ankusa.BlobStore.Checksum`); a body over
  `multipart_threshold_bytes` is a resumable upload in `multipart_part_bytes`
  chunks.

  ## Object operations

  `head_object/3`, `get_object/3`, `put_object/4`, `delete_object/3` and
  `list_page/3` — the gateway's objects API — use the JSON API's own
  vocabulary: an object's version is its `generation`, `if: {:version, g}` is
  `ifGenerationMatch=<g>`, `if: :absent` is `ifGenerationMatch=0`, a read
  `:range` is a `Range` header on an `alt=media` request, and custom metadata
  is the JSON document's `metadata` (the `x-goog-meta-*` headers on a media
  response). A `put_object/4` is one `uploadType=multipart` request, or, above
  `multipart_threshold_bytes`, a resumable upload whose initiation carries the
  content type, the metadata and the condition; the `generation` of the final
  response is the version returned.

  ## Local dev

      config :ankusa,
        storage: %{
          blob_store:
            {Ankusa.BlobStore.GCS, bucket: "ankusa-segments-dev", endpoint: "http://localhost:4588"}
        }

  `docker compose -f docker-compose.integration.yml up -d floci-gcp gcs-bootstrap` brings up
  the emulator and creates the bucket.
  """

  @behaviour Ankusa.BlobStore

  alias Ankusa.BlobStore
  alias Ankusa.BlobStore.{Checksum, HTTP, Options, SSE}

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
    url = media_url(opts, "o/" <> URI.encode_www_form(key), [{"alt", "media"}])

    case request(instance, opts, :small, :get, url, nil, [], nil) do
      {:ok, status, headers, body} when status in 200..299 ->
        with :ok <- Checksum.verify(:md5, body, Checksum.goog_md5(headers)), do: {:ok, body}

      other ->
        HTTP.reply(other)
    end
  end

  @impl true
  def get_range(instance, key, offset, length, opts) do
    opts = Options.with_deadline(opts)
    url = media_url(opts, "o/" <> URI.encode_www_form(key), [{"alt", "media"}])
    range = "bytes=#{offset}-#{offset + length - 1}"
    HTTP.reply(request(instance, opts, :small, :get, url, nil, [{"range", range}], nil))
  end

  @impl true
  def delete(instance, key, opts) do
    opts = Options.with_deadline(opts)
    url = media_url(opts, "o/" <> URI.encode_www_form(key), [])
    _ = request(instance, opts, :small, :delete, url, nil, [], nil)
    :ok
  end

  # Every page (`nextPageToken`), each under a deadline of its own, and a failed
  # page fails the listing.
  @impl true
  def list(instance, prefix, opts), do: list_pages(instance, opts, prefix, nil, [])

  defp list_pages(instance, opts, prefix, token, acc) do
    query = [{"prefix", prefix}] ++ if(token, do: [{"pageToken", token}], else: [])
    page_opts = Options.with_deadline(opts)

    with {:ok, body} <-
           HTTP.reply(
             request(instance, page_opts, :small, :get, media_url(opts, "o", query), nil, [], nil)
           ),
         {:ok, page} when is_map(page) <- JSON.decode(body) do
      acc = [page |> Map.get("items", []) |> Enum.map(& &1["name"]) | acc]

      case page["nextPageToken"] do
        next when is_binary(next) and next != "" ->
          list_pages(instance, opts, prefix, next, acc)

        _ ->
          {:ok, acc |> Enum.reverse() |> Enum.concat() |> Enum.sort()}
      end
    else
      {:error, reason} -> {:error, reason}
      {:ok, _not_a_page} -> {:error, :list_unreadable}
    end
  end

  # ── objects (Ankusa.BlobStore's optional callbacks) ────────────────────

  @impl true
  def head_object(instance, key, opts) do
    opts = Options.with_deadline(opts)

    with {:ok, json} <- metadata_json(instance, opts, key) do
      {:ok, metadata_meta(json)}
    end
  end

  @impl true
  def get_object(instance, key, opts) do
    condition = Keyword.get(opts, :if)
    range = Keyword.get(opts, :range)
    opts = Options.with_deadline(opts)
    media_opts = limit_response(opts, Keyword.get(opts, :max_bytes))
    headers = if(range, do: [HTTP.range_header(range)], else: [])

    url =
      media_url(opts, "o/" <> URI.encode_www_form(key), [
        {"alt", "media"} | condition_query(condition)
      ])

    case request(instance, media_opts, :bulk, :get, url, nil, headers, nil) do
      {:ok, status, _headers, {:truncated, _read}} when status in 200..299 ->
        {:error, :too_large}

      {:ok, status, response_headers, body} when status in 200..299 ->
        with :ok <- read_checksum(range, response_headers, body),
             {:ok, meta} <- read_meta(instance, opts, key, response_headers) do
          {:ok, meta, body}
        end

      other ->
        HTTP.object_reply(other, condition)
    end
  end

  @impl true
  def put_object(instance, key, data, opts) do
    opts = Options.with_deadline(opts)
    size = IO.iodata_length(data)
    condition = Keyword.get(opts, :if)

    if size > Options.get(opts, :multipart_threshold_bytes),
      do: resumable_object(instance, opts, key, IO.iodata_to_binary(data), condition),
      else: multipart_object(instance, opts, key, data, size, condition)
  end

  @impl true
  def delete_object(instance, key, opts) do
    opts = Options.with_deadline(opts)
    url = media_url(opts, "o/" <> URI.encode_www_form(key), [])

    case request(instance, opts, :small, :delete, url, nil, [], nil) do
      {:ok, status, _headers, _body} when status in 200..299 -> :ok
      {:ok, 404, _headers, _body} -> :ok
      other -> HTTP.reply(other)
    end
  end

  @impl true
  def list_page(instance, prefix, opts) do
    opts = Options.with_deadline(opts)
    cursor = Keyword.get(opts, :cursor)
    delimiter = Keyword.get(opts, :delimiter)
    limit = Keyword.get(opts, :limit, 1000)

    query =
      [{"prefix", prefix}, {"maxResults", Integer.to_string(limit)}] ++
        if(delimiter, do: [{"delimiter", delimiter}], else: []) ++
        if(cursor, do: [{"pageToken", cursor}], else: [])

    with {:ok, body} <-
           HTTP.reply(
             request(instance, opts, :small, :get, media_url(opts, "o", query), nil, [], nil)
           ),
         {:ok, page} when is_map(page) <- JSON.decode(body) do
      {:ok,
       %{
         objects:
           Enum.map(Map.get(page, "items", []), fn item ->
             %{key: item["name"], size: int_value(item["size"])}
           end),
         prefixes: Map.get(page, "prefixes", []),
         next: next_token(page)
       }}
    else
      {:error, reason} -> {:error, reason}
      {:ok, _not_a_page} -> {:error, :list_unreadable}
    end
  end

  # The metadata document: size, generation, content type, update time and the
  # custom metadata GCS stored.
  defp metadata_json(instance, opts, key) do
    url = media_url(opts, "o/" <> URI.encode_www_form(key), [])

    case request(instance, opts, :small, :get, url, nil, [], nil) do
      {:ok, status, _headers, body} when status in 200..299 ->
        case JSON.decode(body) do
          {:ok, json} when is_map(json) -> {:ok, json}
          _not_a_document -> {:error, :metadata_unreadable}
        end

      other ->
        HTTP.object_reply(other, nil)
    end
  end

  defp metadata_meta(json) do
    %{
      size: int_value(json["size"]),
      version: version_value(json["generation"]),
      content_type: json["contentType"],
      last_modified: rfc3339_seconds(json["updated"]),
      metadata: string_map(json["metadata"])
    }
  end

  # A media response carries the generation and the custom metadata as
  # `x-goog-generation` and `x-goog-meta-*`; when they are absent (an emulator,
  # or a store that omits them) the metadata document is read once more.
  defp read_meta(instance, opts, key, headers) do
    if HTTP.header(headers, "x-goog-generation") do
      {:ok, media_meta(headers, nil)}
    else
      with {:ok, json} <- metadata_json(instance, opts, key) do
        {:ok, media_meta(headers, json)}
      end
    end
  end

  defp media_meta(headers, json) do
    json = json || %{}

    %{
      size: HTTP.object_size(headers) || int_value(json["size"]),
      version: HTTP.header(headers, "x-goog-generation") || version_value(json["generation"]),
      content_type: HTTP.header(headers, "content-type") || json["contentType"],
      last_modified:
        HTTP.http_date(HTTP.header(headers, "last-modified")) || rfc3339_seconds(json["updated"]),
      metadata: media_metadata(headers, json)
    }
  end

  defp media_metadata(headers, json) do
    case HTTP.metadata(headers, "x-goog-meta-") do
      metadata when map_size(metadata) > 0 -> metadata
      _none -> string_map(json["metadata"])
    end
  end

  # A ranged read has no whole-object digest to check.
  defp read_checksum(range, _headers, _body) when not is_nil(range), do: :ok

  defp read_checksum(nil, headers, body),
    do: Checksum.verify(:md5, body, Checksum.goog_md5(headers))

  # A `uploadType=multipart` upload: the JSON metadata part (name, content type
  # and the custom metadata), then the media part, in one `multipart/related`
  # body. The precondition rides the query.
  defp multipart_object(instance, opts, key, data, size, condition) do
    boundary = boundary()
    query = [{"uploadType", "multipart"}] ++ SSE.query(opts, :gcs) ++ condition_query(condition)
    body = multipart_body(key, opts, data, boundary)
    content_type = "multipart/related; boundary=\"#{boundary}\""

    case request(
           instance,
           opts,
           Options.kind(size),
           :post,
           media_url(opts, "o", query),
           body,
           SSE.headers(opts, :gcs),
           content_type
         ) do
      {:ok, status, response_headers, response_body} when status in 200..299 ->
        with {:ok, json} <- uploaded_json(response_body) do
          object_generation(json, response_headers)
        end

      other ->
        HTTP.object_reply(other, condition)
    end
  end

  defp multipart_body(key, opts, data, boundary) do
    json = JSON.encode!(upload_document(key, opts))

    [
      "--",
      boundary,
      "\r\n",
      "content-type: application/json; charset=UTF-8\r\n\r\n",
      json,
      "\r\n",
      "--",
      boundary,
      "\r\n",
      "content-type: ",
      content_type(opts),
      "\r\n\r\n",
      data,
      "\r\n",
      "--",
      boundary,
      "--\r\n"
    ]
  end

  defp boundary, do: "ankusa" <> Integer.to_string(:erlang.unique_integer([:positive]), 16)

  defp upload_document(key, opts) do
    document = %{"name" => key, "contentType" => content_type(opts)}

    case Keyword.get(opts, :metadata) || %{} do
      metadata when map_size(metadata) == 0 -> document
      metadata -> Map.put(document, "metadata", string_map(metadata))
    end
  end

  defp uploaded_json(body) do
    case JSON.decode(body) do
      {:ok, json} when is_map(json) -> {:ok, json}
      _not_a_document -> {:error, :upload_unreadable}
    end
  end

  defp object_generation(json, headers) do
    case json["generation"] || HTTP.header(headers, "x-goog-generation") do
      nil -> {:error, :missing_version}
      version -> {:ok, to_string(version)}
    end
  end

  # A big object is a resumable upload: the initiation carries the content type
  # and the custom metadata in its JSON, and the precondition in its query; the
  # generation of the final response is the version.
  defp resumable_object(instance, opts, key, data, condition) do
    case resumable_upload(instance, opts, key, data, condition, upload_document(key, opts)) do
      {:ok, nil} -> {:error, :missing_version}
      {:ok, version} -> {:ok, to_string(version)}
      {:error, _reason} = error -> error
    end
  end

  defp content_type(opts), do: Keyword.get(opts, :content_type, "application/octet-stream")

  defp condition_query(nil), do: []
  defp condition_query(:absent), do: [{"ifGenerationMatch", "0"}]
  defp condition_query({:version, version}), do: [{"ifGenerationMatch", to_string(version)}]

  defp int_value(nil), do: 0
  defp int_value(value) when is_integer(value), do: value

  defp int_value(value) when is_binary(value) do
    case Integer.parse(value) do
      {size, ""} -> size
      _not_an_integer -> 0
    end
  end

  defp int_value(_value), do: 0

  defp version_value(nil), do: ""
  defp version_value(version), do: to_string(version)

  defp string_map(nil), do: %{}

  defp string_map(map) when is_map(map) do
    Map.new(map, fn {name, value} -> {String.downcase(to_string(name)), to_string(value)} end)
  end

  defp string_map(_not_a_map), do: %{}

  defp rfc3339_seconds(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> DateTime.to_unix(datetime)
      _not_a_date -> nil
    end
  end

  defp rfc3339_seconds(_value), do: nil

  defp next_token(page) do
    case page["nextPageToken"] do
      token when is_binary(token) and token != "" -> token
      _none -> nil
    end
  end

  defp limit_response(opts, nil), do: opts

  defp limit_response(opts, max_bytes) do
    req_options =
      opts |> Keyword.get(:req_options, []) |> Keyword.put(:max_response_bytes, max_bytes)

    Keyword.put(opts, :req_options, req_options)
  end

  # ── writes ─────────────────────────────────────────────────────────────

  defp write(instance, key, data, opts, conditional?) do
    opts = Options.with_deadline(opts)
    size = IO.iodata_length(data)

    if size > Options.get(opts, :multipart_threshold_bytes),
      do: resumable(instance, opts, key, IO.iodata_to_binary(data), conditional?),
      else: simple(instance, opts, key, data, size, conditional?)
  end

  defp simple(instance, opts, key, data, size, conditional?) do
    condition = if conditional?, do: :absent, else: nil

    url =
      media_url(
        opts,
        "o",
        [{"uploadType", "media"}, {"name", key}] ++
          SSE.query(opts, :gcs) ++ condition_query(condition)
      )

    headers = SSE.headers(opts, :gcs) ++ [{"x-goog-hash", "md5=" <> Checksum.md5_base64(data)}]

    instance
    |> request(opts, Options.kind(size), :post, url, data, headers, "application/octet-stream")
    |> HTTP.write_reply(exists_statuses(conditional?))
  end

  # A failed `ifGenerationMatch=0` is a 412.
  defp exists_statuses(true), do: [412]
  defp exists_statuses(false), do: []

  # A resumable upload: one request opens a session (with the preconditions and
  # the encryption key), then each chunk of `multipart_part_bytes` is a `PUT`
  # with its `Content-Range`, answered `308` until the last. Each request has its
  # own `upload_timeout_ms` inside the operation's one deadline.
  defp resumable(instance, opts, key, data, conditional?) do
    condition = if conditional?, do: :absent, else: nil

    case resumable_upload(instance, opts, key, data, condition, %{"name" => key}) do
      {:ok, _version} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp resumable_upload(instance, opts, key, data, condition, document) do
    query =
      [{"uploadType", "resumable"}, {"name", key}] ++
        SSE.query(opts, :gcs) ++ condition_query(condition)

    headers =
      SSE.headers(opts, :gcs) ++
        [
          {"x-upload-content-type", content_type(opts)},
          {"x-upload-content-length", Integer.to_string(byte_size(data))}
        ]

    body = JSON.encode!(document)

    case request(
           instance,
           opts,
           :small,
           :post,
           media_url(opts, "o", query),
           body,
           headers,
           "application/json; charset=UTF-8"
         ) do
      {:ok, status, response_headers, _body} when status in 200..299 ->
        with {:ok, session} <- session_uri(opts, response_headers) do
          case upload_chunks(instance, opts, session, data, condition) do
            {:ok, chunk_headers, chunk_body} ->
              {:ok, final_generation(chunk_headers, chunk_body)}

            {:error, _reason} = error ->
              abort_upload(instance, opts, session)
              error
          end
        end

      other ->
        HTTP.object_reply(other, condition)
    end
  end

  defp final_generation(headers, body) do
    case JSON.decode(body) do
      {:ok, json} when is_map(json) ->
        json["generation"] || HTTP.header(headers, "x-goog-generation")

      _not_a_document ->
        HTTP.header(headers, "x-goog-generation")
    end
  end

  # The session URI carries the bearer token on every chunk: only ever follow one
  # on the endpoint the upload was opened on.
  defp session_uri(opts, headers) do
    location = Checksum.first(headers, "location")

    if is_binary(location) and String.starts_with?(location, endpoint(opts) <> "/"),
      do: {:ok, location},
      else: {:error, :bad_upload_location}
  end

  defp upload_chunks(instance, opts, session, data, condition) do
    total = byte_size(data)

    data
    |> HTTP.chunks(Options.get(opts, :multipart_part_bytes))
    |> Enum.reduce_while(0, fn chunk, offset ->
      last = offset + byte_size(chunk) - 1
      range = [{"content-range", "bytes #{offset}-#{last}/#{total}"}]
      final? = last + 1 == total

      case request(
             instance,
             opts,
             :bulk,
             :put,
             session,
             chunk,
             range,
             "application/octet-stream"
           ) do
        {:ok, 308, _headers, _body} when not final? ->
          {:cont, last + 1}

        {:ok, status, headers, body} when status in 200..299 and final? ->
          {:halt, {:ok, headers, body}}

        other ->
          {:halt, HTTP.object_reply(other, condition)}
      end
    end)
    |> case do
      {:ok, _headers, _body} = done -> done
      {:error, _reason} = error -> error
      _incomplete -> {:error, :upload_incomplete}
    end
  end

  # Best effort, under a short deadline of its own: the operation's may be spent.
  defp abort_upload(instance, opts, session) do
    opts =
      opts
      |> Keyword.put(:deadline, System.monotonic_time(:millisecond) + 5_000)
      |> Keyword.put(:retries, 0)

    _ = request(instance, opts, :small, :delete, session, nil, [], nil)
    :ok
  end

  # ── internals ──────────────────────────────────────────────────────────

  defp bucket(opts), do: Keyword.fetch!(opts, :bucket)

  @doc false
  @spec endpoint(keyword()) :: String.t()
  def endpoint(opts), do: Keyword.get(opts, :endpoint, "https://storage.googleapis.com")

  defp base(opts, prefix), do: endpoint(opts) <> "/storage/v1/b/#{bucket(opts)}/#{prefix}"

  # `uploadType=media` and `=resumable` use the `/upload/...` tree; everything
  # else reads under `/storage/v1/...`.
  defp media_url(opts, "o" <> _ = suffix, [{"uploadType", _type} | _] = query) do
    endpoint(opts) <>
      "/upload/storage/v1/b/#{bucket(opts)}/#{suffix}" <> query_suffix(query)
  end

  defp media_url(opts, suffix, query), do: base(opts, suffix) <> query_suffix(query)

  defp query_suffix([]), do: ""
  defp query_suffix(query), do: "?" <> URI.encode_query(query)

  # The token is fetched per attempt (a retry carries a fresh one), and a `401`
  # drops the provider's cached token and sends once more.
  defp request(instance, opts, kind, method, url, body, headers, content_type) do
    HTTP.with_refresh(Keyword.get(opts, :token_provider), fn ->
      HTTP.request(instance, opts, {endpoint(opts), bucket(opts)}, kind, fn ->
        {method, url, auth_headers(opts) ++ headers ++ content_type_header(content_type), body}
      end)
    end)
  end

  defp content_type_header(nil), do: []
  defp content_type_header(content_type), do: [{"content-type", content_type}]

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
end
