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

  # ── writes ─────────────────────────────────────────────────────────────

  defp write(instance, key, data, opts, conditional?) do
    opts = Options.with_deadline(opts)
    size = IO.iodata_length(data)

    if size > Options.get(opts, :multipart_threshold_bytes),
      do: resumable(instance, opts, key, IO.iodata_to_binary(data), conditional?),
      else: simple(instance, opts, key, data, size, conditional?)
  end

  defp simple(instance, opts, key, data, size, conditional?) do
    url =
      media_url(
        opts,
        "o",
        [{"uploadType", "media"}, {"name", key}] ++
          SSE.query(opts, :gcs) ++ precondition(conditional?)
      )

    headers = SSE.headers(opts, :gcs) ++ [{"x-goog-hash", "md5=" <> Checksum.md5_base64(data)}]

    instance
    |> request(opts, Options.kind(size), :post, url, data, headers, "application/octet-stream")
    |> HTTP.write_reply(exists_statuses(conditional?))
  end

  # A failed `ifGenerationMatch=0` is a 412.
  defp precondition(true), do: [{"ifGenerationMatch", "0"}]
  defp precondition(false), do: []
  defp exists_statuses(true), do: [412]
  defp exists_statuses(false), do: []

  # A resumable upload: one request opens a session (with the preconditions and
  # the encryption key), then each chunk of `multipart_part_bytes` is a `PUT`
  # with its `Content-Range`, answered `308` until the last. Each request has its
  # own `upload_timeout_ms` inside the operation's one deadline.
  defp resumable(instance, opts, key, data, conditional?) do
    query =
      [{"uploadType", "resumable"}, {"name", key}] ++
        SSE.query(opts, :gcs) ++ precondition(conditional?)

    headers =
      SSE.headers(opts, :gcs) ++
        [
          {"x-upload-content-type", "application/octet-stream"},
          {"x-upload-content-length", Integer.to_string(byte_size(data))}
        ]

    body = JSON.encode!(%{"name" => key})

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
          case upload_chunks(instance, opts, session, data, conditional?) do
            :ok ->
              :ok

            {:error, _reason} = error ->
              abort_upload(instance, opts, session)
              error
          end
        end

      other ->
        HTTP.write_reply(other, exists_statuses(conditional?))
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

  defp upload_chunks(instance, opts, session, data, conditional?) do
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
        {:ok, 308, _headers, _body} when not final? -> {:cont, last + 1}
        {:ok, status, _headers, _body} when status in 200..299 and final? -> {:halt, :ok}
        other -> {:halt, HTTP.write_reply(other, exists_statuses(conditional?))}
      end
    end)
    |> case do
      :ok -> :ok
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
