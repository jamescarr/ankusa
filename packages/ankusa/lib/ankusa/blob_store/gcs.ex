defmodule Ankusa.BlobStore.GCS do
  @moduledoc """
  `Ankusa.BlobStore` backed by Google Cloud Storage's JSON API, via
  [`Req`](https://hex.pm/packages/req). Works against real GCS or the
  `floci-gcp` emulator in `docker-compose.integration.yml`.

  This adapter deliberately carries **no credential dependency**: `:token_provider`
  is a callback you point at whatever your deployment already uses (Goth, ADC),
  rather than a bundled OAuth2 client.

  opts:

    * `:bucket`         — required
    * `:endpoint`       — default `"https://storage.googleapis.com"`; point
                           at `http://localhost:4588` for floci-gcp
    * `:token_provider` — `{module, fun, args}`, applied per request, must
                           return `{:ok, bearer_token} | :error`. Omit for the
                           emulator (unauthenticated). **Required against real
                           GCS** — wire up your own token source (Goth, ADC,
                           whatever your deployment already uses) and pass it
                           here.
    * `:timeout_ms`     — default `10_000`, for both connect and response
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

  alias Ankusa.BlobStore.{Retry, SSE}
  alias Ankusa.HttpClient

  @impl true
  def put(instance, key, data, opts) do
    url =
      media_url(
        opts,
        "o",
        [{"uploadType", "media"}, {"name", key}] ++ SSE.query(opts, :gcs)
      )

    case request(
           instance,
           opts,
           :post,
           url,
           data,
           SSE.headers(opts, :gcs),
           "application/octet-stream"
         ) do
      {:ok, _body} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def get(instance, key, opts) do
    url = media_url(opts, "o/" <> URI.encode_www_form(key), [{"alt", "media"}])
    request(instance, opts, :get, url, "", [], nil)
  end

  @impl true
  def get_range(instance, key, offset, length, opts) do
    url = media_url(opts, "o/" <> URI.encode_www_form(key), [{"alt", "media"}])
    range = "bytes=#{offset}-#{offset + length - 1}"
    request(instance, opts, :get, url, nil, [{"range", range}], nil)
  end

  @impl true
  def delete(instance, key, opts) do
    url = media_url(opts, "o/" <> URI.encode_www_form(key), [])
    _ = request(instance, opts, :delete, url, nil, [], nil)
    :ok
  end

  # Every page (`nextPageToken`), and a failed page fails the listing.
  @impl true
  def list(instance, prefix, opts), do: list_pages(instance, opts, prefix, nil, [])

  defp list_pages(instance, opts, prefix, token, acc) do
    query = [{"prefix", prefix}] ++ if(token, do: [{"pageToken", token}], else: [])

    with {:ok, body} <-
           request(instance, opts, :get, media_url(opts, "o", query), nil, [], nil),
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

  # ── internals ──────────────────────────────────────────────────────────

  defp bucket(opts), do: Keyword.fetch!(opts, :bucket)

  defp base(opts, prefix) do
    Keyword.get(opts, :endpoint, "https://storage.googleapis.com") <>
      "/storage/v1/b/#{bucket(opts)}/#{prefix}"
  end

  # `uploadType=media` uses the `/upload/...` tree; everything else reads
  # under `/storage/v1/...`.
  defp media_url(opts, "o" <> _ = suffix, [{"uploadType", "media"} | _] = query) do
    Keyword.get(opts, :endpoint, "https://storage.googleapis.com") <>
      "/upload/storage/v1/b/#{bucket(opts)}/#{suffix}" <> query_suffix(query)
  end

  defp media_url(opts, suffix, query), do: base(opts, suffix) <> query_suffix(query)

  defp query_suffix([]), do: ""
  defp query_suffix(query), do: "?" <> URI.encode_query(query)

  defp request(instance, opts, method, url, body, headers, content_type) do
    timeout = Keyword.get(opts, :timeout_ms, 10_000)
    req_options = Keyword.get(opts, :req_options, [])

    result =
      Retry.run(instance, opts, fn ->
        all_headers = auth_headers(opts) ++ headers ++ content_type_header(content_type)
        HttpClient.request(method, url, all_headers, body, timeout, req_options)
      end)

    case result do
      # Keep the status visible so 404 can mean :not_found.
      {:ok, status, body} when status in 200..299 -> {:ok, body}
      {:ok, 404, _body} -> {:error, :not_found}
      {:ok, status, body} -> {:error, {:status, status, body}}
      {:error, reason} -> {:error, reason}
    end
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
