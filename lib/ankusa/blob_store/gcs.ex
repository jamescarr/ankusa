defmodule Ankusa.BlobStore.GCS do
  @moduledoc """
  `Ankusa.BlobStore` backed by Google Cloud Storage's JSON API, via
  [`Req`](https://hex.pm/packages/req). Works against real GCS or the
  `floci-gcp` emulator in `docker-compose.yml`.

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

  ## Local dev

      config :ankusa,
        storage: %{
          blob_store:
            {Ankusa.BlobStore.GCS, bucket: "ankusa-segments-dev", endpoint: "http://localhost:4588"}
        }

  `docker compose up -d floci-gcp gcs-bootstrap` brings up the emulator and
  creates the bucket.
  """

  @behaviour Ankusa.BlobStore

  alias Ankusa.HttpClient

  @impl true
  def put(_instance, key, data, opts) do
    url = media_url(opts, "o", [{"uploadType", "media"}, {"name", key}])

    case request(opts, :post, url, IO.iodata_to_binary(data), [], "application/octet-stream") do
      {:ok, _body} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def get(_instance, key, opts) do
    url = media_url(opts, "o/" <> URI.encode_www_form(key), [{"alt", "media"}])
    request(opts, :get, url, "", [], nil)
  end

  @impl true
  def get_range(_instance, key, offset, length, opts) do
    url = media_url(opts, "o/" <> URI.encode_www_form(key), [{"alt", "media"}])
    range = "bytes=#{offset}-#{offset + length - 1}"
    request(opts, :get, url, nil, [{"range", range}], nil)
  end

  @impl true
  def delete(_instance, key, opts) do
    url = media_url(opts, "o/" <> URI.encode_www_form(key), [])
    _ = request(opts, :delete, url, nil, [], nil)
    :ok
  end

  @impl true
  def list(_instance, prefix, opts) do
    list_page(prefix, opts, nil, [])
  end

  defp list_page(prefix, opts, page_token, acc) do
    query = [{"prefix", prefix}] ++ page_query(page_token)
    url = media_url(opts, "o", query)

    case request(opts, :get, url, nil, [], nil) do
      {:ok, body} ->
        case JSON.decode(body) do
          {:ok, %{"items" => items} = page} ->
            acc = acc ++ Enum.map(items, & &1["name"])

            case page["nextPageToken"] do
              nil -> Enum.sort(acc)
              token -> list_page(prefix, opts, token, acc)
            end

          {:ok, _no_items} ->
            Enum.sort(acc)

          {:error, _reason} ->
            Enum.sort(acc)
        end

      {:error, _reason} ->
        Enum.sort(acc)
    end
  end

  defp page_query(nil), do: []
  defp page_query(token), do: [{"pageToken", token}]

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

  defp request(opts, method, url, body, headers, content_type) do
    timeout = Keyword.get(opts, :timeout_ms, 10_000)

    case HttpClient.request(
           method,
           url,
           auth_headers(opts) ++ headers ++ content_type_header(content_type),
           body,
           timeout,
           Keyword.get(opts, :req_options, [])
         ) do
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
