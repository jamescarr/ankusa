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
    * `:req_options`    — extra options for `Req`, e.g. a custom Finch pool, a
                           proxy, or `plug:` for `Req.Test` in tests

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
    request(opts, :get, url, "", [{"range", range}], nil)
  end

  @impl true
  def delete(_instance, key, opts) do
    url = media_url(opts, "o/" <> URI.encode_www_form(key), [])
    _ = request(opts, :delete, url, "", [], nil)
    :ok
  end

  @impl true
  def list(_instance, prefix, opts) do
    url = media_url(opts, "o", [{"prefix", prefix}])

    case request(opts, :get, url, "", [], nil) do
      {:ok, body} ->
        case JSON.decode(body) do
          {:ok, %{"items" => items}} -> items |> Enum.map(& &1["name"]) |> Enum.sort()
          {:ok, _no_items} -> []
          {:error, _reason} -> []
        end

      {:error, _reason} ->
        []
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

  defp request(opts, method, url, body, headers, content_type) do
    timeout = Keyword.get(opts, :timeout_ms, 10_000)

    request =
      [
        method: method,
        url: url,
        headers: auth_headers(opts) ++ headers ++ content_type_header(content_type),
        # Segment and claim bodies are raw binaries, never JSON.
        decode_body: false,
        # Keep the status visible so 404 can mean :not_found.
        http_errors: :return,
        # Retries belong to the framework's own tick loops.
        retry: false,
        receive_timeout: timeout,
        connect_options: [timeout: timeout]
      ]
      |> with_body(method, body)
      |> Kernel.++(Keyword.get(opts, :req_options, []))

    case Req.request(request) do
      {:ok, %Req.Response{status: status, body: body}} when status in 200..299 -> {:ok, body}
      {:ok, %Req.Response{status: 404}} -> {:error, :not_found}
      {:ok, %Req.Response{status: status, body: body}} -> {:error, {:status, status, body}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp with_body(request, method, body) when method in [:post, :put], do: request ++ [body: body]
  defp with_body(request, _method, _body), do: request

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
