defmodule Ankusa.BlobStore.GCS do
  @moduledoc """
  `Ankusa.BlobStore` backed by Google Cloud Storage's JSON API, via `:httpc` —
  no HTTP client dependency. Works against real GCS or the `floci-gcp`
  emulator in `docker-compose.yml`.

  opts:

    * `:bucket`         — required
    * `:endpoint`       — default `"https://storage.googleapis.com"`; point
                           at `http://localhost:4588` for floci-gcp
    * `:token_provider` — `{module, fun, args}`, applied per request, must
                           return `{:ok, bearer_token} | :error`. Omit for the
                           emulator (unauthenticated). **Required against real
                           GCS** — this adapter carries no OAuth2 dependency of
                           its own; wire up your own token source (Goth, ADC,
                           whatever your deployment already uses) and pass it
                           here.
    * `:timeout_ms`     — default `10_000`

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
      "/upload/storage/v1/b/#{bucket(opts)}/#{suffix}" <> query_string(query)
  end

  defp media_url(opts, suffix, query), do: base(opts, suffix) <> query_string(query)

  defp query_string([]), do: ""

  defp query_string(query) do
    "?" <>
      Enum.map_join(query, "&", fn {k, v} ->
        "#{URI.encode_www_form(k)}=#{URI.encode_www_form(v)}"
      end)
  end

  defp request(opts, method, url, body, headers, content_type) do
    ensure_started()

    all_headers =
      (auth_headers(opts) ++ headers)
      |> Enum.map(fn {k, v} -> {to_charlist(k), to_charlist(v)} end)

    timeout = Keyword.get(opts, :timeout_ms, 10_000)
    http_opts = [timeout: timeout, connect_timeout: timeout]
    url = to_charlist(url)

    result =
      if method in [:post, :put] do
        ct = to_charlist(content_type || "application/octet-stream")
        :httpc.request(method, {url, all_headers, ct, body}, http_opts, body_format: :binary)
      else
        :httpc.request(method, {url, all_headers}, http_opts, body_format: :binary)
      end

    case result do
      {:ok, {{_v, code, _r}, _h, resp_body}} when code in 200..299 -> {:ok, resp_body}
      {:ok, {{_v, 404, _r}, _h, _resp_body}} -> {:error, :not_found}
      {:ok, {{_v, code, _r}, _h, resp_body}} -> {:error, {:status, code, resp_body}}
      {:error, reason} -> {:error, reason}
    end
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

  defp ensure_started do
    {:ok, _} = Application.ensure_all_started(:inets)
    {:ok, _} = Application.ensure_all_started(:ssl)
    :ok
  end
end
