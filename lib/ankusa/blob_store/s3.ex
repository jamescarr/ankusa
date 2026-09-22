defmodule Ankusa.BlobStore.S3 do
  @moduledoc """
  `Ankusa.BlobStore` backed by S3 or any S3-compatible endpoint (MinIO,
  Cloudflare R2, or the `floci` emulator in `docker-compose.yml`).

  Every request is signed with AWS Signature Version 4 using only `:crypto`
  and `:httpc` — no HTTP client dependency, so this adapter costs the core
  package nothing when unused (see the packaging note in the module source).

  opts:

    * `:bucket`            — required
    * `:region`            — required, e.g. `"us-east-1"`
    * `:access_key_id`     — default `System.get_env("AWS_ACCESS_KEY_ID")`
    * `:secret_access_key` — default `System.get_env("AWS_SECRET_ACCESS_KEY")`
    * `:endpoint`          — default `"https://s3.\#{region}.amazonaws.com"`;
                              point at `http://localhost:4566` for floci/MinIO
    * `:timeout_ms`        — default `10_000`

  Addressing is always path-style (`{endpoint}/{bucket}/{key}`) — the one
  scheme every target (AWS, MinIO, R2, floci) accepts unambiguously.

  ## Local dev

      config :ankusa,
        storage: %{
          blob_store:
            {Ankusa.BlobStore.S3,
             bucket: "ankusa-segments-dev",
             region: "us-east-1",
             endpoint: "http://localhost:4566",
             access_key_id: "test",
             secret_access_key: "test"}
        }

  `docker compose up -d floci s3-bootstrap` brings up the emulator and
  creates the bucket.
  """

  @behaviour Ankusa.BlobStore

  @impl true
  def put(_instance, key, data, opts) do
    case request(opts, :put, key, IO.iodata_to_binary(data), []) do
      {:ok, _body} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def get(_instance, key, opts), do: request(opts, :get, key, "", [])

  @impl true
  def get_range(_instance, key, offset, length, opts) do
    range = "bytes=#{offset}-#{offset + length - 1}"
    request(opts, :get, key, "", [{"range", range}])
  end

  @impl true
  def delete(_instance, key, opts) do
    _ = request(opts, :delete, key, "", [])
    :ok
  end

  @impl true
  def list(_instance, prefix, opts) do
    case list_request(opts, prefix) do
      {:ok, body} -> parse_list_keys(body)
      {:error, _reason} -> []
    end
  end

  # ── signed request (object operations) ───────────────────────────────────

  defp request(opts, method, key, body, extra_headers) do
    endpoint = endpoint(opts)
    path = "/" <> bucket(opts) <> "/" <> uri_encode(key, true)
    sign_and_send(opts, method, endpoint, path, "", body, extra_headers)
  end

  # ── signed request (bucket-level ListObjectsV2) ──────────────────────────

  defp list_request(opts, prefix) do
    endpoint = endpoint(opts)
    path = "/" <> bucket(opts)
    query = canonical_query([{"list-type", "2"}, {"prefix", prefix}])
    sign_and_send(opts, :get, endpoint, path, query, "", [])
  end

  defp sign_and_send(opts, method, endpoint, path, query, body, extra_headers) do
    region = Keyword.fetch!(opts, :region)
    access_key = Keyword.get(opts, :access_key_id) || System.fetch_env!("AWS_ACCESS_KEY_ID")
    secret_key = Keyword.get(opts, :secret_access_key) || System.fetch_env!("AWS_SECRET_ACCESS_KEY")
    timeout = Keyword.get(opts, :timeout_ms, 10_000)

    uri = URI.parse(endpoint)
    now = DateTime.utc_now() |> DateTime.truncate(:second)
    amz_date = DateTime.to_iso8601(now, :basic)
    date8 = String.slice(amz_date, 0, 8)
    payload_hash = hex_sha256(body)

    headers0 =
      [
        {"host", authority(uri)},
        {"x-amz-content-sha256", payload_hash},
        {"x-amz-date", amz_date}
      ] ++ extra_headers

    {canonical_headers, signed_headers} = canonical_headers(headers0)

    canonical_request =
      Enum.join(
        [method_string(method), path, query, canonical_headers, signed_headers, payload_hash],
        "\n"
      )

    credential_scope = "#{date8}/#{region}/s3/aws4_request"

    string_to_sign =
      Enum.join(
        ["AWS4-HMAC-SHA256", amz_date, credential_scope, hex_sha256(canonical_request)],
        "\n"
      )

    signature = sigv4_signature(secret_key, date8, region, string_to_sign)

    authorization =
      "AWS4-HMAC-SHA256 Credential=#{access_key}/#{credential_scope}, " <>
        "SignedHeaders=#{signed_headers}, Signature=#{signature}"

    headers =
      (headers0 ++ [{"authorization", authorization}])
      |> Enum.map(fn {k, v} -> {to_charlist(k), to_charlist(v)} end)

    qs_suffix = if query == "", do: "", else: "?" <> query
    url = to_charlist(endpoint <> path <> qs_suffix)
    http_opts = [timeout: timeout, connect_timeout: timeout]

    ensure_started()

    result =
      if method in [:put, :post] do
        :httpc.request(
          method,
          {url, headers, ~c"application/octet-stream", body},
          http_opts,
          body_format: :binary
        )
      else
        :httpc.request(method, {url, headers}, http_opts, body_format: :binary)
      end

    case result do
      {:ok, {{_v, code, _r}, _h, resp_body}} when code in 200..299 -> {:ok, resp_body}
      {:ok, {{_v, 404, _r}, _h, _resp_body}} -> {:error, :not_found}
      {:ok, {{_v, code, _r}, _h, resp_body}} -> {:error, {:status, code, resp_body}}
      {:error, reason} -> {:error, reason}
    end
  end

  # ── SigV4 primitives ──────────────────────────────────────────────────────

  defp hex_sha256(data), do: Base.encode16(:crypto.hash(:sha256, data), case: :lower)
  defp hmac(key, data), do: :crypto.mac(:hmac, :sha256, key, data)
  defp hex_hmac(key, data), do: Base.encode16(hmac(key, data), case: :lower)

  defp sigv4_signature(secret, date8, region, string_to_sign) do
    k_date = hmac("AWS4" <> secret, date8)
    k_region = hmac(k_date, region)
    k_service = hmac(k_region, "s3")
    k_signing = hmac(k_service, "aws4_request")
    hex_hmac(k_signing, string_to_sign)
  end

  defp canonical_headers(headers) do
    sorted =
      headers
      |> Enum.map(fn {k, v} -> {String.downcase(to_string(k)), String.trim(to_string(v))} end)
      |> Enum.sort_by(fn {k, _} -> k end)

    canonical = Enum.map_join(sorted, "", fn {k, v} -> "#{k}:#{v}\n" end)
    signed = Enum.map_join(sorted, ";", fn {k, _} -> k end)
    {canonical, signed}
  end

  defp canonical_query(params) do
    params
    |> Enum.sort_by(fn {k, _} -> k end)
    |> Enum.map_join("&", fn {k, v} -> "#{uri_encode(k, false)}=#{uri_encode(v, false)}" end)
  end

  # RFC 3986 percent-encoding over raw bytes. `keep_slash?` leaves `/`
  # unescaped for path segments; query-string values escape it (`%2F`).
  defp uri_encode(binary, keep_slash?) do
    for <<byte <- binary>>, into: "" do
      cond do
        byte in ?A..?Z or byte in ?a..?z or byte in ?0..?9 or byte in ~c"-_.~" ->
          <<byte>>

        keep_slash? and byte == ?/ ->
          "/"

        true ->
          "%" <> (Integer.to_string(byte, 16) |> String.pad_leading(2, "0") |> String.upcase())
      end
    end
  end

  defp method_string(method), do: method |> Atom.to_string() |> String.upcase()

  defp authority(%URI{host: host, port: port, scheme: scheme}) do
    default = if scheme == "https", do: 443, else: 80
    if port == default, do: host, else: "#{host}:#{port}"
  end

  defp bucket(opts), do: Keyword.fetch!(opts, :bucket)

  defp endpoint(opts) do
    Keyword.get(opts, :endpoint, "https://s3.#{Keyword.fetch!(opts, :region)}.amazonaws.com")
  end

  # ── ListObjectsV2 XML (stdlib :xmerl, no extra dependency) ────────────────

  defp parse_list_keys(xml_body) do
    {doc, _rest} = :xmerl_scan.string(String.to_charlist(xml_body))

    ~c"//Contents/Key/text()"
    |> :xmerl_xpath.string(doc)
    |> Enum.map(fn {:xmlText, _parents, _pos, _lang, value, _type} -> List.to_string(value) end)
    |> Enum.sort()
  end

  defp ensure_started do
    {:ok, _} = Application.ensure_all_started(:inets)
    {:ok, _} = Application.ensure_all_started(:ssl)
    :ok
  end
end
