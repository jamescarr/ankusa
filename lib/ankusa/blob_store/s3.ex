defmodule Ankusa.BlobStore.S3 do
  @moduledoc """
  `Ankusa.BlobStore` backed by S3 or any S3-compatible endpoint (MinIO,
  Cloudflare R2, or the `floci` emulator in `docker-compose.yml`).

  Requests are signed with
  [`aws_signature`](https://hex.pm/packages/aws_signature) — the SigV4
  implementation behind the official aws-elixir SDK — and sent with
  [`Req`](https://hex.pm/packages/req). Signing is exactly the kind of code not
  to hand-roll: a canonicalization bug is invisible until it fails in
  production, and the failure mode is a signature mismatch on someone else's
  infrastructure.

  opts:

    * `:bucket`            — required
    * `:region`            — required, e.g. `"us-east-1"`
    * `:access_key_id`     — default `System.get_env("AWS_ACCESS_KEY_ID")`
    * `:secret_access_key` — default `System.get_env("AWS_SECRET_ACCESS_KEY")`
    * `:endpoint`          — default `"https://s3.\#{region}.amazonaws.com"`;
                              point at `http://localhost:4566` for floci/MinIO
    * `:timeout_ms`        — default `10_000`, for both connect and response
    * `:req_options`       — extra options for `Req`, e.g. a custom Finch pool
                              (`finch: [name: MyFinch]`), a proxy, or `plug:`
                              for `Req.Test` in tests

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
    request(opts, :get, key, "", [{"range", "bytes=#{offset}-#{offset + length - 1}"}])
  end

  @impl true
  def delete(_instance, key, opts) do
    _ = request(opts, :delete, key, "", [])
    :ok
  end

  @impl true
  def list(_instance, prefix, opts) do
    url =
      endpoint(opts) <>
        "/" <> bucket(opts) <> "?" <> URI.encode_query(list_query(prefix), :rfc3986)

    case signed_request(opts, :get, url, "", []) do
      {:ok, body} -> parse_list_keys(body)
      {:error, _reason} -> []
    end
  end

  # ── requests ──────────────────────────────────────────────────────────────

  defp request(opts, method, key, body, extra_headers) do
    url = endpoint(opts) <> "/" <> bucket(opts) <> "/" <> encode_path(key)
    signed_request(opts, method, url, body, extra_headers)
  end

  defp signed_request(opts, method, url, body, extra_headers) do
    region = Keyword.fetch!(opts, :region)
    access_key = Keyword.get(opts, :access_key_id) || System.fetch_env!("AWS_ACCESS_KEY_ID")
    secret = Keyword.get(opts, :secret_access_key) || System.fetch_env!("AWS_SECRET_ACCESS_KEY")
    timeout = Keyword.get(opts, :timeout_ms, 10_000)

    # The signature covers the host header, so it has to be derived from the same
    # URL handed to the signer — not from anything the HTTP client might do.
    headers = [{"host", authority(url)} | stringify(extra_headers)]

    signed =
      :aws_signature.sign_v4(
        access_key,
        secret,
        region,
        "s3",
        :calendar.universal_time(),
        method |> Atom.to_string() |> String.upcase(),
        url,
        headers,
        body,
        # S3 signs the path exactly as sent; every other service wants it
        # URI-encoded a second time.
        uri_encode_path: false
      )

    case Req.request(
           # The adapter's own options win: they are what its contract depends on
           # (raw bodies, visible status codes, no hidden retries). `:req_options`
           # is appended so a caller can still add a Finch pool, proxy, or
           # `plug:` for `Req.Test`.
           [
             method: method,
             url: url,
             headers: signed,
             body: body,
             # Segments and claim bodies are raw binaries, never JSON.
             decode_body: false,
             # Keep the status visible so 404 can mean :not_found.
             http_errors: :return,
             # Retries belong to the framework's own tick/retry loops; Req's
             # default retry would add hidden latency inside them.
             retry: false,
             receive_timeout: timeout,
             connect_options: [timeout: timeout]
           ] ++ Keyword.get(opts, :req_options, [])
         ) do
      {:ok, %Req.Response{status: status, body: body}} when status in 200..299 -> {:ok, body}
      {:ok, %Req.Response{status: 404}} -> {:error, :not_found}
      {:ok, %Req.Response{status: status, body: body}} -> {:error, {:status, status, body}}
      {:error, reason} -> {:error, reason}
    end
  end

  # ── URL building ──────────────────────────────────────────────────────────

  # Per-segment RFC 3986 percent-encoding: `/` stays a separator, everything
  # outside the unreserved set is escaped.
  defp encode_path(key) do
    unreserved = &URI.char_unreserved?/1

    key
    |> String.split("/")
    |> Enum.map_join("/", &URI.encode(&1, unreserved))
  end

  defp list_query(prefix), do: [{"list-type", "2"}, {"prefix", prefix}]

  defp authority(url) do
    %URI{host: host, port: port, scheme: scheme} = URI.parse(url)
    default = if scheme == "https", do: 443, else: 80
    if port == default, do: host, else: "#{host}:#{port}"
  end

  defp stringify(headers) do
    Enum.map(headers, fn {k, v} -> {to_string(k), v} end)
  end

  defp bucket(opts), do: Keyword.fetch!(opts, :bucket)

  defp endpoint(opts) do
    Keyword.get(opts, :endpoint, "https://s3.#{Keyword.fetch!(opts, :region)}.amazonaws.com")
  end

  # ── ListObjectsV2 XML (stdlib :xmerl, no dependency needed for one xpath) ──

  # A 200 body is not guaranteed to be ListObjectsV2 XML — a proxy error page or
  # an emulator quirk will do it. `:xmerl_scan` *exits* on a malformed document,
  # and this runs inside the claim-check sweeper, so a bad body has to read as
  # "no keys" instead of taking that process down.
  defp parse_list_keys(xml_body) do
    {doc, _rest} = :xmerl_scan.string(String.to_charlist(xml_body), quiet: true)

    ~c"//Contents/Key/text()"
    |> :xmerl_xpath.string(doc)
    |> Enum.map(fn {:xmlText, _parents, _pos, _lang, value, _type} -> List.to_string(value) end)
    |> Enum.sort()
  catch
    :exit, _not_xml -> []
  end
end
