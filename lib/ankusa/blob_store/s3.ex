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
    * `:req_options`       — transport options for the HTTP client, e.g. a
                              custom Finch pool (`finch: [name: MyFinch]`), a
                              proxy (via `:connect_options`), or `plug:` for
                              `Req.Test` in tests. See `Ankusa.HttpClient` —
                              an allowlist, because a redirected or re-tuned
                              request would no longer match its signature.

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

  # :xmerl ships with OTP and is declared in mix.exs's extra_applications, but
  # Elixir's compile-time xref pass can warn on :xmerl_scan/:xmerl_xpath calls
  # depending on module compile order in a fresh build (the app isn't loaded
  # yet at that point in the parallel compile) — a known false positive for
  # OTP stdlib apps that aren't themselves Mix dependencies.
  @compile {:no_warn_undefined, [:xmerl_scan, :xmerl_xpath]}

  alias Ankusa.HttpClient

  @impl true
  def put(_instance, key, data, opts) do
    case request(opts, :put, key, IO.iodata_to_binary(data), []) do
      {:ok, _body} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def get(_instance, key, opts), do: request(opts, :get, key, nil, [])

  @impl true
  def get_range(_instance, key, offset, length, opts) do
    request(opts, :get, key, nil, [{"range", "bytes=#{offset}-#{offset + length - 1}"}])
  end

  @impl true
  def delete(_instance, key, opts) do
    _ = request(opts, :delete, key, nil, [])
    :ok
  end

  @impl true
  def list(_instance, prefix, opts) do
    url =
      endpoint(opts) <>
        "/" <> bucket(opts) <> "?" <> URI.encode_query(list_query(prefix), :rfc3986)

    case signed_request(opts, :get, url, nil, []) do
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
        # A bodyless request signs the empty string — the hash S3 expects as
        # `x-amz-content-sha256` on a GET or DELETE.
        body || "",
        # S3 signs the path exactly as sent; every other service wants it
        # URI-encoded a second time.
        uri_encode_path: false
      )

    case HttpClient.request(
           method,
           url,
           signed,
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
  #
  # The scanner is handed the raw bytes, not a charlist: it decodes the UTF-8 the
  # document declares, so codepoints above 127 read as illegal characters and a
  # listing containing one non-ASCII key would come back empty. Bytes that are not
  # valid UTF-8 in the first place exit the same way.
  defp parse_list_keys(xml_body) do
    {doc, _rest} = :xmerl_scan.string(:binary.bin_to_list(xml_body), quiet: true)

    ~c"//Contents/Key/text()"
    |> :xmerl_xpath.string(doc)
    |> Enum.map(fn {:xmlText, _parents, _pos, _lang, value, _type} -> List.to_string(value) end)
    |> Enum.sort()
  catch
    :exit, _not_xml -> []
  end
end
