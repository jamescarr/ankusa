defmodule Ankusa.BlobStore.S3 do
  @moduledoc """
  `Ankusa.BlobStore` backed by S3 or any S3-compatible endpoint (MinIO,
  Cloudflare R2, or the `floci` emulator in `docker-compose.integration.yml`).

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
    * `:access_key_id`, `:secret_access_key`, `:session_token` — static
                              credentials. Without them the chain in
                              `Ankusa.AWS.Credentials` runs: the
                              `AWS_*` environment, web identity (IRSA), then
                              the EC2 instance role (IMDSv2)
    * `:endpoint`          — default `"https://s3.\#{region}.amazonaws.com"`;
                              point at `http://localhost:4566` for floci/MinIO
    * `:timeout_ms`        — default `10_000`, for both connect and response of one
                              request that carries at most 1 MiB
    * `:upload_timeout_ms`, `:op_deadline_ms`, `:multipart_threshold_bytes`,
      `:multipart_part_bytes`, `:breaker_*`, `:pool_*` — one deadline per
                              operation, multipart above a size, a breaker per
                              bucket and a connection pool per endpoint. See
                              `Ankusa.BlobStore.Options`
    * `:req_options`       — transport options for the HTTP client, e.g. a
                              custom Finch pool (`finch: [name: MyFinch]`), a
                              proxy (via `:connect_options`), or `plug:` for
                              `Req.Test` in tests. See `Ankusa.HttpClient` —
                              an allowlist, because a redirected or re-tuned
                              request would no longer match its signature.
    * `:retries`           — default `2`: extra attempts after a transport
                              error or a `408`/`429`/`5xx`, each re-signed,
                              jittered, `Retry-After` honoured. See
                              `Ankusa.BlobStore.Retry`
    * `:sse`               — `%{type: :aes256}` or `%{type: :kms, key_id: arn}`:
                              server-side encryption, sent (and signed) as
                              `x-amz-server-side-encryption` on every `put/4`.
                              See `Ankusa.BlobStore.SSE`

  `put_if_absent/4` sends `If-None-Match: *` (a `412`/`409` is `:already_exists`);
  every `PUT` carries `x-amz-checksum-sha256` and a `GET` verifies it
  (`Ankusa.BlobStore.Checksum`); a body over `multipart_threshold_bytes` is a
  multipart upload (create, parts each with `Content-MD5`, complete, abort on
  failure).

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

  `docker compose -f docker-compose.integration.yml up -d floci s3-bootstrap` brings up
  the emulator and creates the bucket.
  """

  @behaviour Ankusa.BlobStore

  # :xmerl ships with OTP and is declared in mix.exs's extra_applications, but
  # Elixir's compile-time xref pass can warn on :xmerl_scan/:xmerl_xpath calls
  # depending on module compile order in a fresh build (the app isn't loaded
  # yet at that point in the parallel compile) — a known false positive for
  # OTP stdlib apps that aren't themselves Mix dependencies.
  @compile {:no_warn_undefined, [:xmerl_scan, :xmerl_xpath]}

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
    headers = [{"x-amz-checksum-mode", "ENABLED"}]

    case request(instance, opts, :small, :get, object_url(opts, key), nil, headers) do
      {:ok, status, headers, body} when status in 200..299 ->
        Checksum.check_response(headers, body, :sha256, ["x-amz-checksum-sha256"])

      other ->
        HTTP.reply(other)
    end
  end

  @impl true
  def get_range(instance, key, offset, length, opts) do
    opts = Options.with_deadline(opts)
    headers = [{"range", "bytes=#{offset}-#{offset + length - 1}"}]
    HTTP.reply(request(instance, opts, :small, :get, object_url(opts, key), nil, headers))
  end

  @impl true
  def delete(instance, key, opts) do
    opts = Options.with_deadline(opts)
    _ = request(instance, opts, :small, :delete, object_url(opts, key), nil, [])
    :ok
  end

  # ListObjectsV2 pages hold at most 1,000 keys: follow the continuation token
  # to the end, each page under a deadline of its own. A failed page fails the
  # listing — a short list would read as "these are all the keys".
  @impl true
  def list(instance, prefix, opts), do: list_pages(instance, opts, prefix, nil, [])

  defp list_pages(instance, opts, prefix, token, acc) do
    query =
      [{"list-type", "2"}, {"prefix", prefix}] ++
        if(token, do: [{"continuation-token", token}], else: [])

    url = endpoint(opts) <> "/" <> bucket(opts) <> "?" <> URI.encode_query(query, :rfc3986)
    page_opts = Options.with_deadline(opts)

    with {:ok, body} <- HTTP.reply(request(instance, page_opts, :small, :get, url, nil, [])),
         {:ok, keys, next} <- parse_list_page(body) do
      acc = [keys | acc]

      case next do
        nil -> {:ok, acc |> Enum.reverse() |> Enum.concat() |> Enum.sort()}
        token -> list_pages(instance, opts, prefix, token, acc)
      end
    end
  end

  # ── writes ────────────────────────────────────────────────────────────────

  defp write(instance, key, data, opts, conditional?) do
    opts = Options.with_deadline(opts)
    size = IO.iodata_length(data)

    if size > Options.get(opts, :multipart_threshold_bytes),
      do: multipart(instance, opts, key, IO.iodata_to_binary(data), conditional?),
      else: single(instance, opts, key, data, size, conditional?)
  end

  defp single(instance, opts, key, data, size, conditional?) do
    headers =
      SSE.headers(opts, :s3) ++
        [{"x-amz-checksum-sha256", Checksum.sha256_base64(data)}] ++ precondition(conditional?)

    instance
    |> request(opts, Options.kind(size), :put, object_url(opts, key), data, headers)
    |> HTTP.write_reply(exists_statuses(conditional?))
  end

  # S3 answers a failed `If-None-Match` with 412, and a conflicting concurrent
  # conditional write with 409.
  defp precondition(true), do: [{"if-none-match", "*"}]
  defp precondition(false), do: []
  defp exists_statuses(true), do: [409, 412]
  defp exists_statuses(false), do: []

  # Create, upload each part (its own `Content-MD5`; an `upload_timeout_ms` per
  # part, all inside the operation's one deadline), complete; abort on any
  # failure so no orphan parts are billed.
  defp multipart(instance, opts, key, data, conditional?) do
    url = object_url(opts, key)
    parts = HTTP.chunks(data, Options.get(opts, :multipart_part_bytes))

    case create_upload(instance, opts, url) do
      {:ok, upload_id} ->
        with {:ok, etags} <- upload_parts(instance, opts, url, upload_id, parts),
             :ok <- complete_upload(instance, opts, url, upload_id, etags, conditional?) do
          :ok
        else
          {:error, _reason} = error ->
            abort_upload(instance, opts, url, upload_id)
            error
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp create_upload(instance, opts, url) do
    result =
      request(instance, opts, :small, :post, url <> "?uploads=", "", SSE.headers(opts, :s3))

    with {:ok, body} <- HTTP.reply(result) do
      case parse_upload_id(body) do
        nil -> {:error, :upload_unreadable}
        id -> {:ok, id}
      end
    end
  end

  defp upload_parts(instance, opts, url, upload_id, parts) do
    parts
    |> Enum.with_index(1)
    |> Enum.reduce_while({:ok, []}, fn {part, number}, {:ok, etags} ->
      query = URI.encode_query([{"partNumber", number}, {"uploadId", upload_id}], :rfc3986)
      headers = [{"content-md5", Checksum.md5_base64(part)}]

      case request(instance, opts, :bulk, :put, url <> "?" <> query, part, headers) do
        {:ok, status, headers, _body} when status in 200..299 ->
          case Checksum.first(headers, "etag") do
            nil -> {:halt, {:error, :part_without_etag}}
            etag -> {:cont, {:ok, [{number, etag} | etags]}}
          end

        other ->
          {:halt, HTTP.write_reply(other, [])}
      end
    end)
    |> case do
      {:ok, etags} -> {:ok, Enum.reverse(etags)}
      error -> error
    end
  end

  defp complete_upload(instance, opts, url, upload_id, etags, conditional?) do
    xml = [
      "<CompleteMultipartUpload>",
      for {number, etag} <- etags do
        [
          "<Part><PartNumber>",
          Integer.to_string(number),
          "</PartNumber><ETag>",
          etag,
          "</ETag></Part>"
        ]
      end,
      "</CompleteMultipartUpload>"
    ]

    headers = [{"content-type", "application/xml"}] ++ precondition(conditional?)
    complete_url = url <> "?" <> URI.encode_query([{"uploadId", upload_id}], :rfc3986)

    case request(instance, opts, :small, :post, complete_url, xml, headers) do
      # S3 can answer 200 and put the failure in the body.
      {:ok, status, _headers, body} when status in 200..299 ->
        if String.contains?(body, "<Error>"), do: {:error, {:status, status, body}}, else: :ok

      other ->
        HTTP.write_reply(other, exists_statuses(conditional?))
    end
  end

  # Best effort, under a short deadline of its own: the operation's may be spent.
  defp abort_upload(instance, opts, url, upload_id) do
    opts =
      opts
      |> Keyword.put(:deadline, System.monotonic_time(:millisecond) + 5_000)
      |> Keyword.put(:retries, 0)

    abort_url = url <> "?" <> URI.encode_query([{"uploadId", upload_id}], :rfc3986)
    _ = request(instance, opts, :small, :delete, abort_url, nil, [])
    :ok
  end

  # ── requests ──────────────────────────────────────────────────────────────

  defp request(instance, opts, kind, method, url, body, headers) do
    with {:ok, creds} <- Ankusa.AWS.Credentials.get(opts) do
      # Signed per attempt: the signature carries the time it was made.
      HTTP.request(instance, opts, scope(opts), kind, fn ->
        {method, url, sign(opts, creds, method, url, body, headers), body}
      end)
    end
  end

  defp object_url(opts, key), do: endpoint(opts) <> "/" <> bucket(opts) <> "/" <> encode_path(key)

  defp scope(opts), do: {endpoint(opts), bucket(opts)}

  defp sign(opts, creds, method, url, body, extra_headers) do
    # The signature covers the host header, so it has to be derived from the same
    # URL handed to the signer — not from anything the HTTP client might do.
    # Temporary credentials add their session token as a signed header.
    headers =
      [{"host", authority(url)} | stringify(extra_headers)] ++
        if(creds.session_token, do: [{"x-amz-security-token", creds.session_token}], else: [])

    :aws_signature.sign_v4(
      creds.access_key_id,
      creds.secret_access_key,
      Keyword.fetch!(opts, :region),
      "s3",
      :calendar.universal_time(),
      method |> Atom.to_string() |> String.upcase(),
      url,
      headers,
      # A bodyless request signs the empty string — the hash S3 expects as
      # `x-amz-content-sha256` on a GET or DELETE. A body may be iodata: the
      # signer hashes it as is.
      body || "",
      # S3 signs the path exactly as sent; every other service wants it
      # URI-encoded a second time.
      uri_encode_path: false
    )
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

  defp authority(url) do
    %URI{host: host, port: port, scheme: scheme} = URI.parse(url)
    default = if scheme == "https", do: 443, else: 80
    if port == default, do: host, else: "#{host}:#{port}"
  end

  defp stringify(headers) do
    Enum.map(headers, fn {k, v} -> {to_string(k), v} end)
  end

  defp bucket(opts), do: Keyword.fetch!(opts, :bucket)

  @doc false
  @spec endpoint(keyword()) :: String.t()
  def endpoint(opts) do
    Keyword.get(opts, :endpoint, "https://s3.#{Keyword.fetch!(opts, :region)}.amazonaws.com")
  end

  # ── ListObjectsV2 XML (stdlib :xmerl, no dependency needed for one xpath) ──

  # A 200 body is not guaranteed to be ListObjectsV2 XML — a proxy error page or
  # an emulator quirk will do it. `:xmerl_scan` *exits* on a malformed document;
  # that is an error for the listing, never a crash of the caller.
  #
  # The scanner is handed the raw bytes, not a charlist: it decodes the UTF-8 the
  # document declares, so codepoints above 127 read as illegal characters and a
  # listing containing one non-ASCII key would come back empty. Bytes that are not
  # valid UTF-8 in the first place exit the same way.
  defp parse_list_page(xml_body) do
    {doc, _rest} = :xmerl_scan.string(:binary.bin_to_list(xml_body), quiet: true)

    keys =
      ~c"//Contents/Key/text()"
      |> :xmerl_xpath.string(doc)
      |> Enum.map(fn {:xmlText, _parents, _pos, _lang, value, _type} -> List.to_string(value) end)

    truncated? = text(doc, ~c"//IsTruncated/text()") == "true"
    next = if truncated?, do: text(doc, ~c"//NextContinuationToken/text()")

    if truncated? and next == nil,
      do: {:error, :list_truncated_without_token},
      else: {:ok, keys, next}
  catch
    :exit, _not_xml -> {:error, :list_unreadable}
  end

  defp parse_upload_id(xml_body) do
    {doc, _rest} = :xmerl_scan.string(:binary.bin_to_list(xml_body), quiet: true)
    text(doc, ~c"//UploadId/text()")
  catch
    :exit, _not_xml -> nil
  end

  defp text(doc, path) do
    case :xmerl_xpath.string(path, doc) do
      [{:xmlText, _parents, _pos, _lang, value, _type} | _] -> List.to_string(value)
      _ -> nil
    end
  end
end
