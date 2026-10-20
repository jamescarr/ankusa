defmodule Ankusa.BlobStore.OCI do
  @moduledoc """
  `Ankusa.BlobStore` backed by Oracle Cloud Infrastructure Object Storage, via
  [`Req`](https://hex.pm/packages/req).

  OCI has no bearer-token or pre-signed-URL shortcut that covers arbitrary
  `PUT`/`GET`/`DELETE`/`list` against a bucket, so this adapter signs its own
  requests the way every OCI SDK does: *Signature version 1*, an RSA-SHA256
  signature over the request's `(request-target)`, `date`, and `host` headers
  (plus `content-length`/`content-type`/`x-content-sha256` on `PUT`). The
  signing is implemented with OTP's `:public_key` — no dependency — and pinned
  against OCI's own test keys in
  `test/ankusa/blob_store_oci_signing_test.exs`.

  opts:

    * `:region`          — required, e.g. `"us-ashburn-1"`
    * `:namespace`       — required; the OCI namespace (see the tenancy page)
    * `:bucket`          — required
    * `:key_id`          — optional; overrides the `tenancy/user/fingerprint` key
                           id the API-key opts derive. Instance principals and
                           session tokens sign with a `ST$<token>` key id — pass
                           that here alongside `:private_key`.
    * `:tenancy_ocid`    — required for API-key auth
    * `:user_ocid`       — required for API-key auth
    * `:key_fingerprint` — required for API-key auth; the signing key fingerprint
    * `:private_key`     — required unless `:key_provider` is set; the PEM private key
                           (PKCS#1 or PKCS#8)
    * `:key_provider`    — `{module, fun, args}` returning `{:ok, %{key_id: key_id,
                           private_key: pem}} | :error`, applied per request, in place of
                           `:key_id`/`:private_key`: the identity of an instance principal
                           (`{Ankusa.BlobStore.OCI.InstancePrincipal, :credentials, [[region: …]]}`).
                           On a `401` the adapter calls the module's `invalidate/n` (same
                           arguments) and sends the request once more with fresh credentials
    * `:endpoint`        — default `"https://objectstorage.\#{region}.oraclecloud.com"`
    * `:timeout_ms`      — default `10_000`, for both connect and response of
                           one request that carries at most 1 MiB
    * `:upload_timeout_ms`, `:op_deadline_ms`, `:multipart_threshold_bytes`,
      `:multipart_part_bytes`, `:breaker_*`, `:pool_*` — one deadline per
                           operation, multipart above a size, a breaker per
                           bucket and a connection pool per endpoint. See
                           `Ankusa.BlobStore.Options`
    * `:req_options`     — transport options for the HTTP client. See
                           `Ankusa.HttpClient` for the accepted keys
    * `:retries`         — default `2`: extra attempts after a transport error
                           or a `408`/`429`/`5xx`, each re-signed with a fresh
                           `date`. See `Ankusa.BlobStore.Retry`
    * `:sse`             — `%{type: :kms, key_id: vault_key_ocid}`: every object is
                           encrypted with that Vault key (`opc-sse-kms-key-id`).
                           Object Storage encrypts everything at rest anyway;
                           this picks the key. See `Ankusa.BlobStore.SSE`

  ## Identity

  Two credential shapes:

    * **API key** — `:tenancy_ocid`/`:user_ocid`/`:key_fingerprint`/`:private_key`,
      the default. The right shape off-OCI (CI, laptops, another cloud).
    * **Instance principal** — on OCI compute or OKE, the best credential is the
      instance's own identity: `key_provider: {Ankusa.BlobStore.OCI.InstancePrincipal,
      :credentials, [[region: "us-ashburn-1"]]}` runs the certificate federation
      (IMDS leaf cert → `auth.<region>.oraclecloud.com/v1/x509` → a short-lived
      `ST$` session token plus a session key), caches it and refreshes it before
      it expires and on a `401`. A session token from another source (the OCI SDK,
      a sidecar) still works as `key_id: "ST$<token>"` with its session key as
      `:private_key`; it is the caller's to renew.

  `put_if_absent/4` sends `if-none-match: *` (a `412` is `:already_exists`); every
  `PUT` carries `Content-MD5` and a `GET` verifies `opc-content-md5`
  (`Ankusa.BlobStore.Checksum`); a body over `multipart_threshold_bytes` is a
  multipart upload (create, parts each with `Content-MD5`, commit, abort on
  failure).

  ## Local dev

      config :ankusa,
        storage: %{
          blob_store:
            {Ankusa.BlobStore.OCI,
             region: "us-ashburn-1",
             namespace: System.get_env("OCI_NAMESPACE"),
             bucket: "ankusa-segments",
             tenancy_ocid: System.get_env("OCI_TENANCY"),
             user_ocid: System.get_env("OCI_USER"),
             key_fingerprint: System.get_env("OCI_KEY_FINGERPRINT"),
             private_key: File.read!(System.fetch_env!("OCI_KEY_FILE"))}
        }

  The `floci-oci` emulator in `docker-compose.integration.yml` (:4599) parses
  but never verifies the signature, so local integration testing works with any
  locally generated key; the signing itself is proven by the reference vectors in
  `test/ankusa/blob_store_oci_signing_test.exs`.
  """

  @behaviour Ankusa.BlobStore

  alias Ankusa.BlobStore
  alias Ankusa.BlobStore.{Checksum, HTTP, Options, SSE}
  alias Ankusa.BlobStore.OCI.Signer

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

    case request(instance, opts, :small, :get, object_url(opts, key), nil, []) do
      {:ok, status, headers, body} when status in 200..299 ->
        Checksum.check_response(headers, body, :md5, ["opc-content-md5"])

      other ->
        HTTP.reply(other)
    end
  end

  @impl true
  def get_range(instance, key, offset, length, opts) do
    opts = Options.with_deadline(opts)
    range = "bytes=#{offset}-#{offset + length - 1}"
    headers = [{"range", range}]
    HTTP.reply(request(instance, opts, :small, :get, object_url(opts, key), nil, headers))
  end

  @impl true
  def delete(instance, key, opts) do
    opts = Options.with_deadline(opts)
    _ = request(instance, opts, :small, :delete, object_url(opts, key), nil, [])
    :ok
  end

  # Every page (`nextStartWith` → `start`), each under a deadline of its own, and
  # a failed page fails the listing.
  @impl true
  def list(instance, prefix, opts), do: list_pages(instance, opts, prefix, nil, [])

  defp list_pages(instance, opts, prefix, start, acc) do
    query = [{"prefix", prefix}] ++ if(start, do: [{"start", start}], else: [])

    url =
      endpoint(opts) <>
        "/n/" <>
        namespace(opts) <>
        "/b/" <>
        bucket(opts) <>
        "/o" <>
        "?" <> URI.encode_query(query, :rfc3986)

    page_opts = Options.with_deadline(opts)

    with {:ok, body} <- HTTP.reply(request(instance, page_opts, :small, :get, url, nil, [])),
         {:ok, page} when is_map(page) <- JSON.decode(body) do
      acc = [page |> Map.get("objects", []) |> Enum.map(& &1["name"]) | acc]

      case page["nextStartWith"] do
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
      SSE.headers(opts, :oci) ++
        [
          {"content-type", "application/octet-stream"},
          {"x-content-sha256", Signer.sha256_base64(data)},
          {"content-md5", Checksum.md5_base64(data)}
        ] ++ precondition(conditional?)

    instance
    |> request(opts, Options.kind(size), :put, object_url(opts, key), data, headers)
    |> HTTP.write_reply(exists_statuses(conditional?))
  end

  # A failed `if-none-match: *` is a 412.
  defp precondition(true), do: [{"if-none-match", "*"}]
  defp precondition(false), do: []
  defp exists_statuses(true), do: [412]
  defp exists_statuses(false), do: []

  # Create the upload, PUT each part (its own `Content-MD5`; an
  # `upload_timeout_ms` per part, all inside the operation's one deadline),
  # commit; abort on any failure so no orphan parts are kept.
  defp multipart(instance, opts, key, data, conditional?) do
    url = upload_url(opts, key)
    parts = HTTP.chunks(data, Options.get(opts, :multipart_part_bytes))

    case create_upload(instance, opts, key) do
      {:ok, upload_id} ->
        with {:ok, etags} <- upload_parts(instance, opts, url, upload_id, parts),
             :ok <- commit_upload(instance, opts, url, upload_id, etags, conditional?) do
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

  defp json_headers(body) do
    [{"content-type", "application/json"}, {"x-content-sha256", Signer.sha256_base64(body)}]
  end

  # `POST /n/<ns>/b/<bucket>/u` names the object in the body; every later call
  # names it in the path (`upload_url/2`).
  defp create_upload(instance, opts, key) do
    url = endpoint(opts) <> "/n/" <> namespace(opts) <> "/b/" <> bucket(opts) <> "/u"
    body = JSON.encode!(%{"object" => key})
    headers = SSE.headers(opts, :oci) ++ json_headers(body)

    with {:ok, response} <- HTTP.reply(request(instance, opts, :small, :post, url, body, headers)),
         {:ok, %{"uploadId" => upload_id}} when is_binary(upload_id) <- JSON.decode(response) do
      {:ok, upload_id}
    else
      {:error, reason} -> {:error, reason}
      _unreadable -> {:error, :upload_unreadable}
    end
  end

  defp upload_parts(instance, opts, url, upload_id, parts) do
    parts
    |> Enum.with_index(1)
    |> Enum.reduce_while({:ok, []}, fn {part, number}, {:ok, etags} ->
      query = URI.encode_query([{"uploadId", upload_id}, {"uploadPartNum", number}], :rfc3986)

      headers = [
        {"content-type", "application/octet-stream"},
        {"x-content-sha256", Signer.sha256_base64(part)},
        {"content-md5", Checksum.md5_base64(part)}
      ]

      case request(instance, opts, :bulk, :put, url <> "?" <> query, part, headers) do
        {:ok, status, headers, _body} when status in 200..299 ->
          case Checksum.first(headers, "etag") do
            nil -> {:halt, {:error, :part_without_etag}}
            etag -> {:cont, {:ok, [%{"partNum" => number, "etag" => etag} | etags]}}
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

  defp commit_upload(instance, opts, url, upload_id, etags, conditional?) do
    body = JSON.encode!(%{"partsToCommit" => etags})
    headers = json_headers(body) ++ precondition(conditional?)
    commit_url = url <> "?" <> URI.encode_query([{"uploadId", upload_id}], :rfc3986)

    instance
    |> request(opts, :small, :post, commit_url, body, headers)
    |> HTTP.write_reply(exists_statuses(conditional?))
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

  # Signed per attempt (the signature covers the `date` header) with the identity
  # `identity/1` has at that moment; a `401` drops a provider's cached
  # credentials and sends once more.
  defp request(instance, opts, kind, method, url, body, extra_headers) do
    HTTP.with_refresh(Keyword.get(opts, :key_provider), fn ->
      with {:ok, {key_id, private_key}} <- identity(opts) do
        HTTP.request(instance, opts, {endpoint(opts), bucket_scope(opts)}, kind, fn ->
          headers = [{"date", Signer.rfc1123_date(:calendar.universal_time())} | extra_headers]

          authorization =
            Signer.authorization(
              key_id,
              private_key,
              method,
              url,
              headers,
              body,
              Signer.headers(body != nil)
            )

          {method, url, [{"authorization", authorization} | headers], body}
        end)
      end
    end)
  end

  # `{key_id, pem}` to sign with: a provider's, else the configured API key (or
  # session token and key).
  defp identity(opts) do
    case Keyword.get(opts, :key_provider) do
      {mod, fun, args} ->
        case apply(mod, fun, args) do
          {:ok, %{key_id: key_id, private_key: pem}} -> {:ok, {key_id, pem}}
          _error -> {:error, :no_credentials}
        end

      nil ->
        {:ok, {key_id(opts), private_key!(opts)}}
    end
  end

  defp key_id(opts) do
    Keyword.get_lazy(opts, :key_id, fn ->
      "#{Keyword.fetch!(opts, :tenancy_ocid)}/#{Keyword.fetch!(opts, :user_ocid)}/" <>
        Keyword.fetch!(opts, :key_fingerprint)
    end)
  end

  defp private_key!(opts) do
    case Keyword.fetch(opts, :private_key) do
      {:ok, pem} -> pem
      :error -> raise ArgumentError, "Ankusa.BlobStore.OCI requires :private_key (PEM)"
    end
  end

  defp bucket_scope(opts), do: namespace(opts) <> "/" <> bucket(opts)

  # ── URL building ──────────────────────────────────────────────────────────

  # OCI object names are UTF-8, `%`-encoded per RFC 3986 with `/` kept as a
  # path separator — the same rule as `Ankusa.BlobStore.S3`.
  defp encode_path(key) do
    unreserved = &URI.char_unreserved?/1

    key
    |> String.split("/")
    |> Enum.map_join("/", &URI.encode(&1, unreserved))
  end

  defp object_url(opts, key) do
    endpoint(opts) <>
      "/n/" <> namespace(opts) <> "/b/" <> bucket(opts) <> "/o/" <> encode_path(key)
  end

  defp upload_url(opts, key) do
    endpoint(opts) <>
      "/n/" <> namespace(opts) <> "/b/" <> bucket(opts) <> "/u/" <> encode_path(key)
  end

  defp namespace(opts), do: Keyword.fetch!(opts, :namespace)

  defp bucket(opts), do: Keyword.fetch!(opts, :bucket)

  @doc false
  @spec endpoint(keyword()) :: String.t()
  def endpoint(opts) do
    Keyword.get(
      opts,
      :endpoint,
      "https://objectstorage.#{Keyword.fetch!(opts, :region)}.oraclecloud.com"
    )
  end
end
