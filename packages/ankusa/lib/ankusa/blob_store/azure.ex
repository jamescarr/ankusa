defmodule Ankusa.BlobStore.Azure do
  @moduledoc """
  `Ankusa.BlobStore` backed by Azure Blob Storage, via
  [`Req`](https://hex.pm/packages/req). Works against real Azure or the
  [Azurite](https://learn.microsoft.com/azure/storage/common/storage-use-azurite)
  emulator.

  This adapter deliberately carries **no credential dependency** — the same
  stance as `Ankusa.BlobStore.GCS`. It accepts a credential you already have,
  in either of the two forms Azure supports without the account key:

    * `:sas_token` — a Shared Access Signature query string (no leading `?`),
      generated out-of-band with the Azure CLI, SDK, or portal. The adapter
      appends it to every request and never sees the account key.
    * `:token_provider` — `{module, fun, args}`, applied per request, must
      return `{:ok, bearer_token} | :error`. For a service running on Azure,
      use the built-in `Ankusa.BlobStore.Azure.ManagedIdentity` (system- or
      user-assigned) rather than wiring your own Entra ID token source.

  Shared Key signing (the `Authorization: SharedKey ...` HMAC) is deliberately
  not implemented — it is the kind of security-sensitive canonicalization this
  project does not hand-roll, and a SAS token is the least-privilege credential
  Azure recommends anyway.

  opts:

    * `:account_name`   — required, e.g. `"myaccount"`
    * `:container`      — required; Azure's name for the bucket
    * `:sas_token`      — optional; a SAS query string (no leading `?`)
    * `:token_provider` — optional; `{module, fun, args}` returning a bearer token
    * `:endpoint`       — default `"https://\#{account_name}.blob.core.windows.net"`;
                           point at `"http://127.0.0.1:10000/\#{account_name}"` for Azurite
    * `:timeout_ms`     — default `10_000`, for both connect and response
    * `:req_options`    — transport options for the HTTP client, e.g. a custom
                           Finch pool, a proxy, or `plug:` for `Req.Test` in
                           tests. See `Ankusa.HttpClient` for the accepted keys
    * `:retries`        — default `2`: extra attempts after a transport error
                           or a `408`/`429`/`5xx`. See `Ankusa.BlobStore.Retry`

  ## Local dev

      config :ankusa,
        storage: %{
          blob_store:
            {Ankusa.BlobStore.Azure,
             account_name: "devstoreaccount1",
             container: "ankusa-segments-dev",
             endpoint: "http://127.0.0.1:10000/devstoreaccount1",
             sas_token: System.get_env("AZURE_BLOB_SAS")}
        }

  `docker compose -f docker-compose.integration.yml up -d azurite azure-bootstrap` brings up
  the emulator and creates the container.
  """

  @behaviour Ankusa.BlobStore

  # See Ankusa.BlobStore.S3 for why this false-positive suppression exists.
  @compile {:no_warn_undefined, [:xmerl_scan, :xmerl_xpath]}

  alias Ankusa.BlobStore.Retry
  alias Ankusa.HttpClient

  @api_version "2024-11-04"

  @impl true
  def put(_instance, key, data, opts) do
    headers = [
      {"x-ms-blob-type", "BlockBlob"},
      {"content-type", "application/octet-stream"}
    ]

    case request(opts, :put, blob_url(opts, key), data, headers) do
      {:ok, _body} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def get(_instance, key, opts), do: request(opts, :get, blob_url(opts, key), nil, [])

  @impl true
  def get_range(_instance, key, offset, length, opts) do
    range = "bytes=#{offset}-#{offset + length - 1}"
    request(opts, :get, blob_url(opts, key), nil, [{"range", range}])
  end

  @impl true
  def delete(_instance, key, opts) do
    _ = request(opts, :delete, blob_url(opts, key), nil, [])
    :ok
  end

  # Every page (`NextMarker`), and a failed page fails the listing.
  @impl true
  def list(_instance, prefix, opts), do: list_pages(opts, prefix, nil, [])

  defp list_pages(opts, prefix, marker, acc) do
    query =
      [{"restype", "container"}, {"comp", "list"}, {"prefix", prefix}] ++
        if(marker, do: [{"marker", marker}], else: [])

    url = endpoint(opts) <> "/" <> container(opts) <> query_suffix(query, opts)

    with {:ok, body} <- request(opts, :get, url, nil, []),
         {:ok, keys, next} <- parse_list_page(body) do
      acc = [keys | acc]

      case next do
        nil -> {:ok, acc |> Enum.reverse() |> Enum.concat() |> Enum.sort()}
        marker -> list_pages(opts, prefix, marker, acc)
      end
    end
  end

  # ── requests ──────────────────────────────────────────────────────────────

  defp request(opts, method, url, body, headers) do
    timeout = Keyword.get(opts, :timeout_ms, 10_000)
    req_options = Keyword.get(opts, :req_options, [])

    result =
      Retry.run(opts, fn ->
        all_headers = auth_headers(opts) ++ version_header(opts) ++ headers
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

  # A SAS already encodes the API version it was minted for; sending
  # `x-ms-version` alongside can only contradict it. The bearer-token path has
  # no `sv=`, so it needs an explicit version.
  defp version_header(opts) do
    if Keyword.has_key?(opts, :sas_token), do: [], else: [{"x-ms-version", @api_version}]
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

  # ── URL building ──────────────────────────────────────────────────────────

  # Azure blob names are UTF-8, `%`-encoded per RFC 3986 with `/` kept as a
  # path separator — the same rule as `Ankusa.BlobStore.S3`.
  defp encode_path(key) do
    unreserved = &URI.char_unreserved?/1

    key
    |> String.split("/")
    |> Enum.map_join("/", &URI.encode(&1, unreserved))
  end

  defp blob_url(opts, key) do
    endpoint(opts) <> "/" <> container(opts) <> "/" <> encode_path(key) <> sas_suffix(opts)
  end

  defp query_suffix(query, opts) do
    "?" <> URI.encode_query(query, :rfc3986) <> sas_suffix(opts, "&")
  end

  defp sas_suffix(opts, joiner \\ "?") do
    case Keyword.get(opts, :sas_token) do
      nil -> ""
      token -> joiner <> token
    end
  end

  defp container(opts), do: Keyword.fetch!(opts, :container)

  defp endpoint(opts) do
    Keyword.get(
      opts,
      :endpoint,
      "https://#{Keyword.fetch!(opts, :account_name)}.blob.core.windows.net"
    )
  end

  # ── ListBlobs XML (stdlib :xmerl, the same approach as S3's ListObjectsV2) ──

  # A 200 body is not guaranteed to be ListBlobs XML — a proxy error page or an
  # emulator quirk will do it. `:xmerl_scan` *exits* on a malformed document;
  # that is an error for the listing, never a crash of the caller.
  defp parse_list_page(xml_body) do
    {doc, _rest} = :xmerl_scan.string(:binary.bin_to_list(xml_body), quiet: true)

    keys =
      ~c"//Blob/Name/text()"
      |> :xmerl_xpath.string(doc)
      |> Enum.map(fn {:xmlText, _parents, _pos, _lang, value, _type} -> List.to_string(value) end)

    next =
      case :xmerl_xpath.string(~c"//NextMarker/text()", doc) do
        [{:xmlText, _parents, _pos, _lang, value, _type} | _] when value != [] ->
          List.to_string(value)

        _ ->
          nil
      end

    {:ok, keys, next}
  catch
    :exit, _not_xml -> {:error, :list_unreadable}
  end
end
