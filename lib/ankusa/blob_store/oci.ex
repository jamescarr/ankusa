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
    * `:tenancy_ocid`    — required
    * `:user_ocid`       — required
    * `:key_fingerprint` — required; the fingerprint of the API signing key
    * `:private_key`     — required; the PEM private key (PKCS#1 or PKCS#8)
    * `:endpoint`        — default `"https://objectstorage.\#{region}.oraclecloud.com"`
    * `:timeout_ms`      — default `10_000`, for both connect and response
    * `:req_options`     — transport options for the HTTP client. See
                           `Ankusa.HttpClient` for the accepted keys

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

  OCI publishes no Object Storage emulator, so unlike S3/GCS/Azure there is no
  local `docker compose` path here; the signing is proven by the reference
  vectors instead (see the test named above).
  """

  @behaviour Ankusa.BlobStore

  alias Ankusa.HttpClient

  @generic_headers ["date", "(request-target)", "host"]
  @body_headers ["content-length", "content-type", "x-content-sha256"]

  @impl true
  def put(_instance, key, data, opts) do
    body = IO.iodata_to_binary(data)
    url = object_url(opts, key)

    extra = [
      {"content-type", "application/octet-stream"},
      {"x-content-sha256", sha256_base64(body)}
    ]

    case request(opts, :put, url, body, extra) do
      {:ok, _body} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def get(_instance, key, opts), do: request(opts, :get, object_url(opts, key), nil, [])

  @impl true
  def get_range(_instance, key, offset, length, opts) do
    range = "bytes=#{offset}-#{offset + length - 1}"
    request(opts, :get, object_url(opts, key), nil, [{"range", range}])
  end

  @impl true
  def delete(_instance, key, opts) do
    _ = request(opts, :delete, object_url(opts, key), nil, [])
    :ok
  end

  @impl true
  def list(_instance, prefix, opts) do
    url =
      endpoint(opts) <>
        "/n/" <>
        namespace(opts) <>
        "/b/" <>
        bucket(opts) <>
        "/o" <>
        "?" <> URI.encode_query([{"prefix", prefix}], :rfc3986)

    case request(opts, :get, url, nil, []) do
      {:ok, body} ->
        case JSON.decode(body) do
          {:ok, %{"objects" => objects}} -> objects |> Enum.map(& &1["name"]) |> Enum.sort()
          {:ok, _no_objects} -> []
          {:error, _reason} -> []
        end

      {:error, _reason} ->
        []
    end
  end

  # ── requests ──────────────────────────────────────────────────────────────

  defp request(opts, method, url, body, extra_headers) do
    date = rfc1123_date(:calendar.universal_time())
    headers = [{"date", date} | extra_headers]
    authorization = sign(opts, method, url, headers, body)
    timeout = Keyword.get(opts, :timeout_ms, 10_000)

    case HttpClient.request(
           method,
           url,
           [{"authorization", authorization} | headers],
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

  # ── signing (OCI Signature version 1) ─────────────────────────────────────

  defp sign(opts, method, url, headers, body) do
    signing_headers =
      if body == nil, do: @generic_headers, else: @generic_headers ++ @body_headers

    signing_string =
      Enum.map_join(signing_headers, "\n", fn name ->
        "#{name}: #{header_value(name, method, url, headers, body)}"
      end)

    signature =
      opts
      |> private_key!()
      |> then(&:public_key.sign(signing_string, :sha256, &1))
      |> Base.encode64()

    "Signature version=\"1\",keyId=\"#{key_id(opts)}\",algorithm=\"rsa-sha256\"," <>
      "headers=\"#{Enum.join(signing_headers, " ")}\",signature=\"#{signature}\""
  end

  defp header_value("(request-target)", method, url, _headers, _body) do
    method = method |> Atom.to_string() |> String.downcase()

    %URI{path: path, query: query} = URI.parse(url)
    target = if query in [nil, ""], do: path, else: path <> "?" <> query
    method <> " " <> target
  end

  defp header_value("host", _method, url, _headers, _body), do: authority(url)

  defp header_value("content-length", _method, _url, _headers, body),
    do: Integer.to_string(byte_size(body))

  defp header_value(name, _method, _url, headers, _body) do
    {^name, value} = List.keyfind(headers, name, 0)
    value
  end

  defp key_id(opts) do
    "#{Keyword.fetch!(opts, :tenancy_ocid)}/#{Keyword.fetch!(opts, :user_ocid)}/" <>
      Keyword.fetch!(opts, :key_fingerprint)
  end

  defp private_key!(opts) do
    case Keyword.fetch(opts, :private_key) do
      {:ok, pem} ->
        pem
        |> :public_key.pem_decode()
        |> hd()
        |> :public_key.pem_entry_decode()

      :error ->
        raise ArgumentError, "Ankusa.BlobStore.OCI requires :private_key (PEM)"
    end
  end

  defp sha256_base64(body), do: Base.encode64(:crypto.hash(:sha256, body))

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

  defp namespace(opts), do: Keyword.fetch!(opts, :namespace)

  defp bucket(opts), do: Keyword.fetch!(opts, :bucket)

  defp endpoint(opts) do
    Keyword.get(
      opts,
      :endpoint,
      "https://objectstorage.#{Keyword.fetch!(opts, :region)}.oraclecloud.com"
    )
  end

  defp authority(url) do
    %URI{host: host, port: port, scheme: scheme} = URI.parse(url)
    default = if scheme == "https", do: 443, else: 80
    if port == default, do: host, else: "#{host}:#{port}"
  end

  # RFC 1123 GMT: "Sun, 06 Nov 1994 08:49:37 GMT"
  defp rfc1123_date({{y, mo, d}, {h, mi, s}}) do
    "#{day_name(y, mo, d)}, #{pad(d)} #{month_name(mo)} #{y} #{pad(h)}:#{pad(mi)}:#{pad(s)} GMT"
  end

  # `:calendar.day_of_the_week/1` → 1 (Monday) .. 7 (Sunday)
  @days %{1 => "Mon", 2 => "Tue", 3 => "Wed", 4 => "Thu", 5 => "Fri", 6 => "Sat", 7 => "Sun"}
  defp day_name(y, mo, d), do: Map.fetch!(@days, :calendar.day_of_the_week({y, mo, d}))

  @months %{
    1 => "Jan",
    2 => "Feb",
    3 => "Mar",
    4 => "Apr",
    5 => "May",
    6 => "Jun",
    7 => "Jul",
    8 => "Aug",
    9 => "Sep",
    10 => "Oct",
    11 => "Nov",
    12 => "Dec"
  }
  defp month_name(mo), do: Map.fetch!(@months, mo)

  defp pad(n), do: n |> Integer.to_string() |> String.pad_leading(2, "0")
end
