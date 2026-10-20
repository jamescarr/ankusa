defmodule Ankusa.BlobStore.OCI.Signer do
  @moduledoc false
  # OCI "Signature version 1": an RSA-SHA256 signature over a request's
  # `(request-target)`, `date` and `host` headers (plus `content-length`,
  # `content-type` and `x-content-sha256` when it has a body), with OTP's
  # `:public_key`. Shared by `Ankusa.BlobStore.OCI` (every object request) and
  # `Ankusa.BlobStore.OCI.InstancePrincipal` (the federation request), which
  # signs a different header set.

  @generic_headers ["date", "(request-target)", "host"]
  @body_headers ["content-length", "content-type", "x-content-sha256"]

  @doc "The header names an object request signs: `body?` adds the body's three."
  @spec headers(boolean()) :: [String.t()]
  def headers(false), do: @generic_headers
  def headers(true), do: @generic_headers ++ @body_headers

  @doc """
  The `authorization` header value for `method`/`url`, signed with the PEM
  `private_key` under `key_id`, over `signing_headers` (names; values come from
  `headers`, the request's `[{name, value}]`, or from the request itself for
  `(request-target)`, `host` and `content-length`).
  """
  @spec authorization(
          String.t(),
          String.t(),
          atom(),
          String.t(),
          [{String.t(), String.t()}],
          iodata() | nil,
          [String.t()]
        ) :: String.t()
  def authorization(key_id, private_key, method, url, headers, body, signing_headers) do
    signing_string =
      Enum.map_join(signing_headers, "\n", fn name ->
        "#{name}: #{header_value(name, method, url, headers, body)}"
      end)

    signature =
      private_key
      |> decode_key!()
      |> then(&:public_key.sign(signing_string, :sha256, &1))
      |> Base.encode64()

    ~s(Signature version="1",keyId="#{key_id}",algorithm="rsa-sha256",) <>
      ~s(headers="#{Enum.join(signing_headers, " ")}",signature="#{signature}")
  end

  defp header_value("(request-target)", method, url, _headers, _body) do
    method = method |> Atom.to_string() |> String.downcase()

    %URI{path: path, query: query} = URI.parse(url)
    target = if query in [nil, ""], do: path, else: path <> "?" <> query
    method <> " " <> target
  end

  defp header_value("host", _method, url, _headers, _body), do: authority(url)

  defp header_value("content-length", _method, _url, _headers, body),
    do: Integer.to_string(IO.iodata_length(body))

  defp header_value(name, _method, _url, headers, _body) do
    {^name, value} = List.keyfind(headers, name, 0)
    value
  end

  defp decode_key!(pem) do
    pem
    |> :public_key.pem_decode()
    |> hd()
    |> :public_key.pem_entry_decode()
  end

  @doc "`host[:port]` of a URL, the port left out when it is the scheme's default."
  @spec authority(String.t()) :: String.t()
  def authority(url) do
    %URI{host: host, port: port, scheme: scheme} = URI.parse(url)
    default = if scheme == "https", do: 443, else: 80
    if port == default, do: host, else: "#{host}:#{port}"
  end

  @doc "Base64 SHA-256 of a body, the `x-content-sha256` header."
  @spec sha256_base64(iodata()) :: String.t()
  def sha256_base64(body), do: Base.encode64(:crypto.hash(:sha256, body))

  @doc "RFC 1123 GMT: `\"Sun, 06 Nov 1994 08:49:37 GMT\"`."
  @spec rfc1123_date(:calendar.datetime()) :: String.t()
  def rfc1123_date({{y, mo, d}, {h, mi, s}}) do
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
