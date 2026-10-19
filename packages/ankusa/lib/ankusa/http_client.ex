defmodule Ankusa.HttpClient do
  @moduledoc """
  The one place the outbound adapters issue HTTP requests: the object stores
  (`Ankusa.BlobStore.S3`, `GCS`, `Azure`, `OCI`), `Ankusa.Sink.Http`,
  `Ankusa.Sink.SQS`, and `Ankusa.Sink.GooglePubSub`.

  Those adapters share more than a client. Every request they send is a *function
  of its own bytes* — a signed S3 URL, a signed `SendMessage`, a forwarded hook
  body — which makes four options part of each adapter's contract rather than a
  preference:

    * `retry: false` — retries belong to the framework's own loops
      (`Ankusa.RetryPolicy` for the sinks, `Ankusa.BlobStore.Retry` around one
      object-store request, re-signed per attempt). Req's default retry would
      add hidden latency inside those, and resend a signature as it ages.
    * `redirect: false` — a followed redirect re-issues a POST as a GET, so a
      302 from a sink endpoint would be reported as a successful delivery. A
      redirect on a signed request is never more useful: the signature covers the
      host and path it was made for.
    * `http_errors: :return` — a non-2xx status is a value each adapter maps
      (`{:error, :not_found}`), not an exception.
    * `decode_body: false` — these bodies are segments, claims and hook payloads,
      not JSON; the one JSON reply (SQS's) is checked against the bytes sent, so
      its adapter decodes it itself.

  ## `:req_options` is an allowlist

  Callers reach this module through each adapter's `:req_options`, and the
  accepted keys are `#{inspect([:finch, :connect_options, :pool_timeout, :plug])}`:
  transport tuning that cannot change the request. Anything else raises, because
  the keys that *can* change it — `:params`, `:base_url`, `:auth`, `:headers` —
  would alter a URL that has already been signed, or replace the headers carrying
  a credential. Silently ignoring them would be a worse failure than refusing
  them: the caller would believe a proxy or a pool was in use.

  ## TLS is verified, always

  An `https` request verifies the peer's certificate chain and hostname
  (`verify: :verify_peer`) and negotiates TLS 1.2 or 1.3 only. The trust store is
  the operating system's, with `castore`'s bundle behind it where OTP cannot
  load that; `:cacertfile` in `opts` names a PEM bundle instead (a private CA).
  Turning verification off is not a `:req_options` setting: a `transport_opts:
  [verify: :verify_none]` raises `ArgumentError` from `validate!/2`, and the one
  way past it is `allow_insecure_tls: true` in `opts`, which `Ankusa.Sink.Http`
  exposes per sink for a receiver that cannot present a certificate.

  ## A named pool owns its transport

  `finch: [name: MyFinch]` sends the request through a pool the caller started,
  and that pool's `conn_opts` are the connection options (timeout, TLS, proxy):
  Req refuses `:finch` beside `:connect_options`, so the request carries none.
  Asking for TLS settings per request on top of a named pool (`:cacertfile`,
  `allow_insecure_tls: true`, `:connect_options`) raises `ArgumentError` from
  `validate!/3`, rather than being ignored. `Ankusa.Sink.Http.Pool` is such a
  pool.
  """

  @transport_options [:finch, :connect_options, :pool_timeout, :plug]
  @tls_versions [:"tlsv1.3", :"tlsv1.2"]

  @doc """
  Issue one request, returning the status and raw body.

  `timeout_ms` is applied to both the connection and the response. A
  `:connect_options` in `req_options` is merged over it, so supplying a proxy
  never drops the timeout.

  `:max_response_bytes` in `opts` (not a transport option; it is taken out
  before the request is built) stops reading the response once its body passes
  that many bytes: the body is then `{:truncated, bytes_read}` and the
  connection is closed rather than drained. Without it the whole body is read,
  which the blob stores need.

  `:cacertfile` and `:allow_insecure_tls` in `opts` are likewise taken out
  before the request is built; see "TLS is verified, always".
  """
  @spec request(
          atom(),
          String.t(),
          [{String.t(), String.t()}],
          iodata() | nil,
          timeout(),
          keyword()
        ) :: {:ok, non_neg_integer(), term()} | {:error, term()}
  def request(method, url, headers, body, timeout_ms, opts \\ []) do
    case request_with_headers(method, url, headers, body, timeout_ms, opts) do
      {:ok, status, _headers, body} -> {:ok, status, body}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  `request/6`, plus the response headers (`%{lowercase_name => [value]}`), for
  callers that read one such as `retry-after`.
  """
  @spec request_with_headers(
          atom(),
          String.t(),
          [{String.t(), String.t()}],
          iodata() | nil,
          timeout(),
          keyword()
        ) :: {:ok, non_neg_integer(), %{String.t() => [String.t()]}, term()} | {:error, term()}
  def request_with_headers(method, url, headers, body, timeout_ms, opts \\ []) do
    {max_bytes, opts} = Keyword.pop(opts, :max_response_bytes)
    {cacertfile, opts} = Keyword.pop(opts, :cacertfile)
    {insecure_tls?, opts} = Keyword.pop(opts, :allow_insecure_tls, false)
    validate!(opts, insecure_tls?, cacertfile)

    request =
      [
        method: method,
        url: url,
        headers: headers,
        decode_body: false,
        http_errors: :return,
        retry: false,
        redirect: false,
        receive_timeout: timeout_ms
      ] ++
        connect_option(url, timeout_ms, opts, %{cacertfile: cacertfile, insecure?: insecure_tls?}) ++
        Keyword.drop(opts, [:connect_options]) ++ body_option(body) ++ cap_option(max_bytes)

    case Req.request(request) do
      {:ok, %Req.Response{status: status, headers: headers, body: body}} ->
        {:ok, status, headers, body}

      {:error, reason} ->
        {:error, reason}
    end
  rescue
    # Finch raises when no pooled connection frees up within its checkout
    # timeout (5 s by default): that is a transport failure like a refused
    # connection, for the caller's retry and breaker to see, not a crash of
    # whatever was reading.
    error in RuntimeError -> {:error, error}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  # A named pool owns the connection options; otherwise the request carries them.
  defp connect_option(url, timeout_ms, opts, tls) do
    if Keyword.has_key?(opts, :finch),
      do: [],
      else: [
        connect_options:
          connect_options(url, timeout_ms, Keyword.get(opts, :connect_options, []), tls)
      ]
  end

  defp body_option(nil), do: []
  defp body_option(body), do: [body: body]

  defp cap_option(nil), do: []

  # Accumulate into the response body until it passes `max`, then stop reading
  # (`:halt` closes the connection instead of returning it to the pool).
  defp cap_option(max) when is_integer(max) and max >= 0 do
    [
      into: fn {:data, data}, {req, resp} ->
        body = if is_binary(resp.body), do: resp.body <> data, else: data

        if byte_size(body) > max do
          {:halt, {req, %{resp | body: {:truncated, byte_size(body)}}}}
        else
          {:cont, {req, %{resp | body: body}}}
        end
      end
    ]
  end

  # The connect timeout, then the TLS options for an `https` URL, then the
  # caller's own `:connect_options` over both. `transport_opts` are Mint's
  # `:gen_tcp` options for `http`, so the TLS ones must not reach an `http` URL.
  defp connect_options(url, timeout_ms, connect_options, tls) do
    base = Keyword.merge([timeout: timeout_ms], Keyword.delete(connect_options, :transport_opts))
    caller = Keyword.get(connect_options, :transport_opts, [])

    if https?(url) do
      Keyword.put(base, :transport_opts, Keyword.merge(tls_options(tls), caller))
    else
      if caller == [], do: base, else: Keyword.put(base, :transport_opts, caller)
    end
  end

  defp tls_options(%{insecure?: true}), do: [verify: :verify_none]

  defp tls_options(%{cacertfile: cacertfile}) do
    [verify: :verify_peer, versions: @tls_versions] ++
      if(cacertfile, do: [cacertfile: cacertfile], else: [])
  end

  defp https?(url) do
    case URI.parse(url) do
      %URI{scheme: scheme} when is_binary(scheme) -> String.downcase(scheme) == "https"
      _other -> false
    end
  end

  @doc """
  Raise `ArgumentError` unless `req_options` holds only transport keys
  (`:finch`, `:connect_options`, `:pool_timeout`, `:plug`) and does not turn off
  TLS verification (`connect_options: [transport_opts: [verify: :verify_none]]`)
  without `allow_insecure_tls?`, nor asks a named `:finch` pool (see "A named
  pool owns its transport") for per-request connection options: `:connect_options`,
  a `cacertfile` or `allow_insecure_tls?: true`.

  `request/6` calls it on every request, and `Ankusa.Sink.Http.validate_opts!/1`
  calls it when a config is validated, so a bad `:req_options` on an HTTP sink
  fails at boot, not at its first delivery.
  """
  @spec validate!(keyword(), boolean(), String.t() | nil) :: :ok
  def validate!(req_options, allow_insecure_tls? \\ false, cacertfile \\ nil) do
    case Keyword.keys(req_options) -- @transport_options do
      [] ->
        :ok

      [key | _] ->
        raise ArgumentError,
              "unsupported :req_options key #{inspect(key)} — the adapters accept " <>
                "#{Enum.map_join(@transport_options, ", ", &inspect/1)}. Keys that " <>
                "change the request itself (:params, :base_url, :auth, :headers) " <>
                "would alter a URL that has already been signed or replace the " <>
                "headers carrying a credential."
    end

    transport_opts =
      req_options |> Keyword.get(:connect_options, []) |> Keyword.get(:transport_opts, [])

    if Keyword.get(transport_opts, :verify) == :verify_none and not allow_insecure_tls? do
      raise ArgumentError,
            ":req_options connect_options: [transport_opts: [verify: :verify_none]] " <>
              "turns off TLS peer verification, which is refused. Name a private CA " <>
              "with :cacertfile instead, or set allow_insecure_tls: true on the one " <>
              "sink that must talk to an unverifiable receiver."
    end

    if Keyword.has_key?(req_options, :finch) and
         (Keyword.has_key?(req_options, :connect_options) or allow_insecure_tls? or
            cacertfile != nil) do
      raise ArgumentError,
            "a named :finch pool owns its connection options: :connect_options, a " <>
              "cacertfile and allow_insecure_tls cannot be combined with it. Configure " <>
              "the pool's conn_opts (TLS, proxy, timeout) when you start it."
    end

    :ok
  end
end
