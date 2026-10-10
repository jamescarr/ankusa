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
  """

  @transport_options [:finch, :connect_options, :pool_timeout, :plug]

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
    validate!(opts)

    request =
      [
        method: method,
        url: url,
        headers: headers,
        decode_body: false,
        http_errors: :return,
        retry: false,
        redirect: false,
        receive_timeout: timeout_ms,
        connect_options:
          Keyword.merge([timeout: timeout_ms], Keyword.get(opts, :connect_options, []))
      ] ++ Keyword.drop(opts, [:connect_options]) ++ body_option(body) ++ cap_option(max_bytes)

    case Req.request(request) do
      {:ok, %Req.Response{status: status, headers: headers, body: body}} ->
        {:ok, status, headers, body}

      {:error, reason} ->
        {:error, reason}
    end
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

  defp validate!(opts) do
    case Keyword.keys(opts) -- @transport_options do
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
  end
end
