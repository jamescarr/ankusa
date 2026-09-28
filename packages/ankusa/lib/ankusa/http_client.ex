defmodule Ankusa.HttpClient do
  @moduledoc """
  The one place the outbound adapters issue HTTP requests: `Ankusa.BlobStore.S3`,
  `Ankusa.BlobStore.GCS`, and `Ankusa.Sink.Http`.

  Those three share more than a client. Every request they send is a *function of
  its own bytes* — a signed S3 URL, a forwarded hook body — which makes four
  options part of each adapter's contract rather than a preference:

    * `retry: false` — retries belong to the framework's own loops
      (`Ankusa.RetryPolicy`, the batcher's backoff). Req's default retry would
      add hidden latency inside the retry it is already being wrapped in.
    * `redirect: false` — a followed redirect re-issues a POST as a GET, so a
      302 from a sink endpoint would be reported as a successful delivery. A
      redirect on a signed request is never more useful: the signature covers the
      host and path it was made for.
    * `http_errors: :return` — a non-2xx status is a value each adapter maps
      (`{:error, :not_found}`), not an exception.
    * `decode_body: false` — these bodies are segments, claims and hook payloads,
      not JSON.

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
      ] ++ Keyword.drop(opts, [:connect_options]) ++ body_option(body)

    case Req.request(request) do
      {:ok, %Req.Response{status: status, body: body}} -> {:ok, status, body}
      {:error, reason} -> {:error, reason}
    end
  end

  defp body_option(nil), do: []
  defp body_option(body), do: [body: body]

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
