defmodule Ankusa.Sink.Http do
  @moduledoc """
  Forward the raw hook body to an HTTP endpoint, via
  [`Req`](https://hex.pm/packages/req).

  `opts`:

    * `:url`        — required target URL
    * `:method`     — HTTP method (default `:post`)
    * `:headers`    — extra request headers as `[{key, value}]` strings
    * `:timeout_ms` — request/connect timeout (default `5000`)
    * `:req_options` — extra options for `Req` (custom Finch pool, proxy, or
                       `plug:` for `Req.Test` in tests)

  The original `env.body` is sent verbatim with the envelope's content-type
  (falling back to `application/octet-stream`). Identity headers `x-ankusa-id`,
  `x-ankusa-source`, and `x-ankusa-seq` are always added. A `2xx` response is `:ok`;
  any other status is `{:error, {:status, code}}`; a transport failure is
  `{:error, reason}`.
  """

  @behaviour Ankusa.Sink

  @impl true
  def deliver(env, _ctx, opts) do
    timeout = Keyword.get(opts, :timeout_ms, 5000)

    headers =
      [
        {"x-ankusa-id", env.id},
        {"x-ankusa-source", env.source_id},
        {"x-ankusa-seq", to_string(env.seq)},
        {"content-type", env.content_type || "application/octet-stream"}
      ] ++
        Enum.map(Keyword.get(opts, :headers, []), fn {k, v} -> {to_string(k), to_string(v)} end)

    request =
      [
        method: Keyword.get(opts, :method, :post),
        url: Keyword.fetch!(opts, :url),
        headers: headers,
        body: env.body,
        # The body is forwarded verbatim; nothing here parses a response.
        decode_body: false,
        http_errors: :return,
        # Retries belong to the source's Ankusa.RetryPolicy, not hidden in here.
        retry: false,
        receive_timeout: timeout,
        connect_options: [timeout: timeout]
      ] ++ Keyword.get(opts, :req_options, [])

    case Req.request(request) do
      {:ok, %Req.Response{status: status}} when status in 200..299 -> :ok
      {:ok, %Req.Response{status: status}} -> {:error, {:status, status}}
      {:error, reason} -> {:error, reason}
    end
  end
end
