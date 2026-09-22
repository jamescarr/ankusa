defmodule Ankusa.Sink.Http do
  @moduledoc """
  Forward the raw hook body to an HTTP endpoint via `:httpc`.

  `opts`:

    * `:url`        — required target URL
    * `:method`     — HTTP method (default `:post`)
    * `:headers`    — extra request headers as `[{key, value}]` strings
    * `:timeout_ms` — request/connect timeout (default `5000`)

  The original `env.body` is sent verbatim with the envelope's content-type
  (falling back to `application/octet-stream`). Identity headers `x-ankusa-id`,
  `x-ankusa-source`, and `x-ankusa-seq` are always added. A `2xx` response is `:ok`;
  any other status is `{:error, {:status, code}}`; a transport failure is
  `{:error, reason}`.
  """

  @behaviour Ankusa.Sink

  @impl true
  def deliver(env, _ctx, opts) do
    ensure_started()

    url = Keyword.fetch!(opts, :url)
    method = Keyword.get(opts, :method, :post)
    extra = Keyword.get(opts, :headers, [])
    timeout = Keyword.get(opts, :timeout_ms, 5000)
    content_type = env.content_type || "application/octet-stream"

    headers =
      [
        {~c"x-ankusa-id", to_charlist(env.id)},
        {~c"x-ankusa-source", to_charlist(env.source_id)},
        {~c"x-ankusa-seq", to_charlist(to_string(env.seq))}
      ] ++ Enum.map(extra, fn {k, v} -> {to_charlist(k), to_charlist(v)} end)

    request = {to_charlist(url), headers, to_charlist(content_type), env.body}
    http_opts = [timeout: timeout, connect_timeout: timeout]

    case :httpc.request(method, request, http_opts, body_format: :binary) do
      {:ok, {{_http, code, _reason}, _resp_headers, _resp_body}} when code in 200..299 ->
        :ok

      {:ok, {{_http, code, _reason}, _resp_headers, _resp_body}} ->
        {:error, {:status, code}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ensure_started do
    {:ok, _} = Application.ensure_all_started(:inets)
    {:ok, _} = Application.ensure_all_started(:ssl)
    :ok
  end
end
