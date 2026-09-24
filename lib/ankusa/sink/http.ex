defmodule Ankusa.Sink.Http do
  @moduledoc """
  Forward the raw hook body to an HTTP endpoint, via
  [`Req`](https://hex.pm/packages/req).

  `opts`:

    * `:url`        — required target URL
    * `:method`     — HTTP method (default `:post`)
    * `:headers`    — extra request headers as `[{key, value}]` strings
    * `:timeout_ms` — request/connect timeout (default `5000`)
    * `:ordered`    — when `true`, deliveries to this sink for the same
                      `{tenant_id, source_id}` run one at a time in `seq` order
                      (default `false`: deliveries have no ordering constraint
                      and run concurrently)
    * `:req_options` — transport options for the HTTP client (custom Finch pool,
                       proxy, or `plug:` for `Req.Test` in tests). See
                       `Ankusa.HttpClient` for the accepted keys

  The original `env.body` is sent verbatim with the envelope's content-type
  (falling back to `application/octet-stream`). Identity headers `x-ankusa-id`,
  `x-ankusa-source`, `x-ankusa-seq`, and (when set) `x-ankusa-tenant` are
  always added. A `2xx` response is `:ok`;

  Redirects are never followed: this body is the hook, and a followed redirect
  would re-send it as a `GET`. A `3xx` is reported as its status, for the
  source's retry policy to act on.
  """

  @behaviour Ankusa.Sink

  alias Ankusa.HttpClient

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
        tenant_header(env.tenant_id) ++
        Enum.map(Keyword.get(opts, :headers, []), fn {k, v} -> {to_string(k), to_string(v)} end)

    case HttpClient.request(
           Keyword.get(opts, :method, :post),
           Keyword.fetch!(opts, :url),
           headers,
           env.body,
           timeout,
           Keyword.get(opts, :req_options, [])
         ) do
      {:ok, status, _body} when status in 200..299 -> :ok
      {:ok, status, _body} -> {:error, {:status, status}}
      {:error, reason} -> {:error, reason}
    end
  end

  # HTTP endpoints are not guaranteed to process concurrent requests in order,
  # so ordering is opt-in: `ordered: true` serializes per {tenant, source}.
  @impl true
  def ordering_key(env, opts) do
    if Keyword.get(opts, :ordered, false), do: {env.tenant_id, env.source_id}, else: nil
  end

  defp tenant_header(tenant_id) when is_binary(tenant_id), do: [{"x-ankusa-tenant", tenant_id}]
  defp tenant_header(_), do: []
end
