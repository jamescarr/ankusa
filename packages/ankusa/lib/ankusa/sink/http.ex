defmodule Ankusa.Sink.Http do
  @moduledoc """
  Forward the raw hook body to an HTTP endpoint, via
  [`Req`](https://hex.pm/packages/req).

  `opts`:

    * `:url`        — required target URL
    * `:method`     — HTTP method (default `:post`)
    * `:headers`    — extra request headers as `[{key, value}]` strings
    * `:timeout_ms` — request/connect timeout (default `5000`)
    * `:req_options` — transport options for the HTTP client (custom Finch pool,
                       proxy, or `plug:` for `Req.Test` in tests). See
                       `Ankusa.HttpClient` for the accepted keys

  The original `env.body` is sent verbatim with the envelope's content-type
  (falling back to `application/octet-stream`). Identity headers `x-ankusa-id`,
  `x-ankusa-source`, `x-ankusa-idempotency-key` (the key a consumer dedupes on,
  see `Ankusa.Envelope.idempotency_key/1`) and (when set) `x-ankusa-tenant` are
  always added, plus `x-ankusa-dedupe-key` and `x-ankusa-replay-id` when the
  hook carries them.
  Provider request headers are forwarded per the source's `forward_headers`
  option (see `Ankusa.Sink.Message.forwarded_headers/2`); a forwarded name that
  collides with one of these or with `opts[:headers]` is dropped. A `2xx`
  response is `:ok`;

  Redirects are never followed: this body is the hook, and a followed redirect
  would re-send it as a `GET`. A `3xx` is reported as its status, for the
  source's retry policy to act on.
  """

  @behaviour Ankusa.Sink

  alias Ankusa.{Envelope, HttpClient}
  alias Ankusa.Sink.Message

  @impl true
  def deliver(env, ctx, opts) do
    timeout = Keyword.get(opts, :timeout_ms, 5000)

    opts_headers =
      Enum.map(Keyword.get(opts, :headers, []), fn {k, v} -> {to_string(k), to_string(v)} end)

    own =
      [
        {"x-ankusa-id", env.id},
        {"x-ankusa-idempotency-key", Envelope.idempotency_key(env)},
        {"x-ankusa-source", env.source_id},
        {"content-type", env.content_type || "application/octet-stream"}
      ] ++
        tenant_header(env.tenant_id) ++
        dedupe_header(env.dedupe_key) ++
        replay_header(ctx[:replay_id])

    forwarded =
      Message.forwarded_headers(env, ctx[:forward_headers] || :default)
      |> Enum.reject(fn {name, _value} -> owned?(name, own) or owned?(name, opts_headers) end)

    headers = forwarded ++ own ++ opts_headers

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

  # A forwarded name that collides with a header the sink sets, or with one the
  # operator configured in `opts[:headers]`, is dropped (compared lowercased).
  defp owned?(name, headers) do
    Enum.any?(headers, fn {k, _v} -> String.downcase(to_string(k)) == name end)
  end

  defp dedupe_header(key) when is_binary(key), do: [{"x-ankusa-dedupe-key", key}]
  defp dedupe_header(_), do: []

  defp replay_header(replay_id) when is_binary(replay_id), do: [{"x-ankusa-replay-id", replay_id}]
  defp replay_header(_), do: []

  defp tenant_header(tenant_id) when is_binary(tenant_id), do: [{"x-ankusa-tenant", tenant_id}]
  defp tenant_header(_), do: []
end
