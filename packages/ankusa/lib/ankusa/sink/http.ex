defmodule Ankusa.Sink.Http do
  @moduledoc """
  Forward the raw hook body to an HTTP endpoint, via
  [`Req`](https://hex.pm/packages/req).

  `opts`:

    * `:url`        — required target URL
    * `:method`     — HTTP method (default `:post`)
    * `:headers`    — extra request headers as `[{key, value}]` strings
    * `:timeout_ms` — request/connect timeout (default `5000`)
    * `:secret`     — sign every delivery ([Standard
                       Webhooks](https://www.standardwebhooks.com/)): a
                       `whsec_…` secret, or a list of them while rotating (one
                       `v1,` signature each). See `Ankusa.Sink.Http.Signer`
    * `:max_response_bytes` — read at most this much of the response body
                       (default `65_536`); a longer one is cut off and the
                       connection closed. Only the status is used, so a large
                       or endless response cannot pin the attempt or its memory
    * `:req_options` — transport options for the HTTP client (custom Finch pool,
                       proxy, or `plug:` for `Req.Test` in tests). See
                       `Ankusa.HttpClient` for the accepted keys

  The original `env.body` is sent verbatim with the envelope's content-type
  (falling back to `application/octet-stream`). Identity headers `x-ankusa-id`,
  `x-ankusa-source`, `x-ankusa-idempotency-key` (the key a consumer dedupes on,
  see `Ankusa.Envelope.idempotency_key/1`) and (when set) `x-ankusa-tenant` are
  always added, plus `x-ankusa-dedupe-key` and `x-ankusa-replay-id` when the
  hook carries them, and `webhook-id`/`webhook-timestamp`/`webhook-signature`
  with a `:secret`.
  Provider request headers are forwarded per the source's `forward_headers`
  option (see `Ankusa.Sink.Message.forwarded_headers/2`); a forwarded name that
  collides with one of these or with `opts[:headers]` is dropped.

  ## Responses

  | Status | Result | What dispatch does |
  | --- | --- | --- |
  | `2xx` | `:ok` | delivered |
  | `400`, `401`, `403`, `404`, `410`, `413`, `422` | `{:error, {:permanent, {:status, s}}}` | dead-letter now |
  | `408`, `429`, `5xx` with a `Retry-After` | `{:error, {:retry_after, ms, {:status, s}}}` | retry no sooner than that |
  | anything else | `{:error, {:status, s}}` | the retry policy |

  `401` and `403` are permanent because a credential (or `:secret`) fix is
  followed by a DLQ replay, not by hours of retries against a receiver that
  will keep refusing. `Retry-After` is read as seconds or as an HTTP-date.
  See "Error classes" in `Ankusa.Sink`.

  Redirects are never followed: this body is the hook, and a followed redirect
  would re-send it as a `GET`. A `3xx` is reported as its status, for the
  source's retry policy to act on.
  """

  @behaviour Ankusa.Sink

  alias Ankusa.{Envelope, HttpClient}
  alias Ankusa.Sink.Http.Signer
  alias Ankusa.Sink.Message

  @permanent [400, 401, 403, 404, 410, 413, 422]
  @default_max_response_bytes 65_536

  @impl true
  def deliver(env, ctx, opts) do
    with {:ok, signature} <- signature(env, opts) do
      send_hook(env, ctx, opts, signature)
    end
  end

  defp signature(env, opts) do
    case Keyword.get(opts, :secret) do
      nil ->
        {:ok, []}

      secrets ->
        case Signer.headers(env.id, env.body, secrets, System.system_time(:second)) do
          {:ok, headers} -> {:ok, headers}
          :error -> {:error, {:permanent, :bad_secret}}
        end
    end
  end

  defp send_hook(env, ctx, opts, signature) do
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
        replay_header(ctx[:replay_id]) ++
        signature

    forwarded =
      Message.forwarded_headers(env, ctx[:forward_headers] || :default)
      |> Enum.reject(fn {name, _value} -> owned?(name, own) or owned?(name, opts_headers) end)

    headers = forwarded ++ own ++ opts_headers

    client_opts =
      Keyword.put(
        Keyword.get(opts, :req_options, []),
        :max_response_bytes,
        Keyword.get(opts, :max_response_bytes, @default_max_response_bytes)
      )

    case HttpClient.request_with_headers(
           Keyword.get(opts, :method, :post),
           Keyword.fetch!(opts, :url),
           headers,
           env.body,
           timeout,
           client_opts
         ) do
      {:ok, status, _headers, _body} when status in 200..299 ->
        :ok

      {:ok, status, _headers, _body} when status in @permanent ->
        {:error, {:permanent, {:status, status}}}

      {:ok, status, headers, _body} when status in [408, 429] or status in 500..599 ->
        case retry_after_ms(headers) do
          nil -> {:error, {:status, status}}
          ms -> {:error, {:retry_after, ms, {:status, status}}}
        end

      {:ok, status, _headers, _body} ->
        {:error, {:status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc false
  # `Retry-After` as milliseconds from now: delay-seconds, or an IMF-fixdate
  # (`Sun, 06 Nov 1994 08:49:37 GMT`) in the future. `nil` when absent,
  # unparseable, zero or in the past.
  @spec retry_after_ms(%{String.t() => [String.t()]}) :: pos_integer() | nil
  def retry_after_ms(headers) do
    case Map.get(headers, "retry-after") do
      [value | _] -> value |> String.trim() |> parse_retry_after()
      _ -> nil
    end
  end

  defp parse_retry_after(value) do
    case Integer.parse(value) do
      {seconds, ""} when seconds > 0 -> seconds * 1000
      {_seconds, ""} -> nil
      _ -> http_date_ms(value)
    end
  end

  @months ~w(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec)

  defp http_date_ms(value) do
    with [_, day, month, year, h, m, s] <-
           Regex.run(~r/\A\w{3}, (\d{2}) (\w{3}) (\d{4}) (\d{2}):(\d{2}):(\d{2}) GMT\z/, value),
         index when is_integer(index) <- Enum.find_index(@months, &(&1 == month)),
         {:ok, date} <- Date.new(String.to_integer(year), index + 1, String.to_integer(day)),
         {:ok, time} <- Time.new(String.to_integer(h), String.to_integer(m), String.to_integer(s)),
         {:ok, at} <- DateTime.new(date, time, "Etc/UTC") do
      ms = DateTime.diff(at, DateTime.utc_now(), :millisecond)
      if ms > 0, do: ms, else: nil
    else
      _ -> nil
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
