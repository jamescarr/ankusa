defmodule Ankusa.Sink.Http do
  @moduledoc """
  Forward the raw hook body to an HTTP endpoint, via
  [`Req`](https://hex.pm/packages/req).

  `opts`:

    * `:url`        — required target URL
    * `:method`     — HTTP method (default `:post`)
    * `:headers`    — extra request headers as `[{key, value}]` strings
    * `:timeout_ms` — request/connect timeout (default `5000`)
    * `:secret`     — **required unless `allow_unsigned: true`.** Signs every
                       delivery ([Standard
                       Webhooks](https://www.standardwebhooks.com/)): a
                       `whsec_…` secret, or a list of them while rotating (one
                       `v1,` signature each). See `Ankusa.Sink.Http.Signer`
    * `:allow_unsigned` — `true` sends deliveries with no signature (default
                       `false`). Without it a sink with no `:secret` is refused
                       by `validate_opts!/1` at load, and a delivery attempt
                       for one that skipped it is dead-lettered: any host that
                       can reach the receiver's URL could otherwise inject
                       hooks. A `:secret` still signs when this is also set
    * `:max_response_bytes` — read at most this much of the response body
                       (default `65_536`); a longer one is cut off and the
                       connection closed. Only the status is used, so a large
                       or endless response cannot pin the attempt or its memory
    * `:req_options` — transport options for the HTTP client (custom Finch pool,
                       proxy, or `plug:` for `Req.Test` in tests). See
                       `Ankusa.HttpClient` for the accepted keys
    * `:cacertfile` — a PEM CA bundle to verify the receiver's certificate
                       against (a private CA); default: the system's trust store
    * `:allow_insecure_tls` — `true` skips certificate verification for this
                       sink (default `false`; a `verify_none` in `:req_options`
                       is refused without it). Such a sink's connection is
                       also pinned to the address the egress check resolved

  The original `env.body` is sent verbatim with the envelope's content-type
  (falling back to `application/octet-stream`). Identity headers `x-ankusa-id`,
  `x-ankusa-source`, `x-ankusa-idempotency-key` (the key a consumer dedupes on,
  see `Ankusa.Envelope.idempotency_key/1`), `x-ankusa-verification` (`ok`,
  `failed` or `skipped`: how the hook was verified at ingest) and (when set)
  `x-ankusa-tenant` are always added, plus `x-ankusa-verification-flagged: true`
  when a failed check was let through by `on_verify_failure: accept_flag` (treat
  that hook as unverified), `x-ankusa-dedupe-key` and `x-ankusa-replay-id` when
  the hook carries them, and `webhook-id`/`webhook-timestamp`/`webhook-signature`
  with a `:secret` (always, unless `allow_unsigned: true`).
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

  ## Egress policy

  Every attempt is checked against `Ankusa.Config`'s `egress` policy before a
  byte is sent (`Ankusa.HttpClient.Egress`): `https` only unless the policy
  allows `http`, and no address on the private network, loopback or link-local
  range (the cloud metadata service included) unless the policy allows it. A
  denied target is `{:error, {:permanent, {:egress_denied, reason}}}`: dead-letter
  now, like a `401`; fix the policy or the URL, then replay. A name that does not
  resolve is `{:error, {:egress_denied, {:resolve, host, reason}}}` and is retried.
  The policy comes from the instance in `ctx.instance`; a ctx with none gets the
  default, which denies the private ranges.
  """

  @behaviour Ankusa.Sink

  alias Ankusa.{Envelope, HttpClient}
  alias Ankusa.HttpClient.Egress
  alias Ankusa.Sink.Http.Signer
  alias Ankusa.Sink.Message

  @permanent [400, 401, 403, 404, 410, 413, 422]
  @default_max_response_bytes 65_536

  @impl true
  def deliver(env, ctx, opts) do
    with {:ok, pinned} <- egress(ctx, opts),
         {:ok, signature} <- signature(env, opts) do
      send_hook(env, ctx, opts, signature, pinned)
    end
  end

  @doc """
  Raise `ArgumentError` unless `opts` is a usable HTTP sink config: it has a
  `:secret` or `allow_unsigned: true`, the `:req_options` are
  transport-only and keep TLS verification on, `:allow_insecure_tls` is a
  boolean and `:cacertfile` names a file. Boot
  (`Ankusa.HttpClient.Egress.validate_config!/1`) and the image's config loader
  call it, so a bad sink fails at load instead of at its first delivery; the
  admin API turns the same failure into `400 invalid_source`.
  """
  @spec validate_opts!(keyword()) :: :ok
  def validate_opts!(opts) do
    validate_signing!(opts)

    insecure? = Keyword.get(opts, :allow_insecure_tls, false)

    unless is_boolean(insecure?) do
      raise ArgumentError,
            "Sink.Http allow_insecure_tls must be true or false, got: #{inspect(insecure?)}"
    end

    HttpClient.validate!(Keyword.get(opts, :req_options, []), insecure?)

    case Keyword.get(opts, :cacertfile) do
      nil ->
        :ok

      path when is_binary(path) ->
        unless File.regular?(path) do
          raise ArgumentError, "Sink.Http cacertfile #{inspect(path)} is not a readable file"
        end

        :ok

      other ->
        raise ArgumentError, "Sink.Http cacertfile must be a path, got: #{inspect(other)}"
    end
  end

  defp validate_signing!(opts) do
    unsigned? = Keyword.get(opts, :allow_unsigned, false)

    unless is_boolean(unsigned?) do
      raise ArgumentError,
            "Sink.Http allow_unsigned must be true or false, got: #{inspect(unsigned?)}"
    end

    case {signing_secret(opts), unsigned?} do
      {nil, false} ->
        raise ArgumentError,
              "Sink.Http requires secret: deliveries are signed so the receiver can tell " <>
                "them from anything else that reaches its URL (set allow_unsigned: true " <>
                "to send them unsigned)"

      _signed_or_allowed ->
        :ok
    end
  end

  # `nil`, `""` and `[]` name no secret: a sink configured with one is unsigned
  # in all but name, so it is held to the same rule as one with no `:secret`.
  defp signing_secret(opts) do
    case Keyword.get(opts, :secret) do
      empty when empty in [nil, "", []] -> nil
      secret -> secret
    end
  end

  # The policy check, and the address to connect to. A denied target is permanent
  # (the same URL is denied until the policy changes); a failed lookup is not.
  defp egress(ctx, opts) do
    pin_opts =
      [timeout: Keyword.get(opts, :timeout_ms, 5000), pin_https: insecure_tls?(opts)] ++
        resolver_option(ctx)

    case Egress.pin(Keyword.fetch!(opts, :url), policy(ctx), pin_opts) do
      {:ok, pinned} -> {:ok, pinned}
      {:error, {:egress_denied, {:resolve, _host, _reason}}} = lookup_failed -> lookup_failed
      {:error, {:egress_denied, _reason} = denied} -> {:error, {:permanent, denied}}
    end
  end

  defp policy(ctx) do
    with instance when is_atom(instance) and instance != nil <- ctx[:instance],
         {:ok, config} <- Ankusa.fetch_config(instance) do
      config.egress
    else
      _no_config -> Egress.default_policy()
    end
  end

  # `ctx.egress_resolver` replaces the DNS lookup: tests only, dispatch never sets it.
  defp resolver_option(ctx) do
    case ctx[:egress_resolver] do
      nil -> []
      resolver -> [resolver: resolver]
    end
  end

  defp insecure_tls?(opts), do: Keyword.get(opts, :allow_insecure_tls, false)

  # A sink that skipped `validate_opts!/1` (a source built in code, or read from
  # a store written before this rule) is still refused here: it would send hooks
  # anyone can forge. Permanent: the same config fails until it is fixed.
  defp signature(env, opts) do
    case {signing_secret(opts), Keyword.get(opts, :allow_unsigned, false)} do
      {nil, true} ->
        {:ok, []}

      {nil, _unsigned} ->
        {:error, {:permanent, :unsigned_sink}}

      {secrets, _unsigned} ->
        case Signer.headers(env.id, env.body, secrets, System.system_time(:second)) do
          {:ok, headers} -> {:ok, headers}
          :error -> {:error, {:permanent, :bad_secret}}
        end
    end
  end

  defp send_hook(env, ctx, opts, signature, pinned) do
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
        verification_headers(env.verification) ++
        tenant_header(env.tenant_id) ++
        dedupe_header(env.dedupe_key) ++
        replay_header(ctx[:replay_id]) ++
        signature

    forwarded =
      Message.forwarded_headers(env, ctx[:forward_headers] || :default)
      |> Enum.reject(fn {name, _value} -> owned?(name, own) or owned?(name, opts_headers) end)

    headers = forwarded ++ own ++ pin_headers(pinned, opts_headers) ++ opts_headers

    client_opts =
      opts
      |> Keyword.get(:req_options, [])
      |> merge_connect_options(pinned.connect_options)
      |> Keyword.put(
        :max_response_bytes,
        Keyword.get(opts, :max_response_bytes, @default_max_response_bytes)
      )
      |> Keyword.put(:allow_insecure_tls, insecure_tls?(opts))
      |> put_cacertfile(Keyword.get(opts, :cacertfile))

    case HttpClient.request_with_headers(
           Keyword.get(opts, :method, :post),
           pinned.url,
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

  # The `host` header that names the receiver when the URL names its address,
  # unless the operator configured one of their own.
  defp pin_headers(%{headers: headers}, opts_headers),
    do: Enum.reject(headers, fn {name, _value} -> owned?(name, opts_headers) end)

  defp merge_connect_options(req_options, []), do: req_options

  defp merge_connect_options(req_options, pinned) do
    Keyword.update(req_options, :connect_options, pinned, fn existing ->
      Keyword.merge(existing, pinned, fn
        :transport_opts, mine, theirs -> Keyword.merge(mine, theirs)
        _key, _mine, theirs -> theirs
      end)
    end)
  end

  defp put_cacertfile(client_opts, nil), do: client_opts
  defp put_cacertfile(client_opts, path), do: Keyword.put(client_opts, :cacertfile, path)

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

  # Always says how the hook was verified (`Message.verification/1`); the flagged
  # header only when a failed check was let through.
  defp verification_headers(verification) do
    %{status: status, flagged: flagged} = Message.verification(verification)

    [{"x-ankusa-verification", Atom.to_string(status)}] ++
      if flagged, do: [{"x-ankusa-verification-flagged", "true"}], else: []
  end

  defp replay_header(replay_id) when is_binary(replay_id), do: [{"x-ankusa-replay-id", replay_id}]
  defp replay_header(_), do: []

  defp tenant_header(tenant_id) when is_binary(tenant_id), do: [{"x-ankusa-tenant", tenant_id}]
  defp tenant_header(_), do: []
end
