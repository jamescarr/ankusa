defmodule Ankusa.Sink.Http do
  @moduledoc """
  Forward the raw hook body to an HTTP endpoint, via
  [`Req`](https://hex.pm/packages/req).

  `opts`:

    * `:url`        — required target URL, or one computed per hook (see
                       "Computed destinations")
    * `:method`     — HTTP method (default `:post`)
    * `:headers`    — extra request headers as `[{key, value}]` strings, or
                       computed per hook
    * `:timeout_ms` — request/connect timeout (default `5000`)
    * `:secret`     — **required unless `allow_unsigned: true`.** Signs every
                       delivery ([Standard
                       Webhooks](https://www.standardwebhooks.com/)): a
                       `whsec_…` secret, or a list of them while rotating (one
                       `v1,` signature each); computed per hook like `:url`.
                       See `Ankusa.Sink.Http.Signer`
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
    * `:pool`       — this sink's connection pool, over the instance's
                       `egress.pools`: `:pool_size`, `:pool_count`,
                       `:pool_timeout` (ms to wait for a free connection) and
                       `:protocols`. See "Connection pools"
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
  | `400`, `401`, `403`, `404`, `410`, `413`, `422`, `451` | `{:error, {:permanent, {:status, s}}}` | dead-letter now |
  | `408`, `429`, `5xx` with a `Retry-After` | `{:error, {:retry_after, ms, {:status, s}}}` | retry no sooner than that |
  | `425` | `{:error, {:retry_after, ms, {:status, 425}}}`, `ms` the `Retry-After` or else a full-jitter pause | retry no sooner than that |
  | `409` | `{:error, {:status, 409}}` | the retry policy: the receiver's state usually moves; bounded by `retry.max_elapsed_ms` |
  | anything else | `{:error, {:status, s}}` | the retry policy |

  `451 Unavailable For Legal Reasons` is permanent with the other refusals in
  the first row: the receiver will not take the hook from this sender, and no
  amount of retrying changes that.

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

  ## Computed destinations

  `:url`, `:headers` and `:secret` may each be a `{mod, fun, args}` (called as
  `mod.fun(env, args...)`) or a 1-arity function of the `Ankusa.Envelope`,
  resolved on **every attempt**, so one shared source delivers each tenant's
  hooks to that tenant's receiver:

      {Ankusa.Sink.Http,
       url: fn env -> Tenants.endpoint(env.tenant_id) end,
       secret: {Tenants, :signing_secret, []},
       headers: fn env -> [{"x-api-key", Tenants.api_key(env.tenant_id)}] end}

  A function returns what the static option holds (`:url` a string, `:headers`
  a list of `{name, value}`, `:secret` what `:secret` accepts, `nil` for none),
  or `{:error, reason}` to fail the attempt (`{:error, {:permanent, reason}}` to
  dead-letter it, for a tenant with no endpoint); a raise is a transient
  failure. A value of the wrong shape is `{:permanent, {:computed, key,
  :invalid}}`. The function runs on the delivery task: it must not block on
  slow I/O. These are Elixir options; the YAML loader and the admin API only
  build static sinks.

  The resolved URL is a target like any other: the egress policy above is
  applied to it on every attempt, so a tenant-supplied URL cannot reach the
  private network, and the signing rule is applied to the resolved secret (an
  empty one is `{:permanent, :unsigned_sink}` unless `allow_unsigned: true`).

  ## Connection pools

  Every delivery goes through the node's owned pools
  (`Ankusa.HttpClient.Pools`), never Req's default of one pool per origin kept
  for the life of the VM: one pool per receiver address and TLS profile, sized
  by the instance's `egress.pools` (`pool_size` connections, 32 by default, so
  several sinks posting to one receiver are not capped below
  `dispatch.concurrency`) or this sink's `:pool`, closed after `pool_max_idle_ms`
  idle and when the source is deleted, at most `max_pools` live per instance and
  `max_pools_per_tenant` per tenant. A request that cannot check a connection out
  within `pool_timeout` fails that attempt as a transport error; one that would
  open a pool past a ceiling with nothing idle to close is `{:error, {:unavailable,
  :pool_limit}}`; both are retried.

  The pool is keyed by the address the egress check validated: its connections go
  to that address and verify the peer against the original name, so a name that
  later resolves somewhere else is checked again on its next attempt and never
  reaches a pool opened for the old answer. `:cacertfile` and
  `:allow_insecure_tls` are part of the pool's connection options, so a computed
  `:url` may use them too.
  """

  @behaviour Ankusa.Sink

  alias Ankusa.{Envelope, HttpClient}
  alias Ankusa.HttpClient.{Egress, Pools}
  alias Ankusa.Sink.Description
  alias Ankusa.Sink.Http.Signer
  alias Ankusa.Sink.Message

  @permanent [400, 401, 403, 404, 410, 413, 422, 451]
  @default_max_response_bytes 65_536
  @computed_keys [:url, :headers, :secret]

  @impl true
  def deliver(env, ctx, opts) do
    with {:ok, resolved} <- resolve(opts, env),
         {:ok, pinned} <- egress(ctx, resolved),
         {:ok, signature} <- signature(env, resolved) do
      send_hook(env, ctx, resolved, signature, pinned)
    end
  end

  @impl true
  def describe(_subject, opts) do
    {protocol, host} = destination(Keyword.get(opts, :url))

    %Description{
      protocol: protocol,
      host: host,
      payload: :body,
      computed: for(key <- @computed_keys, computed?(Keyword.get(opts, key)), do: key)
    }
  end

  defp destination(url) when is_binary(url) do
    case URI.parse(url) do
      %URI{scheme: scheme, host: host, port: port} when is_binary(scheme) and is_binary(host) ->
        scheme = String.downcase(scheme)
        {scheme, if(port in [nil, URI.default_port(scheme)], do: host, else: "#{host}:#{port}")}

      %URI{} ->
        {"http", nil}
    end
  end

  defp destination(_computed_or_missing), do: {"http", nil}

  defp computed?(spec), do: is_function(spec, 1) or mfa?(spec)
  defp mfa?({mod, fun, args}), do: is_atom(mod) and is_atom(fun) and is_list(args)
  defp mfa?(_other), do: false

  # `:url`, `:headers` and `:secret` as this envelope's attempt sees them. A
  # static option is left alone; a computed one is called and checked.
  defp resolve(opts, env) do
    Enum.reduce_while(@computed_keys, {:ok, opts}, fn key, {:ok, acc} ->
      with spec when spec != nil <- Keyword.get(opts, key),
           true <- computed?(spec),
           {:ok, value} <- compute(spec, env, key) do
        {:cont, {:ok, Keyword.put(acc, key, value)}}
      else
        {:error, _reason} = error -> {:halt, error}
        _static_or_unset -> {:cont, {:ok, acc}}
      end
    end)
  end

  defp compute(spec, env, key) do
    value = if is_function(spec, 1), do: spec.(env), else: apply_mfa(spec, env)

    case value do
      {:error, _reason} = error -> error
      value -> checked(key, value)
    end
  end

  defp apply_mfa({mod, fun, args}, env), do: apply(mod, fun, [env | args])

  # The value is never echoed: a URL, a header or a secret may be a credential.
  defp checked(:url, url) when is_binary(url) and url != "", do: {:ok, url}
  defp checked(:secret, secret) when is_nil(secret) or is_binary(secret), do: {:ok, secret}

  defp checked(:secret, secrets) when is_list(secrets) do
    if Enum.all?(secrets, &is_binary/1), do: {:ok, secrets}, else: invalid(:secret)
  end

  defp checked(:headers, headers) when is_list(headers) do
    if Enum.all?(headers, &header?/1), do: {:ok, headers}, else: invalid(:headers)
  end

  defp checked(key, _value), do: invalid(key)

  defp header?({name, value}),
    do:
      (is_binary(name) or is_atom(name)) and
        (is_binary(value) or is_atom(value) or is_integer(value))

  defp header?(_other), do: false

  defp invalid(key), do: {:error, {:permanent, {:computed, key, :invalid}}}

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
    validate_computed!(opts)
    validate_signing!(opts)
    Pools.validate_override!(Keyword.get(opts, :pool, []))

    insecure? = Keyword.get(opts, :allow_insecure_tls, false)

    unless is_boolean(insecure?) do
      raise ArgumentError,
            "Sink.Http allow_insecure_tls must be true or false, got: #{inspect(insecure?)}"
    end

    HttpClient.validate!(
      Keyword.get(opts, :req_options, []),
      insecure?,
      Keyword.get(opts, :cacertfile)
    )

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

  defp validate_computed!(opts) do
    for key <- @computed_keys,
        {:ok, spec} <- [Keyword.fetch(opts, key)],
        do: validate_spec!(key, spec)

    :ok
  end

  defp validate_spec!(key, spec) when is_function(spec) and not is_function(spec, 1) do
    raise ArgumentError,
          "Sink.Http #{inspect(key)} function must take the envelope (arity 1), got arity " <>
            "#{:erlang.fun_info(spec, :arity) |> elem(1)}"
  end

  defp validate_spec!(key, {mod, fun, args} = spec) do
    if mfa?(spec) do
      Code.ensure_loaded(mod)

      unless function_exported?(mod, fun, length(args) + 1) do
        raise ArgumentError,
              "Sink.Http #{inspect(key)} #{inspect(mod)}.#{fun}/#{length(args) + 1} is not " <>
                "defined: a {mod, fun, args} is called as mod.fun(envelope, args...)"
      end
    end

    :ok
  end

  defp validate_spec!(_key, _static), do: :ok

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
      [timeout: Keyword.get(opts, :timeout_ms, 5000)] ++
        resolver_option(ctx)

    case Egress.pin(Keyword.fetch!(opts, :url), policy(ctx), pin_opts) do
      {:ok, pinned} -> {:ok, pinned}
      {:error, {:egress_denied, {:resolve, _host, _reason}}} = lookup_failed -> lookup_failed
      {:error, {:egress_denied, _reason} = denied} -> {:error, {:permanent, denied}}
    end
  end

  # Whose pool a delivery is: the instance's `egress.pools` apply, the tenant's
  # ceiling counts it and the source's deletion closes it. `:pool` is the sink's
  # own override of the instance's sizes.
  defp pool_context(env, ctx, opts) do
    [
      instance: ctx[:instance],
      tenant: env.tenant_id,
      source: {env.tenant_id, env.source_id},
      pool: Keyword.get(opts, :pool, [])
    ]
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
      Message.forwarded_headers(env, ctx)
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
      |> Keyword.merge(pool_context(env, ctx, opts))

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

      # 425 Too Early: the receiver refused to process the request yet (replay
      # protection while a handshake completes); it will take it shortly, so it is
      # a pause (its `Retry-After`, else a jittered one), not a failure to back off.
      {:ok, 425, headers, _body} ->
        ms = retry_after_ms(headers) || Ankusa.Sink.throttle_after_ms(Map.get(ctx, :attempt, 1))
        {:error, {:retry_after, ms, {:status, 425}}}

      # 409 Conflict is transient on purpose: the receiver's state (a lock, a
      # not-yet-created parent resource) usually moves, and a conflict that does
      # not is bounded by the retry horizon like any other failure.
      {:ok, 409, _headers, _body} ->
        {:error, {:status, 409}}

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
