defmodule Ankusa.Gateway.Authn do
  @moduledoc """
  The gateway's authentication plug: runs the chain of
  `gateway.auth.authenticators` (see `Ankusa.Gateway.Authenticator`) for every
  route but `GET /health` and `GET /ready`, and assigns

    * `principal` — the `Ankusa.Principal` the first accepting authenticator
      returned;
    * `principal_via` — the `type` of that authenticator (`:token`, `:jwt`,
      `:mtls`, ...; a custom authenticator's module), or `:anonymous` for the
      empty chain;
    * `subject_key` — 16 bytes fingerprinting the whole principal (how it was
      authenticated, its id, scopes, tenants, groups and claims), computed
      once per authentication: what decision caches key on, so two principals
      that differ in anything an authorizer may read never share a decision.

  The chain is tried in order. An authenticator with no credential of its form
  in the request (`credential/2` is `:none`) is passed over without being
  asked; one that answers `:skip` hands the request to the next. All skip is
  `401 {"error":"unauthenticated"}` with `www-authenticate: Bearer
  realm="ankusa"`, as is an authenticator's refusal; one that could not decide
  is `503 {"error":"auth_unavailable"}` with a `retry-after` of 1 to 3 seconds.

  An empty chain hands every request `Ankusa.Principal.operator/0` (every
  scope, every tenant); `Ankusa.Gateway.Config` only accepts it on loopback or
  with `auth.allow_insecure: true`.

  ## The authentication cache

  Each authenticator's result is cached (`Ankusa.Gateway.Cache`, `:authn`)
  under `{its index, sha256(credential)}`: a hit ends the chain at the first
  authenticator that reads that credential (no issuer peek, no signature
  check), restoring the principal, `principal_via` and `subject_key` it had.
  An entry lives `auth.cache.ttl_ms`, never past the credential's own expiry
  (`0` turns the cache off); a refusal is cached for `auth.cache.negative_ttl_ms`
  only by an authenticator whose `cache_negative?/0` says so (one that calls
  out). A credential over 16 KiB is not one: that authenticator is passed over.
  Only the hash of a credential is kept.

  Emits `[:ankusa, :gateway, :authn]`: `%{duration}`, `%{instance,
  authenticator: name | "none", result: :ok | :unauthenticated | :unavailable,
  cached: boolean}`.
  """

  @behaviour Plug

  import Plug.Conn

  alias Ankusa.Gateway.{Authenticator, Cache}
  alias Ankusa.{Http, Principal, Telemetry}

  @exempt [["health"], ["ready"]]
  @challenge ~s(Bearer realm="ankusa")
  @max_credential_bytes 16_384

  @impl true
  def init(opts), do: opts

  @impl true
  def call(%Plug.Conn{path_info: path} = conn, _opts) when path in @exempt, do: conn

  def call(conn, _opts) do
    instance = Keyword.get(conn.assigns[:ankusa_opts] || [], :instance, :default)
    gateway = Ankusa.Gateway.config(instance)
    started = System.monotonic_time()
    {result, name, cached} = authenticate(conn, instance, gateway)

    Telemetry.emit(
      [:gateway, :authn],
      %{duration: System.monotonic_time() - started},
      %{instance: instance, authenticator: name, result: result_tag(result), cached: cached}
    )

    case result do
      {:ok, principal, via, subject_key} ->
        conn
        |> assign(:principal, principal)
        |> assign(:principal_via, via)
        |> assign(:subject_key, subject_key)

      {:error, :unauthenticated} ->
        conn
        |> put_resp_header("www-authenticate", @challenge)
        |> Http.send_json(401, %{error: "unauthenticated"})
        |> halt()

      {:error, :link_expired} ->
        conn |> Http.send_json(410, %{error: "link_expired"}) |> halt()

      {:error, {:unavailable, _reason}} ->
        conn
        |> put_resp_header("retry-after", Integer.to_string(Enum.random(1..3)))
        |> Http.send_json(503, %{error: "auth_unavailable"})
        |> halt()
    end
  end

  @doc """
  The 16-byte fingerprint of a principal authenticated by `via`: the first 16
  bytes of the sha256 of `{via, id, scopes, tenants, groups, claims}` in the
  external term format, deterministic.
  """
  @spec subject_key(term(), Principal.t()) :: binary()
  def subject_key(via, %Principal{} = p) do
    term = {via, p.id, p.scopes, p.tenants, p.groups, p.claims}
    binary_part(:crypto.hash(:sha256, :erlang.term_to_binary(term, [:deterministic])), 0, 16)
  end

  defp result_tag({:ok, _principal, _via, _key}), do: :ok
  defp result_tag({:error, {:unavailable, _}}), do: :unavailable
  defp result_tag({:error, _}), do: :unauthenticated

  # `{result, authenticator name, cached?}`.
  defp authenticate(_conn, _instance, %{auth: %{authenticators: []}}) do
    principal = Principal.operator()
    {{:ok, principal, :anonymous, subject_key(:anonymous, principal)}, "none", false}
  end

  defp authenticate(conn, instance, %{auth: %{authenticators: authenticators, cache: cache}}) do
    authenticators
    |> Enum.with_index()
    |> Enum.reduce_while({{:error, :unauthenticated}, "none", false}, fn {authenticator, index},
                                                                         acc ->
      ctx = ctx(instance, index, authenticator)

      case authenticator.module.credential(conn, ctx) do
        {:ok, credential} when byte_size(credential) <= @max_credential_bytes ->
          cached_step(authenticator, conn, ctx, credential, cache, acc)

        _none_or_too_long ->
          {:cont, acc}
      end
    end)
  end

  defp cached_step(authenticator, conn, ctx, credential, cache, acc) do
    key = {ctx.index, :crypto.hash(:sha256, credential)}
    now = System.os_time(:second)

    case Cache.fetch(ctx.instance, :authn, key) do
      {:ok, {:ok, principal, via, subject_key, expires_at}}
      when expires_at == nil or expires_at > now ->
        {:halt, {{:ok, principal, via, subject_key}, ctx.name, true}}

      {:ok, :negative} ->
        {:halt, {{:error, :unauthenticated}, ctx.name, true}}

      _miss ->
        step(authenticator, conn, ctx, key, cache, acc)
    end
  end

  defp step(authenticator, conn, ctx, key, cache, acc) do
    case authenticator.module.authenticate(conn, ctx) do
      {:ok, %Principal{} = principal, expires_at} ->
        via = authenticator.type
        subject_key = subject_key(via, principal)
        value = {:ok, principal, via, subject_key, expires_at}
        remember(ctx.instance, key, value, expires_at, cache)
        {:halt, {{:ok, principal, via, subject_key}, ctx.name, false}}

      :skip ->
        {:cont, acc}

      {:error, :unauthenticated} = error ->
        if negative?(authenticator.module),
          do: Cache.put(ctx.instance, :authn, key, :negative, cache.negative_ttl_ms)

        {:halt, {error, ctx.name, false}}

      {:error, _} = error ->
        {:halt, {error, ctx.name, false}}
    end
  end

  # Never past the credential's own expiry, and not at all with `ttl_ms: 0`.
  defp remember(_instance, _key, _value, _expires_at, %{ttl_ms: 0}), do: :ok

  defp remember(instance, key, value, nil, %{ttl_ms: ttl}),
    do: Cache.put(instance, :authn, key, value, ttl)

  defp remember(instance, key, value, expires_at, %{ttl_ms: ttl}) do
    left_ms = (expires_at - System.os_time(:second)) * 1000
    Cache.put(instance, :authn, key, value, min(ttl, left_ms))
  end

  defp negative?(module),
    do: function_exported?(module, :cache_negative?, 0) and module.cache_negative?()

  @spec ctx(atom(), non_neg_integer(), map()) :: Authenticator.ctx()
  defp ctx(instance, index, %{name: name, config: config}),
    do: %{instance: instance, index: index, name: name, config: config}
end
