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
  with `auth.allow_insecure: true`. With `gateway.sharing.enabled` the chain
  ends with the share-link authenticator (`Ankusa.Gateway.Authn.Link`); a
  chain of that one alone is still empty to a request without a link.

  ## Enrichment

  After an authenticator accepts a credential and before the principal is
  fingerprinted, `gateway.auth.enrichers` runs in order: each may add groups
  and claims (`Ankusa.Gateway.Enricher`), so an authorizer decides on the
  groups the identity provider's API knows, not only those in the token. What
  is fingerprinted and cached is the enriched principal. An enricher that
  cannot answer is `503 auth_unavailable`, or — with `on_unavailable: :skip` —
  the principal is used unenriched and cached for `auth.cache.negative_ttl_ms`
  instead of `auth.cache.ttl_ms`. The anonymous principal (an empty chain) is
  not enriched.

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

  alias Ankusa.Gateway.{Authenticator, Cache, Caller, Enricher}
  alias Ankusa.{Http, Principal, Telemetry}

  @exempt [["health"], ["ready"]]
  @challenge ~s(Bearer realm="ankusa")
  @max_credential_bytes 16_384
  @max_groups 1_000

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
  defp authenticate(_conn, _instance, %{auth: %{authenticators: []}}), do: anonymous()

  # A chain of only the share-link authenticator (sharing on, no chain of
  # your own) is an empty chain to whoever brings no link.
  defp authenticate(conn, instance, %{auth: %{authenticators: authenticators}} = gateway) do
    case chain(conn, instance, gateway) do
      {{:error, :unauthenticated}, "none", false} = refused ->
        if Enum.all?(authenticators, &(&1.type == :link)), do: anonymous(), else: refused

      result ->
        result
    end
  end

  defp anonymous do
    principal = Principal.operator()
    {{:ok, principal, :anonymous, subject_key(:anonymous, principal)}, "none", false}
  end

  defp chain(conn, instance, %{auth: %{authenticators: authenticators} = auth}) do
    authenticators
    |> Enum.with_index()
    |> Enum.reduce_while({{:error, :unauthenticated}, "none", false}, fn {authenticator, index},
                                                                         acc ->
      ctx = ctx(instance, index, authenticator)

      case authenticator.module.credential(conn, ctx) do
        {:ok, credential} when byte_size(credential) <= @max_credential_bytes ->
          cached_step(authenticator, conn, ctx, credential, auth, acc)

        _none_or_too_long ->
          {:cont, acc}
      end
    end)
  end

  defp cached_step(authenticator, conn, ctx, credential, auth, acc) do
    key = {ctx.index, :crypto.hash(:sha256, credential)}
    now = System.os_time(:second)

    case Cache.fetch(ctx.instance, :authn, key) do
      {:ok, {:ok, principal, via, subject_key, expires_at}}
      when expires_at == nil or expires_at > now ->
        {:halt, {{:ok, principal, via, subject_key}, ctx.name, true}}

      {:ok, :negative} ->
        {:halt, {{:error, :unauthenticated}, ctx.name, true}}

      _miss ->
        step(authenticator, conn, ctx, key, auth, acc)
    end
  end

  defp step(authenticator, conn, ctx, key, auth, acc) do
    case authenticator.module.authenticate(conn, ctx) do
      {:ok, %Principal{} = principal, expires_at} ->
        # A share link is a grant, not a person: nothing to enrich.
        enriched =
          if authenticator.type == :link, do: {:ok, principal}, else: enrich(ctx, auth, principal)

        case enriched do
          {:ok, enriched} ->
            authenticated(authenticator, ctx, key, auth, enriched, expires_at, false)

          {:degraded, enriched} ->
            authenticated(authenticator, ctx, key, auth, enriched, expires_at, true)

          {:error, reason} ->
            {:halt, {{:error, {:unavailable, reason}}, ctx.name, false}}
        end

      :skip ->
        {:cont, acc}

      {:error, :unauthenticated} = error ->
        if negative?(authenticator.module),
          do: Cache.put(ctx.instance, :authn, key, :negative, auth.cache.negative_ttl_ms)

        {:halt, {error, ctx.name, false}}

      {:error, _} = error ->
        {:halt, {error, ctx.name, false}}
    end
  end

  defp authenticated(authenticator, ctx, key, auth, principal, expires_at, degraded?) do
    via = authenticator.type
    subject_key = subject_key(via, principal)
    value = {:ok, principal, via, subject_key, expires_at}

    if degraded?,
      do: remember_degraded(ctx.instance, key, value, expires_at, auth.cache),
      else: remember(ctx.instance, key, value, expires_at, auth.cache)

    {:halt, {{:ok, principal, via, subject_key}, ctx.name, false}}
  end

  # ── enrichers ───────────────────────────────────────────────────────────────

  # `{:ok, principal}` enriched, `{:degraded, principal}` when one answered
  # `on_unavailable: :skip`, or `{:error, reason}` (`503 auth_unavailable`).
  defp enrich(_ctx, %{enrichers: []}, principal), do: {:ok, principal}

  defp enrich(ctx, %{enrichers: enrichers}, principal) do
    enrichers
    |> Enum.with_index()
    |> Enum.reduce_while({principal, false}, fn {enricher, index}, {current, degraded?} ->
      if applies?(enricher, ctx.name) do
        case enrich_one(ctx, index, enricher, current) do
          {:ok, enriched} -> {:cont, {enriched, degraded?}}
          :skipped -> {:cont, {current, true}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      else
        {:cont, {current, degraded?}}
      end
    end)
    |> case do
      {:error, _reason} = error -> error
      {principal, true} -> {:degraded, principal}
      {principal, false} -> {:ok, principal}
    end
  end

  defp applies?(%{config: config}, authenticator),
    do: config.only == [] or authenticator in config.only

  defp enrich_one(ctx, index, %{config: config}, principal) do
    key = Enricher.key(index, ctx.name, principal.id)

    case Cache.fetch(ctx.instance, :authn, key) do
      {:ok, %{groups: groups, claims: claims}} ->
        {:ok, merge(principal, groups, claims)}

      _miss ->
        deadline = System.monotonic_time(:millisecond) + config.timeout_ms
        item = {principal, ctx.name}

        case Caller.call(ctx.instance, {:enrich, index}, key, :authn, item, deadline) do
          {:ok, %{groups: groups, claims: claims}} -> {:ok, merge(principal, groups, claims)}
          {:error, reason} -> unavailable(config, reason)
        end
    end
  end

  defp unavailable(%{on_unavailable: :skip}, _reason), do: :skipped
  defp unavailable(_config, reason), do: {:error, reason}

  # What an enricher contributed: its groups joined to the token's, its claims
  # below the token's (an enricher adds, it does not rewrite).
  defp merge(principal, groups, claims) do
    %{
      principal
      | groups: (principal.groups ++ groups) |> Enum.uniq() |> Enum.take(@max_groups),
        claims: Map.merge(claims, principal.claims)
    }
  end

  # Never past the credential's own expiry, and not at all with `ttl_ms: 0`.
  defp remember(_instance, _key, _value, _expires_at, %{ttl_ms: 0}), do: :ok

  defp remember(instance, key, value, nil, %{ttl_ms: ttl}),
    do: Cache.put(instance, :authn, key, value, ttl)

  defp remember(instance, key, value, expires_at, %{ttl_ms: ttl}) do
    left_ms = (expires_at - System.os_time(:second)) * 1000
    Cache.put(instance, :authn, key, value, min(ttl, left_ms))
  end

  # An unenriched principal is only good until the source may answer again.
  defp remember_degraded(_instance, _key, _value, _expires_at, %{ttl_ms: 0}), do: :ok

  defp remember_degraded(instance, key, value, expires_at, cache) do
    remember(instance, key, value, expires_at, %{cache | ttl_ms: cache.negative_ttl_ms})
  end

  defp negative?(module),
    do: function_exported?(module, :cache_negative?, 0) and module.cache_negative?()

  @spec ctx(atom(), non_neg_integer(), map()) :: Authenticator.ctx()
  defp ctx(instance, index, %{name: name, config: config}),
    do: %{instance: instance, index: index, name: name, config: config}
end
