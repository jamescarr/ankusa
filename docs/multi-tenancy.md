# Multi-tenancy and catch-URL routing

This is the seam that makes the framework fit three different products with
no code change: a single-tenant standalone gateway, a multi-tenant SaaS
where every customer gets their own catch URL, and a product minting opaque
per-integration URLs, in whatever shape it wants, at runtime.

## The problem this solves

A webhook framework's URL scheme is not one thing. `POST /webhooks/:source_id`
is fine for a single operator with a handful of known providers. It's wrong
for a SaaS handing each of thousands of customers their own endpoint, and
wrong again for a product minting unguessable tokens on demand. Hardcoding
any one scheme into the router would force every other use case to fork the
router.

## `Ankusa.Route` and `Ankusa.RouteResolver`

`Ankusa.Route` is the resolved identity of a request:

```elixir
%Ankusa.Route{source_id: "stripe", tenant_id: "acme", params: %{}}
```

`Ankusa.RouteResolver` is the behaviour that produces one from a raw
`Plug.Conn`:

```elixir
@callback resolve(instance :: atom(), conn :: Plug.Conn.t(), opts :: keyword()) ::
            {:ok, Ankusa.Route.t()} | :error
```

The router (`Ankusa.Edge.Router`) is a catch-all `POST` that does nothing but
call the configured resolver, then hand the result to `Ankusa.Edge.Ingest`. A
resolver does **URL-scheme work only** — it never reads the body, verifies a
signature, or touches storage. It answers "which endpoint is this?" and
nothing else; policy (verify/dedup/sinks) still comes from `Ankusa.SourceStore`
keyed by the returned `source_id`.

## Shipped resolvers

**`Ankusa.RouteResolver.Path`** (default) — `POST /webhooks/:source_id`.
`tenant_id` is left `nil`, so `Ankusa.Edge.Ingest` falls back to the resolved
source's own `tenant_id` (default `"default"`). This is the single-tenant
case: one operator, a handful of sources, tenancy doesn't vary by URL.

```elixir
config :ankusa, route_resolver: {Ankusa.RouteResolver.Path, prefix: ["webhooks"]}  # prefix is the default
```

**`Ankusa.RouteResolver.TenantPath`** — `POST /webhooks/:tenant_id/:source_id`.
The tenant is carried in the URL and is **authoritative** — it wins over
whatever the resolved source's own `tenant_id` says. One instance serves
many tenants over one path scheme.

```elixir
config :ankusa, route_resolver: {Ankusa.RouteResolver.TenantPath, prefix: ["webhooks"]}
```

```
POST /webhooks/acme/stripe    →  %Route{tenant_id: "acme", source_id: "stripe"}
POST /webhooks/globex/stripe  →  %Route{tenant_id: "globex", source_id: "stripe"}
```

## Writing your own scheme

This is the extension point for a real product's catch-URL story — an
opaque-token scheme (`POST /webhooks/catch/:app_id/:token`), a
host-routed scheme (`https://<tenant>.hooks.example.com/:source`), or
anything else. Implement the one callback:

```elixir
defmodule MyApp.RouteResolver.OpaqueToken do
  @behaviour Ankusa.RouteResolver
  alias Ankusa.Route

  @impl true
  def resolve(_instance, conn, _opts) do
    case conn.path_info do
      ["webhooks", "catch", app_id, token] ->
        # look up `token` in your own endpoint table/cache here —
        # this is exactly where a control-plane-backed SourceStore.Ecto
        # would live too
        {:ok, %Route{source_id: token, params: %{app_id: app_id}}}

      _ ->
        :error
    end
  end
end
```

An unresolvable URL shape returns `:error`, which the router turns into a
`404` — the same response an unknown `source_id` gets, so a probe can't tell
"malformed URL" from "URL shape is fine but nothing's registered there."

## Tenant scoping — what `tenant_id` actually does

`tenant_id` on a `%Ankusa.Source{}` (default `"default"`) is the
**dedup/storage/retention scope**. Concretely:

- **Dedup**: the WAL's uniqueness constraint is `(tenant_id, source_id,
  dedup_key)`, not `(source_id, dedup_key)`. Two tenants can both send an
  event with `dedup_key: "evt_1"` and both commit — they never collide.
  `WAL.DiskLog`'s dedup key is the tuple `{tenant_id, source_id, dedup_key}`
  directly; `WAL.Postgres`'s dedup ledger has `tenant_id` as a real column
  and part of its primary key.
- **Storage**: the segment index row (`Ankusa.Storage.Index`) carries
  `tenant_id`, so per-tenant retention/deletion is a real, queryable
  dimension, not something bolted on after the fact.
- **Delivery**: `tenant_id` is in every `Ankusa.Sink`'s `ctx` map
  (`ctx.tenant_id`), so a sink can route, tag, or partition by it — e.g.
  `Ankusa.Sink.RabbitMQ`'s default routing key doesn't include it, but a
  custom `:routing_key` function easily can (`"ankusa.#{env.tenant_id}.#{env.source_id}"`).

Resolution order for a given request: `route.tenant_id` (if the resolver set
one) wins; otherwise `source.tenant_id` (if the source's config set one);
otherwise `"default"`. This means `TenantPath` and per-source `tenant_id`
can coexist — a resolver-provided tenant always overrides a source's
declared one, never the reverse.

## What isn't built yet

Everything above works today against `SourceStore.Static` (sources declared
in `config.exs`), which means the *set* of valid `source_id`s is still
fixed at boot. A real multi-tenant SaaS or a product minting opaque catch
URLs at runtime needs a
**dynamic** endpoint store — mint a catch URL via an API call, have it work
immediately, no redeploy — which means a DB-backed `SourceStore` (e.g.
`SourceStore.Ecto`, read-through cached, invalidated on write) plus a small
control-plane API to create/revoke endpoints. That's a real gap, not a
subtlety: `RouteResolver`/`Route`/tenant-scoped storage are the seams that
make it *possible*; the dynamic store itself isn't shipped yet (see the
root README's "Not yet implemented" list).
