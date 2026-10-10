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

The router (`Ankusa.Edge.Router`) is a catch-all `POST`. In order it: runs the
route guard (when routes are enabled), calls the configured resolver, checks
`Content-Length` against `max_body_bytes` (`413`), looks the source up before
reading any body (`404`), checks header bytes (`400 invalid_header`), reads
the body within the bound, then hands the result to `Ankusa.Edge.Ingest`. A
resolver does **URL-scheme work only**: it never reads the body, verifies a
signature, or touches storage. It answers "which endpoint is this?" and
nothing else; policy (verify/sinks) still comes from `Ankusa.SourceStore`
keyed by the returned `source_id`.

## Shipped resolvers

**`Ankusa.RouteResolver.Path`** (default) resolves `POST /webhooks/:source_id`.
`tenant_id` is left `nil`, so `Ankusa.Edge.Ingest` falls back to the resolved
source's own `tenant_id` (default `"default"`). This is the single-tenant
case: one operator, a handful of sources, tenancy doesn't vary by URL.

```elixir
config :ankusa, route_resolver: {Ankusa.RouteResolver.Path, prefix: ["webhooks"]}  # prefix is the default
```

**`Ankusa.RouteResolver.TenantPath`** resolves `POST /webhooks/:tenant_id/:source_id`.
The tenant is carried in the URL and names the tenant a *shared* source (one
whose `tenant_id` is `"default"`) stores the hook under: one instance serves
many tenants over one path scheme. A source that belongs to a tenant answers
only that tenant's URL; see [binding](#tenant-scoping-what-tenant_id-actually-does).

```elixir
config :ankusa, route_resolver: {Ankusa.RouteResolver.TenantPath, prefix: ["webhooks"]}
```

```
POST /webhooks/acme/stripe    →  %Route{tenant_id: "acme", source_id: "stripe"}
POST /webhooks/globex/stripe  →  %Route{tenant_id: "globex", source_id: "stripe"}
```

## Writing your own scheme

This is the extension point for a real product's catch-URL story. An
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
        # look up `token` in your own endpoint table/cache here.
        # This is exactly where a control-plane-backed SourceStore.Ecto
        # would live too
        {:ok, %Route{source_id: token, params: %{app_id: app_id}}}

      _ ->
        :error
    end
  end
end
```

An unresolvable URL shape returns `:error`, which the router turns into a
`404`, the same response an unknown `source_id` gets, so a probe can't tell
"malformed URL" from "URL shape is fine but nothing's registered there."

## Tenant scoping: what `tenant_id` actually does

`tenant_id` on a `%Ankusa.Source{}` (default `"default"`) is the
**storage/retention scope**. It also travels with every delivery. Concretely:

- **Storage**: `tenant_id` is a field of `Ankusa.Envelope`, so it is inside
  every stored hook and every segment's bytes. The store's segment catalogue is
  keyed by `seq` and event-id range, not by tenant, so it cannot answer "which
  segments hold tenant X" — a per-tenant retention rule has to read the
  segments. Claim packs are the exception: their keys are tenant-prefixed
  (`claims/tenant=acme/...`), so a tenant's claims are deletable by prefix.
- **Delivery**: `tenant_id` is in every `Ankusa.Sink`'s `ctx` map
  (`ctx.tenant_id`), so a sink can route, tag, or partition by it. For
  example, `Ankusa.Sink.RabbitMQ`'s default routing key doesn't include it, but a
  custom `:routing_key` function easily can (`"ankusa.#{env.tenant_id}.#{env.source_id}"`).

Resolution order for a given request: `route.tenant_id` (if the resolver set
one) wins; otherwise `source.tenant_id` (if the source's config set one);
otherwise `"default"`.

**A tenant's source answers only its tenant.** A source whose `tenant_id` is
anything but `"default"` is bound to that tenant: a route naming another
tenant (`POST /webhooks/globex/acme-stripe` for a source owned by `acme`) is
the same `404` as a source that does not exist, so one tenant cannot write
into another's storage scope and the URL reveals nothing about which sources
other tenants have. A `"default"` source is shared — the one `stripe` source
behind `/webhooks/:tenant/stripe` for every customer — and stores each hook
under the URL's tenant. (`"default"` is what a source gets when none is set,
which is why it means "shared" rather than "owned by a tenant called
default".)

A shared source trusts the URL's tenant: anyone who can post to
`/webhooks/<victim>/stripe` files hooks under `<victim>`'s storage scope and
spends its rate limit. Give a shared source a real verifier (the provider's
signature), so only the provider's hooks get in, or give each tenant a source
of its own, which the binding above then protects. A source created through
the admin API under the tenant `default` is shared the same way.

## Dynamic sources

Everything above is a store decision, not a boot-time one: the router resolves
a `source_id` and asks the configured `Ankusa.SourceStore` for it, so a source
can appear at runtime with no redeploy. Two stores ship:

- **`SourceStore.Static`** (the default) reads sources declared in `config.exs`
  and nothing else. The admin API's write routes answer
  `409 source_store_read_only` against it.
- **`SourceStore.Persistent`** (the image's `source_store.type: persistent`)
  seeds the same `sources:` map and adds tenant-scoped ones through the admin
  API: `GET|POST /v1/tenants/{tenant}/sources` and
  `GET|PUT|DELETE /v1/tenants/{tenant}/sources/{name}`, with the same spec
  validation as the YAML file. Each source is one synced key in the node's
  store, so a created or updated source works immediately and survives a
  restart. That is what makes a product minting per-customer catch URLs
  possible without a redeploy: `RouteResolver`/`Route` gave you the URL shape,
  this gives you the runtime endpoint.

`SourceStore.Persistent` keeps its sources in this node's store, so it is
node-local like the rest of it: that node's admin API writes, that node's edge
reads. A fleet wants either one node serving the source API, or an external
store — a DB-backed `SourceStore.Ecto`, read-through cached, invalidated on
write — which the `Ankusa.SourceStore` behaviour is the seam for.

One consequence for `wal.type: none`: its boot check — every statically
configured source needs at least one sink whose `:ok` means durable
(`c:Ankusa.Sink.durable?/1`, enforced by `Ankusa.Queue.validate_config!/1`) —
only sees sources in the config. A source created at runtime through the admin
API is not checked, because the store's decoder has no instance config, so a
`wal: :none` node with a writable source store can be handed a log-only source
at runtime. Give runtime-created sources durable sinks, or run `wal.type: disk`
on the node that serves the admin API's source routes.
