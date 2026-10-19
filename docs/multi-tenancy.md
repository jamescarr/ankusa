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
config :ankusa,
  route_resolver: {Ankusa.RouteResolver.TenantPath, prefix: ["webhooks"]},
  rate_limits: %{default: %{rate: 50, burst: 100}}
```

The URL names the tenant, so a sender can invent as many tenants as it likes.
`TenantPath` therefore refuses to boot without a finite `rate_limits.default`:
every tenant the config does not list gets that limit rather than none. Give a
tenant that needs more its own entry, and add `max_queue` (and
`max_queue_bytes`) so that one tenant cannot fill the batchers for the others
(`429 tenant_overload`, not `503`); see
[`configuration.md#rate-limits`](configuration.md#rate-limits).

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
  An HTTP sink can route by it too: `Sink.Http`'s `url`, `headers` and `secret`
  take a function of the envelope, so one shared source delivers each tenant's
  hooks to that tenant's receiver (see
  [`delivery.md`](delivery.md#sinkhttp-per-tenant-destinations)).

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

When the tenant comes from the request (any resolver but `RouteResolver.Path`),
a shared source with no verifier (`verify: {type: none}`, or
`Ankusa.Verifier.None`) has to say that it means it: `trust_url_tenant: true`,
the same key in YAML, an admin API spec and the Elixir source options.
Without it:

- a source in the config refuses to boot, and the image's `check-config`
  exits 78, naming the source — on every node that reads that config,
  whatever its roles;
- an admin API write (`POST`/`PUT` under `/v1/tenants/default/sources`) is
  `400 invalid_source` with the same message;
- a source the store already held from before the rule (an API-managed one
  written by an older node) keeps serving as it did, and the node logs a
  warning naming it at boot. Saving it again needs the flag, or a verifier.

A source that sets the flag behaves exactly as an unflagged one did before:
the request's tenant is taken as given.

The rule does not cover a shared source that has a verifier but
`on_verify_failure: accept_flag`: a request that fails verification is still
committed, flagged, under whatever tenant the URL names. It spends the
source's quarantine bucket rather than that tenant's rate limit, but it lands
in the tenant's storage scope. Every delivery says so: the message's
`verification.flagged` is `true` and `Sink.Http` sends
`x-ankusa-verification-flagged: true` (see
[`delivery.md`](delivery.md#idempotent-receivers)), so a consumer can refuse
or set aside a hook that was not verified, and the tenant it names with it.
Under tenant routing, give a shared source `reject` or `quarantine` instead if
nothing downstream checks the flag.

## Routes are owned by a tenant and bound to a source

With route management on (`routes.enabled`), the route that admitted a request
is part of the answer, not only a gate in front of it. A route has a `tenant_id`
(default `"default"`, the shared tenant) and an optional `source_id`
([configuration](configuration.md#route-management)), and the edge holds them
against what the resolver and the source store say. A disagreement is the
`404` an unknown source gets, so it tells the sender nothing:

| Route | Request | Result |
| --- | --- | --- |
| `tenant_id: acme` | resolves to a source owned by `globex` | `404` |
| `tenant_id: acme` | `TenantPath` names `globex` | `404` |
| `tenant_id: acme` | resolves to a shared (`default`) source, no tenant in the URL | `201`, stored under `acme` |
| `tenant_id: acme` | resolves to a source owned by `acme` | `201`, stored under `acme` |
| `source_id: acme.billing` | resolves to another source | `404` |
| `tenant_id: default`, no `source_id` | anything | as without routes: the source and the URL decide |

That makes a route the place a tenant is resolved for a shared source whose URL
carries none; where a per-tenant source and a shared source could both apply,
the per-tenant source's owner wins, and the route has to agree with it. A route
owned by a tenant is also what a tenant-scoped principal manages:
`/v1/tenants/<tenant>/routes` needs `tenant:<tenant>` and cannot see or change
another tenant's routes, and a route's lifecycle events and audit rows carry its
tenant, so a replay or an audit read filtered to one tenant sees its own route
changes.

## Erasing a tenant

`DELETE /v1/tenants/{tenant}?confirm={tenant}` (scope `tenants:write` **and**
`tenant:*`: a token for one tenant cannot erase even its own) or
`Ankusa.Tenant.delete/3` removes everything this node holds for one tenant, as
a resumable, audited background job. `?confirm` is the tenant typed again, so a
wrong path in a script is a `400`, not a deleted customer. The request answers
`202` once the job's progress row (`m:erase:<tenant>`) is synced, `200` if one
is already running; `GET /v1/tenants/{tenant}/erase` reports the `stage` or the
`erased_at` time. `default` and a tenant that a configured (static or seeded)
source names are `409`.

The tenant is **closed first**: its new hooks are `503 tenant_erasing`, the
quarantine pen refuses them, dispatch stops claiming its rows, and the queue
stage waits (bounded, retried) until the tenant's `inflight` counter is 0, so
an attempt that is running cannot write a delivery row back after its row was
deleted. Then, in order and each recorded before the next: its sources and
routes (audited, each a `deleted` lifecycle event), its rate-limit override,
its pending, in-flight and dead rows and archive obligations (reclaiming the
hooks nobody else holds), its quarantine rows, every object under
`claims/tenant=<tenant>/` in every claim store, its dedupe keys, and the
redacted entities its lifecycle events carry (after the publisher has
delivered those events). The last stage is one synced batch: the tombstone
`m:erased:<tenant>`, the tenant's ordinal and counters, and one `tenant.delete`
audit row, which is the lifecycle event `io.ankusa.tenant.deleted`. A kill
between stages resumes at the next boot; a kill inside the batch leaves either
all of it or none.

What stays: the audit rows (hashes, counts, names, never a body), the
multi-tenant segments in the archive until the bucket's lifecycle rule expires
them — `Ankusa.Storage.fetch/2` answers `{:error, :gone}` (a `410` where it is
served) for every hook of the tenant received before the tombstone, and archive
replay skips them — and any backup that predates the erase (restore, then erase
again). A lifecycle replay after an erase emits the one `tenant.deleted` event
and nothing else of the tenant. Ordinals are never reused, so an erased tenant
still counts towards `tenants.max`. A tenant that sends again is a new tenant.
Fleet-shared Redis state (`ankusa_redis` rate limits and dedupe keys) is not
part of this job yet; the sources and routes of a Redis source or route store
are, because they go through the same store API. Proof:
`test/ankusa/tenant_test.exs` (`erase leaves no trace`, `resumes after kill -9`,
`a running attempt cannot put a row back`).

## Dynamic sources

Everything above is a store decision, not a boot-time one: the router resolves
a `source_id` and asks the configured `Ankusa.SourceStore` for it, so a source
can appear at runtime with no redeploy. Core ships two stores, and
`ankusa_redis` a third (below):

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
  this gives you the runtime endpoint. Deleting a source that still has
  undelivered hooks on the node is `409 source_has_deliveries`; add
  `?deliveries=dead_letter` to dead-letter them first, so a source re-created
  under the same name starts with an empty queue.

`SourceStore.Persistent` keeps its sources in this node's store, so it is
node-local like the rest of it: that node's admin API writes, that node's edge
reads. A fleet whose nodes all serve the source API uses
**`SourceStore.Redis`** (package `ankusa_redis`; the image's
`source_store.type: redis` with `url`, and optionally `namespace` and
`tick_ms`): the same API and validation, but API-managed sources live in
Redis, and every node configured with the same `namespace` serves them. Each
node reads from an in-memory mirror, never from Redis on the request path; a
write bumps a version and publishes it, and the other nodes reload within a
pub/sub round trip (or the `tick_ms` check, if they missed it). Seeds from
`sources:` stay this node's config and are never written to Redis. A node
that cannot reach Redis on its first boot refuses to start rather than run
with no sources; once running, an outage leaves the mirror serving hooks, and
source writes fail until Redis is back.

**A stored source's secrets rest encrypted** when `secrets.kek` is set (either
store): its `verify.secret` and its sinks' credentials are AES-256-GCM
encrypted before the row or the Redis entry is written, so a backup of the
store or a dump of Redis holds none of them in the clear, while the
source's type, topics and the like stay readable. The admin API and the
in-memory mirror still see the plain spec, and an update that omits the
secret keeps the stored one as before. A source no configured key opens
refuses the node's boot rather than `404`ing its hooks; rotate with
`secrets.kek_previous` and `rewrap`. See
[`storage.md#encryption-at-rest`](storage.md#encryption-at-rest).

One consequence for `wal.type: none`: every source needs at least one sink
whose `:ok` means durable (`c:Ankusa.Sink.durable?/1`). Boot enforces it for
sources in the config (`Ankusa.Queue.validate_config!/1`), and a source
write enforces it for the rest: the admin API refuses to create a source, or
update one, into a source with no durable sink (`400 invalid_source`), whichever
writable store holds it. A source that arrives without that check — stored
before it existed, or written to a shared Redis source store by a node that
runs `wal.type: disk` — is answered `503` at ingest until it is updated to have
a durable sink.
