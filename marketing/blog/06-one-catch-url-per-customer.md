---
title: "One catch URL per customer: multi-tenant webhooks without a second service"
date: 2026-11-17
slug: one-catch-url-per-customer
description: "Per-customer webhook URLs in Ankusa: route resolvers, runtime sources, rotated-secret quarantine, Redis-shared routes, and lifecycle events plus AsyncAPI."
tags: [webhooks, elixir, multi-tenancy, redis, asyncapi]
draft: true
---

If you run a SaaS that receives webhooks on behalf of your customers, you have probably built this twice. The first version was one shared endpoint with the customer sorted out in application code. The second was a small service that mints URLs, stores secrets, and forwards to the real app. That second service ends up with its own database, its own deploys, and its own outage.

I wanted the catch-URL part to be a seam in the receiver instead of a product next to it. This post covers how that works in Ankusa, where it is node-local, and what I left unauthenticated on purpose. If you are new to the project, start with [the launch post]({{BLOG_URL}}/ankusa-launch). For what happens after a hook is accepted, see [Never ack what you didn't save]({{BLOG_URL}}/never-ack-what-you-didnt-save).

## The need

A single operator with a handful of known providers is happy with `POST /webhooks/:source_id`. A SaaS handing each of thousands of customers their own endpoint is not. A product minting unguessable tokens on demand is not either. Hardcoding any one URL scheme into the router would force every other use case to fork it, so Ankusa does not hardcode one.

The router, `Ankusa.Edge.Router`, is a catch-all `POST`. It calls the configured resolver and hands the result to `Ankusa.Edge.Ingest`. Verification, sinks, and rate limits all happen after that, keyed by what the resolver returned.

## Resolvers: the URL shape is one callback

A resolver turns a raw `Plug.Conn` into a route. `Ankusa.Route` is the resolved identity of a request:

```elixir
%Ankusa.Route{source_id: "stripe", tenant_id: "acme", params: %{}}
```

The behaviour is one callback:

```elixir
@callback resolve(instance :: atom(), conn :: Plug.Conn.t(), opts :: keyword()) ::
            {:ok, Ankusa.Route.t()} | :error
```

A resolver does URL-scheme work only. It never reads the body, verifies a signature, or touches storage. It answers "which endpoint is this?" and nothing else. Policy, meaning verification and sinks, still comes from `Ankusa.SourceStore`, keyed by the `source_id` it returns.

Two resolvers ship. `Ankusa.RouteResolver.Path` is the default and resolves `POST /webhooks/:source_id`. It leaves `tenant_id` unset, so ingest falls back to the source's own `tenant_id`, which defaults to `"default"`. That is the single-tenant case.

```elixir
config :ankusa, route_resolver: {Ankusa.RouteResolver.Path, prefix: ["webhooks"]}  # prefix is the default
```

`Ankusa.RouteResolver.TenantPath` resolves `POST /webhooks/:tenant_id/:source_id`. The tenant in the URL is authoritative: it wins over whatever the resolved source says its tenant is. One instance serves many tenants over one path scheme.

```elixir
config :ankusa, route_resolver: {Ankusa.RouteResolver.TenantPath, prefix: ["webhooks"]}
```

```
POST /webhooks/acme/stripe    →  %Route{tenant_id: "acme", source_id: "stripe"}
POST /webhooks/globex/stripe  →  %Route{tenant_id: "globex", source_id: "stripe"}
```

That covers "customer-name in the path". A product that mints per-integration URLs usually wants something less guessable, such as an opaque token per integration, or a subdomain per tenant. For that you write your own resolver. This is the example from the docs, for `POST /webhooks/catch/:app_id/:token`:

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

An unresolvable URL returns `:error`, which the router turns into a `404`. That is the same response an unknown `source_id` gets, so a probe cannot tell "malformed URL" from "URL shape is fine but nothing is registered there". Note the comment in the middle: the token lookup is yours. The resolver is where you decide what a valid URL looks like, and I did not want to guess at your endpoint table.

### What `tenant_id` does once it is set

`tenant_id` is the storage and retention scope, and it travels with every delivery. It is a field of `Ankusa.Envelope`, so it sits inside every stored hook. It is also in every sink's `ctx` map as `ctx.tenant_id`, so a sink can route, tag, or partition by it. A custom RabbitMQ `:routing_key` function can include it, for example.

Resolution order for a request: `route.tenant_id` if the resolver set one, else `source.tenant_id`, else `"default"`. A resolver-provided tenant always overrides a source's declared one, never the reverse.

## Sources at runtime

A resolver gives you the URL. You still need an endpoint behind it, created when a customer signs up and not when you deploy.

The router asks the configured `Ankusa.SourceStore` for the source, so a source can appear at runtime with no redeploy. Two stores ship. `SourceStore.Static` is the default: it reads the sources declared in config and nothing else, and the admin API's write routes answer `409 source_store_read_only` against it. `SourceStore.Persistent` is selected with `source_store.type: persistent`. It seeds the same `sources:` map and adds tenant-scoped sources through the admin API:

- `GET|POST /v1/tenants/{tenant}/sources`
- `GET|PUT|DELETE /v1/tenants/{tenant}/sources/{name}`

The spec validation is the same as for the YAML file. Each source is one synced key in the node's store, so a created or updated source works immediately and survives a restart.

Here is the caveat that matters for a fleet. `SourceStore.Persistent` keeps sources in this node's store, so it is node-local like the rest of it: that node's admin API writes, that node's edge reads. A fleet wants either one node serving the source API, or an external store behind the `Ankusa.SourceStore` behaviour. A database-backed `SourceStore.Ecto`, read-through cached and invalidated on write, is the seam the docs point at. I have not shipped one.

Port 4002 is unauthenticated, and it listens on 127.0.0.1 by default (`admin.ip`). Whatever creates sources should reach it over a private path; if you publish it from a container, front it with your own proxy or network policy.

### The rotated-secret case

Per-customer sources mean per-customer secrets, and customers rotate them at bad moments. If a verification fails and the source has `on_verify_failure: quarantine`, Ankusa answers `202` and holds the hook in a durable pen instead of rejecting it. The pen is bounded by a per-source token bucket (burst 100, 20 per s) and a byte cap of 1 GiB. A full pen answers `503 quarantine_full` and never evicts.

For a rotation window, `secret` takes a list:

```yaml
verify: {type: stripe, secret: ["${STRIPE_WHSEC_NEW}", "${STRIPE_WHSEC}"]}
```

If the customer's new secret reached you late, or you pasted the wrong one, the held hooks are still there. Fix the source's secret, then release them:

```sh
curl localhost:4002/v1/quarantine  # hooks held after a failed verification
# fixed the secret? release the held hooks that now verify:
curl -XPOST localhost:4002/v1/replays -d '{"kind":"quarantine"}'
```

That is a replay job like the DLQ one. It re-verifies each held hook against the source's current verifier and commits the ones that now pass, with their original `id`. The ones that still fail stay held. Delivery is still at-least-once, so the receiver stays idempotent on `x-ankusa-idempotency-key`. Release does not change that.

## Routes shared across nodes

Sources say what to do with a hook. Routes say whether a request is allowed through at all: a path, allowed methods, an enabled flag, and IP rules. Route management is opt-in: `routes.enabled` defaults to `false`.

The default route store is `ets`. It is memory-only and seeded from `routes.seed` on every boot, so a route deleted through the API returns after a restart unless you also remove it from the seed. I do not call that durable. It is enough for a single node paired with the seed.

To share routes across edge nodes, use the Redis store from the `ankusa_redis` package:

```yaml
routes:
  enabled: true
  store: {type: redis, url: redis://cache:6379, namespace: ankusa:routes}
```

Every node with the same `namespace` enforces the same routes. A write bumps a version counter and publishes it, each node reloads on the broadcast, and a periodic tick is the safety net for a missed one. A node keeps serving its in-memory snapshot through a Redis outage, and only writes report `503 store_unavailable`. The Redis store seeds a namespace once, on its first boot, so a route deleted there is not brought back by a restart.

The management API runs on port 4003, its own listener and never the ingest port:

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/admin/routes?enabled=&limit=&cursor=` | list, id-ordered, cursor-paginated |
| `POST` / `PUT` / `PATCH` / `DELETE` | `/admin/routes[/:id]` | create, replace, update, delete |
| `GET` / `PUT` | `/admin/ip-rules` | the global rules and default |
| `POST` | `/admin/routes/test` | dry run: `{method, path, ip}` → decision, reason, route id, and the rule that decided it |
| `GET` | `/health` | `{status, routes}` |

This port is unauthenticated by design, and it listens on 127.0.0.1 by default (`routes.admin.ip`). Ankusa does not know what auth scheme your deployment wants, and I did not want to pick one for you. Front it with your proxy or a network policy before anything else can reach it.

## Telling the rest of your system

When a customer's endpoint is created, deleted, or changes, something else in your stack usually needs to know: billing, a dashboard, a cache. I did not want that to be a polling loop against the sources API.

Lifecycle events are off by default. Give the instance a sink to deliver them to:

```yaml
lifecycle:
  sinks:
    - {type: kafka, brokers: ["redpanda:9092"], topic: ankusa.lifecycle}
```

Every change made through the admin API's source endpoints, the route-management API, or `Ankusa.SourceStore.put/5`, `delete/3` and `Ankusa.Routes` becomes one CloudEvents 1.0 event: `io.ankusa.source.created`, `updated` or `deleted`, and the same three for `io.ankusa.route.*`. The `data` field is the entity exactly as the admin API returns it, with secrets redacted.

The event travels as the `body_base64` of an ordinary queue message, so reading one from Kafka looks like this:

```sh
rpk topic consume ankusa.lifecycle -n 1 -f '%v\n' | jq -r .body_base64 | base64 -d | jq .type
# "io.ankusa.source.created"
```

These events are best effort, and the limits are specific:

- They bypass the store and dispatch. Nothing is written to disk.
- The publisher's queue holds 10,000 pending sink deliveries. When it is full, or when retries run out, the event is dropped for that sink and counted in `ankusa_lifecycle_dropped_total`.
- Pending events are lost when the node stops, and there is no ordering.
- Only the node that served the change emits, so a route written on one node of a Redis-backed fleet is announced once.

The change itself has already happened, and the call that made it never waits on a broker. If you need a complete picture, read the source and route lists from the admin API. Treat the events as a hint to go look, not as a ledger.

### An AsyncAPI document for your consumers

Separately, with `admin.enabled: true`, port 4002 serves an AsyncAPI 3.0 document:

```sh
curl -s localhost:4002/asyncapi.json
# content-type: application/asyncapi+json
```

It is built from the configuration as it is now, so a source created through the admin API a second ago is in it. It carries no credentials: no URL userinfo, no SASL password, no header value. A team consuming from your Kafka topic can ask the instance where hooks go instead of reading its YAML.

## What this is not

- Delivery is at-least-once to every sink. Per-customer URLs do not change that, so receivers must be idempotent.
- `SourceStore.Persistent` is node-local. A fleet needs one node serving the source API or an external `Ankusa.SourceStore`, and I have not written the external one.
- The routes and admin ports (4002, 4003) have no authentication. They listen on 127.0.0.1 by default; front them with your own proxy or network policy if you publish them.
- Lifecycle events are best effort.
- Everything is 0.x and APIs may change.

If you run a SaaS with per-customer endpoints, I want to know whether the resolver callback is the right shape, and whether a shipped `SourceStore.Ecto` would be more useful to you than the docs pointing at the seam. The full multi-tenancy write-up is [in the repo](https://github.com/jamescarr/ankusa/blob/main/docs/multi-tenancy.md), the lifecycle and AsyncAPI one is [here](https://github.com/jamescarr/ankusa/blob/main/docs/asyncapi.md), and the module docs are on [HexDocs](https://hexdocs.pm/ankusa).

## Try it

```sh
docker run -d --name ankusa \
  -p 4000:4000 -p 127.0.0.1:4002:4002 -e ANKUSA_ADMIN_IP=0.0.0.0 \
  -v ankusa-data:/var/lib/ankusa \
  jamescarr/ankusa:edge

# the image ships a healthcheck: wait for it rather than racing the listener
until [ "$(docker inspect --format '{{.State.Health.Status}}' ankusa)" = healthy ]; do sleep 1; done

curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' -d '{"id":"evt_1"}'
# => {"id":"01a0...","status":"accepted"}   (returned only after the store fsync)
```

The [quickstart](https://github.com/jamescarr/ankusa/blob/main/docs/quickstart.md) walks through outages, dead letters, replay, and pointing a real provider at it. The code is at <https://github.com/jamescarr/ankusa>. It's 0.x; tell me what breaks: <https://github.com/jamescarr/ankusa/issues>
