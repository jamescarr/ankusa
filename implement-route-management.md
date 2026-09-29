# Ankusa Route Management Plan

## Implementation status

This file is the original plan. The shipped feature keeps its behaviour, but
deviates from the plan in these places — the code, not this document, is the
source of truth:

- **Definitions store, not a Nebulex multilevel cache.** There is no L1/L2.
  `Ankusa.Routes.Store` is the behaviour, `Ankusa.Routes.Store.ETS` is the
  default (definitions in node memory, capped, seeded from `routes.seed` on
  every boot, nothing evicted), and `Ankusa.Routes.Store.Redis` in the optional
  `ankusa_redis` package holds them in Redis for multi-node deployments. The
  decision cache is core's own `Nebulex.Adapters.Local` (`nebulex_local`) — it
  caches decisions only, and a mutation publishes a new snapshot whose `epoch`
  retires them all at once.
- **The guard and the API are core modules.** `Ankusa.Edge.RouteGuard` (a
  `Plug`, in front of the WAL) and `Ankusa.Routes.Router`, served on
  `routes.admin.port` — default **4003**, not 4001 — under the paths
  `/admin/routes` and `/admin/ip-rules`. There is no
  `AnkusaServer.Plugs.RouteGuard` and no `AnkusaServer.Admin.Router`; the
  library owns both, so an embedder gets the same guard the image runs.
- **CIDR is the `cidr` package.** No `Ankusa.Net.CIDR` module exists, and no
  hand-rolled prefix bit math: `Ankusa.Net` is only the `:inet`-tuple boundary
  plus the IPv4-mapped-to-IPv4 normalization, and rules carry parsed `%CIDR{}`
  values.
- **No bearer token — unauthenticated by design.** The management API is
  as open as `admin.port`: Ankusa does not know what auth scheme a deployment
  wants, so it does not pick one, and it logs a warning and asks you to front
  the port with your own proxy, mesh, or network policy. This is a recorded
  decision to revisit if the deployment model changes, not an oversight.
- **Optional file persistence (phase 6) is not implemented.** The ETS store is
  memory-only; `routes.seed` plus an idempotent external apply is the durability
  story, or use the Redis store.
- **CIDR property tests became example tests.** `stream_data` is not a
  dependency; the suites are example-based over the `cidr` package's own
  guarantees.

## Behavior

- Off by default. With no route config, Ankusa captures everything, as it does today.
- When enabled, the default is deny. A request is captured only if it passes the IP check and matches an enabled route.
- Rejected requests are never captured or written to the outbox.

## Request Path Order

1. Resolve the client IP.
2. Check IP rules. This is cheap and needs no route lookup.
3. Match method and path against routes.
4. Capture on success. Otherwise reject.

Checking IP first means blocked senders never cause a cache or Redis read.

## Route Model


| Field                       | Notes                                     |
| --------------------------- | ----------------------------------------- |
| `id`                        | Stable slug, client-supplied or generated |
| `path`                      | Pattern (see below)                       |
| `methods`                   | Defaults to `["POST"]`                    |
| `enabled`                   | Boolean                                   |
| `ip_rules`                  | Optional per-route rule list              |
| `metadata`                  | Free-form map (owner, sender name)        |
| `inserted_at`, `updated_at` | Timestamps                                |


Path patterns:

- Exact: `/hooks/stripe`
- Named segment: `/hooks/:tenant/github`
- Trailing wildcard: `/hooks/shopify/*`
- No regex in v1. It invites ReDoS and is hard to reason about.

Normalize before matching: strip the trailing slash, collapse duplicate slashes, and reject `..` and encoded slashes.

## Storage with Nebulex

Use a multilevel cache.

- **L1:** `Nebulex.Adapters.Local` (ETS). Set `max_size`, a short TTL, and `gc_interval`.
- **L2:** Redis via `NebulexRedisAdapter`, added only when `redis_url` is configured.
- **Reads:** L1, then L2, then backfill L1.
- **Writes:** Go to all levels.

There is a design problem to settle here. The local adapter evicts by generation when it hits `max_size`. If ETS is the only store, eviction silently deletes a real route and you start rejecting legitimate webhooks. Two rules avoid that:

- **Route definitions are the source of truth, not cache entries.** In standalone mode the definitions live in ETS with a hard cap. Creating a route past the cap returns `409`. Nothing is evicted. With Redis configured, Redis holds the definitions and L1 is a true cache.
- **Cache decisions, not just routes.** Pattern routes can't be found by a single key lookup. Keep the compiled pattern list in memory and rebuild it on change. Cache the resulting decision in L1, keyed by `{method, normalized_path}`. Cache negative results too, with a shorter TTL, so a scanner hitting random paths doesn't reach Redis every time.

### Multi-node invalidation

- Each write bumps a `routes_version` counter.
- Broadcast the change over Redis pub/sub, or `Phoenix.PubSub` / `:pg` for clustered BEAM nodes.
- Every node drops its L1 decisions and recompiles patterns.
- The short L1 TTL (around 30s) is the safety net if a broadcast is missed.

### Standalone durability

ETS routes vanish on restart. Support a static `routes:` seed in config that loads at boot. Optionally add a `persist: {:file, path}` snapshot written on each mutation. Runtime API mutations are ephemeral without one of these.

## IP Rules

Rule shape: `%{action: :allow | :deny, cidr: "10.0.0.0/8"}`.

Semantics:

- Ordered list, first match wins.
- Explicit `default: :allow | :deny` when nothing matches.
- Standard CIDR matching. `0.0.0.0/0` matches all IPv4 and `::/0` matches all IPv6. A bare address is `/32` or `/128`.
- Normalize IPv4-mapped IPv6 (`::ffff:1.2.3.4`) to IPv4 before matching.
- Parse CIDRs at write time and store them as `{address_int, mask}` so the hot path is a bitmask compare. `inet_cidr` from Hex works, or write it yourself in about 40 lines.

Layering: global rules are a floor. A global deny always wins. If a route defines `ip_rules`, they replace the global allow list for that route. This lets you pin a Stripe route to Stripe's published ranges while keeping a global ban list.

### Client IP resolution

This is where people get burned.

- Default to the socket peer address.
- Add `trusted_proxies` (CIDR list). If the peer is in it, walk `X-Forwarded-For` right to left and take the first address not in the trusted set.
- Never trust `X-Forwarded-For` from an untrusted peer. It is trivially spoofed and would defeat the whole allowlist.

## Management API

Run it on a separate listener or port from webhook ingress. Require a bearer token, and refuse to start the admin API without one.


| Method      | Path                 | Purpose                                                                         |
| ----------- | -------------------- | ------------------------------------------------------------------------------- |
| `GET`       | `/admin/routes`      | List, with `?enabled=` and cursor pagination                                    |
| `POST`      | `/admin/routes`      | Create                                                                          |
| `GET`       | `/admin/routes/:id`  | Fetch                                                                           |
| `PUT`       | `/admin/routes/:id`  | Replace, idempotent                                                             |
| `PATCH`     | `/admin/routes/:id`  | Partial update, enable/disable                                                  |
| `DELETE`    | `/admin/routes/:id`  | Remove                                                                          |
| `GET`/`PUT` | `/admin/ip-rules`    | Global rules and default                                                        |
| `POST`      | `/admin/routes/test` | Dry run: given `{method, path, ip}`, return the decision and which rule matched |


The dry-run endpoint is worth building early. It makes debugging "why was my webhook rejected" trivial.

Validation: unique ids, valid patterns, valid CIDRs, and rejection of two enabled routes with identical method and path.

## Rejection Behavior

- No route match: `404`. Don't confirm which paths exist.
- IP denied: `403`, configurable to `404` for uniformity.
- Log rejections at debug, sampled. Emit telemetry: `[:ankusa, :routes, :match]`, `[:ankusa, :routes, :reject]` with reason metadata (`:no_route`, `:ip_denied`, `:method`).
- Some senders retry on 4xx, so document that.

## Config Sketch

```elixir
config :ankusa_server, :routes,
  enabled: true,
  max_routes: 10_000,
  cache: [max_size: 50_000, ttl: :timer.seconds(30)],
  redis_url: System.get_env("ANKUSA_REDIS_URL"),
  trusted_proxies: ["10.0.0.0/8"],
  ip_rules: [default: :allow, rules: []],
  admin: [port: 4001, token: System.fetch_env!("ANKUSA_ADMIN_TOKEN")],
  seed: []

```

## Modules

- `Ankusa.Routes`: public context (CRUD, `authorize/3`)
- `Ankusa.Routes.Route` and `Ankusa.Routes.Matcher` (compiled patterns)
- `Ankusa.Routes.Cache` (Nebulex multilevel)
- `[Ankusa.Net](http://Ankusa.Net).CIDR` and `[Ankusa.Net](http://Ankusa.Net).ClientIP`
- `AnkusaServer.Plugs.RouteGuard` (ingress)
- `AnkusaServer.Admin.Router` (management API)

## Phases

1. CIDR module and client IP resolution, with property tests.
2. Route model, matcher, and the ETS-only store with hard cap and seed.
3. `RouteGuard` plug and telemetry.
4. Admin API including dry run.
5. Nebulex multilevel with Redis, version counter, and invalidation broadcast.
6. Optional file persistence.

## Route Changes are Triggered as Events

Use the mechanism we have for internal telemetry eventing for now, we will define how to listen for events ankusa emits for users later. 



## Per Route Rate Limits

Yes, let's leave this open for a future implementation, not yet though.

## Nebulex 3.x is fine

We will use that version, no reason to use 2.x