# Claim Check: `Ankusa.ClaimCheck`

One contract for every producer and consumer in the system, Elixir or not:
**check bytes in, get a ticket; present the ticket, get the bytes back.**
The storage engine (LocalFS/S3/GCS, via `Ankusa.BlobStore`) and the network
topology (in-process vs. an HTTP hop) stay hidden behind it.

The queue sinks' fat-payload offload (`Sink.RabbitMQ`, `Sink.Kafka`) is the
shipped user of this — see
[`delivery.md`](delivery.md#sinkrabbitmq--queue-delivery) — but it's a
general-purpose gateway, usable anywhere a payload is too big to carry
inline.

## The contract

```elixir
@callback store(instance :: atom(), Ticket.t(), data :: iodata(), opts :: keyword()) ::
            :ok | {:error, reason()}
@callback fetch(instance :: atom(), Ticket.t(), opts :: keyword()) ::
            {:ok, binary()} | {:error, reason()}

@spec check_in(atom(), iodata(), meta(), keyword()) :: {:ok, Ticket.t()} | {:error, reason()}
@spec redeem(atom(), Ticket.t(), keyword()) :: {:ok, binary()} | {:error, reason()}
```

`Ankusa.ClaimCheck` is both the behaviour and the instance-scoped facade
(same pattern as `Ankusa.BlobStore`). **The facade is smart; adapters are
dumb transport.** `check_in/4` builds and validates the ticket, enforces the
size cap, and stores it; `redeem/3` fetches the bytes and verifies them
end-to-end against the ticket — every adapter (`Direct`, `Remote`) only
moves bytes.

```elixir
{:ok, ticket} = Ankusa.ClaimCheck.check_in(:default, body, %{tenant_id: "acme", id: env.id})
{:ok, ^body} = Ankusa.ClaimCheck.redeem(:default, ticket)
```

### Errors

| Reason | Meaning | Retry? |
| --- | --- | --- |
| `:not_found` | no claim at that key | No — dead-letter |
| `:integrity_mismatch` | bytes don't match the ticket's `size`/`sha256` | No — dead-letter |
| `:invalid_tenant` / `:invalid_id` | malformed input | No — caller bug |
| `:too_large` | over `claim_check.max_bytes` | No — caller bug |
| `:forbidden` | token valid, tenant out of scope | No — misconfiguration, alert |
| `:unsupported_ticket_version` | a ticket from a future format version | No — upgrade the reader |
| `:unauthorized` | missing/bad bearer token | No — misconfiguration, alert |
| `{:unavailable, reason}` | transport/store hiccup | Yes |

## The ticket

The canonical, versioned value that crosses every boundary — process, HTTP,
and non-BEAM consumers alike:

```json
{"v": 1, "tenant_id": "acme", "id": "0199a1c2-...-7...",
 "size": 3145728, "sha256": "9f86d0...", "content_type": "application/json"}
```

- **`v`** — format indicator. A reader that doesn't recognize it fails
  closed (`:unsupported_ticket_version`), never guesses.
- **`id`** — caller-supplied, and must be a UUIDv7 (the same id type as
  `Ankusa.Envelope.id`). Three reasons it's required, not generated:
  1. **Idempotent retries.** A dispatch retry reuses `env.id`, hitting the
     same key — no orphan object from a retried check-in.
  2. **Free retention.** The creation time is embedded in the id, so
     `Ankusa.ClaimCheck.Sweeper` needs no extra metadata or `stat` call.
  3. **Unguessable.** 74 random bits per id.
- **`tenant_id`** — any non-empty UTF-8 string up to 256 bytes. Not
  restricted to a safe character set: a resolver-provided tenant
  (`multi-tenancy.md`) shouldn't become a permanent dispatch failure. It's
  percent-encoded into the storage key instead (see below).
- **`size`, `sha256`** — computed by the facade from the actual bytes,
  **never** trusted from a caller or a remote server.
- **`content_type`** — advisory only. Never stored as object metadata
  (`BlobStore` has no metadata API); carried only in the ticket.
- **Not signed.** Ids are unguessable, the storage key is always *derived*
  (never client-supplied — see below), and integrity is end-to-end via
  `sha256`. Revisit signing only if tickets are ever handed to parties
  outside the token-authenticated trust boundary (see "Open decisions").

## Storage key

```
claims/<percent-encoded tenant_id>/<id>
```

- `claims/` is a fixed constant — never configurable. A producer and the
  gateway disagreeing on a prefix would only surface as a production 404.
- `Ankusa.ClaimCheck.Ticket.key/1` percent-encodes every byte outside
  `[A-Za-z0-9_-]` (including `.`, so `.`/`..` can never appear) — injective,
  traversal-proof on `BlobStore.LocalFS`, and it's the *only* function that
  builds this key. A ticket never carries the key itself, so redeeming a
  ticket can never be steered at an arbitrary object (a compaction segment,
  or another tenant's claim).
- Flat per-tenant directories double as the retention/deletion boundary:
  deleting a tenant's data is a prefix delete of `claims/<enc(tenant)>/`.
- Claims and segments (`seg/...`) share a bucket by default and never
  collide; lifecycle rules and the sweeper only ever target `claims/`.

## Modes: pick by trust boundary, not by fleet size

| Caller | Adapter | Why |
| --- | --- | --- |
| Ankusa node with blob-store credentials (any fleet size) | `Direct` | No extra network hop — a shared bucket is already a working distributed claim check |
| Ankusa node deliberately *without* blob-store credentials | `Remote` | Credential isolation |
| Non-BEAM consumer (a worker, a third-party service) | HTTP API (`Remote` or a plain HTTP client) | No cloud SDK, no credentials, tenant-scoped authz |
| Anything, when the store is `LocalFS` on another host | HTTP API | `LocalFS` isn't network-reachable any other way |

A ticket issued through `Direct` redeems through `Remote` and vice versa —
that's the testable form of "the code doesn't notice the difference"
(`test/ankusa/claim_check/cross_mode_test.exs`).

### `Ankusa.ClaimCheck.Direct`

Calls the instance's configured `Ankusa.BlobStore` in-process.

```elixir
config :ankusa, claim_check: %{adapter: {Ankusa.ClaimCheck.Direct, []}}  # the default
```

opts: `:blob_store` — `{module, opts}`; default: the instance's
`storage.blob_store`.

### `Ankusa.ClaimCheck.Remote`

HTTP client (via `Req` — mirrors `Ankusa.Sink.Http`)
against a `:claim_check`-role `Ankusa.ClaimCheck.Router`.

```elixir
config :ankusa,
  claim_check: %{adapter: {Ankusa.ClaimCheck.Remote, url: "http://claim-check.internal:4001", token: "..."}}
```

opts: `:url` (required), `:token` (optional; omit it when the gateway has no
`api_tokens`), `:timeout_ms` (default `10_000`).

**Architecture rule: RPC only downstream of the WAL.** Per
[`architecture.md`](architecture.md)'s "no component may require another to
be reachable at runtime," `Remote` is an RPC dependency and belongs only on
retryable paths after a hook is already durably committed — dispatch sinks,
external consumers. A gateway outage there means delayed delivery, not
loss, because the WAL/queue still hold the work. **The edge's pre-ack path
must never check in via `Remote`.** Nothing ships an edge-time check-in
today; if one is ever added, it must be `Direct`-only.

## The `:claim_check` role

Off by default (`roles` defaults to `[:edge, :dispatch, :storage]`) — it
opens a port that serves stored payloads. Enable with
`ANKUSA_ROLES=claim_check` or `roles: [:claim_check]`. Its own `Bandit`
listener (`claim_check.port`, default `4001`), separate from the edge on
purpose: the edge is internet-facing and the claim API is internal-network
only.

A node running only `:claim_check` needs no WAL — `Ankusa.Instance` boots
the configured `Ankusa.WAL` only when `:edge`, `:dispatch`, or `:storage` is
enabled, so a claim-check-only fleet needs nothing but blob-store
credentials.

### HTTP API (v1)

With `api_tokens` configured, every `/v1/claims/*` request requires
`authorization: Bearer <token>`. With none, the gateway is open (see below).

| Request | Success | Errors |
| --- | --- | --- |
| `PUT /v1/claims/:tenant_id/:id` — raw body, optional `content-type`/`x-ankusa-sha256` | `201 {"ticket": {...}}` | `400 invalid_tenant\|invalid_id`, `401 unauthorized`, `403 forbidden_tenant`, `413 payload_too_large`, `422 integrity_mismatch`, `503` + `Retry-After: 1` |
| `GET /v1/claims/:tenant_id/:id` | `200`, raw bytes, `content-type: application/octet-stream` | `400`, `401`, `403`, `404 not_found`, `503` + `Retry-After: 1` |
| `GET /health` (unauthenticated) | `200 {"status":"ok"}` | — |

The client supplies the id and uses `PUT` (not a server-generated id), so a
retried check-in is idempotent under plain HTTP semantics. There is no
`DELETE` and no list endpoint in v1.

The server-side `GET` handler is pure byte transport — it has no
ground-truth ticket for an inbound request (the URL carries only
`tenant_id`/`id`, never `size`/`sha256`), so it never runs an integrity
check itself. Integrity is verified end-to-end by the actual redeemer, in
`Ankusa.ClaimCheck.redeem/3`, against the real ticket it holds.

### Authentication and tenant authorization

```elixir
config :ankusa, claim_check: %{api_tokens: %{"<token>" => :all | ["acme", "globex"]}}
```

Tokens are optional. With `api_tokens` configured, they are stored
`sha256(token) => scope`; a request hashes the presented token and does one
map lookup — raw tokens are never compared byte-by-byte. `:all` authorizes
every tenant; a list scopes to exactly those. With no `api_tokens`, the
gateway is open: every request is authorized for every tenant, and
authentication is delegated to whatever fronts the port (a proxy, SSO,
network policy) — the same stance as the admin API.

Static config is enough today. Dynamic, per-tenant tokens depend on the
same missing control plane as `SourceStore.Ecto` — see
[`multi-tenancy.md`](multi-tenancy.md#what-isnt-built-yet).

## Configuration

```elixir
config :ankusa,
  claim_check: %{
    adapter: {Ankusa.ClaimCheck.Direct, []},
    max_bytes: 8_000_000,
    # :claim_check role only
    port: 4001,
    api_tokens: %{},
    # LocalFS retention only
    retention_days: nil,
    sweep_interval_ms: 3_600_000
  }
```

| Key | Default | Meaning |
| --- | --- | --- |
| `claim_check.adapter` | `{Ankusa.ClaimCheck.Direct, []}` | `{module, opts}` implementing `Ankusa.ClaimCheck`. |
| `claim_check.max_bytes` | `8_000_000` | Hard cap on a checked-in body. `Ankusa.ClaimCheck.validate_config!/1` fails boot if this is smaller than `max_body_bytes` on a `:dispatch` node — that combination would dead-letter hooks the edge already accepted. |
| `claim_check.port` | `4001` | The `:claim_check` role's Bandit port. |
| `claim_check.api_tokens` | `%{}` | `%{token => :all \| [tenant_id, ...]}`. Optional; empty leaves the gateway open, with authentication delegated to whatever fronts the port. |
| `claim_check.retention_days` | `nil` | LocalFS-only sweeper retention; `nil` disables the sweeper. |
| `claim_check.sweep_interval_ms` | `3_600_000` | Sweeper tick interval. |

`Ankusa.ClaimCheck.validate_config!/1` runs at instance boot and fails fast
(raises, doesn't just log) on:

- `claim_check.adapter` set to `Remote` on a `:claim_check`-role node — it
  would proxy the API to itself.
- `claim_check.max_bytes` smaller than `max_body_bytes` on a `:dispatch`
  node.
- `claim_check.retention_days` set against a non-`LocalFS` claim store —
  the sweeper only ever covers `LocalFS`; use a bucket lifecycle rule
  instead.

## Retention

- **S3/GCS: a bucket lifecycle rule on the `claims/` prefix.** Documented,
  not automated — `BlobStore.S3`/`BlobStore.GCS`'s `list/3` doesn't page, so
  an in-process sweep over a large bucket would silently miss keys.
- **LocalFS: `Ankusa.ClaimCheck.Sweeper`**, started in the `:storage` role
  only when `claim_check.retention_days` is set. Each tick lists everything
  under `claims/`, reads the UUIDv7 embedded in each claim's own id for its
  creation time (no extra metadata or `stat` call), and deletes claims older
  than the cutoff. A key it can't positively date (malformed, non-UUIDv7 id)
  is left alone rather than guessed at.
- **Retention must cover the longest time a message can sit in any consumer
  queue, plus DLQ replay windows.** This is the one way a claim check loses
  data: the claim expires before its message is redeemed. Size retention
  generously relative to `Ankusa.Dispatch.RetryPolicy`'s `max_attempts`
  and any manual `Ankusa.Dispatch.replay/2` window you expect to use.

## Semantics

- **Idempotency**: check-in is `PUT`-by-`(tenant_id, id)`. Re-checking in
  the same id with the *same* bytes returns the same ticket. Checking in
  *different* bytes under the same id is a caller bug — not prevented at
  write time (no `BlobStore` adapter supports conditional puts), but
  *detected* at redeem time: the earlier ticket's `sha256` no longer
  matches, so it fails loudly as `:integrity_mismatch` rather than silently
  serving stale bytes.
- **Durability**: the ticket returns only after the adapter reports a
  durable write. Never publish a message carrying a ticket before
  `check_in/4` returns `{:ok, _}` — the queue sinks already order it
  this way.
- **No delete on redeem.** A fan-out exchange means multiple independent
  consumers redeem the same claim. Removal is purely time-based (retention,
  above).

## Telemetry

Emitted by the facade — covers every adapter and every caller, Direct or
Remote:

- `[:ankusa, :claim_check, :check_in]` — measurements `%{duration, size}`;
  meta `%{instance, tenant_id, id, adapter, result}`, plus `content_type`
  when the caller passes one
- `[:ankusa, :claim_check, :redeem]` — measurements `%{duration, size}`;
  meta `%{instance, tenant_id, id, adapter, result}`
- `[:ankusa, :claim_check, :sweep]` — measurements `%{deleted, scanned, duration}`

## Open decisions (deferred, each with a trigger)

- **Presigned-URL redemption** for S3/GCS (`GET` → `302` to a short-lived
  signed URL), to take the gateway out of the byte path. Trigger: gateway
  egress cost or latency shows up in telemetry.
- **Streaming/multipart** above `max_bytes`. Trigger: a source that
  legitimately needs bodies larger than `max_body_bytes`.
- **Signed tickets.** Trigger: tickets handed to parties outside the
  token-authenticated boundary.
- **Dynamic tenant tokens.** Trigger: `SourceStore.Ecto` and the control
  plane land.
- **Edge-time size tiering** (checking a fat body in before the WAL ack).
  Would need its own plan; `Direct`-only per the RPC rule above.
