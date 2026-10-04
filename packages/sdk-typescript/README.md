# ankusa

The client SDK for [Ankusa](https://github.com/jamescarr/ankusa) deployments:
one npm package meant to bundle everything a non-Elixir consumer needs to
talk to an Ankusa deployment — the
[claim-check gateway](https://github.com/jamescarr/ankusa/blob/main/docs/claim-check.md)
client, the route-management client (`routes.admin.port`), the operator
(`admin.port`) client, a helper for receiving Ankusa's HTTP sink deliveries,
and a decoder for the queue message those deliveries carry (with the
idempotency-key helper that goes with it).

## Claim-check client

Redeem a claim-check ref, verify the bytes it returns against the sha256 the
queue message carries next to it, and classify failures into dead-letter vs. retry,
without holding any object-store credentials. Generated from the framework's
own contract,
[`priv/openapi/claim_check.v1.yaml`](../ankusa/priv/openapi/claim_check.v1.yaml),
via [`openapi-typescript`](https://openapi-ts.dev/) + [`openapi-fetch`](https://openapi-ts.dev/openapi-fetch/).
The spec is the source of truth; this package conforms to it, not the
reverse.

### Install

```sh
npm install ankusa
```

Not published yet. Until the first release, depend on it as a local path,
the same way the Elixir packages in this monorepo depend on `ankusa` core
before their first Hex release:

```json
{
  "dependencies": {
    "ankusa": "file:../../sdks/typescript"
  }
}
```

A `file:` dependency resolves to compiled output (`dist/`), so run `npm run
build` here at least once before a consumer installs it that way.

### Use

```ts
import { ClaimCheckError, createClaimCheckClient } from "ankusa";

const claimCheck = createClaimCheckClient({ baseUrl: process.env.CLAIM_CHECK_URL ?? "http://localhost:4001" });

// `ref` is the queue message's `claim` field, `sha256` its `sha256` field:
//   claim:  urn:ankusa:claim:v1:<tenant>:<claim_id>   (claim_id: uppercase ULID)
//   sha256: 64 lowercase hex chars, the digest of the claim's bytes
async function resolveBody(ref: string, sha256: string): Promise<Buffer> {
  try {
    return await claimCheck.redeem(ref, sha256);
  } catch (err) {
    if (err instanceof ClaimCheckError && !err.retryable) {
      // bad ref, 404, or an integrity mismatch: dead-letter, don't requeue
      throw err;
    }
    // gateway unreachable or 5xx: safe to retry
    throw err;
  }
}
```

`redeem()` does three things `GET /v1/claims/...` alone doesn't:

1. Parses the ref into its tenant id, claim id, and gateway path
   (`parseClaimRef`, also exported standalone), and checks `sha256` is 64
   lowercase hex chars.
2. Fetches the bytes from `GET /v1/claims/{tenant_id}/{claim_id}`.
3. Verifies them against `sha256` (the gateway
   itself does not check this, see "Redeem a claim" in
   [`docs/claim-check.md`](https://github.com/jamescarr/ankusa/blob/main/docs/claim-check.md))
   before ever returning them to you.

Every failure is a `ClaimCheckError` subclass with a `retryable` boolean, so a
consumer needs exactly one bit to decide dead-letter vs. retry:

| Class | `retryable` | Cause |
| --- | --- | --- |
| `InvalidClaimRefError` | `false` | `ref` isn't a well-formed claim-check URN, or `sha256` isn't 64 lowercase hex chars |
| `ClaimNotFoundError` | `false` | gateway `404`: expired by retention, or never written |
| `ClaimRejectedError` | `false` | gateway `4xx` other than `404` (`.status`, `.body`) |
| `ClaimIntegrityError` | `false` | the bytes' sha256 doesn't match the expected `sha256` |
| `ClaimCheckUnavailableError` | `true` | gateway `5xx`/`503`, or unreachable |

`health()` hits `GET /health` for a liveness probe.

The workers in
[`rabbitmq-consumer`](https://github.com/jamescarr/ankusa/tree/main/examples/rabbitmq-consumer/worker)
and
[`kafka-sqs-consumer`](https://github.com/jamescarr/ankusa/tree/main/examples/kafka-sqs-consumer/worker)
depend on this package.

## Routes client

Manage route definitions and the global IP rules on the route-management
listener (`routes.admin.port`, default 4003) — the `routes` tag of
[`priv/openapi/admin.v1.yaml`](../ankusa/priv/openapi/admin.v1.yaml).

```ts
import { RouteNotFoundError, createRoutesClient } from "ankusa";

const routes = createRoutesClient({ baseUrl: "http://localhost:4003" });

await routes.createRoute({ id: "stripe", path: "/webhooks/stripe" });
await routes.getIpRules();   // { default: "allow", rules: [] }
await routes.testRoute({ method: "POST", path: "/webhooks/stripe", ip: "203.0.113.7" });
```

Methods: `health()`, `listRoutes()`, `createRoute()`, `getRoute()`,
`replaceRoute()`, `updateRoute()`, `deleteRoute()`, `getIpRules()`,
`putIpRules()`, `testRoute()`. The id-taking methods reject an id that is not a
string, is empty, or is `.`/`..` with `InvalidRouteIdError` before making any
request: a URL parser normalizes those away, so they'd address the collection
endpoint instead of a route. Every other id is percent-encoded as one path
segment.

Failures are `RoutesError` subclasses with a `retryable` boolean:
`InvalidRouteIdError` and `RouteNotFoundError` (404), `RoutesRejectedError`
(any other `4xx`, carrying `code`, `field`, `message`, `conflicting_id`,
`max_routes`), and `RoutesUnavailableError` (`5xx`, an unfollowed `3xx`, or
unreachable — retryable).

## Admin client

The operator API on `admin.port` (default 4002): health, Prometheus metrics,
the redacted config, the DLQ, replay jobs, and the quarantine list — the
`operations`, `dlq`, `replays`, and `quarantine` tags of `admin.v1.yaml`.

```ts
import { createAdminClient } from "ankusa";

const admin = createAdminClient({ baseUrl: "http://localhost:4002" });

await admin.health();                        // { status, instance, roles }
await admin.listDeadLetters({ limit: 10 });
await admin.listQuarantined();

// A replay job re-sends dead rows (`kind: "dlq"`) or archived hooks over a
// `received_at` window (`kind: "archive"`), at `rate` items per second and
// only while live traffic leaves dispatch capacity free.
const job = await admin.createReplay({ kind: "dlq", source_id: "demo", rate: 500 });
await admin.getReplay(job.id);
await admin.listReplays();                   // { replays: Replay[] }
await admin.updateReplay(job.id, { rate: 2_000 });
await admin.updateReplay(job.id, { state: "paused" });   // "running" resumes, "cancelled" stops
```

Methods: `health()`, `metrics()` (Prometheus text), `config()`,
`listDeadLetters()`, `createReplay()`, `getReplay()`, `listReplays()`,
`updateReplay()`, `listQuarantined()`. `createReplay` is idempotent: posting
the same kind and filter while a matching `running`/`paused` job exists
returns that job. Failures are `AdminError` subclasses: `RoleNotEnabledError`
(409 `role_not_enabled`, carrying `role`), `AdminRejectedError` (any other
`4xx`, carrying `code` — e.g. `replay_not_found` or `replay_finished`), and
`AdminUnavailableError` (`5xx`, an unfollowed `3xx`, or unreachable —
retryable).

## Webhook helper

A worker consuming Ankusa's HTTP sink doesn't need a client — it needs the
hook's identity, which Ankusa attaches as headers. `parseHeaders` reads them
case-insensitively from any header mapping: a `Headers`, a plain object, or
Node's `IncomingHttpHeaders`.

```ts
import { parseHeaders } from "ankusa";

// `req.headers` is whatever your framework hands you (`http.IncomingHttpHeaders`,
// an Express `req.headers`, a WHATWG `Headers`, ...).
const hook = parseHeaders(req.headers);
// hook = { id, source, tenant, contentType, dedupeKey, replayId, idempotencyKey }

// Dedupe on `idempotencyKey(hook)`: delivery is at-least-once, so a retried
// hook arrives twice. `x-ankusa-id` is always the identity, so a delivery
// without it is a framework bug rather than a tolerable request: `parseHeaders`
// raises `MissingHookIdError` instead of returning a blank id.
```

Every other header is optional — `tenant` is `null` unless the source has
one, `source`/`contentType` default to `""`/`null`, and
`dedupeKey`/`replayId`/`idempotencyKey` are `null` when absent. See "HTTP
handoff" in
[`docs/integrations.md`](https://github.com/jamescarr/ankusa/blob/main/docs/integrations.md)
for the full contract. `dedupeKey` is the provider's event key when the source
has a dedupe rule; `replayId` is set only on replayed deliveries;
`idempotencyKey` is the tenant-scoped key Ankusa computed and shipped in
`x-ankusa-idempotency-key`.

## Consuming queue messages

Ankusa's sinks carry a JSON envelope (`v: 1`) alongside the body — as the
broker payload for RabbitMQ/Kafka/NATS/Redis, or as the HTTP body for the
webhook helper. `decodeMessage` parses it and verifies the bytes it names, so
a consumer never touches a corrupt or truncated body:

```ts
import { decodeMessage, idempotencyKey, InvalidMessageError } from "ankusa";

try {
  const message = decodeMessage(raw);            // raw: string | Uint8Array
  // message = { v, id, source_id, tenant_id, received_at, content_type, size,
  //             body_base64, claim, sha256, dedupe_key, replay_id,
  //             idempotency_key, headers }
  const body = message.body ??                     // decoded inline bytes (Uint8Array)
    await claimCheck.redeem(message.claim!, message.sha256!);   // or the claim gateway

  // Ankusa computes the key once per hook and ships it as
  // `message.idempotency_key`: `tenant:source_id:dedupe_key` when a provider
  // event key is set, else `id` — so provider retries (same event key)
  // collapse to one row. `idempotencyKey` reads it. Replays keep the original
  // key, so dedupe drops them unless you pass `{ includeReplay: true }`.
  const key = idempotencyKey(message);

  await db.query(
    `insert into processed_webhooks (idempotency_key, body)
     values ($1, $2)
     on conflict (idempotency_key) do nothing`,
    [key, body],
  );
} catch (err) {
  if (err instanceof InvalidMessageError && !err.retryable) {
    // invalid_json / unsupported_version / integrity / ...: dead-letter, don't retry
    throw err;
  }
  throw err;   // transport failure: retry
}
```

`InvalidMessageError` carries `retryable: false`, a `code` (`invalid_json`,
`not_an_object`, `unsupported_version`, `invalid_field`, `ambiguous_body`,
`missing_body`, `invalid_body_base64`, `size_mismatch`, `integrity`,
`tenant_mismatch`), and the offending `field` when the rule is about one key.
Body delivery is at-least-once, so every consumer must dedupe on the key —
`message.idempotency_key`, which `idempotencyKey(message)` returns. For a
message from a node older than the field the helper computes the same key
itself: `tenant:source_id:dedupe_key` (tenant `default` when there is none)
when `dedupe_key` is set, else `id`.

`idempotencyKey` also takes the `HookHeaders` an HTTP receiver already has
(`parseHeaders(req.headers)`), where it reads `x-ankusa-idempotency-key`
(falling back to computing it, with `source` playing `source_id`).

## Layout

```
src/
  index.ts            # umbrella barrel: re-exports every client this package bundles
  claim-check/         # the claim-check gateway client
    index.ts           # barrel for this client
    client.ts
    ref.ts
    errors.ts
    claim-check-schema.d.ts   # generated, see "Develop"
    client.test.ts
  routes/              # the route-management client (routes.admin.port)
    index.ts
    client.ts
    errors.ts
    client.test.ts
  admin/               # the operator client (admin.port)
    index.ts
    client.ts
    errors.ts
    admin-schema.d.ts   # generated, see "Develop"
    client.test.ts
  webhook/             # the helper for receiving HTTP-sink deliveries
    index.ts
    headers.ts
    webhook.test.ts
  message/             # the queue-message decoder + idempotency-key helper
    index.ts
    message.test.ts
```

A future client (say, an ingest helper) gets its own `src/<name>/` directory
with the same shape, re-exported from `src/index.ts`.

## Develop

```sh
npm install
npm run generate:types   # regenerate every schema from the OpenAPI specs:
                         #   claim_check.v1.yaml -> src/claim-check/claim-check-schema.d.ts
                         #   admin.v1.yaml       -> src/admin/admin-schema.d.ts
                         # (the routes client is generated from admin.v1.yaml too)
npm run typecheck
npm test
npm run build            # emits dist/, what npm actually publishes ("files": ["dist"])
```
