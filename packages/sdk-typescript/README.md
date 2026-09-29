# ankusa

The client SDK for [Ankusa](https://github.com/jamescarr/ankusa) deployments:
one npm package meant to bundle everything a non-Elixir consumer needs to
talk to an Ankusa deployment — the
[claim-check gateway](https://github.com/jamescarr/ankusa/blob/main/docs/claim-check.md)
client, the route-management client (`routes.admin.port`), the operator
(`admin.port`) client, and a helper for receiving Ankusa's HTTP sink
deliveries.

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
the redacted config, the DLQ, and the quarantine list — the `operations`,
`dlq`, and `quarantine` tags of `admin.v1.yaml`.

```ts
import { createAdminClient } from "ankusa";

const admin = createAdminClient({ baseUrl: "http://localhost:4002" });

await admin.health();                        // { status, instance, roles }
await admin.listDeadLetters({ limit: 10 });
await admin.replayDeadLetters({ source_id: "demo" });
await admin.listQuarantined();
```

Methods: `health()`, `metrics()` (Prometheus text), `config()`,
`listDeadLetters()`, `replayDeadLetters()`, `listQuarantined()`. Failures are
`AdminError` subclasses: `RoleNotEnabledError` (409 `role_not_enabled`,
carrying `role`), `AdminRejectedError` (any other `4xx`, carrying `code`), and
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
// hook = { id, source, seq, tenant, contentType }

// Dedupe on `hook.id`: delivery is at-least-once, so a retried hook arrives twice.
// `x-ankusa-id` is the identity to dedupe on, so a delivery without it is a
// framework bug rather than a tolerable request: `parseHeaders` raises
// `MissingHookIdError` instead of returning a blank id.
```

Every other header is optional — `seq` is `null` unless it is all ASCII
digits, `tenant` is `null` unless the source has one, and `source`/`contentType`
default to `""`/`null`. See "HTTP handoff" in
[`docs/integrations.md`](https://github.com/jamescarr/ankusa/blob/main/docs/integrations.md)
for the full contract.

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
