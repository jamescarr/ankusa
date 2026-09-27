# ankusa

The client SDK for [Ankusa](https://github.com/jamescarr/ankusa) deployments —
one npm package meant to bundle everything a non-Elixir consumer needs to
talk to an Ankusa deployment. Today that's the
[claim-check gateway](https://github.com/jamescarr/ankusa/blob/main/docs/claim-check.md)
client; more clients (ingest, admin) land here as they're built.

## Claim-check client

Redeem a claim-check ref, verify the bytes it returns against the ref's own
declared size and sha256, and classify failures into dead-letter vs. retry —
without holding any object-store credentials. Generated from the framework's
own contract,
[`priv/openapi/claim_check.v1.yaml`](../../priv/openapi/claim_check.v1.yaml),
via [`openapi-typescript`](https://openapi-ts.dev/) + [`openapi-fetch`](https://openapi-ts.dev/openapi-fetch/).
The spec is the source of truth; this package conforms to it, not the
reverse.

### Install

```sh
npm install ankusa
```

Not published yet — until the first release, depend on it as a local path,
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

// `ref` is the queue message's `claim` field:
//   urn:ankusa:claim:v1:<tenant>:<object_id>:<offset>:<length>:sha256-<hex>
async function resolveBody(ref: string): Promise<Buffer> {
  try {
    return await claimCheck.redeem(ref);
  } catch (err) {
    if (err instanceof ClaimCheckError && !err.retryable) {
      // bad ref, 404, or an integrity mismatch — dead-letter, don't requeue
      throw err;
    }
    // gateway unreachable or 5xx — safe to retry
    throw err;
  }
}
```

`redeem()` does three things `GET /v1/claims/...` alone doesn't:

1. Parses the ref into the gateway's path segments (`parseClaimRef`, also
   exported standalone).
2. Fetches the bytes.
3. Verifies them against the ref's declared length and sha256 — the gateway
   itself does not check this (see "Redeem a claim" in
   [`docs/claim-check.md`](https://github.com/jamescarr/ankusa/blob/main/docs/claim-check.md)) —
   before ever returning them to you.

Every failure is a `ClaimCheckError` subclass with a `retryable` boolean, so a
consumer needs exactly one bit to decide dead-letter vs. retry:

| Class | `retryable` | Cause |
| --- | --- | --- |
| `InvalidClaimRefError` | `false` | `ref` isn't a well-formed claim-check URN |
| `ClaimNotFoundError` | `false` | gateway `404`: expired by retention, or never written |
| `ClaimRejectedError` | `false` | gateway `4xx` other than `404` (`.status`, `.body`) |
| `ClaimIntegrityError` | `false` | wrong length, or sha256 doesn't match the ref |
| `ClaimCheckUnavailableError` | `true` | gateway `5xx`/`503`, or unreachable |

`health()` hits `GET /health` for a liveness probe.

The workers in
[`rabbitmq-consumer`](https://github.com/jamescarr/ankusa/tree/main/examples/rabbitmq-consumer/worker)
and
[`kafka-sqs-consumer`](https://github.com/jamescarr/ankusa/tree/main/examples/kafka-sqs-consumer/worker)
depend on this package.

## Layout

```
src/
  index.ts            # umbrella barrel — re-exports every client this package bundles
  claim-check/         # the claim-check gateway client
    index.ts           # barrel for this client
    client.ts
    ref.ts
    errors.ts
    claim-check-schema.d.ts   # generated — see "Develop"
    client.test.ts
```

A future client (say, an ingest helper) gets its own `src/<name>/` directory
with the same shape, re-exported from `src/index.ts`.

## Develop

```sh
npm install
npm run generate:types   # regenerate src/claim-check/claim-check-schema.d.ts from the OpenAPI spec
npm run typecheck
npm test
npm run build            # emits dist/ — what npm actually publishes ("files": ["dist"])
```
