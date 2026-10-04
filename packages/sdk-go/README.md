# ankusa (Go)

The Go client SDK for [Ankusa](https://github.com/jamescarr/ankusa)
deployments: the claim-check gateway client, the route-management client
(`routes.admin.port`), the operator (`admin.port`) client, a decoder for the
v1 queue message, and a helper for receiving Ankusa's HTTP sink deliveries. It
implements exactly the surface in
[`conformance/`](https://github.com/jamescarr/ankusa/tree/main/conformance),
the language-neutral vectors every Ankusa SDK passes.

## Install

```sh
go get github.com/jamescarr/ankusa/packages/sdk-go
```

The package name is `ankusa`:

```go
import ankusa "github.com/jamescarr/ankusa/packages/sdk-go"
```

## Claim-check client

Redeem a claim-check ref, verify the bytes it returns against the sha256 the
queue message carries next to it, and classify failures into dead-letter vs.
retry, without holding any object-store credentials.

```go
claimCheck, err := ankusa.NewClaimCheckClient(os.Getenv("CLAIM_CHECK_URL"), ankusa.Options{})
if err != nil {
    return err
}

// `ref` is the queue message's `claim` field, `sha256` its `sha256` field:
//   claim:  urn:ankusa:claim:v1:<tenant>:<claim_id>   (claim_id: uppercase ULID)
//   sha256: 64 lowercase hex chars, the digest of the claim's bytes
body, err := claimCheck.Redeem(ctx, ref, sha256)
if err != nil {
    var apiErr ankusa.Error
    if errors.As(err, &apiErr) && !apiErr.Retryable() {
        // bad ref, 404, or an integrity mismatch: dead-letter, don't requeue
    }
    return err
}
_ = body // the verified bytes
```

`Redeem` parses the ref, checks `sha256` is 64 lowercase hex chars, fetches
`GET /v1/claims/{tenant_id}/{claim_id}`, and verifies the bytes against
`sha256` before returning them. `Health` hits `GET /health` for a liveness
probe.

## Routes client

Manage route definitions and the global IP rules on the route-management
listener (`routes.admin.port`, default 4003).

```go
routes, err := ankusa.NewRoutesClient("http://localhost:4003", ankusa.Options{})
if err != nil {
    return err
}

_, err = routes.CreateRoute(ctx, ankusa.RouteInput{ID: "stripe", Path: "/webhooks/stripe"})
page, err := routes.ListRoutes(ctx, ankusa.ListRoutesParams{Enabled: &enabled, Limit: 10})
_, err = routes.GetIPRules(ctx)
_, err = routes.TestRoute(ctx, ankusa.DryRunRequest{Method: "POST", Path: "/webhooks/stripe", IP: "203.0.113.7"})
```

Methods: `Health`, `ListRoutes`, `CreateRoute`, `GetRoute`, `ReplaceRoute`,
`UpdateRoute`, `DeleteRoute`, `GetIPRules`, `PutIPRules`, `TestRoute`. The
id-taking methods reject an id that is empty or exactly `.`/`..` with
`InvalidRouteIdError` before making any request; every other id is
percent-encoded as one path segment.

## Admin client

The operator API on `admin.port` (default 4002): health, Prometheus metrics,
the redacted config, the DLQ, replay jobs, and the quarantine list.

```go
admin, err := ankusa.NewAdminClient("http://localhost:4002", ankusa.Options{})
if err != nil {
    return err
}

health, err := admin.Health(ctx)                             // {Status, Instance, Roles}
metrics, err := admin.Metrics(ctx)                           // Prometheus text
dlq, err := admin.ListDeadLetters(ctx, ankusa.ListDeadLettersParams{Limit: 10})
quarantine, err := admin.ListQuarantined(ctx, ankusa.ListQuarantinedParams{})

// Replay jobs. CreateReplay answers 202 with a new job, or 200 with an
// existing running/paused job whose filter matches, so a retried POST is
// idempotent.
replay, err := admin.CreateReplay(ctx, ankusa.ReplaySpec{Kind: "dlq", SourceID: "demo", Rate: 500})
replay, err = admin.GetReplay(ctx, replay.ID)                // *AdminRejectedError{404, "replay_not_found"} when absent
replays, err := admin.ListReplays(ctx)                       // ReplayList, newest first
replay, err = admin.UpdateReplay(ctx, replay.ID, ankusa.ReplayPatch{State: "paused"}) // 409 "replay_finished" once done
```

## Webhook helper

A worker consuming Ankusa's HTTP sink doesn't need a client — it needs the
hook's identity, which Ankusa attaches as headers:

```go
hook, err := ankusa.ParseHeaders(r.Header)
if err != nil {
    return err // *MissingHookIdError
}
// hook.ID, hook.Source, hook.Tenant, hook.ContentType, hook.DedupeKey,
// hook.ReplayID, hook.ShippedIdempotencyKey

// The key to dedupe on: the tenant-scoped key Ankusa shipped in
// x-ankusa-idempotency-key (computed from the hook's own fields only for a
// sender that predates the header). Pass true to reprocess replays instead of
// dropping them.
key := hook.IdempotencyKey(false)

// A delivery without x-ankusa-id is a framework bug, so ParseHeaders returns
// *MissingHookIdError instead of a blank id.
```

`x-ankusa-id` is required; `x-ankusa-source`, `x-ankusa-tenant`,
`content-type`, `x-ankusa-dedupe-key`, `x-ankusa-replay-id`, and
`x-ankusa-idempotency-key` are optional (`Source` defaults to `""`; `Tenant`,
`ContentType`, `DedupeKey`, `ReplayID`, and `ShippedIdempotencyKey` are `nil`
when absent or empty).

## Consuming queue messages

A worker reading a broker's deliveries gets the v1 queue message as JSON.
Decode it, compute the idempotency key, and record that key in the same
transaction as the side effect, so a redelivery is a no-op:

```go
message, err := ankusa.DecodeMessage(payload)
if err != nil {
    var bad *ankusa.InvalidMessageError
    if errors.As(err, &bad) {
        // bad.Code: invalid_json, not_an_object, unsupported_version,
        // invalid_field (bad.Field names the key), ambiguous_body,
        // missing_body, invalid_body_base64, size_mismatch, integrity, or
        // tenant_mismatch. Retryable() is false: dead-letter, never retry.
    }
    return err
}

key := message.IdempotencyKey(false) // the key Ankusa shipped, else computed

// One transaction: insert the processed-ids row and, only if it was new, run
// the effect.
//
//   INSERT INTO processed_webhooks (idempotency_key) VALUES ($1)
//     ON CONFLICT (idempotency_key) DO NOTHING;
//   if rows_affected == 0 { return nil } // already processed
//   ... apply the effect ...
```

`Message.Body` holds the decoded inline body; for a claim message it is nil
and `Claim`/`Sha256` go to `ClaimCheckClient.Redeem`. Absent `dedupe_key`,
`replay_id`, `idempotency_key`, and `sha256` decode to nil, and absent
`headers` decodes to an empty map. `Message.ShippedIdempotencyKey` is the
wire's `idempotency_key`: the tenant-scoped key Ankusa computed once
(`tenant:source_id:dedupe_key`, else `id`). `IdempotencyKey` returns it, and
for a message from a node that predates the field computes the same key itself
(tenant `default` when there is none). `IdempotencyKey(true)` appends
`#replay:<replay_id>`, so a replay of an event already processed is reprocessed
rather than dropped; leave it false (the default) to drop replays.

## Errors

Every error returned from a request or parser is a concrete `*XxxError`
implementing the `Error` interface (`error` plus `Retryable() bool`), so one
bit decides dead-letter vs. retry:

| Type | `Retryable()` | Cause |
| --- | --- | --- |
| `InvalidClaimRefError` | `false` | `ref` isn't a well-formed claim-check URN, or `sha256` isn't 64 lowercase hex chars |
| `ClaimNotFoundError` | `false` | gateway `404`: expired by retention, or never written |
| `ClaimRejectedError` | `false` | gateway `4xx` other than `404` (`Status`, `Body`) |
| `ClaimIntegrityError` | `false` | the bytes' sha256 doesn't match the expected `sha256` |
| `ClaimCheckUnavailableError` | `true` | gateway unreachable, timeout, `5xx`, an unfollowed `3xx`, or any other non-`200` |
| `InvalidMessageError` | `false` | `DecodeMessage` could not decode the queue message (`Code`, `Field`) |
| `MissingHookIdError` | `false` | `x-ankusa-id` is absent or empty |
| `InvalidRouteIdError` | `false` | route id is empty or exactly `.`/`..` |
| `RouteNotFoundError` | `false` | routes listener `404` |
| `RoutesRejectedError` | `false` | routes listener `4xx` other than `404` (`Status`, `Code`, `Field`, `Message`, `ConflictingID`, `MaxRoutes`) |
| `RoutesUnavailableError` | `true` | routes listener unreachable, `5xx`, an unfollowed `3xx`, or a non-JSON success body |
| `RoleNotEnabledError` | `false` | admin listener `409` `role_not_enabled` (`Role`) |
| `AdminRejectedError` | `false` | admin listener `4xx` other than that 409 (`Status`, `Code`) |
| `AdminUnavailableError` | `true` | admin listener unreachable, `5xx`, or an unfollowed `3xx` |

Use `errors.As`, not a type assertion, so wrapped causes still match:

```go
var apiErr ankusa.Error
if errors.As(err, &apiErr) && !apiErr.Retryable() {
    // dead-letter
}
```

Caller misuse is a plain error instead, raised before any request is made: a
base URL that is not an absolute http(s) URL (from any `New…Client`), or a
request body that cannot be encoded as JSON (an unencodable `Metadata` value
in `CreateRoute`, `ReplaceRoute`, or `UpdateRoute`). `errors.As` finds no
`ankusa.Error` in them, so the pattern above hands them back to the caller:
they are bugs in the call, not delivery failures that will ever succeed.

The `*UnavailableError` types wrap the underlying transport error: `errors.Is(err, context.DeadlineExceeded)` matches a timed-out request.

## Develop

```sh
mise run check:package sdk-go   # tidy, gofmt, vet, go test -race
mise run check:conformance      # this package's cases plus every other SDK's
```

The conformance runner lives in `conformance_test.go` and reads the vectors
from `../../conformance/cases/`; it imports only this package's public
surface. `mise run release:prepare minor sdk-go` (then `release:tag`) is the
release flow.
