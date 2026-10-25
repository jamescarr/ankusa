# Conformance

Language-neutral vectors every `packages/sdk-*` must pass, so the SDKs can't
drift from each other. `features.json` is the registry of features and
operations; `cases/*.json` are the test vectors; `check.mjs` validates both and
then runs every registered SDK's native runner.

`mise run check:conformance` is the gate. CI runs it in the `conformance` job.

## Layout

- `features.json` — `{"operations": [...], "features": [{"id", "description"}]}`.
  Every `cases/*.json` case names one `feature` id and one `operation`.
- `cases/<file>.json` — `{"cases": [Case, ...]}`.
- `sdks.json` — the SDK registry the checker runs. See "Registering an SDK".
- `check.mjs` — the checker (Node, stdlib only). It also regenerates the
  producer vectors and compares them with the checked-in file (see "Producer
  vectors"), so it needs Elixir and the `packages/ankusa` dependencies.

## Case format

```jsonc
{
  "id": "globally-unique-string",   // the test name every SDK registers
  "feature": "<features[].id>",
  "operation": "<operations[]>",
  "input": { /* per-operation, below */ },
  "expect": {
    // exactly one of:
    "ok": { /* operation-specific */ },
    "error": { "class": "ExportedErrorName", "retryable": true, "status": 404, "body": "" },
    // optional, both forms:
    "requests": [{ "method": "GET", "path": "/v1/claims/acme/01...", "headers": { "authorization": "Bearer t" }, "body": null }]
  }
}
```

- `expect.error.class` must equal the **exact exported class name** of the
  thrown error (`type(err) is cls` in Python, `err.constructor === cls` in
  TypeScript — no subclass matching). Only the other keys present in
  `expect.error` are compared, by deep equality against the error's attribute
  of the same name.
- A case that expects `InvalidMessageError` or `InvalidSignatureError` must
  say `"retryable": false`: input no retry can fix is permanent, and a consumer
  sends it to its terminal queue (`consumer.failure_policy`;
  [`docs/delivery.md`](../docs/delivery.md#consumer-failure-policy)).
  `check.mjs` rejects the case otherwise.
- `requests`, when present: the recorded requests must match in count and
  order; `method` and `path` must be equal (`path` includes the query string
  exactly as sent: parameter order is input order, booleans go out as
  `true`/`false`); `headers` is a subset match (lowercase names). `[]` means no
  request was made.
- `requests[i].body`, when present, is compared by deep equality against the
  request body the SDK actually sent, parsed as JSON — so it is `null` when the
  request had no body. Absent means the body is not asserted.

### Inputs by operation

- `parse_claim_ref`: `{"ref": string}` → `ok` is
  `{"tenant_id", "claim_id", "path"}`. A v1 ref
  (`urn:ankusa:claim:v1:<tenant>:<claim_id>`) gives the bare gateway path; a
  v2 ref (`urn:ankusa:claim:v2:<tenant>:<claim_id>:<exp_unix>:<signature>`,
  `exp_unix` 1–12 decimal digits with no leading zero, `signature` exactly 43
  characters of `[A-Za-z0-9_-]`) gives the same path followed by
  `?exp=<exp_unix>&sig=<signature>`. No SDK checks the signature or the
  expiry: the gateway does, and `redeem` sends the path as parsed.
- `parse_headers`: `{"headers": {string: string}}` → `ok` is
  `{"id", "source", "tenant": string|null, "content_type": string|null,
  "dedupe_key": string|null, "replay_id": string|null,
  "idempotency_key": string|null, "received_at": int|null,
  "body_sha256": string|null, "verification": {"status": string,
  "flagged": bool}|null}`. `dedupe_key` comes from
  `x-ankusa-dedupe-key`, `replay_id` from `x-ankusa-replay-id`,
  `idempotency_key` from `x-ankusa-idempotency-key`; each is `null` when the
  header is absent or empty. `received_at` comes from `x-ankusa-received-at`
  (when the edge accepted the hook, unix milliseconds): an integer when the
  value is 1 to 15 decimal digits and nothing else, otherwise `null`.
  `body_sha256` comes from `x-ankusa-body-sha256` (the SHA-256 of the body
  sent): the value as is when it is exactly 64 lowercase hex characters,
  otherwise `null` (`webhook.delivery_metadata`). Both are advisory: an
  absent or malformed one is `null`, never an error. `verification` comes from
  `x-ankusa-verification`/`x-ankusa-verification-flagged` (`flagged` is
  `true` only when the latter header is exactly `"true"`); `null` when
  `x-ankusa-verification` is absent or empty.
- `redeem`: `{"client"?: Client, "gateway": Gateway, "ref": string, "sha256": string}`
  → `ok` is `{"body": Body}`. Runners compare bytes: both the expected `Body`
  and the returned bytes become `{"base64": ...}` before deep-equal.
- `health`: `{"client"?: Client, "gateway": Gateway}` → `ok` is the parsed JSON
  object.
- `decode_message`: `{"message": string}` → `ok` is the decoded message:
  `{"v", "id", "source_id", "tenant_id", "received_at", "content_type",
  "size", "body_base64": string|null, "claim": string|null,
  "sha256": string|null, "dedupe_key": string|null,
  "replay_id": string|null, "idempotency_key": string|null, "headers": {},
  "verification": {"status": string, "flagged": bool}|null,
  "traceparent": string|null, "tracestate": string|null,
  "instance": string|null, "region": string|null}`.
  `body_base64` here is the decoded body re-encoded as standard base64, so
  runners compare bytes. `traceparent` and `tracestate` are the message's W3C
  trace context, passed through verbatim (the SDK neither parses nor validates
  them): a string, or `null` for a message with none or from a node older than
  the fields. `instance` and `region` are the name of the Ankusa instance that
  published the message and the region it is configured with
  (`message.origin`): a string, or `null` for a message that says so or lacks
  the key (a node older than the fields). `verification` is the message's
  `verification`
  key verbatim; absent or `null` → `null` (a node older than the field);
  present but not an object of a non-empty string `status` and a boolean
  `flagged` → `invalid_field` (field `verification`); any `status` value
  passes through unvalidated. The decode rules run in order and the first failure
  wins; every failure raises
  `InvalidMessageError` with `retryable=false`, `code`, and `field`
  (string or null):
  1. Not JSON → `invalid_json`; JSON that isn't an object → `not_an_object`.
  2. `v` missing or not an integer from `min` to `max` inclusive (the values
     of `supported_versions`: 1 and 2) → `unsupported_version`. The result's
     `v` is the wire `v`; every accepted version decodes by the rules below,
     because a `v: 2` message is a `v: 1` message plus additive fields
     (`message.decode.v2_additive`).
  3. Field types, in this order, else `invalid_field` with `field` set to the
     key name: `id` a non-empty string; `source_id` a string; `received_at` an
     integer; `size` an integer ≥ 0; `tenant_id`/`content_type`/`dedupe_key`/
     `replay_id`/`idempotency_key`/`traceparent`/`tracestate`/`instance`/`region`
     a string, null, or absent (in that order); `headers` absent
     or an object whose values are all strings; `sha256` absent or 64 lowercase
     hex characters.
  4. Body form: both `body_base64` and `claim` → `ambiguous_body`; neither →
     `missing_body`; `body_base64` that isn't valid base64 →
     `invalid_body_base64`; a `claim` that doesn't parse as a claim ref →
     `invalid_field` (`field: "claim"`); `claim` without `sha256` →
     `invalid_field` (`field: "sha256"`).
  5. Inline body: decoded length ≠ `size` → `size_mismatch`; `sha256` present
     and ≠ the lowercase hex sha256 of the decoded body → `integrity`.
  6. Claim: `tenant_id` non-null and the claim ref's tenant ≠ `tenant_id` →
     `tenant_mismatch`.

  Unknown keys are ignored. Absent `dedupe_key`, `replay_id`, `idempotency_key`
  and `sha256` decode to `null`; absent `headers` decodes to `{}`. Absent
  `instance` and `region` decode to `null`.
- `idempotency_key`: `{"message": string}` or `{"headers": {…}}`, plus an
  optional `"include_replay": bool`. The runner decodes the message with
  `decode_message` or parses the headers with `parse_headers`, then calls the
  SDK helper. `ok` is `{"key": string}`. The rule:
  1. Base is the shipped value — the message's `idempotency_key`, or the
     `x-ankusa-idempotency-key` header — when it is a non-empty string.
  2. Otherwise (a message or delivery from a node older than the field) base is
     computed from the hook's own fields: if `dedupe_key` is non-null and
     non-empty, `tenant_id <> ":" <> source_id <> ":" <> dedupe_key` (tenant is
     `"default"` when `tenant_id` is null; `x-ankusa-tenant` for headers),
     otherwise `id`.
  3. With `include_replay: true` and a non-null `replay_id`, append
     `"#replay:" <> replay_id`.

  `include_replay` defaults to `false`, so a consumer that dedupes this way
  drops replays of events it already processed; one that must reprocess them
  sets `include_replay: true`.
- `admin_replay_create`: `{"gateway": Gateway, "spec": object}` → `ok` is the
  full Replay object (202 for a new job, 200 for an existing one).
- `admin_replay_get`: `{"gateway": Gateway, "id": string}` → `ok` is the full
  Replay object.
- `admin_replay_list`: `{"gateway": Gateway}` → `ok` is
  `{"replays": [Replay…]}`.
- `admin_replay_update`: `{"gateway": Gateway, "id": string, "patch": object}`
  → `ok` is the full Replay object.
- `admin_audit_list`: `{"gateway": Gateway, "params"?: object}` → `ok` is the
  parsed JSON object (`{"entries": [...], "next_cursor": string|null}`). Query
  params (`since`, `until`, `actor`, `action`, `tenant`, `cursor`, `limit`)
  travel in the caller's input order, only those given.
- Admin replay routes: 404 is `AdminRejectedError(status 404, code
  replay_not_found)`; a 409 whose `error` is `role_not_enabled` is
  `RoleNotEnabledError(role)`; every other 4xx is `AdminRejectedError(status,
  code = the body's error, scope = the body's scope when present, else null)`;
  anything else that isn't 2xx (5xx, an unfollowed
  3xx redirect, 1xx) or a transport failure is `AdminUnavailableError`.
- `verify_signature`: `{"headers": {string: string}, "body": Body,
  "secrets": [string], "now": int, "tolerance_seconds"?: int}` → `ok` is
  `{"id": string, "timestamp": int}`. `now` is unix seconds and replaces the
  clock; `tolerance_seconds` defaults to 300. A secret that starts with
  `whsec_` is the standard, padded base64 key after the prefix (unpadded is
  `invalid_secret`, as core decodes it); any other string is
  its own UTF-8 bytes. The signed content is
  `<webhook-id>.<webhook-timestamp>.` followed by the raw body bytes. Every
  failure is `InvalidSignatureError` with `retryable=false`, `code`, and
  `field` (string or null); the checks run in this order and the first
  failure wins:
  1. No secrets, or a `whsec_` secret that isn't valid base64 →
     `invalid_secret` (`field: null`).
  2. `webhook-id`, `webhook-timestamp`, `webhook-signature` (names
     case-insensitive), in that order, missing or empty → `missing_header`
     with `field` set to the lowercase name.
  3. Timestamp not all decimal digits → `invalid_timestamp`; more than
     `tolerance_seconds` from `now` in either direction →
     `timestamp_out_of_tolerance` (both `field: "webhook-timestamp"`).
  4. `webhook-signature` split on spaces; entries that don't start with `v1,`
     are ignored; no `v1,` entry equal (constant-time) to the base64
     HMAC-SHA256 of any secret → `no_matching_signature`
     (`field: "webhook-signature"`).

  The `v1a,` (Ed25519) entries of the Standard Webhooks specification are
  among the ignored ones: `verify_signature` is the HMAC receiver, so a
  delivery with a valid `v1,` entry beside a `v1a,` one verifies, and one with
  only `v1a,` entries is `no_matching_signature`
  (`webhook.verify_signature.asymmetric`). The `v1a,` signature in those cases
  is the specification's reference message signed with the RFC 8032 test key 1
  (public key `whpk_11qYAYKxCrfVS/7TyWQHOg7hcvPapiMlrwIaaPcHURo=`); core's
  `Ankusa.Verifier.Asymmetric` verifies the same bytes
  (`packages/ankusa/test/ankusa/verifier_test.exs` reads these cases).
- `receive_delivery`: `{"headers": {string: string}, "body": Body,
  "receiver": {"secrets"?: [string], "allow_unsigned"?: bool,
  "tolerance_seconds"?: int}, "now": int}` → `ok` is the `parse_headers`
  object plus `"verified": bool`. This is the receiver-side decision every
  SDK offers its users (the Elixir SDK's `Receiver` plug is built on it),
  separate from any web framework: HTTP sinks are signed by default, so a
  receiver is closed unless it is told otherwise. `receiver` is the
  receiver's configuration; `secrets` defaults to `[]`, `allow_unsigned` to
  `false`, `tolerance_seconds` to 300. The rules run in this order and the
  first failure wins:
  1. `secrets` non-empty → verify the delivery exactly as `verify_signature`
     does (same checks, same errors, same `now` and tolerance); `verified` is
     `true` on success. `allow_unsigned` changes nothing here: a configured
     secret is never weakened.
  2. `secrets` empty and `allow_unsigned` false → `InvalidSignatureError`
     (`retryable=false`, `code: "invalid_secret"`, `field: null`), whatever
     the headers say.
  3. `secrets` empty and `allow_unsigned` true → skip the signature;
     `verified` is `false`.
  4. The hook headers are parsed as `parse_headers` does; a missing or empty
     `x-ankusa-id` → `MissingHookIdError`. So the signature outcome decides
     first: a delivery that fails it is `InvalidSignatureError` even when it
     also has no `x-ankusa-id`.

  A web receiver maps `InvalidSignatureError` to `401`, `MissingHookIdError`
  to `400`.
- `trusted`: `{"message": string}` or `{"headers": {string: string}}` → `ok` is
  `{"trusted": bool}`. The runner decodes the message with `decode_message`
  or parses the headers with `parse_headers`, then calls the SDK's trust
  helper on the result (a message's or a delivery's `verification`).
  `trusted` is `true` only when `verification` is non-null, its `status` is
  `"ok"` and `flagged` is `false`. A flagged hook (accepted despite a failed
  check, `on_verify_failure: accept_flag`), a `failed` or `skipped` one, any
  other `status`, and a hook with no `verification` (a node older than the
  field) are all untrusted: the SDK fails closed.
- `supported_versions`: `{}` → `ok` is `{"min": 1, "max": 2}`: the lowest and
  highest message `v` the SDK accepts (`message.versions`). The runner reads
  the two bounds the SDK exposes (constants, or functions where the language
  has no constants); `decode_message` accepts exactly `min ≤ v ≤ max`.
- `decode_lifecycle`: `{"event": string}` → `ok` is the decoded lifecycle event
  (`lifecycle.decode`). `event` is the JSON of a CloudEvents 1.0 structured-mode
  event, which is the decoded body of a message the instance's lifecycle sinks
  publish (content type `application/cloudevents+json`; the `source_id` of that
  message is `ankusa:lifecycle`). `ok` is `{"specversion": string, "id": string,
  "source": string, "type": string, "subject": string, "time": string,
  "datacontenttype": string|null, "data": object|null, "revision": int|null,
  "actor": string|null, "instance": string|null, "sequence": string|null,
  "replayid": string|null, "traceparent": string|null,
  "tracestate": string|null}`: every field passed through verbatim (`data` as
  the JSON object it is: runners deep-compare it), an absent or `null` optional
  field is `null`. `sequence` stays the decimal string it is on the wire, and
  `time` stays the string (the SDK does not parse it). The rules run in order
  and the first failure wins; every failure raises `InvalidMessageError` with
  `retryable=false`, `code`, and `field` (string or null):
  1. Not JSON → `invalid_json`; JSON that isn't an object → `not_an_object`.
  2. `specversion` missing or not the string `"1.0"` → `unsupported_version`.
  3. Field types, in this order, else `invalid_field` with `field` set to the
     key name: `id`, `source`, `type`, `subject`, `time` non-empty strings;
     `datacontenttype` a string, null, or absent; `data` an object, null, or
     absent; `revision` an integer ≥ 1, null, or absent; `actor`, `instance` a
     string, null, or absent; `sequence` a string of decimal digits only (at
     least one), null, or absent; `replayid`, `traceparent`, `tracestate` a
     string, null, or absent.

  Unknown keys are ignored, and so is an unknown `type`: a consumer written
  against an older SDK still decodes the events a newer node adds.

### Helpers

- `Client` = `{"headers"?: {string: string}, "timeout_ms"?: int, "transport"?: "injected"}`.
  With `"transport": "injected"` no server is started: base URL
  `http://gateway.invalid`, and the SDK's transport hook serves `gateway`
  in-process and records requests.
- `Gateway` = `{"unreachable": true}` (base URL `http://127.0.0.1:1`, nothing
  records, so `requests` is always `[]`) or
  `{"status": int, "headers"?: {string: string}, "body"?: Body, "delay_ms"?: int}`.
  The mock answers every request with this response and always sends
  `content-length`. `delay_ms` sleeps before the status line is sent.
- `Body` = `{"text": string}` (UTF-8) | `{"base64": string}` | `{"json": any}`
  (compact `JSON.stringify` / `json.dumps(separators=(",", ":"))`, content-type
  only if `headers` sets it). A missing body means zero bytes.

## Producer vectors

`cases/producer.json` is the one generated file. Every other case is a decoder
vector somebody wrote by hand, which proves an SDK reads the messages the
vectors describe; these prove the SDKs read the messages Ankusa core actually
writes. `mix ankusa.conformance.gen` (in `packages/ankusa`) builds a fixed set
of hooks (inline and claim bodies, a replay of an ordered sink's hook, a flagged
one, instances with and without a region), encodes each with
`Ankusa.Sink.Message.encode/3`, and writes the message beside the
`decode_message` result an SDK must return (feature `message.producer`).

`check.mjs` runs the same task into a temporary file and fails when it differs
from `cases/producer.json`, naming the cases and the message keys that were
added, removed or changed. So a field added to `encode/3` fails
`mise run check:conformance` until the vectors are regenerated
(`cd packages/ankusa && mix ankusa.conformance.gen`) and committed, and then
every SDK has to decode it. Never edit the file by hand.

## Runner contract

Every SDK ships a native runner that:

1. Loads every `cases/*.json` and registers one test per case, named by its `id`.
2. Never filters or skips cases.
3. Fails on an unknown `operation`.
4. Imports only from the package's public entry point.
5. Maps errors by exact exported class name.
6. Is registered in `sdks.json`.

## Adding a feature

Add it to `features.json` with at least one case. Every SDK then fails until it
implements the feature — that is the point.

## Registering an SDK

Add an entry to `sdks.json`; the checker fails if a `packages/sdk-*` directory
is missing from it, or if it names a package that doesn't exist.

```json
{ "sdks": { "sdk-<name>": { "setup": [["<cmd>", "..."], ...], "run": ["<cmd>", "..."] } } }
```

`setup` entries run in `packages/<name>`, in order, then `run`. The first
non-zero exit stops that SDK's remaining steps and fails the gate.
