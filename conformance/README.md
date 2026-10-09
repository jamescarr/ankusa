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
- `check.mjs` — the checker (Node, stdlib only).

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
  `{"tenant_id", "claim_id", "path"}`.
- `parse_headers`: `{"headers": {string: string}}` → `ok` is
  `{"id", "source", "tenant": string|null, "content_type": string|null,
  "dedupe_key": string|null, "replay_id": string|null,
  "idempotency_key": string|null}`. `dedupe_key` comes from
  `x-ankusa-dedupe-key`, `replay_id` from `x-ankusa-replay-id`,
  `idempotency_key` from `x-ankusa-idempotency-key`; each is `null` when the
  header is absent or empty.
- `redeem`: `{"client"?: Client, "gateway": Gateway, "ref": string, "sha256": string}`
  → `ok` is `{"body": Body}`. Runners compare bytes: both the expected `Body`
  and the returned bytes become `{"base64": ...}` before deep-equal.
- `health`: `{"client"?: Client, "gateway": Gateway}` → `ok` is the parsed JSON
  object.
- `decode_message`: `{"message": string}` → `ok` is the decoded message:
  `{"v", "id", "source_id", "tenant_id", "received_at", "content_type",
  "size", "body_base64": string|null, "claim": string|null,
  "sha256": string|null, "dedupe_key": string|null,
  "replay_id": string|null, "idempotency_key": string|null, "headers": {}}`.
  `body_base64` here is the decoded body re-encoded as standard base64, so
  runners compare bytes. The decode rules run in order and the first failure
  wins; every failure raises
  `InvalidMessageError` with `retryable=false`, `code`, and `field`
  (string or null):
  1. Not JSON → `invalid_json`; JSON that isn't an object → `not_an_object`.
  2. `v` missing or not the integer 1 → `unsupported_version`.
  3. Field types, in this order, else `invalid_field` with `field` set to the
     key name: `id` a non-empty string; `source_id` a string; `received_at` an
     integer; `size` an integer ≥ 0; `tenant_id`/`content_type`/`dedupe_key`/
     `replay_id`/`idempotency_key` a string, null, or absent; `headers` absent
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
  and `sha256` decode to `null`; absent `headers` decodes to `{}`.
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
- Admin replay routes: 404 is `AdminRejectedError(status 404, code
  replay_not_found)`; a 409 whose `error` is `role_not_enabled` is
  `RoleNotEnabledError(role)`; every other 4xx is `AdminRejectedError(status,
  code = the body's error)`; anything else that isn't 2xx (5xx, an unfollowed
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
