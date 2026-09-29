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
  `{"id", "source", "seq": int|null, "tenant": string|null, "content_type": string|null}`.
- `redeem`: `{"client"?: Client, "gateway": Gateway, "ref": string, "sha256": string}`
  → `ok` is `{"body": Body}`. Runners compare bytes: both the expected `Body`
  and the returned bytes become `{"base64": ...}` before deep-equal.
- `health`: `{"client"?: Client, "gateway": Gateway}` → `ok` is the parsed JSON
  object.

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
