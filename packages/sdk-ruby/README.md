# ankusa-sdk

The Ruby client SDK for [Ankusa](https://github.com/jamescarr/ankusa)
deployments: one gem meant to bundle everything a non-Elixir consumer needs to
talk to an Ankusa deployment. Today that's the
[claim-check gateway](https://github.com/jamescarr/ankusa/blob/main/docs/claim-check.md)
client, the route-management and operator (admin) clients, the
source-management client, a webhook-receiving header helper, and the queue
message decoder; more clients (ingest) land here as they're built.

## Install

```sh
gem install ankusa-sdk
```

or in a `Gemfile`:

```ruby
gem "ankusa-sdk"
```

then, in code:

```ruby
require "ankusa/sdk"
```

Before the first RubyGems release, depend on it as a path gem the same way the
Elixir packages in this monorepo depend on `ankusa` core before their first Hex
release:

```ruby
gem "ankusa-sdk", path: "packages/sdk-ruby"
```

Everything the SDK exposes lives under `Ankusa::`.

## Claim-check client

Redeem a claim-check ref, verify the bytes it returns against the message's
`sha256`, and classify failures into dead-letter vs. retry, without holding any
object-store credentials. Conforms to the framework's own contract,
[`priv/openapi/claim_check.v1.yaml`](../ankusa/priv/openapi/claim_check.v1.yaml):
the spec is the source of truth, this package conforms to it, not the reverse.

```ruby
require "ankusa/sdk"

CLAIM_CHECK = Ankusa::ClaimCheckClient.new(ENV.fetch("CLAIM_CHECK_URL", "http://localhost:4001"))

# A queue message that carries a claim also carries its sha256:
#   "claim":  "urn:ankusa:claim:v1:<tenant>:<claim_id>"  (claim_id: uppercase ULID)
#   "sha256": 64-char lowercase hex of the claim's bytes
def resolve_body(message)
  CLAIM_CHECK.redeem(message["claim"], message["sha256"])
rescue Ankusa::ClaimCheckError => e
  # bad ref/sha256, 404, or an integrity mismatch: dead-letter, don't requeue
  raise unless e.retryable?

  # gateway unreachable or 5xx: safe to retry
  raise
end
```

`redeem` does three things `GET /v1/claims/...` alone doesn't:

1. Parses the ref into its tenant id, claim id, and gateway path
   (`GET /v1/claims/{tenant_id}/{claim_id}`) (`Ankusa.parse_claim_ref`, also
   exported standalone).
2. Fetches the bytes.
3. Verifies them against the message's `sha256` (the gateway itself does not
   check this, see "Redeem a claim" in
   [`docs/claim-check.md`](https://github.com/jamescarr/ankusa/blob/main/docs/claim-check.md))
   before ever returning them to you.

Every failure is a `ClaimCheckError` subclass with a `retryable?` method, so a
consumer needs exactly one bit to decide dead-letter vs. retry:

| Class | `retryable?` | Cause |
| --- | --- | --- |
| `InvalidClaimRefError` | `false` | `ref` isn't a well-formed claim-check URN, or `sha256` isn't 64-char lowercase hex |
| `ClaimNotFoundError` | `false` | gateway `404`: expired by retention, or never written |
| `ClaimRejectedError` | `false` | gateway `4xx` other than `404`, `408`, `429` (`.status`, `.body`) |
| `ClaimIntegrityError` | `false` | sha256 of the returned bytes doesn't match |
| `ClaimCheckUnavailableError` | `true` | gateway `5xx`, `408`, `429`, or unreachable |

`health` hits `GET /health` for a liveness probe. There's no `close`: the
default transport opens one connection per request.

## Webhook receiver helper

Every receiver of Ankusa's HTTP sink needs the same handful of headers off each
request; `Ankusa.parse_headers` replaces the hand-rolled
`env["HTTP_X_ANKUSA_ID"]` lookups with one call and a typed result. Names are
matched case-insensitively, so it takes anything from a lowercased header hash
to a Rack env mapped once:

```ruby
require "ankusa/sdk"

# Rack carries these as HTTP_X_ANKUSA_* (content-type as CONTENT_TYPE):
# parse_headers downcases what it is given, but it does not strip Rack's
# HTTP_ prefix or its underscores.
def header_map(env)
  headers = env.filter_map do |key, value|
    [key.delete_prefix("HTTP_").tr("_", "-"), value] if key.start_with?("HTTP_")
  end.to_h
  headers["content-type"] = env["CONTENT_TYPE"] if env["CONTENT_TYPE"]
  headers
end

def call(env)
  hook =
    begin
      Ankusa.parse_headers(header_map(env))
    rescue Ankusa::MissingHookIdError
      return [400, {}, []]
    end

  body = env["rack.input"].read
  # hook.id, hook.source, hook.tenant, hook.content_type
  [200, {}, []]
end
```

`HookHeaders#idempotency_key` is what a receiver dedupes on, not the hook id
(`x-ankusa-id` stays required): delivery is at-least-once (see
"HTTP handoff" in
[`docs/integrations.md`](https://github.com/jamescarr/ankusa/blob/main/docs/integrations.md)),
so the same hook can arrive twice after a retry. When the source extracted the
provider's own event key, `HookHeaders#dedupe_key` carries it (and `replay_id`
marks a replay); `HookHeaders#idempotency_key` returns the tenant-scoped key
Ankusa computed and shipped in `x-ankusa-idempotency-key` — see "Consuming
queue messages". Header lookup is always case-insensitive, regardless of
whether the mapping passed in already is.

### Verifying signed deliveries

An HTTP sink with a `secret` signs every delivery the
[Standard Webhooks](https://www.standardwebhooks.com/) way:

```ruby
# Inside a Rack app's `call(env)`, before anything parses the body:
begin
  body = env["rack.input"].read # the raw request body, exactly as received
  Ankusa.verify_signature(header_map(env), body, [ENV.fetch("ANKUSA_WHSEC")])
rescue Ankusa::InvalidSignatureError => e
  return [401, {"content-type" => "application/json"},
    [JSON.generate(error: "invalid_signature", code: e.code)]]
end
```

Secrets are `whsec_` + base64, or any other string used as its own bytes;
pass several during a rotation. `tolerance_seconds:` (default 300) bounds the
`webhook-timestamp` window. Failures are `Ankusa::InvalidSignatureError` with
`code` and `field`, never retryable.

## Consuming queue messages

Every message sink — RabbitMQ, Kafka, NATS, Redis — delivers one JSON message
per hook (the HTTP sink sends the original body plus `x-ankusa-*` headers
instead):
the identity fields, the body (inline `body_base64` or a claim-check `claim`),
`sha256`, and, when present, `dedupe_key`, `replay_id`, `idempotency_key` and
the forwarded provider `headers`. `Ankusa.decode_message` validates all of it
and `Message#idempotency_key` gives the value to store in a processed-ids
table:

```ruby
require "ankusa/sdk"

CLAIM_CHECK = Ankusa::ClaimCheckClient.new(ENV.fetch("CLAIM_CHECK_URL", "http://localhost:4001"))
DB = PG.connect(ENV.fetch("DATABASE_URL"))
DB.exec("CREATE TABLE IF NOT EXISTS processed (key text primary key)")

def consume(raw)
  begin
    message = Ankusa.decode_message(raw)
  rescue Ankusa::InvalidMessageError => e
    # Poison message: dead-letter, never requeue. `e.retryable?` is false;
    # `e.code` is e.g. "integrity"; `e.field` names the offending key.
    raise
  end

  key = message.idempotency_key   # the key Ankusa shipped (tenant:source_id:dedupe_key, else id)
  return if DB.exec_params("SELECT 1 FROM processed WHERE key = $1", [key]).any?

  body = message.claim ? CLAIM_CHECK.redeem(message.claim, message.sha256) : message.body
  handle(body)
  DB.exec_params("INSERT INTO processed (key) VALUES ($1)", [key])
end
```

- `idempotency_key` is the key Ankusa computed once for the hook:
  `tenant:source_id:dedupe_key` when the source extracted the provider's own
  event key, else `id` — so a provider retry that arrives with a fresh Ankusa
  `id` still collapses onto the same row, and two tenants that share a provider
  event id do not. For a message from a node that predates the field the
  helper computes the same key itself (tenant `default` when there is none).
  The decoded field alone is `message.to_h[:idempotency_key]`.
- A replayed delivery is dropped by default. To reprocess replays instead, pass
  `include_replay: true`: the key then ends in `#replay:<replay_id>`.
- `message.body` is the decoded inline bytes (nil for the claim form);
  `message.headers` holds the forwarded provider request headers (lowercased).
  `message.sha256` is checked against the inline bytes by `decode_message`
  itself, and against the claim's bytes by `CLAIM_CHECK.redeem`.
- `decode_message` raises `InvalidMessageError` before you act on a message:
  bad JSON, a non-object, an unsupported `v`, a bad field type, both or neither
  body form, invalid base64, a length/sha256 mismatch, or a claim whose tenant
  differs from `tenant_id`.

A webhook receiver can take the same shortcut straight off the HTTP sink's
headers with `Ankusa.parse_headers(headers).idempotency_key`, which reads
`x-ankusa-idempotency-key` (falling back to computing it, where `source` and
`tenant` play `source_id` and `tenant_id`).

When a sink has grown a backlog, or a downstream processor failed after the
sink accepted a batch, re-drive it with a replay job over the admin client (see
"Admin client" below): `"kind" => "dlq"` re-sends rows that dead-lettered,
`"kind" => "archive"` re-sends hooks over a time window, `"kind" => "quarantine"`
re-verifies held hooks and releases those that pass. Replays keep the
original `id` and `dedupe_key` and add `replay_id`.

## Routes client

Manage route definitions and the global IP rules on the route-management
listener (`routes.admin.port`, default 4003) — the `routes` tag of
[`priv/openapi/admin.v1.yaml`](../ankusa/priv/openapi/admin.v1.yaml).

```ruby
require "ankusa/sdk"

routes = Ankusa::RoutesClient.new(ENV.fetch("ROUTES_URL", "http://localhost:4003"))

routes.create_route({"id" => "stripe", "path" => "/webhooks/stripe"})
routes.get_ip_rules   # {"default" => "allow", "rules" => []}
routes.test_route({"method" => "POST", "path" => "/webhooks/stripe", "ip" => "203.0.113.7"})
```

Methods: `health`, `list_routes`, `create_route`, `get_route`, `replace_route`,
`update_route`, `delete_route`, `get_ip_rules`, `put_ip_rules`, `test_route`.
Route ids are percent-encoded as one path segment, so `/`, `?`, `#`, `%` and a
space in an id can't reshape the URL.

Failures are `RoutesError` subclasses: `InvalidRouteIdError` (an id that isn't a
string, is empty, or is `.`/`..` — raised before any request, because a URL
parser would otherwise normalize it into the collection endpoint and hand back
the list page as if it were a route), `RouteNotFoundError` (404),
`RoutesRejectedError` (any other 4xx, carrying `code`, `field`, `message`,
`conflicting_id`, `max_routes`), and `RoutesUnavailableError` (5xx, an
unfollowed redirect, a non-JSON success body, or unreachable; retryable).

## Admin client

The operator API on `admin.port` (default 4002): health, Prometheus metrics, the
redacted config, the DLQ, replay jobs, and the quarantine list — the
`operations`, `dlq`, `replays`, and `quarantine` tags of `admin.v1.yaml`.

```ruby
require "ankusa/sdk"

admin = Ankusa::AdminClient.new(ENV.fetch("ADMIN_URL", "http://localhost:4002"))

admin.health                          # {"status" => "ok", "instance" => ..., "roles" => [...]}
admin.list_dead_letters({"limit" => 10})  # {"total" => ..., "entries" => [...]}
admin.list_quarantined

job = admin.create_replay({"kind" => "dlq", "source_id" => "stripe", "rate" => 500})
admin.get_replay(job["id"])
admin.list_replays                    # {"replays" => [Replay, ...]}, newest first
admin.update_replay(job["id"], {"state" => "paused"})   # resume, pause or cancel
```

`create_replay` is idempotent for retries: a second POST of the same filter
while the job is `running`/`paused` returns the existing job (`200`) instead of
starting another (`202`). `list_replays` returns `{"replays" => [...]}` exactly
as the API sends it.

Methods: `health`, `metrics` (Prometheus text), `config`, `list_dead_letters`,
`create_replay`, `get_replay`, `list_replays`, `update_replay`,
`list_quarantined`. Failures are `AdminError` subclasses: `RoleNotEnabledError`
(409 `role_not_enabled`, carrying `role`), `AdminRejectedError` (any other 4xx —
a missing replay is a 404 with `code` `replay_not_found`), and
`AdminUnavailableError` (5xx, an unfollowed redirect, or unreachable;
retryable).

## Sources client

Ankusa's ingest sources are tenant-scoped: a source is addressed as
`<tenant>.<name>`, and `Ankusa.Admin.Router` serves their CRUD API on the same
`admin.port` as the operator API. `SourcesClient` speaks in those terms and
builds the paths for you.

```ruby
require "ankusa/sdk"

sources = Ankusa::SourcesClient.new(ENV.fetch("ADMIN_URL", "http://localhost:4002"))

sources.list_sources("acme")                                   # [Source, ...]
sources.get_source("acme", "billing")                          # Source
sources.create_source("acme", "billing", Ankusa::SourceSpec.new(sinks: [{"type" => "log"}]))
sources.update_source(
  "acme",
  "billing",
  Ankusa::SourceSpec.new(sinks: [{"type" => "log"}], on_verify_failure: "reject")
)
sources.delete_source("acme", "billing")
```

Methods: `server_version`, `list_sources`, `get_source`, `create_source`,
`update_source`, `delete_source`. A write takes the whole spec (`SourceSpec`,
whose `to_request_body` omits unset fields); a read returns a `Source`, which is
always redacted — resending a read-back `verify` map is not the same as
resending the stored secret, so supply secrets through `SourceSpec`.

`expected_version: "0.3.0"` is an optional latch: the first API call fetches
`GET /health`, compares its `"version"` field, and raises
`VersionMismatchError` on a mismatch (the version is cached afterwards, so no
further request checks it). Failures are `SourcesError` subclasses, each
carrying `.status` and `.body`: `SourceNotFoundError` (404),
`SourceConflictError` (409), `SourceStoreReadOnlyError` (409 — the deployment's
source store is a static seed), `SourceInvalidError` (400, or an invalid
tenant/name caught before any request), `VersionMismatchError`, and
`SourcesUnavailableError` (unreachable, timed out, or 5xx).

Tenants and source names must match `[A-Za-z0-9_-]{1,64}`; anything else raises
`SourceInvalidError` before a path is built.

## Layout

```
lib/ankusa/
  sdk.rb                # entry point: require "ankusa/sdk"
  sdk/version.rb
  error.rb              # Ankusa::Error, the root of every SDK error
  transport.rb          # the injectable transport hook + the default Net::HTTP one
  connection.rb         # shared URL/query/body/JSON rules (@api private)
  webhook.rb            # x-ankusa-* header parsing for HTTP-sink receivers
  claim_check.rb        # the claim-check gateway client
  message.rb            # decode the v1 queue message + the idempotency key
  routes.rb             # the route-management client (routes.admin.port)
  admin.rb              # the operator client (admin.port)
  sources.rb            # the tenant-scoped source-management client (admin.port)
test/
  test_helper.rb
  conformance_test.rb     # runs the language-neutral vectors in conformance/
  routes_test.rb
  admin_test.rb
  sources_test.rb
```

## Develop

```sh
bundle install
bundle exec rake              # tests + standard
bundle exec rake conformance  # just the conformance vectors
```
