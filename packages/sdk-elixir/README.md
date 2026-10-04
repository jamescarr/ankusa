# ankusa_sdk

The Elixir client SDK for [Ankusa](https://github.com/jamescarr/ankusa)
deployments: everything an Elixir consumer needs on the *subscribe* side of the
delivery, plus clients for the operator APIs.

- **Receive `Ankusa.Sink.Http` deliveries** with a `Plug`
  (`Ankusa.SDK.Receiver`) that hands each hook to your handler module.
- **Decode queue messages** (`Ankusa.SDK.Message`) published by
  `Ankusa.Sink.RabbitMQ`/`Kafka`/`NATS`/`Redis`, and redeem the claim check
  when the payload was too large to ride inline. The SDK ships **no** broker
  client: bring your own Broadway/AMQP/brod/gnat/Redix consumer.
- **Redeem claim checks** (`Ankusa.SDK.ClaimCheck`) with the end-to-end sha256
  check the gateway itself does not run.
- **Drive the routes, admin, and sources APIs** (`Ankusa.SDK.Routes`,
  `Ankusa.SDK.Admin`, `Ankusa.SDK.Sources`).

The SDK never depends on `ankusa` core and never defines the bare `Ankusa`
module, so an app may load both.

```text
HTTP sink     Ankusa --> Ankusa.SDK.Receiver ---------------------+
                                                                  |
Queue sinks   Ankusa --> your consumer --> Message.decode/1       +--> MyApp.Hooks.handle_hook/2
                                           Message.to_hook/2 -----+
                                           (redeems a claim through Ankusa.SDK.ClaimCheck)
```

## Install

```elixir
# mix.exs
def deps do
  [
    {:ankusa_sdk, "~> 0.3"}
  ]
end
```

Everything the SDK exposes lives under `Ankusa.SDK.`.

## Claim-check client

Redeem a claim-check ref, verify the bytes it returns against the message's
`sha256`, and classify failures into dead-letter vs. retry — without holding any
object-store credentials.

```elixir
claim_check = Ankusa.SDK.ClaimCheck.new(ENV.fetch("CLAIM_CHECK_URL", "http://localhost:4001"))

# claim and sha256 are the queue message's fields:
#   "claim":  "urn:ankusa:claim:v1:<tenant>:<claim_id>"  (claim_id: uppercase ULID)
#   "sha256": 64-char lowercase hex of the claim's bytes
case Ankusa.SDK.ClaimCheck.redeem(claim_check, claim, sha256) do
  {:ok, body} ->
    handle(body)

  {:error, %{retryable: false} = error} ->
    # bad ref/sha256, 404, or an integrity mismatch: dead-letter, don't requeue
    dead_letter(error)

  {:error, %{retryable: true} = error} ->
    # gateway unreachable or 5xx: safe to retry
    requeue(error)
end
```

`redeem/3` does three things a bare `GET /v1/claims/...` doesn't:

1. Parses the ref into its tenant, claim id, and gateway path
   (`Ankusa.SDK.ClaimRef.parse/1`, also usable on its own).
2. Fetches the bytes.
3. Verifies them against the message's `sha256` (the gateway itself does not
   check this — see "Redeem a claim" in
   [`docs/claim-check.md`](https://github.com/jamescarr/ankusa/blob/main/docs/claim-check.md))
   before returning them.

Every failure carries `retryable`, so a consumer needs exactly one bit:

| Error | `retryable` | Cause |
| --- | --- | --- |
| `Ankusa.SDK.InvalidClaimRefError` | `false` | `ref` isn't a well-formed claim-check URN, or `sha256` isn't 64-char lowercase hex |
| `Ankusa.SDK.ClaimNotFoundError` | `false` | gateway `404`: expired by retention, or never written |
| `Ankusa.SDK.ClaimRejectedError` | `false` | gateway `4xx` other than `404` (`:status`, `:body`) |
| `Ankusa.SDK.ClaimIntegrityError` | `false` | sha256 of the returned bytes doesn't match |
| `Ankusa.SDK.ClaimCheckUnavailableError` | `true` | gateway unreachable, or answered anything else |

`health/1` hits `GET /health` for a liveness probe.

## Webhook receiver

`Ankusa.SDK.Receiver` is a `Plug` that reads the raw body, parses the
`x-ankusa-*` headers, calls your handler, and answers `202`, `503`, or `4xx`
matching what `Ankusa.Sink.Http` treats as success.

```elixir
defmodule MyApp.Hooks do
  @behaviour Ankusa.SDK.Handler

  @impl Ankusa.SDK.Handler
  def handle_hook(%Ankusa.SDK.Hook{} = hook, _arg) do
    # Return :ok only once the hook is durably handled; delivery is
    # at-least-once, so dedupe on the idempotency key
    # (Ankusa.SDK.Idempotency.key/2), not on the arrival alone.
    case MyApp.Store.insert(hook.id, hook.body) do
      :inserted -> :ok
      :duplicate -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
```

Standalone (Bandit):

```elixir
# mix.exs: {:bandit, "~> 1.12"}
children = [
  {Bandit, plug: {Ankusa.SDK.Receiver, handler: MyApp.Hooks}, port: 4200}
]
```

In a Phoenix endpoint, mount the receiver **above** the body parsers:

```elixir
defmodule MyAppWeb.Endpoint do
  use Phoenix.Endpoint, otp_app: :my_app

  plug Ankusa.SDK.Receiver, path: "/deliveries", handler: MyApp.Hooks
  plug Plug.Parsers, parsers: [:json], json_decoder: JSON

  # … the rest of the endpoint
end
```

Options: `:handler` (a module, or `{module, arg}`), `:path` (only that path is
handled; everything else passes through untouched), and `:max_body_bytes`
(default `8_000_000`, the same cap core's ingest applies to a raw body).

The handler sees one value whichever transport delivered the hook:

| Field | HTTP | Queue |
| --- | --- | --- |
| `id` | `x-ankusa-id` | message `id` |
| `source_id` | `x-ankusa-source` | message `source_id` |
| `tenant_id` | `x-ankusa-tenant` | message `tenant_id` |
| `content_type` | `content-type` | message `content_type` |
| `body` | the raw request body | inline body, or the redeemed claim |
| `received_at` | `nil` (the HTTP sink doesn't send it) | message `received_at`, unix ms |
| `size` | `byte_size(body)` | message `size` |
| `dedupe_key` | `x-ankusa-dedupe-key` | message `dedupe_key` |
| `replay_id` | `x-ankusa-replay-id` | message `replay_id` |
| `idempotency_key` | `x-ankusa-idempotency-key` | message `idempotency_key` |
| `headers` | every request header, lowercased | the forwarded provider headers |

Dedupe on the key `Ankusa.SDK.Idempotency.key/2` returns (see "Consuming queue
messages" below), not on `id` alone: a provider retry can reach you as several
hook ids.

The header parsing on its own is `Ankusa.SDK.Webhook.parse_headers/1`, for
receivers built on something else.

## Queue messages

`Ankusa.SDK.Message.decode/1` parses the `Ankusa.Sink.Message` JSON that
RabbitMQ, Kafka, NATS, and Redis sinks publish; `Message.to_hook/2` turns it
into the same `Ankusa.SDK.Hook` the Plug delivers, redeeming the claim check
when the payload lived in the claim store. A `ClaimCheck` client is required
even for inline messages, so a consumer cannot forget it the day a payload
crosses the sink's `inline_max_bytes` (64 KiB by default).

A consumer of any broker looks the same:

```elixir
# A Broadway/AMQP/Redix callback, whatever your consumer calls it.
def handle_message(message_body, claim_check) do
  with {:ok, message} <- Ankusa.SDK.Message.decode(message_body),
       {:ok, hook} <- Ankusa.SDK.Message.to_hook(message, claim_check) do
    case MyApp.Hooks.handle_hook(hook, []) do
      :ok -> :ack
      {:error, _reason} -> :requeue
    end
  else
    # Undecodable bytes (InvalidMessageError), a bad ref, a 404, an integrity
    # mismatch: every one is `retryable: false` — dead-letter, don't requeue.
    {:error, %{retryable: false} = error} -> {:dlq, error}
    # An unreachable or 5xx gateway: safe to retry.
    {:error, %{retryable: true} = error} -> {:requeue, error}
  end
end
```

`decode/1` returns an `Ankusa.SDK.InvalidMessageError` with a stable `code`
(`"invalid_json"`, `"not_an_object"`, `"unsupported_version"`, `"invalid_field"`,
`"ambiguous_body"`, `"missing_body"`, `"invalid_body_base64"`,
`"size_mismatch"`, `"integrity"`, `"tenant_mismatch"`) and, when the code is
`"invalid_field"`, the offending `field`. It also verifies the inline body's
length against `size`, its bytes against `sha256`, and a claim's tenant against
`tenant_id`. Keys it doesn't know are ignored, so a newer producer adding a
field doesn't break an older consumer.

Kafka/NATS headers and the RabbitMQ routing key are not read: the JSON body
carries everything.

## Consuming queue messages

A queue consumer is at-least-once end to end, so the handler must make its
effect idempotent. Decode the message, compute the idempotency key, and
insert-or-ignore it into a processed-ids table before doing the work:

```elixir
defmodule MyApp.Consumer do
  alias Ankusa.SDK.{Idempotency, Message}

  def handle(raw, claim_check) do
    with {:ok, message} <- Message.decode(raw),
         {:ok, hook} <- Message.to_hook(message, claim_check),
         key = Idempotency.key(message) do
      case MyApp.Store.claim(key, hook.id) do
        :inserted -> MyApp.Hooks.handle_hook(hook, [])
        :duplicate -> :ok
      end
    else
      # Undecodable bytes and every bad-ref/404/integrity failure are
      # `retryable: false` — dead-letter, don't requeue.
      {:error, %{retryable: false} = error} -> {:dlq, error}
      # An unreachable or 5xx gateway is safe to retry.
      {:error, %{retryable: true} = error} -> {:requeue, error}
    end
  end
end
```

The key is the table's primary key, so the database enforces the dedupe:

```elixir
defmodule MyApp.Repo.Migrations.CreateProcessedHooks do
  use Ecto.Migration

  def change do
    create table(:processed_hooks, primary_key: false) do
      add :idempotency_key, :text, primary_key: true
      add :ankusa_id, :text, null: false
      add :processed_at, :utc_datetime_usec
    end

    create index(:processed_hooks, [:ankusa_id])
  end
end
```

`Ankusa.SDK.Idempotency.key/2` accepts the decoded `Message`, the parsed
`Ankusa.SDK.Webhook.Headers`, or a `Hook`. Ankusa computes the key once per hook
and ships it (the message's `idempotency_key`, the `x-ankusa-idempotency-key`
header), and `key/2` returns that value. It is `tenant:source_id:dedupe_key`
when the source extracted a provider event key (Stripe's `id`, GitHub's
`x-github-delivery`, ...) and the hook id otherwise, so a provider's own
retries collapse even when ingest minted a fresh hook id for each one, and two
tenants that share a provider event id stay apart. For a message or delivery
from a node that predates the field `key/2` computes the same key itself
(tenant `default` when there is none). A replay of an already-processed
delivery computes the same key and is dropped; pass `include_replay: true` when
the consumer must reprocess replays:

```elixir
key = Ankusa.SDK.Idempotency.key(message, include_replay: true)
```

## Routes client

Manage route definitions and the global IP rules on the route-management
listener (`routes.admin.port`, default `4003`).

```elixir
routes = Ankusa.SDK.Routes.new(ENV.fetch("ROUTES_URL", "http://localhost:4003"))

{:ok, page} = Ankusa.SDK.Routes.list_routes(routes, enabled: true, limit: 10)
{:ok, _} = Ankusa.SDK.Routes.create_route(routes, %{"id" => "stripe", "path" => "/webhooks/stripe"})
{:ok, rules} = Ankusa.SDK.Routes.get_ip_rules(routes)
```

Functions: `health/1`, `list_routes/2`, `create_route/2`, `get_route/2`,
`replace_route/3`, `update_route/3`, `delete_route/2`, `get_ip_rules/1`,
`put_ip_rules/2`, `test_route/2`. Route ids are percent-encoded as one path
segment, so `/`, `?`, `#`, `%` and a space in an id can't reshape the URL; an
id that isn't a string, is empty, or is `.`/`..` is refused with
`Ankusa.SDK.InvalidRouteIdError` before any request is sent.

Failures: `InvalidRouteIdError`, `RouteNotFoundError` (`404`),
`RoutesRejectedError` (any other `4xx`, carrying `:code`, `:field`, `:message`,
`:conflicting_id`, `:max_routes`), and `RoutesUnavailableError` (`5xx`, an
unfollowed redirect, a non-JSON success body, or unreachable; retryable).

## Admin client

The operator API on `admin.port` (default `4002`): health, Prometheus metrics,
the redacted config, the DLQ, and the quarantine list.

```elixir
admin = Ankusa.SDK.Admin.new(ENV.fetch("ADMIN_URL", "http://localhost:4002"))

{:ok, _} = Ankusa.SDK.Admin.health(admin)
{:ok, text} = Ankusa.SDK.Admin.metrics(admin)
{:ok, %{"total" => total}} = Ankusa.SDK.Admin.list_dead_letters(admin, limit: 10)
{:ok, _} = Ankusa.SDK.Admin.list_quarantined(admin)

# Replay jobs: re-send the dead-letter queue, or an archived time window.
{:ok, replay} = Ankusa.SDK.Admin.create_replay(admin, %{"kind" => "dlq", "rate" => 500})
{:ok, replay} = Ankusa.SDK.Admin.get_replay(admin, replay["id"])
{:ok, %{"replays" => replays}} = Ankusa.SDK.Admin.list_replays(admin)
{:ok, paused} = Ankusa.SDK.Admin.update_replay(admin, replay["id"], %{"state" => "paused"})
```

Failures: `RoleNotEnabledError` (`409 role_not_enabled`, carrying `:role`),
`AdminRejectedError` (any other `4xx`, carrying `:code` — a missing job is
`404 replay_not_found`), and `AdminUnavailableError` (`5xx`, an unfollowed
redirect, a non-JSON success body, or unreachable; retryable).

## Sources client

Sources are tenant-scoped: a source is addressed as `<tenant>.<name>`, and
`Ankusa.Admin.Router` serves their CRUD API on the same `admin.port` as the
operator API.

```elixir
sources = Ankusa.SDK.Sources.new(ENV.fetch("ADMIN_URL", "http://localhost:4002"))

{:ok, list} = Ankusa.SDK.Sources.list_sources(sources, "acme")
{:ok, source} = Ankusa.SDK.Sources.get_source(sources, "acme", "billing")

spec = %Ankusa.SDK.Sources.Spec{sinks: [%{"type" => "log"}]}
{:ok, _} = Ankusa.SDK.Sources.create_source(sources, "acme", "billing", spec)
{:ok, _} = Ankusa.SDK.Sources.update_source(sources, "acme", "billing", spec)
:ok = Ankusa.SDK.Sources.delete_source(sources, "acme", "billing")
```

A read returns an `Ankusa.SDK.Sources.Source`, which is always redacted —
resending a read-back `verify` map is not the same as resending the stored
secret, so supply secrets through `Ankusa.SDK.Sources.Spec`.

`expected_version: "0.3.0"` is an optional latch: the client is immutable, so
run `Sources.verify_version/1` once at startup and keep the returned client;
every later call then re-checks the cached version without another `/health`
request, and a mismatch is an `Ankusa.SDK.VersionMismatchError` before any API
call.

```elixir
{:ok, sources} =
  Ankusa.SDK.Sources.new("http://localhost:4002", expected_version: "0.3.0")
  |> Ankusa.SDK.Sources.verify_version()
```

Failures, all carrying `:status` and `:body`: `SourceNotFoundError` (`404`),
`SourceConflictError` (`409`), `SourceStoreReadOnlyError` (`409` — the
deployment's source store is a static seed), `SourceInvalidError` (`400`, or an
invalid tenant/name caught before any request), `VersionMismatchError`, and
`SourcesUnavailableError` (unreachable, timed out, or `5xx`). Tenants and names
must match `[A-Za-z0-9_-]{1,64}`.

## Layout

```
lib/ankusa/sdk/
  http.ex                    # the shared Req layer (@moduledoc false)
  errors.ex                  # every exception the SDK returns
  hook.ex                    # one delivered hook, either transport
  handler.ex                 # the behaviour a handler implements
  webhook.ex                 # x-ankusa-* header parsing
  receiver.ex                # the Plug that receives HTTP-sink deliveries
  message.ex                 # queue wire format: decode + to_hook
  idempotency.ex             # the idempotency key for any delivery
  claim_ref.ex               # urn:ankusa:claim:v1:<tenant>:<claim_id>
  claim_check.ex             # the claim-check gateway client
  routes.ex                  # the route-management client
  admin.ex                   # the operator client
  sources.ex                 # the tenant-scoped source client
  sources/spec.ex            # the writable spec
  sources/source.ex          # a stored, redacted source
test/
  conformance_test.exs       # runs the language-neutral vectors in conformance/
  support/                   # the mock gateway, transports, recorder
```

## Develop

```sh
mix deps.get
mix test                      # unit suites + the conformance vectors
mix test test/conformance_test.exs   # just the vectors
```

The same vectors every other SDK runs live in
[`conformance/`](https://github.com/jamescarr/ankusa/tree/main/conformance);
`mise run check:conformance` runs them all.
