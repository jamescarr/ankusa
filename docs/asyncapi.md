# AsyncAPI and lifecycle events

A running Ankusa publishes delivered hooks to Kafka, RabbitMQ, NATS, or Redis.
It describes where, in [AsyncAPI 3.0](https://www.asyncapi.com/docs/reference/specification/v3.0.0),
so a consumer can ask the instance instead of reading its YAML. It can also
announce when a webhook endpoint or route is created, updated, or deleted, on a
channel the same document describes.

## The document

With `admin.enabled: true`:

```sh
curl -s localhost:4002/asyncapi.json
# content-type: application/asyncapi+json
```

It is built from the configuration as it is *now*: a source created through the
admin API a second ago is in it. It carries no credentials: no URL userinfo, no
SASL password, no header value.

| AsyncAPI part | One per | Example |
| --- | --- | --- |
| server | distinct broker (`protocol`, `host`, `pathname`) | `kafka` at `redpanda:9092` |
| channel | distinct address on a server | topic `ankusa.hooks` |
| message | source publishing to the channel | `stripe`, with its tenant and Kafka record key |
| operation | channel | `send`: Ankusa is the publisher |

Every message's payload is [`SinkMessageV1`](delivery.md): the `Ankusa.Sink.Message`
envelope (`id`, `source_id`, `tenant_id`, `received_at`, `content_type`, `size`,
`sha256`, `dedupe_key`, `replay_id`, `idempotency_key`, `verification`, `headers`,
and `body_base64` or `claim`), narrowed to the source. The provider's own body is opaque
bytes inside it; the document does not describe it.

What a consumer learns per transport:

| Sink | Address | Bindings and headers |
| --- | --- | --- |
| `kafka` | the `topic` | record key (`<tenant>/<source_id>` unless `key:` is set), six always-present headers (`ankusa_id`, `ankusa_source_id`, `ankusa_tenant_id`, `ankusa_message_version`, `ankusa_idempotency_key`, `content_type`) plus optional `ankusa_dedupe_key` and `ankusa_replay_id` |
| `rabbitmq` | the routing key (`ankusa.<source_id>` unless `routing_key:` is set) | exchange name, type, vhost; the document advertises no headers, though the wire carries AMQP `message_id` and an `ankusa_idempotency_key` header (plus optional `ankusa_dedupe_key` and `ankusa_replay_id`), which it does not describe |
| `nats` | the `subject` | the same six always-present headers and optional `ankusa_dedupe_key`/`ankusa_replay_id` as Kafka, plus `Nats-Msg-Id` |
| `redis` | the `channel` | none: pub/sub has no headers |
| `sqs` | the queue name (`hooks.fifo`) | the `sqs` channel binding (`queue.name`, `queue.fifoQueue`); the document advertises no headers, though each message carries the Kafka header set as SQS message attributes (`ankusa_tenant_id` only when the hook has a tenant), which it does not describe |
| `google_pubsub` | `projects/<project>/topics/<topic>` | the `googlepubsub` channel binding, plus the `orderingKey` message binding when a static `ordering_key` is set; the document advertises no headers, though each message carries the Kafka header set as Pub/Sub message attributes (`ankusa_tenant_id` only when the hook has a tenant), which it does not describe |

Kafka is one topic with the source in the record key, not a topic per source, so
two Kafka sources on one topic are one channel with two messages. When a
function (an embedding-only option) computes the address per hook, there is
nothing to advertise: the channel has no address and says so. `log` and `http`
sinks are not channels and are left out (an `http` sink's description says it
carries the provider's raw body, not the envelope this document describes).

Validate it with the AsyncAPI CLI:

```sh
curl -s localhost:4002/asyncapi.json -o asyncapi.json
docker run --rm -v "$PWD/asyncapi.json:/app/asyncapi.json" asyncapi/cli validate /app/asyncapi.json
```

AsyncAPI Studio and the AsyncAPI generators read the file as is: JSON is YAML 1.2.

## Lifecycle events

Off by default. Give the instance sinks to deliver them to:

```yaml
lifecycle:
  sinks:
    - {type: kafka, brokers: ["redpanda:9092"], topic: ankusa.lifecycle}
```

(`config.lifecycle.sinks` in Elixir: the same `[{module, opts}]` a source takes.)
Every change made through the admin API's source endpoints, the
route-management API, or `Ankusa.SourceStore.put/5`, `delete/3` and
`Ankusa.Routes` becomes one event:

| `type` | When | `subject` |
| --- | --- | --- |
| `io.ankusa.source.created` / `updated` / `deleted` | a webhook endpoint (source) changes | `<tenant>.<name>` |
| `io.ankusa.route.created` / `updated` / `deleted` | a route changes (`PUT` on a new id is `created`) | the route id |
| `io.ankusa.tenant.deleted` | a tenant was erased ([`multi-tenancy.md`](multi-tenancy.md#erasing-a-tenant)), after the `deleted` event of each of its sources and routes; `data` is `{"tenant": <id>}`, no entity | the tenant id |

The body is a [CloudEvents 1.0](https://cloudevents.io) structured-mode event:
`specversion`, `id`, `source` (`urn:ankusa:instance:<instance>`), `type`,
`subject`, `time` (when the change committed), `datacontenttype`, and `data`:
the entity exactly as the admin API returns it, secrets redacted
(`"secret": "[REDACTED]"`), as the change left it. For a deletion, `data` is
the whole entity as it was the moment before (a source's last view, the
route's definition). These attributes say which change an event is:

| attribute | |
| --- | --- |
| `id` | derived from the change, never minted: the same event has the same `id` however often it is sent |
| `revision` | the subject's change counter, `1` at creation; absent for a store that keeps none (the Redis source store) |
| `actor` | the principal id that made the change (`system` for code) |
| `instance` | the instance name |
| `sequence` | the instance's audit sequence number, a decimal string: events of one instance are totally ordered by it |
| `replayid` | only on an event a replay job re-sent |

The event travels as the `body_base64` of an ordinary `SinkMessageV1` (so the
`content_type` is `application/cloudevents+json`, and the document names the
decoded shape, `LifecycleEventV1`, under the message's `x-ankusa-body`):

```sh
rpk topic consume ankusa.lifecycle -n 1 -f '%v\n' | jq -r .body_base64 | base64 -d | jq .type
# "io.ankusa.source.created"
```

Lifecycle events are durable and ordered. They are not hooks and do not go
through dispatch; each one is the change's audit row
([`Ankusa.Audit`](architecture.md)), flagged for publication and committed in
the same synced batch as the change, with the redacted entity beside it. A
supervised publisher, on every node that has the store, reads those rows in
commit order, keeps the last one that every lifecycle sink has dealt with in the
store (`m:lifecycle_cursor`, synced on every advance), and hands each row to
every lifecycle sink independently:

- A `kill -9` right after the change returns loses neither: the event is
  delivered after the restart. Delivery is **at least once**: a node killed
  after a sink confirmed an event and before the cursor moved sends it again,
  and so does a replay. Every send of one event carries the same `id`, so
  dedupe on it.
- Events arrive in the order the changes committed, and each carries the
  subject's `revision`: three rapid updates of one source arrive as revisions
  *n*, *n*+1, *n*+2. A consumer that keeps the highest `revision` it has seen
  per `subject` and ignores a lower one is never moved backwards by a late
  duplicate.
- A failed delivery is retried with the `dispatch.retry` policy, exponential by
  default. The cursor moves only once every sink confirmed the event or gave up
  on it, so one failing sink holds back the events behind it, for every sink,
  until it confirms or its retries run out. Then the event is dropped *for that
  sink*, logged, and counted as `ankusa_lifecycle_dropped_total` (label
  `reason`: `gave_up`, or `unreadable` for an audit row or entity that could not
  be read). `ankusa_lifecycle_delivered_total` counts the rest. The caller
  always gets the success of its change.
- `POST /v1/replays` with `{"kind": "lifecycle", "since": <ms>, "until": <ms>,
  "tenant": "<tenant>"}` (every key optional; route events belong to tenant
  `default`) re-emits the events the trail held when the job started, over the
  window of the changes' own time. It is how a sink that was down longer than
  the retries wait, or lost events, catches up. A replayed event keeps its `id`
  and adds `replayid`. The job runs on a node with the `dispatch` role and a
  lifecycle sink, at `rate` events per second, and tries a sink three times
  before it counts the delivery as `dead`.
- Turning lifecycle events on starts at the end of the trail: what changed
  while they were off is not announced. The trail, and the entity of each
  event, are kept for `audit.retention_days`; a sink that is offline for longer
  than that loses the older events.
- A source in the Redis store and a route are a dual write (the change lives
  outside the node's store). A node killed halfway leaves a pending audit row,
  and the next boot settles it from the subject's present state, not by guess:
  if the state is the one the change was heading to, it is announced; if it is
  the one it started from, it is not; if it is neither (someone changed it
  meanwhile) a state-refresh event, an `updated` (or `deleted`) one carrying
  what is there now, is announced.
- The edge never resolves `ankusa:lifecycle`: `POST /webhooks/ankusa:lifecycle`
  is a `404`, and a configured source of that id is a boot error.
- The event is not listed by `GET /v1/tenants/{tenant}/sources`, never passes
  through ingest (so the tenant rate limiter and the quarantine never see it and
  `ankusa_ingest_*` does not count it), and is not archived. Source events carry
  the source's tenant; route events carry the route's tenant (`tenant_id`, which
  is `default` for a route that belongs to none), and a claim-checked body needs
  a real tenant. An event over a sink's
  `inline_max_bytes` goes through the claim check inside the sink, like a hook.

Only the node that served the change emits, so a route written on one node of a
Redis-backed fleet is announced once, from that node's trail.

## Describing your own sink

A messaging sink advertises its channel by implementing the optional
`c:Ankusa.Sink.describe/2` callback, returning an `Ankusa.Sink.Description`:

```elixir
@impl true
def describe(%{source_id: source_id, tenant_id: _tenant}, opts) do
  %Ankusa.Sink.Description{
    protocol: "mqtt",
    host: "broker.internal:1883",
    address: "hooks/#{source_id}",
    ankusa_headers: false
  }
end
```

`tenant_id` is `nil` when the tenant varies per hook (a resolver that reads it
from the URL). A description must not carry credentials.

## The library: `async_api_spex`

The document is built with [`async_api_spex`](https://hexdocs.pm/async_api_spex),
a small, dependency-free library in this repo for any Elixir app: AsyncAPI 3.0
structs, `use AsyncApiSpex.Schema` and `use AsyncApiSpex.Message` to declare
payloads once and reference them by name (`Schema` can also derive a schema from
an existing struct), `use AsyncApiSpex.Channel` and `use AsyncApiSpex.Spec` to
declare a publishing module's topic and broker and assemble the document from
those modules, a validator, a Plug
(`AsyncApiSpex.Plug.RenderSpec`) that serves a document as
`application/asyncapi+json`, another (`AsyncApiSpex.Plug.Viewer`) that renders
it as a browsable HTML page, and `mix async_api_spex.gen` to write one to a file.
Its README shows an app declaring its channels.

`Ankusa.AsyncApi.document/1` returns the `%AsyncApiSpex.Document{}` for an
instance; an embedder can serve it from their own router with
`AsyncApiSpex.Plug.RenderSpec.send_spec/2`, or extend it before encoding.
