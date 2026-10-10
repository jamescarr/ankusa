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
`sha256`, `dedupe_key`, `replay_id`, `idempotency_key`, `headers`, and `body_base64`
or `claim`), narrowed to the source. The provider's own body is opaque
bytes inside it; the document does not describe it.

What a consumer learns per transport:

| Sink | Address | Bindings and headers |
| --- | --- | --- |
| `kafka` | the `topic` | record key (`<tenant>/<source_id>` unless `key:` is set), six always-present headers (`ankusa_id`, `ankusa_source_id`, `ankusa_tenant_id`, `ankusa_message_version`, `ankusa_idempotency_key`, `content_type`) plus optional `ankusa_dedupe_key` and `ankusa_replay_id` |
| `rabbitmq` | the routing key (`ankusa.<source_id>` unless `routing_key:` is set) | exchange name, type, vhost; the document advertises no headers, though the wire carries AMQP `message_id` and an `ankusa_idempotency_key` header (plus optional `ankusa_dedupe_key` and `ankusa_replay_id`), which it does not describe |
| `nats` | the `subject` | the same six always-present headers and optional `ankusa_dedupe_key`/`ankusa_replay_id` as Kafka, plus `Nats-Msg-Id` |
| `redis` | the `channel` | none: pub/sub has no headers |
| `sqs` | the queue name (`hooks.fifo`) | the `sqs` channel binding (`queue.name`, `queue.fifoQueue`); the document advertises no headers, though each message carries the Kafka header set as SQS message attributes (`ankusa_tenant_id` only when the hook has a tenant), which it does not describe |

Kafka is one topic with the source in the record key, not a topic per source, so
two Kafka sources on one topic are one channel with two messages. When a
function (an embedding-only option) computes the address per hook, there is
nothing to advertise: the channel has no address and says so. `log` and `http`
sinks are not channels and are left out.

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

The body is a [CloudEvents 1.0](https://cloudevents.io) structured-mode event:
`specversion`, `id`, `source` (`urn:ankusa:instance:<instance>`), `type`,
`subject`, `time`, `datacontenttype`, and `data`: the entity exactly as the
admin API returns it, secrets redacted (`"secret": "[REDACTED]"`). For a
deletion, `data` is the last view of a source, or `{"id": ...}` for a route.

The event travels as the `body_base64` of an ordinary `SinkMessageV1` (so the
`content_type` is `application/cloudevents+json`, and the document names the
decoded shape, `LifecycleEventV1`, under the message's `x-ankusa-body`):

```sh
rpk topic consume ankusa.lifecycle -n 1 -f '%v\n' | jq -r .body_base64 | base64 -d | jq .type
# "io.ankusa.source.created"
```

Lifecycle events bypass the store. They are not hooks: nothing is written to the
store, and they do not go through dispatch. A supervised in-memory publisher,
running on every node that has a lifecycle sink, hands each event to every
lifecycle sink independently, so a broker that is down never holds back another.
It is best effort:

- The change has already happened, and the call that made it returns without
  waiting on a broker. A failed delivery is retried with the `dispatch.retry`
  policy, exponential by default. When the retries run out, when the publisher's
  queue is full (10,000 pending sink deliveries), or when the publisher is not
  running, the event is dropped for that sink, logged, and counted as
  `ankusa_lifecycle_dropped_total` (label `reason`: `gave_up`, `queue_full`,
  `not_running`). `ankusa_lifecycle_delivered_total` counts the rest. The caller
  always gets the success of its change.
- Pending events are lost when the node stops, and there is no ordering:
  deliveries run concurrently and retries reorder them. A consumer that needs a
  complete picture reads the admin API's source and route lists.
- The edge never resolves `ankusa:lifecycle`: `POST /webhooks/ankusa:lifecycle`
  is a `404`, and a configured source of that id is a boot error.
- The event is not listed by `GET /v1/tenants/{tenant}/sources`, never passes
  through ingest (so the tenant rate limiter and the quarantine never see it and
  `ankusa_ingest_*` does not count it), and is not archived. Source events carry
  the source's tenant; route events carry the default tenant, `default`, because
  a claim-checked body needs a real tenant. An event over a sink's
  `inline_max_bytes` goes through the claim check inside the sink, like a hook.

Only the node that served the change emits, so a route written on one node of a
Redis-backed fleet is announced once.

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
`application/asyncapi+json`, and `mix async_api_spex.gen` to write one to a file.
Its README shows an app declaring its channels.

`Ankusa.AsyncApi.document/1` returns the `%AsyncApiSpex.Document{}` for an
instance; an embedder can serve it from their own router with
`AsyncApiSpex.Plug.RenderSpec.send_spec/2`, or extend it before encoding.
