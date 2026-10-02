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
envelope (`id`, `source_id`, `tenant_id`, `received_at`, `size`, and `body_base64`
or `claim` + `sha256`), narrowed to the source. The provider's own body is opaque
bytes inside it; the document does not describe it.

What a consumer learns per transport:

| Sink | Address | Bindings and headers |
| --- | --- | --- |
| `kafka` | the `topic` | record key (`<tenant>/<source_id>` unless `key:` is set), the five `ankusa_*` headers |
| `rabbitmq` | the routing key (`ankusa.<source_id>` unless `routing_key:` is set) | exchange name, type, vhost; no headers |
| `nats` | the `subject` | the five `ankusa_*` headers |
| `redis` | the `channel` | none: pub/sub has no headers |

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

Delivery is the hook pipeline's. The event is committed to the WAL as an
envelope of the reserved source `ankusa:lifecycle` and dispatched with the same
retries, dead-letter queue, [replay](delivery.md), and claim check; under
`wal: {type: none}` it is published inside the call that made the change, and
boot refuses the config unless one lifecycle sink is durable. Two things differ
from a hook:

- The change has already happened. If the event cannot be recorded (the WAL is
  full, the sink refuses under `wal: none`), the caller still gets success; the
  failure is logged and counted as `ankusa_lifecycle_dropped_total`
  (`ankusa_lifecycle_emitted_total` counts the rest, by `type`).
- The edge never resolves `ankusa:lifecycle`: `POST /webhooks/ankusa:lifecycle`
  is a `404`, and a configured source of that id is a boot error.
- It is a WAL envelope and nothing more. It is not listed by
  `GET /v1/tenants/{tenant}/sources` (it lives in no source store), it never
  passes through ingest, so the tenant rate limiter and the quarantine never see
  it and `ankusa_ingest_*` does not count it. The compactor archives it to the
  object store like any WAL record, and a dead-lettered event is counted by
  `ankusa_dispatch_dead_lettered_total{source_id="ankusa:lifecycle"}`. Source
  events carry the source's tenant; route events carry the default tenant,
  `default`, because a claim-checked body needs a real tenant. An event over a
  sink's `inline_max_bytes` goes through the claim check like a hook.

Only the node that served the change emits, so a route written on one node of a
Redis-backed fleet is announced once. Nodes need the `dispatch` role to deliver
what their WAL holds, as for hooks.

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
payloads once and reference them by name, a validator, a Plug
(`AsyncApiSpex.Plug.RenderSpec`) that serves a document as
`application/asyncapi+json`, and `mix async_api_spex.gen` to write one to a file.
Its README shows an app declaring its channels.

`Ankusa.AsyncApi.document/1` returns the `%AsyncApiSpex.Document{}` for an
instance; an embedder can serve it from their own router with
`AsyncApiSpex.Plug.RenderSpec.send_spec/2`, or extend it before encoding.
