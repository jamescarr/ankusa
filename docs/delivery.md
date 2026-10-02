# Delivery: dispatch, sinks, retries, dead letters

## The dispatch pipeline

`Ankusa.Dispatch.Pipeline` is a `GenServer`, one per instance, that is a
scheduler over **delivery rows** (`Ankusa.Queue`), not a poller. A commit
sends it `:wake`, so a new hook is claimed at once; otherwise it sleeps until
the earliest due row. A retry is a row due at `now + backoff`, so it frees its
concurrency slot instead of sleeping in it — a sink that is down for an hour
holds up nothing but its own rows.

For each claimed row dispatch looks the source up via `Ankusa.SourceStore`,
runs one delivery in a `Task.Supervisor` task (up to `dispatch.concurrency`,
default 32), and records the outcome as a single store batch. A row binds to
`(sink index, module)` at ack time; its opts always come from the source as it
is *now*, so a config fix applies to the backlog and no fun or secret is ever
persisted. If the sinks were reordered a row falls back to the one sink with
its module; if it cannot be bound it is dead-lettered as
`{:sink_gone, index, module}`, and a deleted source as
`{:source_gone, source_id}`.

**Deliveries are not ordered.** Two hooks for one sink may run in either
order, and a retry runs after whatever is due before it. A Kafka partition key
or an AMQP routing key only keeps the order hooks were published in, so it does
not restore one. A consumer that needs order has to rebuild it from data it
receives (the provider's own event timestamp or sequence number in the body;
`received_at` is on every message, but ties are possible within a millisecond)
and tolerate redelivery.

Two windows bound memory and keep a stalled destination from walking the
pipeline into an OOM: `dispatch.max_inflight` (default 4096 claimed,
unfinished rows) and `dispatch.max_inflight_bytes` (default 128 MiB, the sum
of their stored hook sizes). `dispatch.batch` (default 128) is the rows
claimed per store scan. Dispatch stops claiming until something completes.

On `:give_up` from the retry policy the row becomes a dead row (the DLQ) and
stops. One failing sink never blocks delivery to the others, and never blocks
the next hook. A sink that **raises, throws, or exits** is treated exactly
like one returning `{:error, reason}`: the retry policy still applies, and the
pipeline keeps running.

## Direct mode

`wal.type: none` replaces the pipeline above with one synchronous call per
request (`Ankusa.Edge.Publish`): the request process publishes the envelope to
each of the source's sinks in declaration order and answers `201` only once
every sink has confirmed. No polling, no concurrency window — and
**the rest of this section does not apply**:

- no retry policy: the first sink refusal is the answer, a `503` with
  `Retry-After`, and the provider's retry *is* the retry;
- no dead-letter queue and no replay: nothing is committed, so there are no
  rows to replay from;
- no `dispatch.*` limits — publishes happen inside the request and never
  overlap, so the destination's own keying is the only ordering in this mode.

What still applies is the sink contract below, plus one optional callback:
`c:Ankusa.Sink.durable?/1`. A sink's `:ok` must mean the hook is accepted by
something that outlives this node, and boot refuses a `wal: :none` config in
which no sink of a statically configured source can promise that
(`Ankusa.Queue.validate_config!/1`). `Sink.Log` returns `false` — nothing durable
happened — and so does `Sink.Redis`: pub/sub keeps no copy, so a subscriber
that disconnects after the publish loses the message. Every other shipped sink
confirms durably. A source created at runtime through the admin API is not
checked, so keep the source store static when you can.

The check is "at least one durable sink", not "every sink is durable": a
non-durable sink is still a required confirmer. A source with `[Kafka, Redis]`
boots, yet every request is a `503` while Redis has no subscribers — and
because sinks run in declaration order, Kafka has already stored the hook each
time, so every provider retry duplicates it there. Put a pub/sub sink in a
`wal: none` source only where that is what you want (replayable hooks are
better served by `wal.type: disk`, which keeps the non-durable sink's failures
in the DLQ instead of in the provider's retry loop).

The one piece of local state this mode has is the quarantine pen: entries are
written to the store only for a source whose `on_verify_failure` is
`quarantine` — the default, `reject`, appends nothing. See
[Quarantine](#quarantine).

## `Ankusa.Sink`

```elixir
@callback deliver(Envelope.t(), ctx(), opts :: keyword()) :: :ok | {:error, term()}
```

`ctx` is `%{instance:, source_id:, tenant_id:, attempt:}`. Delivery is
at-least-once. Return `:ok` only once you're certain the hook was actually
handled; `{:error, reason}` triggers the source's `Ankusa.RetryPolicy`. A
raised exception, throw, or exit is treated as `{:error, ...}` too.

Deliveries are **not ordered**: hooks for one sink may be delivered in any
order, and a retry runs after whatever is due before it. A consumer that needs
order has to rebuild it from data it receives (not from the sink's key, which
only keeps the order hooks were published in) and tolerate redelivery.

Sinks may also implement two optional callbacks: `inline_max_bytes/1`, which
tells dispatch how large a body this sink sends inline, and `durable?/1`,
which says whether `:ok` means the hook is durably accepted (`wal.type: none`
acks on that promise; default `true`):

```elixir
@callback inline_max_bytes(opts :: keyword()) :: pos_integer() | nil
@callback durable?(opts :: keyword()) :: boolean()
@optional_callbacks inline_max_bytes: 1, durable?: 1
```

A queue sink (`Sink.RabbitMQ`, `Sink.Kafka`, `Sink.NATS`, `Sink.Redis`) returns its
`inline_max_bytes` (default 64 KiB). Dispatch checks a body in **once**, before
any sink runs, when it is larger than at least one of its source's sinks'
thresholds, and hands the resulting claim reference to every sink and every
retry in `ctx.claim`. A sink that returns `nil` never uses the claim check.
See [`claim-check.md`](claim-check.md).

| Adapter | Deps | What it does |
| --- | --- | --- |
| `Sink.Log` | none | Default. Logs the delivery; nothing leaves the process. |
| `Sink.Http` | `req` | Forwards the raw body verbatim to a URL, with `x-ankusa-id`/`x-ankusa-source`/`x-ankusa-tenant` (when set) headers. `2xx` is `:ok`; anything else (including transport failure) is `{:error, reason}`. |
| `Sink.RabbitMQ` | `:amqp`, separate `ankusa_rabbitmq` package | Publishes to an exchange. Detailed below. |
| `Sink.Kafka` | `:brod` (native `crc32cer` NIF), separate `ankusa_kafka` package | Produces to a topic, keyed by `tenant_id/source_id`. Detailed below. |
| `Sink.NATS` | `:gnat`, separate `ankusa_nats` package | Publishes to a JetStream subject, acknowledged by the stream. Detailed below. |
| `Sink.Redis` | `:redix`, separate `ankusa_redis` package | Publishes to a Redis pub/sub channel (`PUBLISH`) and reports zero subscribers as an error. Detailed below. |

```elixir
sinks: [{Ankusa.Sink.Http, url: "https://example.internal/stripe", timeout_ms: 5_000}]
```

### Idempotent receivers

Delivery is at-least-once, so every consumer is an idempotent receiver. Two
distinct ids can refer to the same provider event, and they are deduped
differently:

- `x-ankusa-id`, the message `id` on a queue sink, identifies **one stored
  hook**. Every redelivery of that hook (a retry, a DLQ replay, a dispatch
  restart) carries the same `id`, so dedupe on it.
- A provider retry is a **different stored hook** with a different `id`,
  because ingest does no deduplication. Dedupe those on the provider's own
  event id in the body (e.g. Stripe's `id`).

The original request headers are **not forwarded**: `Sink.Http` sends only
`x-ankusa-id`/`x-ankusa-source`/`x-ankusa-tenant`, and
`Sink.Message` carries only `id`, `source_id`, `tenant_id`, `received_at`,
`content_type`, `size`, and the body (or claim). A header-borne id such as
`X-GitHub-Delivery` or `webhook-id` is therefore not available downstream.

### `Sink.RabbitMQ`: queue delivery

The queue-adapter story: an ingest fleet publishing to a broker instead of
(or in addition to) HTTP-forwarding or in-process handling. See
[`examples/rabbitmq-consumer/`](https://github.com/jamescarr/ankusa/tree/main/examples/rabbitmq-consumer/) for a full
worked deployment.

**Publishes to an exchange only, never a queue.** Binding a queue to the
exchange, and everything downstream of that, is the consumer's job. This
mirrors real AMQP topology ownership: producers own exchanges, consumers
own their own queues. Adding a fifth consumer later never touches ingest
config.

**Messages stay small on purpose.** A body under `:inline_max_bytes`
(default 64 KiB) rides along base64-encoded in the message; anything larger
is checked in through `Ankusa.ClaimCheck` (see
[`claim-check.md`](claim-check.md)) and the message carries a claim reference
instead. This extends the queue-and-segment design's "small hot path, big
payloads elsewhere" principle to the broker: RabbitMQ throughput and memory
stay flat regardless of how large a webhook payload is, and any consumer,
BEAM or not, redeems the reference without needing blob-store credentials of
its own.

```elixir
sinks: [
  {Ankusa.Sink.RabbitMQ,
   exchange: "ankusa.events",
   url: "amqp://guest:guest@localhost:5672",
   inline_max_bytes: 65_536,
   routing_key: fn env -> "ankusa.#{env.tenant_id}.#{env.source_id}" end}  # or a static string; default "ankusa.<source_id>"
]
```

Message shape (`Ankusa.Sink.Message`, byte-identical for `Sink.Kafka`):

```jsonc
// inline
{"v": 1, "id": "01a0...", "source_id": "stripe", "tenant_id": "acme", "received_at": 173...,
 "content_type": "application/json", "size": 245, "body_base64": "eyJpZCI6..."}

// fat payload
{"v": 1, "id": "01a0...", "source_id": "stripe", "tenant_id": "acme", "received_at": 173...,
 "content_type": "application/octet-stream", "size": 3145728,
 "claim": "urn:ankusa:claim:v1:acme:01M39VMD8RA3C5HR4RBV67Y002", "sha256": "3bea8a..."}
```

`"v"` changes only when an existing field changes meaning or disappears;
consumers must ignore keys they don't know.

A consumer redeems `claim` with `GET /v1/claims/:tenant/:claim_id` against the
claim-check gateway and checks the bytes against the message's `sha256`. See
[`claim-check.md`](claim-check.md#redeem-a-claim). (An Elixir consumer can
call `Ankusa.ClaimCheck.redeem/3`, which does both.)

**Connection lifecycle**: one supervised connection + confirm-mode channel
per `(instance, exchange)`, started on demand by the first `deliver/3` call,
registered through the same `Ankusa.Registry`/`Ankusa.via` every other
instance-scoped process uses (own `DynamicSupervisor`, booted by
`ankusa_rabbitmq`'s own `Application`, zero changes to `ankusa` core). Every
publish waits for the broker's **confirm** before `deliver/3` returns `:ok`.
A return value dispatch trusts as "delivered" really was persisted by
RabbitMQ, not just handed to a socket. Connection loss doesn't crash the
GenServer; it retries on a timer and replies `{:error, :not_connected}` to
publishes meanwhile, which flows straight into the existing
`Ankusa.RetryPolicy`. No separate reconnect policy to get wrong.

### `Sink.Kafka`: topic delivery

The same story one transport over: an ingest fleet producing to a Kafka
topic instead of an AMQP exchange, publishing the identical
`Ankusa.Sink.Message`. See
[`examples/kafka-sqs-consumer/`](https://github.com/jamescarr/ankusa/tree/main/examples/kafka-sqs-consumer/) for a full
worked deployment (topic → bridge → SQS FIFO → worker).

```elixir
sinks: [
  {Ankusa.Sink.Kafka,
   brokers: ["localhost:9092"],
   topic: "ankusa.events",
   inline_max_bytes: 65_536,
   key: fn env -> "#{env.tenant_id}/#{env.source_id}" end}  # or a static string; this is the default
]
```

**The record key picks the partition.** The same key lands on the same
partition, and a Kafka consumer reads a key's records in the order they were
produced. Dispatch does **not** serialize deliveries sharing a key, so produce
order is not commit order; a destination that needs commit order has to
reorder downstream. Order is also *not* preserved across a DLQ replay or after
the topic's partition count changes. Keys are hashed with brod's `:hash`
(`erlang:phash2/1`), not the Java client's murmur2, so the same key can land
on a different partition than a Java producer would choose.

**It never creates the topic.** An exchange declaration is idempotent and
free; a topic's partition count is a capacity and ordering contract that can
only grow, remapping keys when it does. An unknown topic is an error that
flows into `Ankusa.RetryPolicy`, never a silently auto-created
single-partition topic.

**`deliver/3` returns `:ok` only once every in-sync replica has the record**
(`required_acks: -1`, then a synchronous wait bounded by
`:produce_timeout_ms`, default 5s), the Kafka equivalent of RabbitMQ's
publisher confirms. brod's producer is not idempotent, so a produce retried
after a lost ack can duplicate a record. Delivery is at-least-once anyway,
and consumers dedupe on `id`.

**Client lifecycle**: one brod client per `(instance, :client)`, started on
demand by the first `deliver/3` call under `ankusa_kafka`'s own
`DynamicSupervisor` (`ankusa` core unchanged). brod's client owns
reconnects, leader changes, and metadata refresh. A brod client id must be a
registered atom, so it is `:"ankusa_kafka.<instance>.<client>"`. Both parts
come from config, so the number of atoms is bounded and two instances never
collide. Unreachable brokers, unknown topics, timeouts, and oversized
messages all surface as `{:error, reason}`.

### `Sink.NATS`: subject delivery

The same story into NATS JetStream, publishing the identical
`Ankusa.Sink.Message`: an ingest fleet produces to a subject on a stream the
operator owns, and a consumer, BEAM or not, reads with the JetStream
client of its language.

```elixir
sinks: [
  {Ankusa.Sink.NATS,
   servers: ["localhost:4222"],
   subject: "ankusa.events",                     # or a 1-arity fun: &"ankusa.#{&1.source_id}"
   inline_max_bytes: 65_536,                     # above this the message carries a claim reference
   auth: [username: "ankusa", password: "..."],  # or token:, or nkey_seed: + jwt:
   publish_timeout_ms: 5_000}
]
```

**It never creates the stream.** A stream's storage, retention, replicas, and,
the part that matters here, its set of subjects are one operator's capacity
and ordering contract, the same way a Kafka topic's partition count is. So the
sink publishes to a subject and stops there; a subject no stream covers is
`{:error, :no_stream}`, straight into `Ankusa.RetryPolicy`, never a silently
auto-created stream with someone else's retention policy. Create it yourself,
once, where your topology lives:

```sh
nats stream add ANKUSA --subjects="ankusa.>"
```

**`deliver/3` returns `:ok` only once the stream has it.** The publish is a
NATS request with a reply inbox, and the reply is JetStream's own publish
acknowledgement, `{"stream":"ANKUSA","seq":42}`, awaited up to
`:publish_timeout_ms` (default 5s). A timeout, a rejected message
(`{"error":{"code":400,"description":"message size exceeds maximum allowed"}}`),
a permission violation, or no answering stream all come back as
`{:error, reason}`. Note that an error ack carries `"seq": 0` *and* an
`"error"` in the same body, so a sink that only checks for a stream name and a
sequence would report a refused hook as delivered.

**The subject is the address, the headers are the metadata.** Each message
carries the same five headers the Kafka sink sets (`ankusa_id`,
`ankusa_source_id`, `ankusa_tenant_id`, `ankusa_message_version`,
`content_type`), so a consumer parses one set of fields regardless of
transport. Unlike Kafka, there is no separate record key: NATS has no
partition-ordering contract to lean on and no partition count to change under
you. Order within a subject is the order the stream received it.

**Connection lifecycle**: one gnat connection per `(instance, connection)`,
started on demand by the first `deliver/3` under `ankusa_nats`'s own
`DynamicSupervisor` (`ankusa` core unchanged). gnat completes the handshake
before the start returns, so a `deliver/3` either has a live connection or a
concrete reason it doesn't (`:econnrefused`, `:timeout`, an authorization
error). A lost socket stops that connection. The child is `:temporary`, so
nothing crash-loops, and the next `deliver/3` reconnects inside the source's
retry policy. Server names are tried in the order given.

**At-least-once, as everywhere else.** A publish whose ack is lost can still
have been stored, so consumers dedupe on `id`. JetStream's own
`Nats-Msg-Id` duplicate window is the consumer's tool, deliberately not set
here: a hook replayed from the DLQ is a *new*, intended publish.

### `Sink.Redis`: pub/sub delivery

The same `Ankusa.Sink.Message`, fanned out over Redis pub/sub: an ingest fleet
`PUBLISH`es each delivered hook to a channel, and any number of live
subscribers read it with the Redis client of their language. Good for
in-process caches, dashboards, and internal fan-out to consumers that are
allowed to miss messages.

```elixir
sinks: [
  {Ankusa.Sink.Redis,
   url: "redis://:password@localhost:6379/0",  # rediss:// for TLS
   channel: "ankusa.events",                   # or a 1-arity fun: &"ankusa.#{&1.source_id}"
   inline_max_bytes: 65_536,                   # above this the message carries a claim reference
   publish_timeout_ms: 5_000}
]
```

**Pub/sub keeps no copy, and that is the whole design constraint.** Redis
stores nothing on a channel: a subscriber that is disconnected — or connects a
moment later — never sees the message. Two consequences:

- **Zero subscribers is an error.** `PUBLISH`'s reply is the number of
  subscribers the message was handed to, and `deliver/3` returns
  `{:error, :no_subscribers}` on `0`, so the hook runs into the retry policy,
  ends in the DLQ, and can be replayed, rather than being recorded as
  delivered when nobody heard it. A retry will succeed once a subscriber
  connects.
- **The sink is not durable.** `durable?/1` is `false` — see
  [Direct mode](#direct-mode) — because a subscriber that disconnects after
  the publish loses the message. Redis that *keeps* messages is a Redis
  Stream (`XADD`), a different sink than this one. Under `wal: :none` that
  makes this sink a required confirmer that can never satisfy the ack on its
  own: boot only needs one durable sink per source, but `{:error,
  :no_subscribers}` still turns every ingest into a `503`.

**The channel is the address, the JSON is the metadata.** Pub/sub has no
headers, so the five fields the Kafka and NATS sinks put in headers (`id`,
`source_id`, `tenant_id`, message version, content type) travel only inside
the `Ankusa.Sink.Message` body; a consumer decodes it and reads them there.
The same applies to a claim ticket for a fat payload: redeem it with
`Ankusa.ClaimCheck.redeem/3` or
`GET /v1/claims/:tenant/:claim_id`. Nothing else enforces the claim check — a
Redis consumer that can't reach the claim-check gateway should ask for a
larger `inline_max_bytes` instead.

**Delivery is at-least-once, as everywhere else**, and a retry or a DLQ replay
republishes to every live subscriber, so consumers dedupe on `id`.

**Connection lifecycle**: one Redix connection per `(instance, url)`, started
on demand by the first `deliver/3` under `ankusa_redis`'s own
`DynamicSupervisor` (`ankusa` core unchanged; the package's route store uses
the same dependency). The start is synchronous, so a `deliver/3` either has a
live connection or a concrete reason it doesn't (`{:connection, :econnrefused}`
for a server that isn't there, `{:connection, :timeout}`, or
`{:redis, "WRONGPASS ..."}` for a credential the server rejected). Once up,
Redix reconnects on its own with backoff and a publish sent meanwhile is
`{:error, {:connection, :closed}}`; the child is `:temporary`, so nothing
crash-loops against a server that is gone.

## `Ankusa.RetryPolicy`

```elixir
@callback backoff(attempt :: pos_integer(), opts :: keyword()) :: {:retry, delay_ms} | :give_up
```

`RetryPolicy.Exponential` (the only shipped policy, and the default):
capped binary exponential backoff with optional full jitter.

| Opt | Default |
| --- | --- |
| `:base_ms` | `100` |
| `:max_ms` | `30_000` (ceiling before jitter) |
| `:max_attempts` | `12` |
| `:jitter` | `true`. Multiplies the delay by a random factor in `[0.5, 1.0]` |

Set dispatch-wide via `config.dispatch.retry`; there's currently no
per-source override (see [`configuration.md`](configuration.md)).

## Dead letters and replay

The DLQ is the set of **dead delivery rows** — there is no separate file. When
a row's sink gives up, the row is marked dead and carries the failure as text:
`inspect({:sink, Module, reason})`, the exact `reason` string `GET /v1/dlq`
returns. A dead row is still an obligation, so its hook is kept (and survives
a restart) until the row is replayed and delivered. The row is written in the
same store batch as any other transition, so a give-up cannot be lost to a
power failure.

`Ankusa.Dispatch.replay/2` moves matching dead rows back to pending with a
fresh attempt count, and the pipeline delivers them through the source's
*current* sinks and options:

```elixir
{:ok, 7} = Ankusa.Dispatch.replay(:default, source_id: "stripe", since: System.system_time(:millisecond) - 3_600_000)
```

It returns the number of rows moved back to pending — `{:ok, 0}` when nothing
matched. Delivery is **asynchronous**: by the time it returns the rows are out
of the DLQ and the pipeline will deliver them (a row that fails again is
dead-lettered again). It needs the `:dispatch` role on this node. Filters
(`:source_id`, `:id`, `:since`, a unix-ms lower bound) are all optional and
combine as AND; omit the filter entirely to replay everything. Delivery is
at-least-once, so a replayed hook is a redelivery.

## Quarantine

A rate-limited durable holding pen for envelopes whose source has
`on_verify_failure: :quarantine`. The point: a bad secret rotation should
never silently eat real events, so a quarantined hook is answered `202` (the
provider will not retry it) and kept for an operator to inspect or re-inject.

- Token bucket: 100 burst, refills 20/s. Over the limit, `Ankusa.Edge.Quarantine.put/3`
  returns `:rate_limited` (surfaced to the caller as `401`, not `202`; the
  request is refused outright rather than silently dropped) instead of
  writing. The bucket caps the rate, not the pen's total size: nothing evicts
  or expires held entries, so size the volume for the quarantine you intend
  to keep.
- Stored in this node's `Ankusa.Store` (two keys per entry in one synced batch:
  a summary, and the headers and body), so `put/3` answers `:ok` only once
  both are on disk. A store that cannot take the write makes ingest answer
  `503` and spends no token.
- `Ankusa.Edge.Quarantine.recent/2` lists the newest entries (id, source, time,
  reason; the headers and body stay in the store) for a dashboard or operator
  inspection. It reads the store, so a restart does not empty it.

Under `wal.type: none` this pen, the API-managed sources and the rate-limit
overrides are the node's only local state: nothing is committed, so there is
no queue and no DLQ. Entries are written only when a source opts in with
`on_verify_failure: quarantine`; the `reject` default writes nothing. A
quarantined hook was not delivered by this node, but the provider *was*
answered `202`, so the pen is the only copy — inspect or re-inject it
deliberately. See [Direct mode](#direct-mode).

```elixir
Ankusa.Edge.Quarantine.recent(:default)
# => [%{id: "...", source_id: "stripe", received_at: ..., reason: :no_match}, ...]
```
