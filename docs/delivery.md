# Delivery: dispatch, sinks, retries, dead letters

## The dispatch pipeline

`Ankusa.Dispatch.Pipeline` is a `GenServer`, one per instance, that polls the
WAL every `dispatch.poll_ms` (default 200ms) for records past its own
durable cursor (up to `dispatch.batch` at a time, default 128). For each
envelope it looks the source up via `Ankusa.SourceStore` and enqueues one
delivery job per sink.

Jobs run concurrently, up to `dispatch.concurrency` (default 32) at a time,
each in a `Task.Supervisor` task, so a slow or retrying sink only delays what
actually has to wait for it. What has to wait is decided by the **ordering
key**: deliveries to the same sink with an equal
`c:Ankusa.Sink.ordering_key/2` never overlap and run one at a time, in the
order dispatch read them from the log; different keys are independent. `nil`
means no constraint.

The cursor is a **watermark**, not a per-envelope save point: it sits at the
last seq read when nothing is in flight, and one below the lowest
admitted-but-unfinished seq otherwise. An envelope dispatch has already
finished can therefore sit behind the watermark until an earlier one finishes.
If the node dies first it is redelivered, which at-least-once permits. The
WAL's contract (seq order *is* commit order) is what makes advancing past
finished work safe at all.

Two windows bound memory and keep a stalled destination from walking the
pipeline into an OOM: `dispatch.max_inflight` (default 4096 envelopes) and
`dispatch.max_inflight_bytes` (default 128 MiB of body bytes). Dispatch simply
stops reading until something completes.

On `:give_up` from the retry policy, the envelope is written to the DLQ, in
the pipeline process, so DLQ appends stay serialized, and its jobs stop. One
failing sink never blocks delivery to the others, and never blocks the next
envelope. A sink that **raises, throws, or exits** is treated exactly like one
returning `{:error, reason}`: the retry policy still applies, and the pipeline
keeps running.

### Ordering keys

```elixir
@callback ordering_key(Envelope.t(), opts :: keyword()) :: term() | nil

@optional_callbacks ordering_key: 2
```

A sink's ordering key must be at least as narrow as the ordering its
destination actually guarantees. Claiming a wider scope than the destination
provides is a correctness bug, not a throughput knob:

| Sink | Key | Why |
| --- | --- | --- |
| `Sink.Http` | `{tenant_id, source_id}` when `ordered: true`, else `nil` | An arbitrary HTTP endpoint promises nothing about concurrent requests; opt in when yours does. |
| `Sink.Kafka` | the record key (default `"#{tenant_id}/#{source_id}"`) | The key picks the partition, and a partition is Kafka's ordering scope. |
| `Sink.RabbitMQ` | the routing key (default `"ankusa.#{source_id}"`) | RabbitMQ orders per queue, and the routing key decides the queue. |
| `Sink.NATS` | the subject | Within a subject, the order is the order the stream received it. |
| `Sink.Log` | `nil` | Interleaved log lines are fine. |
| any sink that doesn't implement `ordering_key/2` | `{tenant_id, source_id}` | Conservative default: serialize per source rather than silently interleave. |

## Direct mode

`wal.type: none` replaces the pipeline above with one synchronous call per
request (`Ankusa.Edge.Publish`): the request process publishes the envelope to
each of the source's sinks in declaration order and answers `201` only once
every sink has confirmed. No polling, no concurrency window, no watermark — and
**the rest of this section does not apply**:

- no retry policy: the first sink refusal is the answer, a `503` with
  `Retry-After`, and the provider's retry *is* the retry;
- no dead-letter queue and no replay: there is no log to replay from;
- no `dispatch.*` limits and no ordering key — `Sink.ordering_key/2` is not
  consulted, because publishes happen inside the request and never overlap.
  The destination's own keying is the only ordering in this mode.

What still applies is the sink contract below, plus one optional callback:
`c:Ankusa.Sink.durable?/1`. A sink's `:ok` must mean the hook is accepted by
something that outlives this node, and boot refuses a `wal: :none` config in
which no sink of a statically configured source can promise that
(`Ankusa.WAL.validate_config!/1`). `Sink.Log` returns `false`; every other
shipped sink confirms durably. A source created at runtime through the admin
API is not checked, so keep the source store static when you can.

The one piece of local state this mode has is the quarantine pen: the edge
creates its log at boot (empty), and entries are appended only for a source
whose `on_verify_failure` is `quarantine` — the default, `reject`, appends
nothing. See [Quarantine](#quarantine).

## `Ankusa.Sink`

```elixir
@callback deliver(Envelope.t(), ctx(), opts :: keyword()) :: :ok | {:error, term()}
```

`ctx` is `%{instance:, source_id:, tenant_id:, attempt:}`. Delivery is
at-least-once. Return `:ok` only once you're certain the hook was actually
handled; `{:error, reason}` triggers the source's `Ankusa.RetryPolicy`. A
raised exception, throw, or exit is treated as `{:error, ...}` too.

Sinks may also implement the optional `ordering_key/2` callback, which tells
dispatch which deliveries may run concurrently, see
[Ordering keys](#ordering-keys) above, the optional `inline_max_bytes/1`
callback, which tells dispatch how large a body this sink sends inline, and the
optional `durable?/1` callback, which says whether `:ok` means the hook is
durably accepted (`wal.type: none` acks on that promise; default `true`):

```elixir
@callback inline_max_bytes(opts :: keyword()) :: pos_integer() | nil
@callback durable?(opts :: keyword()) :: boolean()
@optional_callbacks ordering_key: 2, inline_max_bytes: 1, durable?: 1
```

A queue sink (`Sink.RabbitMQ`, `Sink.Kafka`, `Sink.NATS`) returns its
`inline_max_bytes` (default 64 KiB). Dispatch checks a body in **once**, before
any sink runs, when it is larger than at least one of its source's sinks'
thresholds, and hands the resulting claim reference to every sink and every
retry in `ctx.claim`. A sink that returns `nil` never uses the claim check.
See [`claim-check.md`](claim-check.md).

| Adapter | Deps | What it does |
| --- | --- | --- |
| `Sink.Log` | none | Default. Logs the delivery; nothing leaves the process. |
| `Sink.Http` | `req` | Forwards the raw body verbatim to a URL, with `x-ankusa-id`/`x-ankusa-source`/`x-ankusa-tenant` (when set) headers. `2xx` is `:ok`; anything else (including transport failure) is `{:error, reason}`. `ordered: true` serializes per `{tenant_id, source_id}`. |
| `Sink.RabbitMQ` | `:amqp`, separate `ankusa_rabbitmq` package | Publishes to an exchange. Detailed below. |
| `Sink.Kafka` | `:brod` (native `crc32cer` NIF), separate `ankusa_kafka` package | Produces to a topic, keyed by `tenant_id/source_id`. Detailed below. |
| `Sink.NATS` | `:gnat`, separate `ankusa_nats` package | Publishes to a JetStream subject, acknowledged by the stream. Detailed below. |

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
instead. This extends the WAL/segment design's "small hot path, big
payloads elsewhere" principle to the queue: RabbitMQ throughput and memory
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

**The record key is the ordering scope.** The same key lands on the same
partition, and dispatch serializes deliveries sharing that key: one delivery
per key at a time, in the order dispatch read it (Kafka's `ordering_key/2`
returns exactly the record key). Different keys are delivered concurrently.
Order is *not*
preserved across a DLQ replay or after the topic's partition count changes. Keys are hashed with brod's `:hash`
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

`Ankusa.Dispatch.DLQ` is a durable, append-only, length-prefixed log
(`<data_dir>/<instance>/dlq/dlq.log`). One record per give-up:
`%{envelope:, reason:, at:}`. Each append is fsynced before dispatch advances
its cursor past the hook, so a dead letter can't be lost to a power failure.
Reads tolerate a torn trailing record (a partial append) and just drop it, same
discipline as the WAL and the quarantine log.

`Ankusa.Dispatch.replay/2` re-delivers dead-lettered hooks through their
source's current sinks:

```elixir
Ankusa.Dispatch.replay(:default, source_id: "stripe", since: System.system_time(:millisecond) - 3_600_000)
# => 7  (number of entries replayed)
```

Filters (`:source_id`, `:id`, `:since`, a unix-ms lower bound) are all
optional and combine as AND; omit the filter entirely to replay everything.

## Quarantine

A rate-limited durable holding pen for envelopes whose source has
`on_verify_failure: :quarantine`. The point: a bad secret rotation should
never silently eat real events, but a flood of forged requests shouldn't be
able to fill the disk either.

- Token bucket: 100 burst, refills 20/s. Over the limit, `Ankusa.Edge.Quarantine.put/3`
  returns `:rate_limited` (surfaced to the caller as `401`, not `202`.
  The request is refused outright rather than silently dropped) instead of
  writing.
- Durable append-only log (`<data_dir>/<instance>/quarantine/quarantine.log`),
  one `fsync` per write.
- `Ankusa.Edge.Quarantine.recent/1` keeps the last 200 entries in memory
  (headers/body dropped from the in-memory summary. Full record is on
  disk) for a dashboard or operator inspection.

Under `wal.type: none` this pen is the **only** local state the node has (no
WAL, no DLQ). The log is created empty at boot; entries are appended only when
a source opts in with `on_verify_failure: quarantine`, and the `reject` default
appends nothing. It is forensic scratch space, not acked customer data — a hook
in the pen was never acked — which is why the combination is allowed rather
than rejected on a stateless node. See [Direct mode](#direct-mode).

```elixir
Ankusa.Edge.Quarantine.recent(:default)
# => [%{id: "...", source_id: "stripe", received_at: ..., reason: :no_match}, ...]
```
