# Delivery: dispatch, sinks, retries, dead letters

## The dispatch pipeline

`Ankusa.Dispatch.Pipeline` is a `GenServer`, one per instance, that polls the
WAL every `dispatch.poll_ms` (default 200ms) for records past its own
durable cursor (up to `dispatch.batch` at a time, default 128). For each
envelope, it looks the source up via `Ankusa.SourceStore` and delivers to
every one of its `sinks`, in order.

Retries happen **inline**: `deliver_with_retry/4` calls the sink, and on
`{:error, reason}` asks the source's `Ankusa.RetryPolicy` for a backoff,
`Process.sleep`s that long, and retries — all within the same dispatch
GenServer call. A sink that's persistently slow to fail therefore delays
*that batch's* dispatch, not just that one record; this is a deliberate
simplicity trade-off (one poller, no separate per-record retry scheduler)
that's fine at the framework's target scale (`dispatch.batch` records per
poll) and matches "boring" over "clever."

On `:give_up` from the retry policy, the envelope is written to the DLQ and
dispatch moves on — one failing sink never blocks delivery to the others,
and never blocks the next envelope.

## `Ankusa.Sink`

```elixir
@callback deliver(Envelope.t(), ctx(), opts :: keyword()) :: :ok | {:error, term()}
```

`ctx` is `%{instance:, source_id:, tenant_id:, attempt:}`. Delivery is
at-least-once — return `:ok` only once you're certain the hook was actually
handled; `{:error, reason}` triggers the source's `Ankusa.RetryPolicy`.

| Adapter | Deps | What it does |
| --- | --- | --- |
| `Sink.Log` | none | Default. Logs the delivery; nothing leaves the process. |
| `Sink.Http` | none (`:httpc`) | Forwards the raw body verbatim to a URL, with `x-ankusa-id`/`x-ankusa-source`/`x-ankusa-seq` headers. `2xx` is `:ok`; anything else (including transport failure) is `{:error, reason}`. |
| `Sink.RabbitMQ` | `:amqp` — separate `ankusa_rabbitmq` package | Publishes to an exchange. Detailed below. |
| `Sink.Kafka` | `:brod` (native `crc32cer` NIF) — separate `ankusa_kafka` package | Produces to a topic, keyed by `tenant_id/source_id`. Detailed below. |

```elixir
sinks: [{Ankusa.Sink.Http, url: "https://example.internal/stripe", timeout_ms: 5_000}]
```

### `Sink.RabbitMQ` — queue delivery

The queue-adapter story: an ingest fleet publishing to a broker instead of
(or in addition to) HTTP-forwarding or in-process handling. See
[`examples/rabbitmq-consumer/`](../examples/rabbitmq-consumer/) for a full
worked deployment.

**Publishes to an exchange only — never a queue.** Binding a queue to the
exchange, and everything downstream of that, is the consumer's job. This
mirrors real AMQP topology ownership: producers own exchanges, consumers
own their own queues. Adding a fifth consumer later never touches ingest
config.

**Messages stay small on purpose.** A body under `:inline_max_bytes`
(default 8 KiB) rides along base64-encoded in the message; anything larger
is checked in through `Ankusa.ClaimCheck` (see
[`claim-check.md`](claim-check.md)) and the message carries a ticket
instead. This extends the WAL/segment design's "small hot path, big
payloads elsewhere" principle to the queue: RabbitMQ throughput and memory
stay flat regardless of how large a webhook payload is, and any consumer —
BEAM or not — redeems the ticket without needing blob-store credentials of
its own.

```elixir
sinks: [
  {Ankusa.Sink.RabbitMQ,
   exchange: "ankusa.events",
   url: "amqp://guest:guest@localhost:5672",
   inline_max_bytes: 8_192,
   routing_key: fn env -> "ankusa.#{env.tenant_id}.#{env.source_id}" end}  # or a static string; default "ankusa.<source_id>"
]
```

Message shape (`Ankusa.Sink.Message` — byte-identical for `Sink.Kafka`):

```jsonc
// inline
{"v": 1, "id": "01a0...", "source_id": "stripe", "tenant_id": "acme", "received_at": 173...,
 "content_type": "application/json", "size": 245, "body_base64": "eyJpZCI6..."}

// fat payload
{"v": 1, "id": "01a0...", "source_id": "stripe", "tenant_id": "acme", "received_at": 173...,
 "content_type": "application/octet-stream", "size": 3145728,
 "claim": {"v": 1, "tenant_id": "acme", "id": "01a0...", "size": 3145728,
           "sha256": "9f86d0...", "content_type": "application/octet-stream"}}
```

`"v"` changes only when an existing field changes meaning or disappears;
consumers must ignore keys they don't know.

A consumer decodes `claim` back into a `Ankusa.ClaimCheck.Ticket` and calls
`Ankusa.ClaimCheck.redeem/3` (or, for a non-BEAM consumer, `GET
/v1/claims/:tenant_id/:id` against a `:claim_check`-role node — see
[`claim-check.md`](claim-check.md)).

**Connection lifecycle**: one supervised connection + confirm-mode channel
per `(instance, exchange)`, started on demand by the first `deliver/3` call,
registered through the same `Ankusa.Registry`/`Ankusa.via` every other
instance-scoped process uses (own `DynamicSupervisor`, booted by
`ankusa_rabbitmq`'s own `Application` — zero changes to `ankusa` core). Every
publish waits for the broker's **confirm** before `deliver/3` returns `:ok`
— a return value dispatch trusts as "delivered" really was persisted by
RabbitMQ, not just handed to a socket. Connection loss doesn't crash the
GenServer; it retries on a timer and replies `{:error, :not_connected}` to
publishes meanwhile, which flows straight into the existing
`Ankusa.RetryPolicy` — no separate reconnect policy to get wrong.

### `Sink.Kafka` — topic delivery

The same story one transport over: an ingest fleet producing to a Kafka
topic instead of an AMQP exchange, publishing the identical
`Ankusa.Sink.Message`. See
[`examples/kafka-sqs-consumer/`](../examples/kafka-sqs-consumer/) for a full
worked deployment (topic → bridge → SQS FIFO → worker).

```elixir
sinks: [
  {Ankusa.Sink.Kafka,
   brokers: ["localhost:9092"],
   topic: "ankusa.events",
   inline_max_bytes: 8_192,
   key: fn env -> "#{env.tenant_id}/#{env.source_id}" end}  # or a static string; this is the default
]
```

**The record key is the ordering scope.** The same key lands on the same
partition, and a retried record never overtakes a later one from that key
(inline retries block the dispatch batch). Order is *not* preserved across a
DLQ replay, across a fleet sharing a `WAL.Postgres`, or after the topic's
partition count changes. Keys are hashed with brod's `:hash`
(`erlang:phash2/1`), not the Java client's murmur2, so the same key can land
on a different partition than a Java producer would choose.

**It never creates the topic.** An exchange declaration is idempotent and
free; a topic's partition count is a capacity and ordering contract that can
only grow, remapping keys when it does. An unknown topic is an error that
flows into `Ankusa.RetryPolicy` — never a silently auto-created
single-partition topic.

**`deliver/3` returns `:ok` only once every in-sync replica has the record**
(`required_acks: -1`, then a synchronous wait bounded by
`:produce_timeout_ms`, default 5s), the Kafka equivalent of RabbitMQ's
publisher confirms. brod's producer is not idempotent, so a produce retried
after a lost ack can duplicate a record — delivery is at-least-once anyway,
and consumers dedupe on `id`.

**Client lifecycle**: one brod client per `(instance, :client)`, started on
demand by the first `deliver/3` call under `ankusa_kafka`'s own
`DynamicSupervisor` (`ankusa` core unchanged). brod's client owns
reconnects, leader changes, and metadata refresh. A brod client id must be a
registered atom, so it is `:"ankusa_kafka.<instance>.<client>"` — both parts
come from config, so the number of atoms is bounded and two instances never
collide. Unreachable brokers, unknown topics, timeouts, and oversized
messages all surface as `{:error, reason}`.

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
| `:jitter` | `true` — multiplies the delay by a random factor in `[0.5, 1.0]` |

Set dispatch-wide via `config.dispatch.retry`; there's currently no
per-source override (see [`configuration.md`](configuration.md)).

## Dead letters and replay

`Ankusa.Dispatch.DLQ` is a durable, append-only, length-prefixed log
(`<data_dir>/<instance>/dlq/dlq.log`) — one record per give-up:
`%{envelope:, reason:, at:}`. Reads tolerate a torn trailing record (a
partial append) and just drop it, same discipline as the WAL and the
quarantine log.

`Ankusa.Dispatch.replay/2` re-delivers dead-lettered hooks through their
source's current sinks:

```elixir
Ankusa.Dispatch.replay(:default, source_id: "stripe", since: System.system_time(:millisecond) - 3_600_000)
# => 7  (number of entries replayed)
```

Filters (`:source_id`, `:id`, `:since` — a unix-ms lower bound) are all
optional and combine as AND; omit the filter entirely to replay everything.

## Quarantine

A rate-limited durable holding pen for envelopes whose source has
`on_verify_failure: :quarantine`. The point: a bad secret rotation should
never silently eat real events, but a flood of forged requests shouldn't be
able to fill the disk either.

- Token bucket: 100 burst, refills 20/s. Over the limit, `Ankusa.Edge.Quarantine.put/3`
  returns `:rate_limited` (surfaced to the caller as `401`, not `202` —
  the request is refused outright rather than silently dropped) instead of
  writing.
- Durable append-only log (`<data_dir>/<instance>/quarantine/quarantine.log`),
  one `fsync` per write.
- `Ankusa.Edge.Quarantine.recent/1` keeps the last 200 entries in memory
  (headers/body dropped from the in-memory summary — full record is on
  disk) for a dashboard or operator inspection.

```elixir
Ankusa.Edge.Quarantine.recent(:default)
# => [%{id: "...", source_id: "stripe", received_at: ..., reason: :no_match}, ...]
```
