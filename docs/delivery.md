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
`{:source_gone, source_id}`. The admin API refuses to delete a source with
rows still queued on the node (`409 source_has_deliveries`) unless asked to
dead-letter them (`?deliveries=dead_letter`): dispatch then writes them to the
DLQ as `{:source_gone, source_id}` at once, so no later source of the same
name inherits them. A hook accepted while the delete runs can still be queued
after it; dispatch dead-letters it the same way when it claims it, unless the
name was re-created first.

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

A scan costs the tenants with work due, not the tenants there are: dispatch
reads the active-tenant markers up to now, then claims due rows tenant by
tenant, `dispatch.batch` in all, so one tenant with a million queued rows and a
thousand tenants with a row due next week cost a scan the same as one tenant
with one row (`a tick visits only tenants with work` in `dispatch_test.exs`,
`bench/dispatch_tick_bench.exs`). Which tenant's rows a page holds is decided
by [deficit round-robin](#fairness-between-tenants), not by age.

On `:give_up` from the retry policy the row becomes a dead row (the DLQ) and
stops. One failing sink never blocks delivery to the others, and never blocks
the next hook. A sink that **raises, throws, or exits** is treated exactly
like one returning `{:error, reason}`: the retry policy still applies, and the
pipeline keeps running.

If the source store cannot answer for a row's source (a store read error, not
a missing source), the row is written back due a second later with its
attempts unchanged, never dead-lettered as `{:source_gone, _}`; a replay job
in the same state retries its page on the next tick.

An attempt that has not returned after `dispatch.attempt_timeout_ms` (default
30 s) is killed and counts as a failed attempt, `{:attempt_timeout, ms}`, so a
hung sink frees its slot; the fallback claim check-in runs inside the same
deadline. The sink may still complete the delivery after the kill; consumers
dedupe on the idempotency key.

### Fairness between tenants

A tenant is the hook's tenant id (`default` for a hook without one). Three
things keep one tenant from taking what another needs:

- **The claim.** A scan splits its page (`dispatch.batch`) into equal shares
  among the tenants with work due. A tenant with less due than its share leaves
  the rest to the others (further passes within the scan); a tenant the page did
  not reach goes first in the next scan, so with more active tenants than rows
  in a page every tenant still gets its turn. A tenant with a million rows is one
  share of a page, not the head of the line.
- **The window.** Each tenant has its own `max_inflight` and
  `max_inflight_bytes` of claimed, unfinished rows and its own `concurrency`
  (`dispatch.tenants.default`, overridden per tenant id in
  `dispatch.tenants["acme"]`; defaults a quarter of the global window and half
  of `concurrency`). A tenant whose sink is slow claims rows until its window is
  full and then stops: the global window and the slots stay free for the
  others. A tenant at its window is skipped by the scan and by the wake-up
  timer until an outcome frees room. The global limits still apply on top.
  Hooks that all share one tenant (no tenant id, or one `default`) are one
  tenant: raise `dispatch.tenants.default` to the global values to let them use
  the whole window.
- **The slots.** Free slots go round-robin across tenants first, then across the
  sink keys of each tenant, so a tenant with a hundred sources gets no more turns
  than one with one source, and never more running attempts than its
  `concurrency`.

`ankusa_dispatch_tenant_inflight{tenant}` is each tenant's claimed rows (how
close it is to its window), for the 100 tenants holding the most. The
`tenant fairness` case of `dispatch/isolation_test.exs` runs a tenant at 99
times another's load behind a slow sink: the light tenant's p99 delivery lag
stays within 2x of its solo p99 and the heavy tenant never holds more than its
window; `bench/tenant_fairness_bench.exs` prints both tenants' p99.

### A slow or dead sink

Claimed rows wait in one queue per **sink key** — `{tenant_id, source_id, sink
index, sink module}` — and a tenant's free slots go round-robin across the keys
that have work, so one source's backlog never queues ahead of every other
source's. `dispatch.sink_concurrency` (default `8`; `nil` for no cap) caps how
many attempts one key runs at once; because the key carries the tenant, the cap
is per tenant, and a destination that answers slowly can hold at most that many
slots of each tenant for `attempt_timeout_ms` each.

A destination that is down trips its key's **circuit breaker**:
`dispatch.breaker_failures` (default 5; `0` disables breakers) consecutive
failures that are not `{:permanent, _}` open it. While it is open the key's
rows are *parked* — written back due when the breaker's period ends, attempts
unchanged — instead of each spending an attempt and a slot. The period starts
at `dispatch.breaker_open_ms` (30 s) and doubles per consecutive open up to
`dispatch.breaker_max_open_ms` (5 min). Then one attempt runs as a probe:
success closes the breaker, failure reopens it for longer. Every transition
emits `[:ankusa, :dispatch, :breaker]` (counted on `/metrics` as
`ankusa_dispatch_breaker_transitions_total`), an open logs a warning, and the
`ankusa_dispatch_breakers_open` gauge is the number open now.

Two consequences to know: breakers live in the dispatch process's memory, so a
restart closes them all and the first wave after it can again hold slots for
one `attempt_timeout_ms`; and parked rows spend no attempts, so while a
breaker stays open a row's retry horizon is not bounded by wall-clock time.
A test or a deployment that wants every failure to reach the DLQ on the
retry policy's schedule sets `breaker_failures: 0`.

### Replay jobs

Dead rows and archived hooks can be re-sent with a replay job
(`POST /v1/replays`): `kind: dlq` revives dead rows, `kind: archive` re-enqueues
archived hooks over a `received_at` window. A job never bulk-flips rows — it
drips them into the delivery queue at `rate` items per second (default 1 000,
max 100 000), and only while dispatch's oldest-due lag is at most `max_lag_ms`
(default 2 000) and its in-flight window is not full, so a replay uses only
the capacity live traffic leaves free and can be left running. Jobs are
durable: the cursor commits in the same batch as the rows it moved, a restart
resumes it, and a job whose deliveries keep dead-lettering, or keep being
parked behind an open circuit breaker, pauses itself.
A `dlq` job touches only rows dead-lettered at or before its own creation, so
rows that die again during the replay are never picked up twice by one job.
Replayed deliveries keep the hook's original `id`, `dedupe_key` and
`idempotency_key` and carry the job's `replay_id` in the message, the broker
headers, and the HTTP headers. Manage jobs with `GET /v1/replays`,
`GET|PATCH /v1/replays/{id}`.
See the runbook in [`Ankusa.Replay`](https://hexdocs.pm/ankusa/Ankusa.Replay.html).

## Direct mode

`wal.type: none` replaces the pipeline above with one synchronous call per
request (`Ankusa.Edge.Publish`): the request process publishes the envelope to
every sink of the source **concurrently**, all under one overall deadline
(`direct_publish_timeout_ms`, default 8 s — keep it under the provider's own
timeout), and answers `201` only once every sink has confirmed. No polling, no
concurrency window — and **the rest of this section does not apply**:

- no retry policy: any sink failure, crash or timeout makes the answer a `503`
  with `Retry-After`, and the provider's retry *is* the retry; sinks that
  already confirmed keep their copy;
- no dead-letter queue and no replay: nothing is committed, so there are no
  rows to replay from;
- no `dispatch.*` limits — publishes to a hook's sinks overlap, so the
  destination's own keying is the only ordering in this mode.

What still applies is the sink contract below, plus one optional callback:
`c:Ankusa.Sink.durable?/1`. A sink's `:ok` must mean the hook is accepted by
something that outlives this node, and under `wal: :none` a source none of
whose sinks can promise that is refused: at boot for a statically configured
source (`Ankusa.Queue.validate_config!/1`), and on write for one created or
updated at runtime — the admin API answers `400 invalid_source`
(`Ankusa.Queue.check_durable/2`). A source that arrives without that check
(stored before it existed, or written to a shared source store by a
`wal: disk` node) is answered `503` at ingest. `Sink.Log` returns `false` — nothing durable
happened — and so does `Sink.Redis`: pub/sub keeps no copy, so a subscriber
that disconnects after the publish loses the message. Every other shipped sink
confirms durably.

The check is "at least one durable sink", not "every sink is durable": a
non-durable sink is still a required confirmer. A source with `[Kafka, Redis]`
boots, yet every request is a `503` while Redis has no subscribers — and
because sinks publish concurrently, Kafka has already stored the hook each
time, so every provider retry duplicates it there. Put a pub/sub sink in a
`wal: none` source only where that is what you want (replayable hooks are
better served by `wal.type: disk`, which keeps the non-durable sink's failures
in the DLQ instead of in the provider's retry loop).

Local state in this mode is small: the quarantine pen (entries are
written to the store only for a source whose `on_verify_failure` is
`quarantine` — the default, `reject`, appends nothing), API-managed sources, and
rate-limit overrides. With no dispatch role
there is no replay job to release pen entries, only `GET` and `DELETE
/v1/quarantine`. See [Quarantine](#quarantine).

## `Ankusa.Sink`

```elixir
@callback deliver(Envelope.t(), ctx(), opts :: keyword()) :: :ok | {:error, term()}
```

`ctx` is `%{instance:, source_id:, tenant_id:, attempt:}`. Delivery is
at-least-once. Return `:ok` only once you're certain the hook was actually
handled; `{:error, reason}` triggers the source's `Ankusa.RetryPolicy`. A
raised exception, throw, or exit is treated as `{:error, ...}` too, and so is
any other return value (`{:error, {:bad_return, value}}`) on every path:
dispatch, lifecycle events, and the `wal: :none` ack.

**Error classes.** Two reason shapes change what dispatch does:

| `reason` | Dispatch |
| --- | --- |
| `{:permanent, term}` | Dead-letter after this attempt, whatever the policy had left; does not count towards the circuit breaker. For what retrying cannot fix: a `410 Gone`, a record the broker will never take. |
| `{:retry_after, ms, term}` | Next attempt no sooner than `ms` (capped at one hour) or the policy's backoff, whichever is later; the policy still decides when to give up. |
| anything else | Transient: the retry policy. |

`Ankusa.Sink.classify/1` returns the class of a reason.

Deliveries are **not ordered**: hooks for one sink may be delivered in any
order, and a retry runs after whatever is due before it. A consumer that needs
order has to rebuild it from data it receives (not from the sink's key, which
only keeps the order hooks were published in) and tolerate redelivery.

Sinks may also implement three optional callbacks: `inline_max_bytes/1`, which
tells dispatch how large a body this sink sends inline; `durable?/1`,
which says whether `:ok` means the hook is durably accepted (`wal.type: none`
acks on that promise; default `true`); and `describe/2`, where the sink
publishes, for the AsyncAPI document (only messaging sinks implement it):

```elixir
@callback inline_max_bytes(opts :: keyword()) :: pos_integer() | nil
@callback durable?(opts :: keyword()) :: boolean()
@callback describe(
            subject :: %{source_id: String.t(), tenant_id: String.t() | nil},
            opts :: keyword()
          ) :: Ankusa.Sink.Description.t()
@optional_callbacks inline_max_bytes: 1, durable?: 1, describe: 2
```

A queue sink (`Sink.RabbitMQ`, `Sink.Kafka`, `Sink.NATS`, `Sink.Redis`, `Sink.SQS`, `Sink.GooglePubSub`) returns its
`inline_max_bytes` (default 64 KiB). Dispatch checks a body in **once**, before
any sink runs, when it is larger than at least one of its source's sinks'
thresholds, and hands the resulting claim reference to every sink and every
retry in `ctx.claim`. A sink that returns `nil` never uses the claim check.
The callback is user code: if it raises, exits, throws or returns something
other than `nil` or a positive integer, that delivery attempt fails (retried,
then dead-lettered with the reason `inline_max_bytes`); under `wal.type: none`
the request is answered `503`. It never stops dispatch.
See [`claim-check.md`](claim-check.md).

| Adapter | Deps | What it does |
| --- | --- | --- |
| `Sink.Log` | none | Default. Logs the delivery; nothing leaves the process. |
| `Sink.Http` | `req` | Forwards the raw body verbatim to a URL, with `x-ankusa-id`/`x-ankusa-source`/`x-ankusa-idempotency-key`/`x-ankusa-verification`/`x-ankusa-tenant` (when set) headers, and a Standard Webhooks signature: `secret` is required unless `allow_unsigned: true`. The URL, headers and secret may be computed per tenant ([below](#sinkhttp-per-tenant-destinations)). Status mapping below. |
| `Sink.RabbitMQ` | `:amqp`, separate `ankusa_rabbitmq` package | Publishes to an exchange. Detailed below. |
| `Sink.Kafka` | `:brod` (native `crc32cer` NIF), separate `ankusa_kafka` package | Produces to a topic, keyed by `tenant_id/source_id`. Detailed below. |
| `Sink.NATS` | `:gnat`, separate `ankusa_nats` package | Publishes to a JetStream subject, acknowledged by the stream. Detailed below. |
| `Sink.Redis` | `:redix`, separate `ankusa_redis` package | Publishes to a Redis pub/sub channel (`PUBLISH`) and reports zero subscribers as an error. Detailed below. |
| `Sink.SQS` | none (core: `req` + `aws_signature`) | Sends to an SQS queue (`SendMessage`, SigV4), standard or FIFO, confirmed by SQS's `200`. Detailed below. |
| `Sink.GooglePubSub` | none (core: `req`) | Publishes to a Google Cloud Pub/Sub topic (`topics.publish`, a bearer token from your `:token_provider`), confirmed by Pub/Sub's `200`. Detailed below. |

```elixir
sinks: [{Ankusa.Sink.Http, url: "https://example.internal/stripe", secret: "whsec_…", timeout_ms: 5_000}]
```

Deliveries are **signed by default**: a sink with neither `secret` nor
`allow_unsigned: true` is refused at load (and `400 invalid_source` from the
admin API), and one that skipped that check is dead-lettered at its first
attempt (`{:permanent, :unsigned_sink}`) rather than sent unsigned. Every SDK
receiver (`receive_delivery`; the Elixir `Ankusa.SDK.Receiver` plug) is closed
by default in the same way, so an unsigned POST to a receiver is `401`. Tests:
`sink/http_test.exs` ("requires secret"), `ankusa_server/config_test.exs`
("signed by default"), and the `webhook.require_signature` conformance cases.

`Sink.Http` reads the status and at most `max_response_bytes` (64 KiB) of the
response, then closes the connection, so a large or endless response cannot
pin the attempt or its memory. Redirects are never followed.

Before a byte is sent, every attempt is checked against the `egress` policy
([`configuration.md#egress`](configuration.md#egress)): `https` only unless the
policy allows `http`, and no address on the loopback, private or link-local
ranges (the cloud metadata service included) unless the policy allows it. A
denied target is `{:permanent, {:egress_denied, reason}}`: dead-letter now, then
fix the policy or the URL and replay. A name that does not resolve is retried.
`https` connections verify the peer's certificate and hostname; a sink's
`cacertfile` names a private CA, and `allow_insecure_tls: true` is the only way to
turn verification off.

| Status | Result |
| --- | --- |
| `2xx` | `:ok` |
| `400`, `401`, `403`, `404`, `410`, `413`, `422` | `{:permanent, {:status, s}}`: dead-letter now. A credential or `secret` fix is followed by a DLQ replay, not hours of retries against a receiver that keeps refusing. |
| `408`, `429`, `5xx` with `Retry-After` (seconds or an HTTP-date) | `{:retry_after, ms, {:status, s}}` |
| anything else, a transport failure, a timeout | transient: the retry policy |

With `secret` (a `whsec_…` secret, or a list while rotating) every delivery
carries `webhook-id` (the hook id), `webhook-timestamp` and
`webhook-signature` (`v1,<base64 HMAC-SHA256>` of `id.timestamp.body`, one
entry per secret); a secret that does not decode fails the attempt as
`{:permanent, :bad_secret}`, and boot rejects one in a static source. Every
SDK verifies the signature; see
[`integrations.md`](integrations.md#signed-deliveries).

The broker sinks (`Sink.RabbitMQ`, `Sink.Kafka`, `Sink.NATS`, `Sink.Redis`)
open one connection per broker on first use and close it after
`idle_timeout_ms` with no delivery through it (default 10 minutes, `0`
keeps it for good; `Ankusa.Sink.Reaper`), so a deleted or re-pointed source
does not leave its connection open for the life of the node. The next
delivery opens a new one; a delivery caught by the close fails that attempt
and is retried.

### `Sink.Http`: per-tenant destinations

On a shared source (one `stripe` source behind `/webhooks/:tenant/stripe`) the
receiver differs per tenant. In Elixir config, `url`, `headers` and `secret` may
each be a one-arity function of the `%Ankusa.Envelope{}` or a `{mod, fun, args}`
(called as `mod.fun(env, args...)`), resolved on **every attempt**:

```elixir
{Ankusa.Sink.Http,
 url: fn env -> Tenants.endpoint(env.tenant_id) end,
 headers: {Tenants, :auth_headers, []},
 secret: {Tenants, :signing_secret, []}}
```

Everything above applies to the *resolved* values, not the function: each
computed URL is checked against the `egress` policy on every attempt (a
tenant-supplied URL cannot reach the private network, and a name that rebinds
between attempts is caught), and a secret that resolves to nothing is
`{:permanent, :unsigned_sink}` unless `allow_unsigned: true`, so a tenant with
no secret is never sent unsigned. A function returns `{:error, reason}` to fail
the attempt (`{:error, {:permanent, reason}}` to dead-letter a tenant with no
endpoint); a raise is a transient failure; a value of the wrong shape is
`{:permanent, {:computed, key, :invalid}}`, which never echoes the value. YAML
and the admin API build static sinks only. `Ankusa.Sink.describe/3` reports
the options that are `computed`; the sink stays out of the AsyncAPI document,
which describes the envelope and not the raw body this sink sends.

Computed URLs connect through `Ankusa.Sink.Http.Pool`, one Finch per instance,
not Req's default of one pool per origin that is never closed (one per tenant
receiver, so file descriptors and processes would grow with the tenant count
until the node ran out). Its bounds:

| | |
| --- | --- |
| Cap | 4 connections per origin (HTTP/1), so one tenant's slow receiver holds at most 4; 4,096 open origins per node |
| Sweep | an origin with no delivery for 30 s has its pool closed; a connection idle 15 s is not reused |
| Metric | `ankusa_sink_http_pool_origins` |
| Refusal | a delivery that cannot get a connection within 5 s, or that would open origin 4,097, fails that attempt (`{:error, :pool_origins_exhausted}` for the latter) and is retried by the retry policy; deliveries to open origins are never refused for the ceiling |

The pool verifies peers against the system trust store with one 5 s connect
timeout, so a computed `url` cannot also set `cacertfile` or
`allow_insecure_tls` (refused at load): give such a sink a pool of its own with
`req_options: [finch: [name: MyFinch]]`, which owns its TLS options. A plain
`http` URL whose host has only IPv6 addresses cannot be pinned and is
`{:permanent, {:computed, :url, :unpinnable}}`. This pool is interim: owned,
bounded pools for every outbound client are the hardening plan's DEL-3, which
replaces it. Tests: `sink/http_test.exs` ("computed destinations": `computed url
per tenant`, `computed url denied`, the pool and its idle reap).

### Idempotent receivers

Delivery is at-least-once, so every consumer is an idempotent receiver. Dedupe
on the idempotency key; the ids it is built from mean different things:

- `x-ankusa-idempotency-key` (the message's `idempotency_key` on a queue sink)
  is the key to dedupe on. Ankusa computes it once per hook:
  `tenant:source_id:dedupe_key` when the source extracts the provider's event
  key (`dedupe:` on the source), else the hook's `id`. The full rule is under
  [`Sink.RabbitMQ`](#sinkrabbitmq-queue-delivery) below.
- `x-ankusa-id`, the message `id` on a queue sink, identifies **one stored
  hook**. Every redelivery of that hook (a retry, a DLQ replay, a dispatch
  restart) carries the same `id`.
- A provider retry is a **different stored hook** with a different `id`
  unless the source sets `dedupe` (see [`dedupe`](configuration.md#sources)),
  which collapses a repeat within the TTL into the original. Without it the
  idempotency key is that `id` and cannot collapse them: configure the
  source's `dedupe`, or dedupe on the provider's own event id in the body
  (e.g. Stripe's `id`).

Under [`wal.type: dual`](architecture.md#the-core-invariant) two more cases
reach a consumer, both with the **same hook `id` and `idempotency_key`**, so
the same dedupe covers them:

- **Redelivery after a host loss.** A replacement node restores the latest
  backup and replays the intake log
  ([`deployment.md`](deployment.md#host-loss-and-the-intake-log)). A hook that
  was committed after that backup and had already been delivered by the lost
  node is delivered again by the replacement, which has no memory of the
  delivery. The number is bounded by the hooks acked since the lost node's last
  backup (a fresher `backup.interval_ms` shrinks it; the final checkpoint of a
  clean stop makes it zero). Nothing is lost and no second hook is created:
  `duplicate_hooks` in the fleet drill stays 0.
- **A hook whose log half was missing.** A hook whose local write landed and
  whose intake publish did not is delivered normally (dispatch is not gated on the
  log), and the provider, who got a `503`, retries. With a `dedupe:` on the
  source the retry is answered `503 intake_pending` until the reconciler has
  republished the original, then `201` with the first `id`; without one it is a
  new hook with a new `id`, delivered again (as after any lost `2xx`).

Configure `dedupe:` on every source of a dual deployment: it is what turns the
provider's retry of a `503` into the original hook instead of a second one.

Provider request headers are forwarded to every sink per the source's
`forward_headers` option (see [`configuration.md`](configuration.md#sources)):
`Sink.Http` sends them as request headers, and the queue sinks carry them in
the message's `headers` object. Authentication and framing headers and every
`x-ankusa-*` name are never forwarded. A header the source's `store_headers`
policy dropped at ingest is not in the stored hook at all, so no
`forward_headers` setting forwards it; a hashed one is forwarded as its
`sha256:<hex>` digest (see
[`configuration.md#stored-headers`](configuration.md#stored-headers)). So a header-borne id such as
`X-GitHub-Delivery` or `webhook-id` reaches the consumer as a forwarded header,
and as `dedupe_key` when the source extracts it. `Sink.Http` adds its own
`x-ankusa-id`, `x-ankusa-source`, `x-ankusa-idempotency-key`,
`x-ankusa-verification` and (when set) `x-ankusa-tenant`, `x-ankusa-dedupe-key`
and `x-ankusa-replay-id`; a message carries `id`, `source_id`, `tenant_id`,
`received_at`, `content_type`, `size`, `sha256`, `dedupe_key`, `replay_id`,
`idempotency_key`, `verification`, `headers` and the body (or claim).

**Verification.** Every delivery says how the hook was verified at ingest:
`x-ankusa-verification` on `Sink.Http` and the message's `verification` object
on a queue sink, `{"status": "ok" | "failed" | "skipped", "flagged": bool}`.
`skipped` is a hook nothing verified (a source with no verifier, or a lifecycle
event). `flagged` is `true` for a hook a source accepted despite a failed check
(`on_verify_failure: accept_flag`): `Sink.Http` then also sends
`x-ankusa-verification-flagged: true`. Treat a flagged hook, and the tenant it
names, as unverified. The failure's reason is not on the wire.

### `Sink.RabbitMQ`: queue delivery

The queue-adapter story: an ingest fleet publishing to a broker instead of
(or in addition to) HTTP-forwarding or in-process handling. See
[`examples/rabbitmq-consumer/`](https://github.com/jamescarr/ankusa/tree/main/examples/rabbitmq-consumer/) for a full
worked deployment.

**Publishes to an exchange only, never a queue.** Binding a queue to the
exchange, and everything downstream of that, is the consumer's job. This
mirrors real AMQP topology ownership: producers own exchanges, consumers
own their own queues. Adding a fifth consumer later never touches ingest
config. Until at least one queue is bound, a hook is not delivered: every
publish is `mandatory`, so one the exchange routes to no queue fails as
`{:error, {:unroutable, routing_key}}`, is retried, and is dead-lettered once
the retry policy gives up (under `wal.type: none` it is a `503`).

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
 "content_type": "application/json", "size": 245, "dedupe_key": "evt_1", "replay_id": null,
 "idempotency_key": "acme:stripe:evt_1", "verification": {"status": "ok", "flagged": false},
 "headers": {"x-github-event": "push"}, "sha256": "2cf24d...", "body_base64": "eyJpZCI6..."}

// fat payload
{"v": 1, "id": "01a0...", "source_id": "stripe", "tenant_id": "acme", "received_at": 173...,
 "content_type": "application/octet-stream", "size": 3145728, "dedupe_key": "evt_1",
 "replay_id": null, "idempotency_key": "acme:stripe:evt_1",
 "verification": {"status": "ok", "flagged": false}, "headers": {},
 "claim": "urn:ankusa:claim:v1:acme:01M39VMD8RA3C5HR4RBV67Y002", "sha256": "3bea8a..."}
```

`"v"` changes only when an existing field changes meaning or disappears;
consumers must ignore keys they don't know.

Every message carries `sha256` (lowercase hex, inline bodies too), the
provider event `dedupe_key` (or `null`), the `replay_id` of the replay job when
this delivery is a replay (else `null`), the `idempotency_key` (below), the
`verification` result (see [Idempotent receivers](#idempotent-receivers) above),
and the forwarded provider request `headers` (lowercased; the source's
`forward_headers` option decides which — see
[`configuration.md`](configuration.md#sources)).

**Idempotency key.** Consumers dedupe on `idempotency_key`, which Ankusa
computes once per hook and ships everywhere the hook goes: the message's
`idempotency_key` field, the `x-ankusa-idempotency-key` header on `Sink.Http`
deliveries, and the `ankusa_idempotency_key` header on RabbitMQ, Kafka and NATS
messages. It is `tenant:source_id:dedupe_key` when the source extracted a
provider event key (`tenant` is `default` for a hook with no tenant), else the
hook's `id`. Tenant and source ids are `[A-Za-z0-9_-]`, so the first `:` always
ends the tenant; the key is otherwise opaque, so don't parse it. The tenant is
in the key because ingest dedupe scopes by tenant and source: under a
`tenant_path` source, two tenants' hooks with the same provider event id are
two hooks and must stay two keys at the consumer.

Read the key; don't rebuild it. Every SDK's `idempotency_key` helper returns the
shipped value and computes the same formula only for a message or delivery from
a node older than the field. Append `#replay:<replay_id>` to the key (the
helper's `include_replay` option) when a replay must be reprocessed rather than
dropped. Every SDK ships the decoder and the helper; see its README's
"Consuming queue messages".

A consumer redeems `claim` with `GET /v1/claims/:tenant/:claim_id` against the
claim-check gateway and checks the bytes against the message's `sha256`. See
[`claim-check.md`](claim-check.md#redeem-a-claim). (An Elixir consumer can
call `Ankusa.ClaimCheck.redeem/3`, which does both.)

**Connection lifecycle**: one supervised connection + confirm-mode channel
per `(instance, url, exchange)`, started on demand by the first `deliver/3`
call, registered through the same `Ankusa.Registry`/`Ankusa.via` every other
instance-scoped process uses under a digest of the URL (a password in the URL
never appears in a process name, and `:sys.get_state/1` on the connection
shows it redacted). Its own `DynamicSupervisor` is booted by
`ankusa_rabbitmq`'s own `Application`, zero changes to `ankusa` core. The
connection dials after it starts, so a dead broker never stalls the
supervisor. Every publish is `mandatory` and is answered on the broker's
**confirm** of that publish, so `deliver/3` returns `:ok` only when at least
one queue bound to the exchange accepted the message, never just because a
socket took it. Confirms are asynchronous: up to `:max_inflight` (default 256)
publishes are outstanding on the channel at once, so one slow confirm never
serializes the rest. Surviving a broker restart is the queue's property:
messages are always published `persistent`, and durable classic and quorum
queues persist them before confirming. Every other result is an error that
flows straight into the source's `Ankusa.RetryPolicy`:

| Result | Meaning |
| --- | --- |
| `:ok` | Confirmed and not returned: at least one queue accepted it. |
| `{:error, {:unroutable, routing_key}}` | The exchange routed it to no queue. |
| `{:error, :nacked}` | The broker refused it, e.g. a queue's `reject-publish` overflow. |
| `{:error, :confirm_timeout}` | No confirm within `:confirm_timeout_ms` (milliseconds, default 5000). |
| `{:error, :busy}` | `:max_inflight` publishes already await confirms on this connection; nothing was sent. |
| `{:error, :expired}` | The caller's deadline passed while the publish waited in the connection's mailbox; it was never sent, so an abandoned call is never published behind the caller's back. |
| `{:error, {:channel_closed, reason}}` | The broker closed the channel before the confirm (a publish to a deleted exchange is a `404`). The channel is reopened at once on the same connection, re-declaring the exchange. |
| `{:error, {:publish_failed, reason}}` | The publish itself failed. |
| `{:error, :not_connected}` | No channel yet. Connection loss doesn't crash the GenServer; it reconnects every `:retry_ms` (default 5000). |

There is no option to turn `mandatory` off, and no separate reconnect policy
to get wrong. A connection whose sink is removed from config closes after
`:idle_timeout_ms` with no delivery through it (`Ankusa.Sink.Reaper`), so a
deleted source does not leave it open for the life of the node.

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
publisher confirms. brod's producer is not idempotent, and a produce that
timed out cannot be cancelled, so it may still land and its retry duplicate
it. Delivery is at-least-once anyway, and consumers dedupe on the
`idempotency_key`.

**A record the broker will never take is dead-lettered at once**
(`{:permanent, reason}`): one whose value plus key exceeds
`:max_record_bytes` (default 1,000,000, just under the broker's 1 MiB
`message.max.bytes`; raise it with the topic's `max.message.bytes`) is refused
before it is produced, and a broker `message_too_large`, `invalid_message` or
`invalid_record` is permanent too. Retrying any of them for days would only
fill the log.

**Client lifecycle**: one brod client per `(instance, :client)`, started on
demand by the first `deliver/3` call under `ankusa_kafka`'s own
`DynamicSupervisor` (`ankusa` core unchanged). brod's client owns
reconnects, leader changes, and metadata refresh. A brod client id must be a
registered atom, so it is `:"ankusa_kafka.<instance>.<client>"`. Both parts
come from config, so the number of atoms is bounded and two instances never
collide. Unreachable brokers, unknown topics and timeouts surface as
`{:error, reason}`.

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
sequence would report a refused hook as delivered. The one permanent error is
`{:permanent, :max_payload_exceeded}`: a message over the server's
`max_payload` (1 MiB by default) is refused by gnat before it is sent, and is
dead-lettered at once rather than retried. It takes an `:inline_max_bytes`
raised past that limit; at the default (64 KiB) a larger body is claim-checked.

**The subject is the address, the headers are the metadata.** Each message
carries the same headers the Kafka sink sets — always `ankusa_id`,
`ankusa_source_id`, `ankusa_tenant_id`, `ankusa_message_version`,
`ankusa_idempotency_key` and `content_type`, plus `ankusa_dedupe_key` and
`ankusa_replay_id` when present — and `Nats-Msg-Id` (below), so a consumer
parses one set of fields regardless of
transport. Unlike Kafka, there is no separate record key: NATS has no
partition-ordering contract to lean on and no partition count to change under
you. Order within a subject is the order the stream received the messages: the
order Ankusa published them, not the order the hooks arrived (see
[Deliveries are not ordered](#the-dispatch-pipeline)); dedupe on
`idempotency_key`.

**Connection lifecycle**: one `Gnat.ConnectionSupervisor` per `(instance,
connection)`, started on demand by the first `deliver/3` under `ankusa_nats`'s
own `DynamicSupervisor` and found through the instance's registry (nothing
registered globally, so two instances never share a connection; `ankusa` core
unchanged). The start returns at once and the handshake runs in that
supervisor, so a slow or dead server never stalls other deliveries behind it.
It connects to one of `:servers` at a time, picked at random per attempt
(gnat's behaviour), and reconnects with a 2 s backoff whenever the socket
closes. While the connection is not up `deliver/3` returns
`{:error, :not_connected}`, retried by the source's retry policy.

**At-least-once, as everywhere else.** A publish whose ack is lost can still
have been stored, so consumers dedupe on the `idempotency_key`. Every publish
also sets `Nats-Msg-Id` to the hook's `id` — or `id:replay:<replay_id>` on a
replay — so JetStream's duplicate window collapses a re-publish of one
delivery, while a deliberate replay carries a distinct id and is stored.

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
headers, so the fields the Kafka and NATS sinks put in headers (`id`,
`source_id`, `tenant_id`, message version, content type, idempotency key, and
the dedupe key and replay id when present) travel only inside
the `Ankusa.Sink.Message` body; a consumer decodes it and reads them there.
The same applies to a claim ticket for a fat payload: redeem it with
`Ankusa.ClaimCheck.redeem/3` or
`GET /v1/claims/:tenant/:claim_id`. Nothing else enforces the claim check — a
Redis consumer that can't reach the claim-check gateway should ask for a
larger `inline_max_bytes` instead.

**Delivery is at-least-once, as everywhere else**, and a retry or a DLQ replay
republishes to every live subscriber, so consumers dedupe on the `idempotency_key`.

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

### `Sink.SQS`: queue delivery

The same `Ankusa.Sink.Message`, sent to an Amazon SQS queue, standard or FIFO.
Consumers on SQS (Lambda, Celery's SQS transport, a worker polling
`ReceiveMessage`) read it directly, with no Kafka-to-SQS bridge in between. The
sink lives in `ankusa` core: it is one SigV4-signed HTTPS `POST` per delivery
through `Ankusa.HttpClient`, with the signer `BlobStore.S3` already uses and
the same credential chain (`Ankusa.AWS.Credentials`), so it adds no
dependency and starts no process.

```elixir
sinks: [
  {Ankusa.Sink.SQS,
   queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/hooks.fifo",
   region: "us-east-1",
   message_group_id: "stripe",                  # FIFO default: "tenant/source"; or a 1-arity fun
   inline_max_bytes: 65_536,                    # above this the message carries a claim reference
   max_message_bytes: 1_048_576,                # body + attributes; larger is dead-lettered at once
   timeout_ms: 5_000}
   # endpoint: "http://localhost:4566"          # default: the queue URL's origin
   # access_key_id:/secret_access_key:/session_token: — else AWS_* env, IRSA, IMDSv2
]
```

**It never creates the queue.** A queue's type, retention, visibility timeout,
redrive policy and encryption are an operator's contract. A queue that does
not exist is `{:error, {:sqs, 400, "QueueDoesNotExist", message}}`, into the
retry policy and then the DLQ, the way `Sink.NATS` treats a missing stream.

**`deliver/3` returns `:ok` only once SQS has the message.** That is a `200`
from `SendMessage` whose `MD5OfMessageBody` matches the MD5 of the body sent;
SQS answers `200` after storing the message redundantly, so the sink is
durable for [direct mode](#direct-mode). The other outcomes:

| Outcome | Result |
| --- | --- |
| body + attributes over `max_message_bytes` (1 MiB, SQS's limit) | permanent `{:message_too_large, size, max}`, before any request |
| SQS refuses the message itself: `InvalidMessageContents`, `InvalidParameterValue` | permanent `{:sqs, status, code, message}`: dead-lettered at once |
| any other SQS error: `QueueDoesNotExist`, `AccessDenied`, throttling, a 5xx | transient `{:sqs, status, code, message}`: the retry policy |
| a non-2xx that is not an SQS error document (a proxy) | transient `{:status, status, body}` |
| a `200` with a mismatched MD5, no credentials, a transport failure | transient |

**FIFO when the queue URL ends `.fifo`** (AWS requires the suffix). Each send
carries a `MessageGroupId`, `message_group_id` or by default
`"tenant_id/source_id"` (`"/source_id"` without a tenant), the scope of the
Kafka sink's default key: messages in one group are received in the order
Ankusa sent them, which is not the order the hooks arrived. See
[Deliveries are not ordered](#the-dispatch-pipeline); dedupe on
`idempotency_key`. Its
`MessageDeduplicationId` is the hook's `id`, or `id:replay:<replay_id>` on a
replay (the `Nats-Msg-Id` rule), so SQS collapses a retry after a lost
response while a deliberate replay is stored. SQS's deduplication window is
**5 minutes**: a retry later than that can store a second copy, so consumers
still dedupe on the `idempotency_key`. On a standard queue the sink sends no
deduplication id, and a `MessageGroupId` only when `message_group_id` is set
(fair queues).

**The queue is the address, message attributes are the metadata.** Each
message carries `String` attributes `ankusa_id`, `ankusa_source_id`,
`ankusa_tenant_id`, `ankusa_message_version`, `ankusa_idempotency_key` and
`content_type`, plus `ankusa_dedupe_key` and `ankusa_replay_id` when present:
the Kafka header set, at most 8 of SQS's 10 attributes. `ankusa_tenant_id` is
left out when the hook has no tenant, because SQS refuses an empty attribute
value. SQS counts attributes toward the size limit, so `max_message_bytes`
checks body plus attributes.

### `Sink.GooglePubSub`: Google Cloud Pub/Sub delivery

The same `Ankusa.Sink.Message`, published to a Google Cloud Pub/Sub topic.
Consumers on Pub/Sub (Cloud Run, Dataflow, a worker pulling a subscription)
read it directly. Like `Sink.SQS`, the sink lives in `ankusa` core: one
`topics.publish` REST `POST` per delivery through `Ankusa.HttpClient`, so it
adds no dependency and starts no process. Auth is a `:token_provider` MFA, as
for `BlobStore.GCS`; Ankusa bundles no OAuth client. (The `ankusa_server`
image ships one for the metadata server, `auth: metadata`.)

```elixir
sinks: [
  {Ankusa.Sink.GooglePubSub,
   project: "my-project",
   topic: "ankusa-hooks",                       # the topic id, not projects/.../topics/...
   token_provider: {MyApp.Auth, :gcp_token, []},  # {:ok, token} | :error; omit for an emulator
   ordering_key: "stripe",                      # default none; a string, or a 1-arity fun
   inline_max_bytes: 65_536,                    # above this the message carries a claim reference
   max_message_bytes: 10_000_000,               # data + attributes + key; larger is dead-lettered at once
   timeout_ms: 5_000}
   # endpoint: "https://us-east1-pubsub.googleapis.com"  # default: https://pubsub.googleapis.com
]
```

**It never creates the topic or a subscription.** Retention, schema,
encryption and who may subscribe are an operator's contract. A topic that does
not exist is `{:error, {:pubsub, 404, "NOT_FOUND", message}}`, into the retry
policy and then the DLQ. A topic with **no subscription** (and no topic
message retention) answers `200` and discards the message: the sink cannot
detect it.

**`deliver/3` returns `:ok` only once Pub/Sub has the message.** That is a
`200` whose reply names the one message id; Pub/Sub answers `200` after
persisting the message, so the sink is durable for [direct mode](#direct-mode).
The other outcomes:

| Outcome | Result |
| --- | --- |
| data + attributes + ordering key over `max_message_bytes` (10 MB, Pub/Sub's limit) | permanent `{:message_too_large, size, max}`, before any request |
| Pub/Sub refuses the message itself: `INVALID_ARGUMENT` | permanent `{:pubsub, status, "INVALID_ARGUMENT", message}`: dead-lettered at once |
| any other Pub/Sub error: `NOT_FOUND`, `PERMISSION_DENIED`, `UNAUTHENTICATED`, `RESOURCE_EXHAUSTED`, `UNAVAILABLE` | transient `{:pubsub, status, google_status, message}`: the retry policy |
| a non-200 that is not a Google error document (a proxy) | transient `{:status, status, body}` |
| a `200` that does not name one message id, a transport failure | transient |
| the `:token_provider` answers `:error` | transient `:no_credentials`, no request sent |

**There is no publish-side deduplication.** Delivery is at-least-once, so
consumers dedupe on the `idempotency_key` (the `ankusa_idempotency_key`
attribute).

**The topic is the address, message attributes are the metadata.** Each message
carries attributes `ankusa_id`, `ankusa_source_id`, `ankusa_tenant_id`,
`ankusa_message_version`, `ankusa_idempotency_key` and `content_type`, plus
`ankusa_dedupe_key` and `ankusa_replay_id` when present: the same set
`Sink.SQS` sends (`Ankusa.Sink.Message.attributes/2`). `ankusa_tenant_id` is
left out when the hook has no tenant. Pub/Sub counts attributes and the
ordering key toward the size limit, so `max_message_bytes` checks all three
with the data.

**Ordering is opt-in.** With `ordering_key` set, the message carries an
`orderingKey`, which orders delivery only on a subscription with message
ordering enabled, published through one regional endpoint, and caps that key's
throughput at about 1 MB/s. Ankusa's own deliveries are unordered (see
[`Ankusa.Sink`](#ankusasink)), so no key is sent by default.

## `Ankusa.RetryPolicy`

```elixir
@callback backoff(attempt :: pos_integer(), opts :: keyword()) :: {:retry, delay_ms} | :give_up
```

`RetryPolicy.Exponential` (the only shipped policy, and the default):
capped binary exponential backoff with optional full jitter.

| Opt | Default |
| --- | --- |
| `:base_ms` | `100` |
| `:max_ms` | `300_000` (ceiling before jitter) |
| `:max_attempts` | `84` |
| `:jitter` | `true`. Multiplies the delay by a random factor in `[0.5, 1.0]` |

The defaults retry for about 6 hours: 100 ms doubling to the 5-minute cap by
attempt 13, then 5 minutes apart until attempt 84 (21 709.5 s without jitter;
jitter halves a delay at worst, so 3–6 h). Set `max_attempts` lower to
dead-letter sooner.

Set dispatch-wide via `config.dispatch.retry`; there's currently no
per-source override (see [`configuration.md`](configuration.md)).

## Dead letters and replay

The DLQ is the set of **dead delivery rows** — there is no separate file. When
a row's sink gives up, the row is marked dead and carries the failure as text:
`inspect({:sink, Module, reason})`, the exact `reason` string `GET /v1/dlq`
returns. A dead row is still an obligation, so its hook is kept (and survives
a restart) until the row is replayed and delivered. The give-up is one atomic
store batch, but dispatch writes its outcomes without a per-write fsync: a power
failure right after one can undo it, and the hook is retried again
(at-least-once, never lost). The next synced commit makes it durable.

`Ankusa.Replay.start/2` creates a durable replay job that moves matching dead
rows back to pending, a paced page at a time, and the pipeline delivers them
through the source's *current* sinks and options:

```elixir
{:ok, :created, job} =
  Ankusa.Replay.start(:default,
    kind: :dlq,
    source_id: "stripe",
    since: System.system_time(:millisecond) - 3_600_000,
    rate: 1_000
  )
```

The job drips rows into the delivery queue at `rate` items per second (default
1 000, max 100 000) and only while dispatch has spare capacity (its
oldest-due lag stays under `max_lag_ms`, 2 s by default), so it can be left
running against live traffic. It is durable — the cursor commits in the same
batch as the rows it moves — so a restart resumes it; a job whose deliveries
keep dead-lettering, or keep being parked behind an open breaker, pauses
itself. Replayed deliveries keep the hook's
original `id`, `dedupe_key` and `idempotency_key` and carry the job id as
`replay_id`.
`Ankusa.Replay.get/2` and `update/3` watch and steer it
(`:running | :paused | :cancelled`); the admin API exposes the same jobs as
`POST /v1/replays`, `GET /v1/replays[/:id]`, `PATCH /v1/replays/:id`. Delivery
is **asynchronous**: by the time a page is moved the rows are out of the DLQ
and the pipeline will deliver them (a row that fails again is dead-lettered
again). It needs the `:dispatch` role on this node. Filters (`:source_id`,
`:id`, `:since`, `:until`, unix-ms bounds) are all optional and combine as
AND; a `dlq` job touches only rows dead-lettered at or before its own
creation, so rows that die again during the replay are never picked up twice.
`kind: :archive` re-sends archived hooks over a `received_at` window instead,
`kind: :quarantine` releases held hooks that now verify (see
[Quarantine](#quarantine)), and `kind: :lifecycle` re-emits lifecycle events from
the audit trail over a `since`/`until` window to the node's lifecycle sinks (see
[Lifecycle events](asyncapi.md#lifecycle-events)); that job has no `source_id`
or `id`, keeps each event's `id`, and moves its cursor only once the publisher
reports the page delivered.

`GET /v1/dlq`, `GET /v1/replays[/:id]`, `PATCH /v1/replays/:id` and
`POST /v1/replays` are limited by tenant the same way
([scopes](configuration.md#control-plane-authentication)): `?tenant=<id>` (the
`tenant` key of a replay body) names one tenant, a token limited to some
`tenant:<id>` scopes gets only those tenants' dead letters and starts only jobs
whose `tenants` filter is those tenants (a job another tenant's token started is
`404` to it), and naming a tenant the token does not hold is `403`.
`Ankusa.Queue.dead/2` and `Ankusa.Replay.start/2` take the same filter as
`:tenants`. The dead index is also kept by tenant (`?X`, [Tenants, counters and
the active index](storage.md#tenants-counters-and-the-active-index)), so a
tenant-filtered read is one reverse range scan of each named tenant's rows and
reads no hook but those of the page it returns (`tenant scoped dlq reads one
range` in `admin/router_test.exs`); `Ankusa.Queue.pending_for_tenant/2` and
`dead_for_tenant/2` answer a tenant's counts from counters, in constant time,
and `Ankusa.Queue.purge_tenant/3` removes a tenant's pending or dead rows in
synced chunks. A
`lifecycle` job's events belong to their tenant (a route's to `default`), so the
same filter selects them.
Delivery is at-least-once, so a replayed hook is a redelivery.

### Retention and the DLQ ceilings

`queue.dlq` bounds the dead-letter queue (see [`configuration.md`](configuration.md)):
rows older than `retention_days`, and the oldest rows while the DLQ is over
`max_rows` or `max_bytes`, are evicted by `Ankusa.Queue.DlqReaper`, but only once
the hook is archived and `Storage.fetch/2` can find it. Anything else is
back-pressure: new hooks get `503 dlq_full` until the compactor drains, so the
disk stays flat by draining the archive and never by deleting an acked hook.
`DELETE /v1/dlq?tenant&source_id&since&until&limit` (scope `dlq:write`) runs the
same eviction on demand and answers `{deleted, bytes, unarchived, more}`.

## Quarantine

A durable holding pen for envelopes whose source has
`on_verify_failure: :quarantine`. The point: a bad secret rotation should
never silently eat real events, so a quarantined hook is answered `202` (the
provider will not retry it), kept, and released once its source verifies it.

**Keep it out of the pen.** Rotate a secret with a window instead of a cut-over:
`secret` takes a list, newest first, and every key is tried, so hooks signed
with either secret verify while the provider switches over. Every entry must be
non-empty: an empty HMAC key would verify whatever anyone signs with it, so an
unset `${OLD:-}` is a load error, not a skipped slot.

```yaml
verify: {type: stripe, secret: ["${STRIPE_WHSEC_NEW}", "${STRIPE_WHSEC}"]}
```

**Bounded twice**, by the `quarantine` config section, so a flood of forged
requests can't fill the disk, and one source's flood never spends another
source's tokens:

- One token bucket per source: `burst` (default 100) back to back, refilled
  `rate` per second (default 20). Over it, the hook is refused with
  `429 quarantine_rate_limited` and a `Retry-After`; nothing is stored.
- A cap on the pen's total bytes, `max_bytes` (default 1 GiB). A hook that
  would cross it is refused with `503 quarantine_full` and `Retry-After: 60`.
  A full pen refuses new hooks; it never evicts one it already answered `202`
  for. It clears only when an operator releases or purges entries.

The byte cap is shared, though. One flooding source can fill it — a burst of
100 bodies of up to `max_body_bytes`, then 20 a second — and from then on every
source's failed hooks are refused with `503` (providers retry, and nothing
already acked is lost) until you purge the flood with
`DELETE /v1/quarantine?source_id=…`.

Entries are stored in this node's `Ankusa.Store`, two keys per entry in one
synced batch: a summary (id, source, tenant, time, reason, size) and the whole
envelope — method, path, headers, body. `put/3` answers `:ok` only once both
are on disk; a store that cannot take the write makes ingest answer `503` and
spends no token. A restart neither empties the pen nor forgets its size.

**How a forged flood is kept off everything else.** The decisions are made in
the request's own process: a source's bucket is one ETS row updated with a
compare-and-swap (the rate limiter's GCRA), the byte count and the number of
waiting writes are `:ets.update_counter/3`, and a refusal after the token was
taken gives the token back. Only the disk write is shared, and it is a group
commit: callers hand their two rows to the pen process, which commits every
write that has queued meanwhile in one synced `Store.write`. A flood therefore
costs one fsync per group rather than one per hook, holds one of the VM's
dirty-IO schedulers however many callers wait (the ack path's own commits keep
theirs), and never makes another source's `put/3` or an `accept_flag` hook's
charge wait for it. `purge/3` runs in the pen process and so holds up the
writes queued behind it for the length of one purge call; the retention sweep
does not (it runs in a process of its own).

| | |
| --- | --- |
| Cap | 1 000 writes waiting for the pen's group commit (queued or in the group being committed), plus the per-source bucket and `max_bytes` above |
| Sweep | a source's bucket row is deleted once it is full again (every 60 s); a write leaves the count when the pen has answered it |
| Metric | `ankusa_quarantine_pending` (writes waiting), `ankusa_quarantine_bytes`, `ankusa_quarantine_entries` |
| Refusal | the 1 001st waiting write is `503 overload` with `Retry-After: 1`, and spends no token and no bytes; a write the pen does not answer within 5 s is `503 store_unavailable` (it may still land afterwards, so it keeps its token and its bytes counted) |

**Inspect** with `GET /v1/quarantine` on the admin port (newest first, no
bodies) or `Ankusa.Edge.Quarantine.recent/2`.

With [admin authentication](configuration.md#control-plane-authentication) the
list, a purge and a quarantine replay are limited to the caller's tenants:
`?tenant=<id>` names one, a token limited to some `tenant:<id>` scopes sees and
deletes only those tenants' entries, and `tenant:*` is required for the whole
pen. An entry written before the pen kept the tenant belongs to `default`.

**Release** with a `quarantine` replay job, after fixing the source's secret:

```sh
curl -XPOST localhost:4002/v1/replays -d '{"kind":"quarantine","source_id":"stripe"}'
```

The job re-verifies each held hook against its source's *current* verifier,
judging the signature's timestamp window at the hook's receive time (a release
hours later still passes a hook that was on time). A hook that passes is
committed through the normal queue path with its original `id`: dedupe applies,
the archive gets it, and every delivery carries the job's id as `replay_id`.
The commit and the pen delete are one synced batch. A hook that still fails
stays in the pen and counts as `skipped`; one that is a duplicate of a hook
already accepted (the provider's own retry got through) leaves the pen and
counts as `skipped` too. Filters (`source_id`, `id`, `since`/`until` on
`received_at`) are optional, and, like a `dlq` job, a `quarantine` job only
touches hooks quarantined at or before its creation. The job runs in the
Replayer, so it needs the `:dispatch` and `:edge` roles on the node that holds
the pen.

**Purge** what will never verify with `DELETE /v1/quarantine` — the same
filters plus `limit` (default 1000, at most 10 000 per call), oldest first.
It answers `{"deleted": n, "bytes": b}` and frees those bytes against the cap.

```elixir
{:ok, entries} = Ankusa.Edge.Quarantine.recent(:default, 50)
# entries: [%{id: "...", source_id: "stripe", tenant_id: "default", received_at: ...,
#             reason: :no_match, size: 1834}, ...]
{:ok, :created, job} = Ankusa.Replay.start(:default, kind: :quarantine)
{:ok, %{deleted: n, bytes: b}} = Ankusa.Edge.Quarantine.purge(:default, %{source_id: "stripe"}, 1_000)
```

Under `wal.type: none` this pen, the API-managed sources and the rate-limit
overrides are the node's only local state: nothing is committed, so there is
no queue, no DLQ, and no Replayer to release from — the pen is inspect and
purge only there, which is why `config-examples/direct.yml` recommends
`reject`. Entries are written only when a source opts in with
`on_verify_failure: quarantine`; the `reject` default writes nothing. See
[Direct mode](#direct-mode).
