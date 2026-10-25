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

**A sink that batches** (`Sink.SQS`, `Sink.GooglePubSub`) is the exception to the
limit on attempts running at once: its attempts wait in the destination's
batcher for their batch's response, so counting them against
`dispatch.concurrency`, a tenant's `concurrency` or `dispatch.sink_concurrency`
would cap every batch at that many messages. They are not counted;
`sink_concurrency` counts the sink's *requests* in flight instead, and the
claimed-row windows below (`dispatch.max_inflight`, `dispatch.tenants`) bound
how many attempts wait. Each attempt still settles its own row, times out on its
own, and moves its own tenant's breaker. See
[`Sink.SQS`](#sinksqs-queue-delivery).

**Deliveries are not ordered.** Two hooks for one sink may run in either
order, and a retry runs after whatever is due before it. A Kafka partition key
or an AMQP routing key only keeps the order hooks were published in, so it does
not restore one. A consumer that needs order has to rebuild it from data it
receives (the provider's own event timestamp or sequence number in the body;
`received_at` is on every message, but ties are possible within a millisecond)
and tolerate redelivery.

**Deliveries are not ordered unless a sink entry opts in.** Two hooks for one
sink may run in either order, and a retry runs after whatever is due before it.
A Kafka partition key or an AMQP routing key only keeps the order hooks were
published in, so it does not restore one. A consumer that needs order per key
(a Stripe customer's events, a Salesforce record's updates) sets
`ordered: true` and an `ordering_key` on the sink entry: see
[Ordering lanes](#ordering-lanes). Without it, a consumer has to rebuild order
from data it receives (the provider's own event timestamp or sequence number in
the body; `received_at` is on every message, but ties are possible within a
millisecond) and tolerate redelivery.

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
one `attempt_timeout_ms`; and parked rows spend no attempts, but they do age:
a row parked behind an open breaker is due again no later than its [retry
horizon](#retry-horizon), and is dead-lettered there, so a breaker that stays
open for longer than a delivery's `max_elapsed_ms` dead-letters its backlog.
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

**Error classes.** Three reason shapes change what dispatch does:

| `reason` | Dispatch |
| --- | --- |
| `{:permanent, term}` | Dead-letter after this attempt, whatever the policy had left; does not count towards the circuit breaker. For what retrying cannot fix: a `410 Gone`, a record the broker will never take. |
| `{:permanent, {:config, term}}` | A permanent failure that is the operator's to fix, not the hook's: a credential the destination refuses (`AccessDenied`, `PERMISSION_DENIED`), a queue or topic that does not exist. Dead-letters like any `{:permanent, _}`, and counts in `ankusa_dispatch_config_errors_total{sink}` (the alert that a source needs a config fix and then a DLQ replay). |
| `{:retry_after, ms, term}` | Next attempt no sooner than `ms` (capped at `dispatch.max_retry_after_ms`, default one hour) or the policy's backoff, whichever is later; the policy still decides when to give up. |
| anything else | Transient: the retry policy. |

Whatever the class, a delivery still failing `max_elapsed_ms` (default 24 h,
per source: `retry:`) after its first attempt is dead-lettered; see [Retry
horizon](#retry-horizon).

`Ankusa.Sink.classify/1` returns the class of a reason.

Deliveries are **not ordered** unless the sink entry says `ordered: true` (see
[Ordering lanes](#ordering-lanes)): otherwise hooks for one sink may be delivered
in any order, and a retry runs after whatever is due before it. A consumer that
needs order without lanes has to rebuild it from data it receives (not from the
sink's key, which only keeps the order hooks were published in) and tolerate
redelivery.

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
| `400`, `401`, `403`, `404`, `410`, `413`, `422`, `451` | `{:permanent, {:status, s}}`: dead-letter now. A credential or `secret` fix is followed by a DLQ replay, not hours of retries against a receiver that keeps refusing; a `451` is the receiver refusing this sender for legal reasons, which no retry changes. |
| `408`, `429`, `5xx` with `Retry-After` (seconds or an HTTP-date) | `{:retry_after, ms, {:status, s}}` |
| `425` | `{:retry_after, ms, {:status, 425}}`: the `Retry-After`, else a full-jitter pause (1 to `min(30 s, 500 ms · 2^(attempt-1))`). `Too Early` means the receiver will take it shortly |
| `409` | transient `{:status, 409}`: the receiver's state (a lock, a missing parent) usually moves; a conflict that does not is bounded by the retry horizon |
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

### `Sink.Http`: connection pools

Every `Sink.Http` delivery (a static URL as much as a computed one) goes
through `Ankusa.HttpClient.Pools`, the node's owned Finch, not Req's default of
one 50-connection pool per origin that is never closed (one per tenant receiver,
so file descriptors and processes would grow with the tenant count until the node
ran out). There is one pool per receiver *address* and TLS profile, per instance,
started on delivery. Its bounds (`egress.pools` in the
[configuration](configuration.md#outbound-connection-pools)):

| | |
| --- | --- |
| Cap | `pool_size` (32) HTTP/1 connections per pool, so one receiver's slow endpoint holds at most that many; `max_pools` (1,024) live pools per instance and `max_pools_per_tenant` (256) per tenant, so one tenant with a thousand receivers only ever churns its own pools |
| Sweep | every 30 s a pool with no request in flight for `pool_max_idle_ms` (60 s) is closed; a connection idle `conn_max_idle_ms` (30 s) is not reused; a deleted source's idle pools are closed at once |
| Metric | gauge `ankusa_http_pools`; counters `ankusa_http_pool_closed_total{reason}` and `ankusa_http_pool_refused_total{scope}` |
| Refusal | at a ceiling the least recently used idle pool is closed to make room; with every pool in scope serving a request the delivery fails that attempt as `{:error, {:unavailable, :pool_limit}}`; a request that cannot get a connection within `pool_timeout` (5 s) fails it as a transport error. Both are retried by the retry policy |

A pool belongs to its receiver, not to a sink: several sinks (of any sources)
posting to one endpoint share it, up to `dispatch.concurrency` attempts at once.
Size `pool_size` for that, not for one sink: a pool of 8 against a 100 ms
receiver tops out at 80 requests a second however many attempts wait, and boot
warns when `pool_size * pool_count` is below `dispatch.concurrency`. A sink sets
its own `pool: [pool_size:, pool_count:, pool_timeout:, protocols:]`.
Connections negotiate HTTP/2 where the receiver offers it (`protocols`).

The pool is keyed by the address the egress check validated: its connections go
to that address and verify the peer against the name (SNI and certificate), so a
name that later resolves somewhere denied is refused on its next attempt, before
a pool is touched, and one that moves to another public address gets a pool of its
own. `cacertfile` and `allow_insecure_tls` are part of the pool's connection
options, so a computed `url` may use them. Tests: `http_client_test.exs`
(`pool reaped`, `http2`, the ceilings and the refusal), `sink/http_test.exs`
("computed destinations": `computed url per tenant`, `computed url denied`,
`every connection of a pool goes to the pinned address`, `a name that turns
private between two deliveries through one pool is denied on the second`),
`bench/pools_bench.exs` (10⁵ origins, throughput at one origin).

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
`x-ankusa-verification`, `x-ankusa-received-at` (unix milliseconds, the
message's `received_at`), `x-ankusa-body-sha256` (the lowercase hex SHA-256 of
the body sent) and (when set) `x-ankusa-tenant`, `x-ankusa-dedupe-key`
and `x-ankusa-replay-id`; a message carries `id`, `source_id`, `tenant_id`,
`received_at`, `content_type`, `size`, `sha256`, `dedupe_key`, `replay_id`,
`idempotency_key`, `verification`, `headers` and the body (or claim).

**Trace context travels with the hook.** A request's W3C `traceparent` (and
`tracestate`) is stored on the hook and delivered by every sink: the
`traceparent`/`tracestate` headers on `Sink.Http` (a provider's own header is
replaced, not forwarded beside it), the `traceparent`/`tracestate` fields of the
queue message (`null` for a hook with none) and the same pair as the broker's
headers or attributes, and the CloudEvents `traceparent` extension on lifecycle
events. A consumer continues the trace from them (the SDKs decode both fields).
Retries, replays and claim-checked bodies carry the same context. See
[Tracing](configuration.md#tracing).

**Forwarded headers are capped** so a provider with large headers cannot push a
small body over a transport limit: per source, `max_headers` (64) names and
`forward_headers_max_bytes` (16 KiB) of names plus values
([`configuration.md`](configuration.md#sources)). Names are considered in order
of arrival; a header past either cap is dropped whole (never truncated), and
each drop is counted as `ankusa_sink_headers_dropped_total{reason="count" |
"bytes"}` (`[:ankusa, :sink, :headers_dropped]`). The budget for one queue
message is therefore `inline_max_bytes × 4/3` (base64) plus about 1 KiB of
envelope plus at most `forward_headers_max_bytes`; the default 64 KiB is about
106 KiB. Before publishing, every queue sink checks the message against its
limit (`max_message_bytes`; `max_record_bytes` for Kafka): an inline message
over it is checked in and sent as a claim instead, and a body that is over it
by itself (`inline_max_bytes` at least its size) is dead-lettered at once as
`{:permanent, {:message_too_large, size, max}}`, with no broker call.

**Verification.** Every delivery says how the hook was verified at ingest:
`x-ankusa-verification` on `Sink.Http` and the message's `verification` object
on a queue sink, `{"status": "ok" | "failed" | "skipped", "flagged": bool}`.
`skipped` is a hook nothing verified (a source with no verifier, or a lifecycle
event). `flagged` is `true` for a hook a source accepted despite a failed check
(`on_verify_failure: accept_flag`): `Sink.Http` then also sends
`x-ankusa-verification-flagged: true`. Treat a flagged hook, and the tenant it
names, as unverified. The failure's reason is not on the wire. Every SDK exposes
the object and a trust check that is `true` only for `ok` and not flagged
(`isTrusted`, `is_trusted`, `Trusted()`, `trusted?`; see
[`integrations.md`](integrations.md#signed-deliveries)); the
conformance features are `message.verification` and `webhook.verification`.

### Consumer failure policy

Delivery is at-least-once, and so is a consumer's retry: a message a consumer
requeues comes back unchanged. That is right for a failure that can pass (the
claim gateway is down, the database is busy) and wrong for input that cannot
(the same bytes fail the same way every time, and a requeue loop on one of
them holds a partition or a queue's head). So every failure a consumer meets
is one of two kinds, and every SDK says which with one bit, `retryable`:

| What the consumer meets | Error (every SDK) | `retryable` | Do |
| --- | --- | --- | --- |
| a message or lifecycle event that is not JSON (empty and truncated included), not an object, or has a field of the wrong type | `InvalidMessageError`, `code` `invalid_json`, `not_an_object`, `invalid_field` (with `field`) | `false` | terminal queue, counted |
| a message with a `v` the SDK does not accept, or a lifecycle event whose `specversion` is not `1.0` | `InvalidMessageError`, `code` `unsupported_version` | `false` | terminal queue, counted; the consumer is older than the producer |
| a body that contradicts its own metadata: both or neither of `body_base64` and `claim`, bad base64, a `size` or `sha256` that does not match, a claim of another tenant | `InvalidMessageError`, `code` `ambiguous_body`, `missing_body`, `invalid_body_base64`, `size_mismatch`, `integrity`, `tenant_mismatch` | `false` | terminal queue, counted |
| a claim ref that does not parse, a claim the gateway no longer has (`404`) or refuses (`4xx`), bytes that fail the `sha256` | `InvalidClaimRefError`, `ClaimNotFoundError`, `ClaimRejectedError`, `ClaimIntegrityError` | `false` | terminal queue, counted |
| an HTTP delivery whose signature does not verify | `InvalidSignatureError` | `false` | answer `401`: the sink dead-letters it at once |
| an HTTP delivery with no `x-ankusa-id` | `MissingHookIdError` | not an attribute of this error | answer `400`: the sink dead-letters it at once |
| the claim gateway unreachable, `408`, `429`, `5xx` | `ClaimCheckUnavailableError` | `true` | requeue with a delay |
| your own handler failing | yours | yours | HTTP: answer `503` and the sink retries; a queue: requeue |

The rule behind the table: **a decode error or a version mismatch is
permanent.** Retrying cannot change the bytes, so the consumer sends the message
to its terminal queue (the broker's dead-letter queue, a table, a log
topic) with the error's `code` and `field`, acks the original, and moves on.
Never requeue on `retryable: false`, and never drop it silently:

- **Count it.** Count terminal-queue arrivals by `code` (a stable, small set,
  safe as a metric label). A rate of `unsupported_version` means the consumer
  is older than the producer: upgrade consumers before moving a producer to a
  new wire version (additive fields within the versions the SDK accepts never
  trigger it). Any other `code` means a producer or a transport is writing
  bytes Ankusa did not.
- **Keep the bytes.** The terminal queue holds the original message, so a fixed
  consumer can replay it; the idempotency key (`x-ankusa-idempotency-key`, the
  message's `idempotency_key`) makes a second pass harmless.
- **Answer an HTTP poison body with a permanent status, not a `5xx`.**
  `Sink.Http` dead-letters `400`, `401`, `403`, `404`, `410`, `413`, `422` and
  `451` at once (see the status table under [`Ankusa.Sink`](#ankusasink)) and
  retries `408`, `429` and `5xx`. A consumer that answers `500` for a body it
  can never accept is asking for the same delivery again until `dispatch.retry`
  gives up.

The conformance vectors pin the permanent half for every SDK: the
`consumer.failure_policy` feature (`conformance/cases/failure_policy.json`)
decodes empty, truncated, non-object and future-version input and expects
`retryable: false`, and `check.mjs` fails any case that expects
`InvalidMessageError` or `InvalidSignatureError` without it.

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
 "headers": {"x-github-event": "push"}, "instance": "default", "region": "eu-west-1",
 "sha256": "2cf24d...", "body_base64": "eyJpZCI6..."}

// fat payload
{"v": 1, "id": "01a0...", "source_id": "stripe", "tenant_id": "acme", "received_at": 173...,
 "content_type": "application/octet-stream", "size": 3145728, "dedupe_key": "evt_1",
 "replay_id": null, "idempotency_key": "acme:stripe:evt_1",
 "verification": {"status": "ok", "flagged": false}, "headers": {},
 "instance": "default", "region": "eu-west-1",
 "claim": "urn:ankusa:claim:v1:acme:01M39VMD8RA3C5HR4RBV67Y002", "sha256": "3bea8a..."}
```

**Versioning.** `"v"` is the message format's major version. The producer writes
`"v": 1`.
Every SDK accepts `min_supported <= v <= max_supported` (1 and 2; each SDK
exposes both bounds) and decodes either into its one current message type,
reporting the `v` the wire carried. A `v: 2` message is defined as a `v: 1`
message plus additive fields, so a consumer on an SDK that accepts 2 reads it
exactly like a `v: 1` message and ignores the keys it does not know; a producer
can therefore move to an additive `v: 2` without breaking a deployed consumer
that was upgraded to the SDK that accepts it. Consumers must ignore keys they
don't know. A field that changes meaning or disappears needs a `v` no SDK
accepts yet (3 or later): a message with a `v` outside the range is
`unsupported_version`, a permanent failure to dead-letter, never retry.

The messages core writes are pinned by the producer vectors in
[`conformance/cases/producer.json`](https://github.com/jamescarr/ankusa/blob/main/conformance/cases/producer.json),
generated from `Sink.Message.encode/3` by `mix ankusa.conformance.gen`. `mise run
check:conformance` regenerates them and fails when the checked-in file differs,
so a field added to the encoder without a vector update fails the build, and
every SDK has to decode what the encoder writes.

Every message carries `sha256` (lowercase hex, inline bodies too), the
provider event `dedupe_key` (or `null`), the `replay_id` of the replay job when
this delivery is a replay (else `null`), the `idempotency_key` (below), the
`verification` result (see [Idempotent receivers](#idempotent-receivers) above),
the forwarded provider request `headers` (lowercased; the source's
`forward_headers` option decides which — see
[`configuration.md`](configuration.md#sources)), and where it was published
from: `instance`, the name of the Ankusa instance, and `region`, the instance's
configured `dispatch.region` (YAML `node.region`; `null` when none is set).

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
fill the log. A raised `:inline_max_bytes` does not make that a trap: an inline
message whose base64 would pass `:max_record_bytes` is checked in and sent as a
claim, and only a body that is over the limit by itself is refused.

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
sequence would report a refused hook as delivered. Two errors are permanent:
`{:permanent, :max_payload_exceeded}`, a message over the server's
`max_payload` (1 MiB by default) that gnat refuses before sending it, and
`{:permanent, {:message_too_large, size, max}}`, below.
Both are dead-lettered at once rather than retried. The sink checks the
size limit itself first, as `:max_message_bytes` (default 1,048,576): an
inline message over it is claim-checked instead, and a body that is over it by
itself (an `:inline_max_bytes` raised past it) is refused with no server call.
Raise `:max_message_bytes` together with the server's `max_payload`.

**The subject is the address, the headers are the metadata.** Each message
carries the same headers the Kafka sink sets — always `ankusa_id`,
`ankusa_source_id`, `ankusa_tenant_id`, `ankusa_message_version`,
`ankusa_idempotency_key` and `content_type`, plus `ankusa_dedupe_key` and
`ankusa_replay_id` when present — and `Nats-Msg-Id` (below), so a consumer
parses one set of fields regardless of
transport. Unlike Kafka, there is no separate record key: NATS has no
partition-ordering contract to lean on and no partition count to change under
you. Order within a subject is the order the stream received the messages: the
order Ankusa published them. With `ordered: true` that is the order of a lane
(see [Ordering lanes](#ordering-lanes)); the key travels as the
`ankusa_ordering_key` header, and one subject keeps the order of sequential
publishes (consumer-side, not enforced). Dedupe on
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
sink lives in `ankusa` core: dispatch's deliveries go out in a SigV4-signed
`SendMessageBatch` of up to 10 ([Batching](#sinksqs-queue-delivery), below);
anything else is one `SendMessage`. Both go through `Ankusa.HttpClient`, with the
signer `BlobStore.S3` already uses and the same credential chain
(`Ankusa.AWS.Credentials`), so it adds no dependency.

```elixir
sinks: [
  {Ankusa.Sink.SQS,
   queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/hooks.fifo",
   region: "us-east-1",
   message_group_id: "stripe",                  # FIFO default: "tenant/source"; or a 1-arity fun
   inline_max_bytes: 65_536,                    # above this the message carries a claim reference
   max_message_bytes: 1_048_576,                # body + attributes; larger is dead-lettered at once
   timeout_ms: 5_000,
   batch: true,                                 # false: one SendMessage per delivery
   batch_linger_ms: 20,                         # how long a batch waits to fill
   batch_max_entries: 10,                       # ≤ 10
   batch_max_bytes: 262_144,                    # ≤ 1_048_576, SQS's batch limit
   batch_queue_max: 4_096,                      # queue ceilings: entries, bytes, entries per tenant
   batch_queue_max_bytes: 134_217_728,
   batch_queue_tenant_max: 1_024,
   batch_max_inflight: 8}                       # requests in flight; default dispatch.sink_concurrency
   # endpoint: "http://localhost:4566"          # default: the queue URL's origin
   # access_key_id:/secret_access_key:/session_token: — else AWS_* env, IRSA, IMDSv2
]
```

**It never creates the queue.** A queue's type, retention, visibility timeout,
redrive policy and encryption are an operator's contract. A queue that does
not exist is `{:error, {:permanent, {:config, {:sqs, 400, "QueueDoesNotExist",
message}}}}`: dead-lettered at once, since only an operator creating the queue
fixes it, and a DLQ replay then re-sends the hooks, the way `Sink.NATS` treats a missing stream.

**`deliver/3` returns `:ok` only once SQS has the message.** That is a `200`
from `SendMessage`, or an entry under `Successful` in a `SendMessageBatch`
reply, whose `MD5OfMessageBody` matches the MD5 of the body sent;
SQS answers `200` after storing the message redundantly, so the sink is
durable for [direct mode](#direct-mode). The other outcomes:

| Outcome | Result |
| --- | --- |
| body + attributes over `max_message_bytes` (1 MiB, SQS's limit) | permanent `{:message_too_large, size, max}`, before any request |
| SQS refuses the message itself: `InvalidMessageContents`, `InvalidParameterValue`, `MissingParameter`, `InvalidAttributeName`, `InvalidAttributeValue` | permanent `{:sqs, status, code, message}`: dead-lettered at once |
| SQS refuses this sink's credentials or queue: `AccessDenied`, `QueueDoesNotExist`, `InvalidClientTokenId`, `UnrecognizedClientException`, `SignatureDoesNotMatch`, `InvalidAccessKeyId`, `MissingAuthenticationToken` | `{:permanent, {:config, {:sqs, status, code, message}}}`: dead-lettered at once, counted in `ankusa_dispatch_config_errors_total{sink}`; fix the source, then replay |
| throttling: `Throttling`, `ThrottlingException`, `RequestThrottled`, `TooManyRequestsException`, `ServiceUnavailable`, `KmsThrottled`, or any `429`/`503` | `{:retry_after, ms, {:sqs, status, code, message}}`, `ms` a full-jitter draw (1 to `min(30 s, 500 ms · 2^(attempt-1))`) |
| any other SQS error: a 5xx, an expired token | transient `{:sqs, status, code, message}`: the retry policy |
| a non-2xx that is not an SQS error document (a proxy) | transient `{:status, status, body}`; a `429`/`503` is a `{:retry_after, ms, _}` |
| a `200` with a mismatched MD5, no credentials, a transport failure | transient |

**FIFO when the queue URL ends `.fifo`** (AWS requires the suffix). Each send
carries a `MessageGroupId`: `message_group_id`, or by default
`"tenant_id/source_id"` (`"/source_id"` without a tenant), the scope of the
Kafka sink's default key. With `ordered: true` the group is the lane's key
instead (see [Ordering lanes](#ordering-lanes); a standard queue is rejected at
load), sent one message at a time, so SQS shows the consumer the lane's order;
the key also travels as the `ankusa_ordering_key` attribute. Without it, messages
in one group are received in the order Ankusa sent them, which is not the order
the hooks arrived; dedupe on `idempotency_key`. Its
`MessageDeduplicationId` is the hook's `id`, or `id:replay:<replay_id>` on a
replay (the `Nats-Msg-Id` rule), so SQS collapses a retry after a lost
response while a deliberate replay is stored. SQS's deduplication window is
**5 minutes**: a retry later than that can store a second copy, so consumers
still dedupe on the `idempotency_key`. On a standard queue the sink sends no
deduplication id, and a `MessageGroupId` only when `message_group_id` is set
(fair queues).

**Batching.** A delivery from dispatch waits in its queue's batcher
(`Ankusa.Sink.Batcher`: one per queue and set of credentials, started by the
instance) and goes out in a `SendMessageBatch` with up to nine others, 20 ms
after the first arrived or at once when the batch is full (10 entries,
256 KiB). The hook's row stays claimed until its own entry is answered, so a
batcher that dies or an attempt that times out loses nothing; the retry carries
the same deduplication id. The reply is read per entry:

| Entry | Result |
| --- | --- |
| `Successful`, MD5 of the body sent | `:ok` |
| `Successful`, another MD5; absent from the reply | transient `{:md5_mismatch, …}` / `{:unexpected_reply, …}` |
| `Failed` with `InvalidMessageContents`, `InvalidParameterValue`, `MissingParameter`, `InvalidAttributeName`, `InvalidAttributeValue` | permanent `{:sqs, status, code, message}`: that entry alone is dead-lettered |
| `Failed` with `ThrottlingException`, `RequestThrottled`, `ServiceUnavailable`, `KmsThrottled` | `{:retry_after, ms, {:sqs, …}}`, `ms` full jitter |
| any other `Failed` (`InternalError`, …) | transient `{:sqs, status, code, message}`: the retry policy |
| the request fails as a whole (a `5xx`, a timeout, `QueueDoesNotExist`, `AccessDenied`) | every entry gets that failure, classified as a single `SendMessage` would (a refused queue or credential is a permanent `{:config, _}` for each; a `5xx` is retried on each entry's own schedule; a `429`/`503` is a `retry_after`) |

An entry's `Id` is its position in the request (`"0"`…`"9"`), because a hook
id has `:` in it and can appear twice in a batch (an original and its replay);
`MessageDeduplicationId` is unchanged. A batch may hold hooks of many tenants
and sources: entries are taken round-robin across tenants, each entry is
settled, retried and breaker-counted as its own tenant's (an open breaker keeps
that tenant's hooks out of the batch, it does not fail the others'), and
`dispatch.sink_concurrency` counts **requests in flight per queue** for a
batching sink, not messages (see [Batching sinks](#the-dispatch-pipeline)).
On a FIFO queue a `MessageGroupId` has one request in flight at a time and
its entries are sent in the order they were admitted (SQS enqueues a batch's
messages in the order sent); a message that fails and is retried is a later
send, as it always was. The batcher's queue is bounded (rule 3 of the hardening
rules): `batch_queue_max` 4 096 entries, `batch_queue_max_bytes` 128 MiB and
`batch_queue_tenant_max` 1 024 entries per tenant; a delivery past one is
refused with `{:retry_after, ms, {:batch_queue_full, :entries | :bytes |
:tenant}}` and counted in `ankusa_sink_batch_refused_total{sink,reason}`. The
other series are `ankusa_sink_batch_requests_total`,
`ankusa_sink_batch_entries_total`, `ankusa_sink_batch_bisections_total` (Pub/Sub
only) and the gauges `ankusa_sink_batch_batchers`, `ankusa_sink_batch_queued`
and `ankusa_sink_batch_queued_bytes`. Direct mode and lifecycle events publish
with one `SendMessage` each; `batch: false` does the same for dispatch. `mix run
bench/sqs_batch_bench.exs` prints requests and messages per second against a
stub that answers in 20 ms.

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
read it directly. Like `Sink.SQS`, the sink lives in `ankusa` core: dispatch's
deliveries go out in one `topics.publish` REST `POST` of up to 1 000
([Batching](#sinkgooglepubsub-google-cloud-pubsub-delivery), below), anything
else in one of its own, through `Ankusa.HttpClient`, so it adds no dependency.
Auth is a `:token_provider` MFA, as
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
   timeout_ms: 5_000,
   batch: true,                                 # false: one publish per delivery
   batch_linger_ms: 20,                         # how long a batch waits to fill
   batch_max_entries: 1_000,                    # ≤ 1_000
   batch_max_bytes: 7_500_000,                  # data before base64; ≤ 7_500_000
   batch_queue_max: 4_096,                      # queue ceilings: messages, bytes, messages per tenant
   batch_queue_max_bytes: 134_217_728,
   batch_queue_tenant_max: 1_024,
   batch_max_inflight: 8}                       # requests in flight; default dispatch.sink_concurrency
   # endpoint: "https://us-east1-pubsub.googleapis.com"  # default: https://pubsub.googleapis.com
]
```

**It never creates the topic or a subscription.** Retention, schema,
encryption and who may subscribe are an operator's contract. A topic that does
not exist is `{:error, {:permanent, {:config, {:pubsub, 404, "NOT_FOUND",
message}}}}`: dead-lettered at once, since only an operator creating the topic
fixes it, and a DLQ replay then re-publishes the hooks. A topic with **no subscription** (and no topic
message retention) answers `200` and discards the message: the sink cannot
detect it.

**`deliver/3` returns `:ok` only once Pub/Sub has the message.** That is a
`200` whose reply names one message id per message sent; Pub/Sub answers `200`
after persisting the messages, so the sink is durable for [direct mode](#direct-mode).
The other outcomes:

| Outcome | Result |
| --- | --- |
| data + attributes + ordering key over `max_message_bytes` (10 MB, Pub/Sub's limit) | permanent `{:message_too_large, size, max}`, before any request (and never in a batch) |
| Pub/Sub refuses the message itself: `INVALID_ARGUMENT` | permanent `{:pubsub, status, "INVALID_ARGUMENT", message}`: dead-lettered at once (a batch is bisected to find it) |
| Pub/Sub refuses this sink's credentials or destination: `PERMISSION_DENIED`, `UNAUTHENTICATED`, `NOT_FOUND` (topic) | `{:permanent, {:config, {:pubsub, status, google_status, message}}}`: dead-lettered at once, counted in `ankusa_dispatch_config_errors_total{sink}`; fix the source, then replay |
| `RESOURCE_EXHAUSTED`, or any `429`/`503` | `{:retry_after, ms, {:pubsub, status, google_status, message}}`, `ms` a full-jitter draw (1 to `min(30 s, 500 ms · 2^(attempt-1))`) |
| any other Pub/Sub error: `UNAVAILABLE`, `INTERNAL`, … | transient `{:pubsub, status, google_status, message}`: the retry policy |
| a non-200 that is not a Google error document (a proxy) | transient `{:status, status, body}`; a `429`/`503` is a `{:retry_after, ms, _}` |
| a `200` that does not name one message id per message, a transport failure | transient |
| the `:token_provider` answers `:error` | transient `:no_credentials`, no request sent |

**There is no publish-side deduplication.** Delivery is at-least-once, so
consumers dedupe on the `idempotency_key` (the `ankusa_idempotency_key`
attribute).

**Batching.** A delivery from dispatch waits in its topic's batcher
(`Ankusa.Sink.Batcher`: one per topic and set of credentials) and is published in
one `topics.publish` with up to 999 others, 20 ms after the first arrived or at
once when the batch is full (1 000 messages, 7.5 MB of data: the REST body is
the data in base64, and Pub/Sub's request limit is 10 MB). The hook's row stays
claimed until its batch is answered, so a batcher that dies or an attempt that
times out loses nothing.

`publish` has **no per-message results**: the topic accepts the request or
refuses it as a whole. So:

| Reply | Every message of the batch |
| --- | --- |
| `200` with one id per message | `:ok` |
| `RESOURCE_EXHAUSTED`, a `429`/`503` | `{:retry_after, ms, reason}`, each with its own jitter |
| `UNAVAILABLE`, a `5xx`, a timeout, no credentials | transient, each retrying on its own schedule |
| `NOT_FOUND`, `PERMISSION_DENIED`, `UNAUTHENTICATED` | permanent `{:config, {:pubsub, …}}` for each (no message caused it, so the batch is not split) |
| `INVALID_ARGUMENT` | **bisected**, below |

`INVALID_ARGUMENT` says one message is invalid; failing the request would
dead-letter up to 999 good ones (and retrying it would loop on the one bad
one). The size and attribute checks run before a message joins a batch, so the
usual causes never reach one; for the rest the request is split in two, left half
then right (the messages keep their order), and a half that is refused again is
split again, until the refused message stands alone: it is `{:permanent,
{:pubsub, 400, "INVALID_ARGUMENT", message}}` and goes to the DLQ, and every other
message of the batch is delivered. One bad message among 999 good ones costs
about `2 · log₂ 1000 = 20` extra requests, counted in
`ankusa_sink_batch_bisections_total`. The work is bounded at `2 · (⌈log₂ n⌉ + 1)`
splits per batch; messages still unresolved after that are transient
`{:batch_rejected, reason}` and retry in other batches.

The rest is as for `Sink.SQS` (see [Batching](#sinksqs-queue-delivery) there):
a batch may hold many tenants' hooks, taken round-robin, each settled and
breaker-counted on its own; `dispatch.sink_concurrency` counts requests in
flight per topic; the queue is bounded at 4 096 messages, 128 MiB and 1 024 per
tenant, and a delivery past a ceiling is refused with `{:retry_after, ms,
{:batch_queue_full, reason}}`; an `ordering_key` has one request in flight at a
time, its messages in the order they were admitted. `batch: false` (or direct
mode) publishes one message per request.

**The topic is the address, message attributes are the metadata.** Each message
carries attributes `ankusa_id`, `ankusa_source_id`, `ankusa_tenant_id`,
`ankusa_message_version`, `ankusa_idempotency_key` and `content_type`, plus
`ankusa_dedupe_key` and `ankusa_replay_id` when present: the same set
`Sink.SQS` sends (`Ankusa.Sink.Message.attributes/2`). `ankusa_tenant_id` is
left out when the hook has no tenant. Pub/Sub counts attributes and the
ordering key toward the size limit, so `max_message_bytes` checks all three
with the data.

**Ordering is opt-in.** With `ordered: true` (see
[Ordering lanes](#ordering-lanes)) the message carries the lane's key as its
`orderingKey`, which orders delivery only on a subscription with message
ordering enabled, published through one regional endpoint, and caps that key's
throughput at about 1 MB/s. The sink's own `ordering_key` (a string or function)
keeps its meaning on an unordered sink and sends a key on every message, but
Ankusa's own deliveries are then unordered (see [`Ankusa.Sink`](#ankusasink)).
No key is sent by default.

## Ordering lanes

Opt-in, per sink entry of a source (ADR
[0004](https://github.com/jamescarr/ankusa/blob/main/docs/adr/0004-ordering-lanes.md)):

```elixir
sinks: [{Ankusa.Sink.Http, url: "...", ordered: true, ordering_key: {:header, "x-entity-id"}}]
```

`ordered: true` and `ordering_key`: `{:header, name}`, `{:json, "data.object.customer"}`
(a dot path into the JSON body, read up to 1 MiB) or `{mod, fun, args}` (called
as `apply(mod, fun, [envelope | args])`). In YAML: `ordered: true` and
`ordering_key: {header: x-entity-id}` or `{json: data.object.customer}`.
Without a key every hook of the sink is one lane. A hook whose key cannot be
read goes to the sink's shared keyless lane and counts in
`ankusa_dispatch_lane_key_missing_total{sink}`; an ordered sink never falls back
to unordered.

A lane is `{tenant, source, sink, key}`. The key is read at ingest, from the
complete header list and the body, so a `{:header, name}` key must survive the
source's `store_headers` policy (a source that drops or hashes it is rejected).
`ordered: true` needs `wal.type` `disk` or `dual`, and on SQS a `.fifo` queue.

Ankusa hands a lane's hooks to the sink **one at a time, each after the previous
returned `:ok`**, in the order they were committed on this node (`seq`). Only the
head of a lane is a due row or a claim; the hooks behind it wait in the store and
hold no slot, window room or process. A retry or a breaker park blocks its own
lane and nothing else; when the head is delivered or dead-lettered the next hook
becomes the head, so a poison hook never holds its key forever (ordering is
deliberately broken at that point; a replay re-enters the dead hook as a member).
Lane state is in the store (`?O`, `?H`, `?P`, see [`storage.md`](storage.md)), so
a restart keeps it: a claimed head is delivered again before any later hook (a
duplicate, in order).

What the destination guarantees is its own: `Sink.SQS` sends the key as
`MessageGroupId`, `Sink.GooglePubSub` as `orderingKey`, `Sink.Kafka` as the
record key, `Sink.Http` adds `x-ankusa-ordering-key`; `Sink.RabbitMQ`,
`Sink.NATS` and `Sink.Redis` publish sequentially and carry the key as
`ankusa_ordering_key` (a header, or the `ordering_key` field of the Redis body):
consumer-side, documented, not enforced.

A batching sink (`Sink.SQS`, `Sink.GooglePubSub`) still batches an ordered sink: the
head of each lane waits in the destination's batcher like any attempt, so one
request can carry the heads of many lanes but never two hooks of one lane (a lane
has one head at a time), and the batcher's own per-group rule (one request in
flight per `MessageGroupId` / `orderingKey`) holds the key (unverified: no test
drives an ordered lane through a batcher).

A lane's throughput is one delivery round trip per hook; a lane is blocked for as
long as its head keeps retrying (up to `retry.max_elapsed_ms`). Order is per node
and by `seq`: two nodes accepting hooks of one key have no common order, so route
a key to one node. Metrics: `ankusa_dispatch_lanes`, `ankusa_dispatch_lanes_blocked`.


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
| `:max_attempts` | `nil`: no attempt limit, the horizon below is the bound |
| `:jitter` | `true`. Multiplies the delay by a random factor in `[0.5, 1.0]` |
| `:max_elapsed_ms` | `86_400_000` (24 h): the horizon. Not read by the policy; every policy and every source's `retry:` shares the key |

The defaults retry for 24 hours: 100 ms doubling to the 5-minute cap by
attempt 13, then 5 to 2.5 minutes apart (jitter halves a delay at worst), so
roughly 290 to 580 attempts before the dead letter. Set `max_elapsed_ms` lower
to dead-letter sooner, or `max_attempts` to bound the count.

### Retry horizon

Whatever the policy answers, dispatch gives every delivery `max_elapsed_ms`
from its **first attempt** (the moment dispatch first took the row; it is kept
in the delivery row as `first_attempt_at`, so it survives a restart, and a row
written before the field existed starts its horizon when the new release first
takes it). A delivery that fails at or after that time is dead-lettered, and no
retry is ever scheduled later than it (`Ankusa.RetryPolicy.schedule/5`): the
last attempt runs *on* the horizon. A 30-hour outage of a sink therefore
dead-letters its hooks at 24 hours, not at the policy's attempt count, and a
DLQ replay (which gives the row a fresh horizon) re-drives them.

Rows *parked* behind an open circuit breaker (see [A slow or dead
sink](#a-slow-or-dead-sink)) spend no attempts but not forever: a parked row
comes due no later than its horizon, and one that is parked again at or past it
is dead-lettered as `{:retry_horizon, module}` without another attempt.

`Retry-After` and throttle answers (`{:retry_after, ms, _}`) are capped by
`dispatch.max_retry_after_ms` (default 1 h) and by the horizon.

A source overrides the instance's policy with `retry:` (`base_ms`, `max_ms`,
`max_attempts`, `jitter`, `max_elapsed_ms`; see
[`configuration.md`](configuration.md#sources)): its deliveries run under the
instance's options with those laid over them. The source is read when a row is
claimed, so a change applies to the backlog like any other source change.

**Claim-checked sources are bounded by the bucket's lifecycle rule.** A body in
an object store is deleted by the bucket's own lifecycle rule
(`claim_check.bucket_expiry_days`), which cannot see that the hook is still
retrying. That rule has to outlast `max_elapsed_ms` + the DLQ retention + the
redeem grace, so a source with a longer `retry.max_elapsed_ms` is refused (`422
retry_exceeds_claim_expiry` from the admin API; boot refuses a stored one) when
it routes to a claim-checking sink and the rule is declared. The shipped 24 h
default needs 1 + `queue.dlq.retention_days` + the grace in days, not the 21
days the old 84-attempt default was assumed to need. Lifecycle events
(`Ankusa.Lifecycle`) have no source: they run under the instance's
`dispatch.retry` and its horizon, so one failing lifecycle sink holds the
cursor for at most that long.

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
