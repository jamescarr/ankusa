# ADR 0004: Opt-in ordering lanes

- Status: accepted, 2026-10-24
- Plan: `DEL-1.a`; implemented by `DEL-1.b`
- Store format: 7 → 8 (`Ankusa.Store.Migrate` registry entry `{7, 8, Ankusa.Store.Migrations.OrderingLanes}`; "8" is the next free number on main today, `DEL-1.b` takes the next free one when it lands)
- Builds on: [ADR 0001](0001-tenant-key-layout.md) (tenant ordinals, active-tenant markers, counters), `TEN-3` (per-tenant dispatch)

## Context

Delivery is unordered by design (`Ankusa.Sink` moduledoc, "Deliveries are **not ordered**"; `Dispatch.Pipeline` moduledoc, "Concurrency"). Hooks for one sink run up to `dispatch.sink_concurrency` (8) at a time, and a retry is a row due later, so it runs after whatever is due before it. A Kafka key, an SQS group or a Pub/Sub ordering key therefore only keeps the order hooks were *published* in. Some consumers need the provider's order per entity (a Stripe customer's events, a Salesforce record's updates), and rebuilding it from body timestamps is the consumer's problem today. The decision taken with the user is to bring per-key ordering back **as an opt-in per sink entry**.

The old lanes (a per-key in-memory queue in the pipeline, dropped when the single RocksDB store replaced the WAL; `critical-review.md` D1) are the thing not to rebuild. Their failures set the constraints:

1. **A stuck lane held shared resources.** Envelopes queued behind a dead lane stayed in the admission window, so one dead key starved every tenant. A lane must hold no slot, no window room and no process while it waits.
2. **Lane state lived in memory.** A restart forgot which hook was first. State must be in the store, in the same atomic batch as the row transition.
3. **Nothing bounded a lane.** The backlog behind a key is ordinary queue backlog, so it must count against the tenant's existing queue bounds and nothing else.

Constraints from the code on main (`11b5cb1`), all checked:

- **`Dispatch.Pipeline` is the one process that moves pending rows** (claim, retry, park, dead-letter, outcome flush); `Queue.Writer` only appends, `Dispatch.Replayer` only revives dead rows. Writers and the replayer do blind puts and never read-then-write on shared rows: batches are atomic, not serializable.
- **Commit order is `seq` order**: the single `Queue.Writer` per instance assigns `seq` and commits batches one after the other. A scan does *not* claim in `seq` order (a batch stamped earlier can become visible later, see the `unsynced` retry in `pipeline.ex`), so the order of a lane cannot be left to the claim scan.
- **A row has exactly one index key** (`deliveries.ex` moduledoc), every transition is one batch, and scanned families need sentinels (`Keys.sentinels/0`).
- **Both tenant shapes are first-class**: the lane key carries the tenant ordinal, so a shared `TenantPath` source gets lanes per tenant.
- **Migrations are forward-only, boot-time, bounded, resumable** (F6).

## Decision

### 1. Configuration

Two options on a source's sink entry, read by core, not by the sink module:

```elixir
sinks: [{Ankusa.Sink.Http, url: "...", ordered: true, ordering_key: {:header, "x-entity-id"}}]
```

- `ordered: true` (default `false`) and `ordering_key: {:header, name} | {:json, path} | {mod, fun, args}`. `{:json, "data.object.customer"}` is a dot path, normalized to segments as `Ankusa.Dedupe` does; `{mod, fun, args}` is called as `apply(mod, fun, [envelope | args])` and may return a binary. YAML has `ordered: true` and `ordering_key: {header: x-entity-id}` or `{json: data.object.customer}` (no MFA, like every function-valued option).
- `ordering_key` without `ordered: true` keeps its present meaning on `Sink.GooglePubSub` (a string or a 1-arity fun, passed to the sink) and is an error on every other sink. With `ordered: true` it takes only the three forms above; a string or a fun is rejected when the source loads. The pipeline drops `:ordered` and `:ordering_key` from the opts it hands a sink, which receives the key as `ctx.ordering_key` (section 6).
- `ordered: true` on an SQS sink whose `queue_url` does not end `.fifo` is rejected at load: a standard queue cannot show its consumer an order.
- Nothing is added to the instance config (`configuration.md` gains the two sink options, not a key). The per-visit promotion budget is the existing `dispatch.batch`.

### 2. The key and the lane

The key is resolved **at ingest**, in `Edge.Ingest` beside `Dedupe.key/2`, from the complete header list and the body, because the lane a row joins must be fixed before it is committed. Every other path that makes delivery rows (archive replay, a released quarantine hook) resolves it by the same function from the stored envelope, so a `{:header, name}` key must survive the source's `store_headers` policy: a source whose override (or the instance policy) drops or hashes that header is rejected when it loads.

- A key longer than 128 bytes (the SQS group id limit) becomes `"sha256:" <> hex`, as `Dedupe` does at 200.
- `{:json, path}` decodes the body only up to 1 MiB (`@json_key_max_bytes`), sharing the decode with a `dedupe: json` on the same source; a larger body, a missing path or a non-string value is "no key". `{mod, fun, args}` runs in the request process under `rescue`/`catch`: a raise or a non-binary is "no key". Its cost is on the ack path, so it is the caller's to keep pure and fast; the bench gate covers an ordered source.
- **No key → the sink's shared keyless lane** (an ordered sink never silently falls back to unordered). It counts `ankusa_dispatch_lane_key_missing_total{sink}` (label: the sink module). The adapters' key for a keyless hook is today's default group, `"<tenant>/<source>"`.
- **Lane id**: `lane = binary_part(sha256(<<sink::16, source_id::binary, 0, flag::8, key::binary>>), 0, 16)`, with `flag` 1 for the keyless lane (empty key) and 0 otherwise. Hashed so the index key has a fixed width and no delimiter can be forged; 128 bits, and a collision would only merge two lanes (more ordering, never less). The tenant is not in the hash: it is the ordinal in front of it. A lane is therefore `{tenant, source, sink index, key}`, as planned.
- The writer entry gains `lanes`, aligned with `sinks`: `nil` (unordered) or `{lane, key}`. `Queue.prepare/3` builds the delivery row of each sink (and now stamps `lane` and `ordering_key` into it), and `Queue.Writer`'s `build_ops/5` and `redrive_batch/3` (the two callers of `Deliveries.new_ops/6`) turn a laned row into the member of section 4.1 instead of a `?D` row.
- The delivery row gains `lane` (16 bytes) and `ordering_key` (≤ 128 bytes), additive keys under the row versioning. **Delivery follows the row, not the current config**: a row with a `lane` drains as a lane after the source is reconfigured, and a hook ingested before `ordered` was turned on is unordered. Turning `ordered` on or off applies to new hooks only.

### 3. Layout

All in the `index` column family, every key beginning with the tenant ordinal (ADR 0001), so one tenant's lane state is one contiguous range per family:

| Family | Key | Value | Meaning |
| --- | --- | --- | --- |
| lane member (`?O`) | `<<?O, ord::32, lane::128, seq::64, sink::16>>` | `<<size::32>>` | a pending row waiting its turn, in `seq` order |
| lane head (`?H`) | `<<?H, ord::32, lane::128>>` | `<<seq::64, sink::16>>` | the one row of the lane that is in `?D` or `?F`; exists exactly while the lane is busy |
| lane marker (`?P`) | `<<?P, ord::32, lane::128>>` | `<<>>` | "this lane may have a member and no head": promotion work |

A row of a lane is in exactly one place: a member (`?O`, plus its `deliveries` row), the head (`?H` plus `?D` or `?F`), or the DLQ. `Ankusa.Store.Keys` gains the three families with sentinels (`lane_members`, `lane_heads`, `lane_markers`; `?O`, `?H`, `?P` are unused in the `index` column family today), and `Keys.counter/2` accepts fields 1..12. Nothing outside `Ankusa.Queue.Deliveries` (a new `Deliveries.Lanes` part of it) builds these keys.

A lane head's `?D` row is the existing delivery path unchanged, with one change to the due value `<<size::32, kind::8>>`: `kind` 2 is a first attempt of a lane head and 3 a retry of one (0 and 1 stay as they are). `Deliveries.claim/2` and `retry_ops/6` read it, so a claim or a retry moves the lane counters in the same batch:

| Counter (per tenant, node aggregate under ordinal 0) | Field | Changed by |
| --- | --- | --- |
| `lanes` (busy lanes = heads) | 10 | promote +1, release −1 |
| `lanes_blocked` (heads waiting out a retry or a breaker park) | 11 | `retry_ops` of a lane row +1, `claim` of a kind 3 row −1 |
| `lane_members` | 12 | member put +1, promote −1, purge/dead-letter −1 each |

`pending` and `bytes` keep their meaning: a member is a pending row. It counts against the tenant's queue bounds, and a backlog behind one key is refused like any other (`503`/`429` with `Retry-After`), so one hot key can only exhaust its own tenant's share.

### 4. Transitions

Each is one atomic batch, built by `Deliveries` (one module owns the keys, as in ADR 0001):

1. **Ingest** (`Queue.Writer`, blind puts): for a laned sink, the `deliveries` row (with `lane`), the `?O` member, the `?P` marker, the tenant's `?A` active marker, `pending +1`, `bytes +size`, `lane_members +1`. **No `?D` row**, and no read of lane state.
2. **Promote** (pipeline only): for a lane with a member and no head, in one batch: delete the lowest member (the first `?O` of the lane), put `?H`, put `?D` (kind 2, due now, with its `?A` marker), delete `?P`, `lane_members −1`, `lanes +1`. The row then follows the unchanged claim/attempt path, so `first_attempt_at` (DEL-2) starts at the first claim: **waiting in a lane does not spend the retry horizon**.
3. **Retry / breaker park of the head**: `retry_ops`/`park` rewrite its `?D` with kind 3, `lanes_blocked +1`; `?H` and every member stay. Retry blocks the lane and nothing else: the head holds no slot and no window room while it waits, and other lanes, other keys and other tenants are untouched.
4. **Release** (pipeline only), on **every** transition that ends a head's life in `?D`/`?F`: delivered, dead-lettered by retry policy or horizon, `{:permanent, _}`, `:sink_gone`/`:source_gone`, an undecodable row or hook, an orphaned row (`orphan_ops`), `dead_letter_source`. One function, `Lanes.release_ops/…`, is appended to `delivered_ops`/`dead_ops`/`orphan_ops` for a row with a `lane`: read the lane's first member; if there is one, the promote batch of (2) for it, otherwise delete `?H` and `lanes −1`. **A dead-lettered head releases the lane**: a poison hook must not hold its key forever; the lane's next hook goes ahead of it, and a replay (below) re-enters the DLQ'd one.
5. **Race closure.** Lane state is moved only by the pipeline, and the read of the next member and the write that releases the head happen in **one step with no other lane work between** (release ops are built in `flush/1` and in the in-step dead-letter paths, not when the outcome is buffered; a failed flush rebuilds them). A writer that commits a member after that read also commits its `?P`; the sweep below then promotes it. The invariant the proof tests: *every `?O` member has an `?H` or a `?P`; every `?H` names exactly one row in `?D` or `?F`*.
6. **Sweep** (the pipeline, once per visit of a tenant, before the due fold, so a promoted row is claimed in the same scan): read up to `share` of the tenant's `?P` markers. For each: a head exists → delete the marker (the head's release will read the members); no head and a member exists → promote; no head and no member → delete the marker, then read the lane's members once more and put the marker back if one appeared (the writer's put came either before the delete, and the second read sees its member, or after it, and the marker is back). The stale-marker sweep of `Deliveries.sweep_markers/5` must not delete a tenant's `?A` marker while it has a `?P` marker (one seek). The sweep runs only while the node's `lane_members` aggregate is above 0 (one read per scan): **a deployment with no ordered sink pays nothing**, which the bench gate proves.
7. **Restart** (`recover_inflight`): a claimed head goes back to `?D` as a first attempt, `?H` untouched; members stay. Nothing is rebuilt in memory. The head's redelivery is a duplicate of that hook, before any later one.
8. **Replay.** A DLQ revive (`revive_ops`) of a row with a `lane` re-enters as a member plus `?P`, not as `?D`: it runs when it is the lowest waiting member, never beside the head, and before any later member. If a later hook is already the head, the revived one runs after it (replay cannot reach back past a delivered hook). Archive replay builds new rows through `new_ops` and takes the ingest path of (1).
9. **Erase and delete.** `purge_tenant` range-deletes the tenant's `?O`/`?H`/`?P` with its other ranges (`lane_members`, `lanes` and `lanes_blocked` go with its counters). `dead_letter_source` dead-letters a deleted source's members and heads in the same batch, per lane.

### 5. Format 7 → 8

The registry entry rewrites nothing (no lane exists yet): it stamps format 8, so a format 7 image refuses a store that may hold kind 2/3 due rows and `?O`/`?H`/`?P` keys (`{:error, {:store_format_newer, 8, 7}}`). A store that never enables `ordered` is bit-identical to format 7 apart from the stamp. **Rollback boundary: the first start of a format 8 image** (the same boundary as ADRs 0001 and 0003). Fixture `store-v8.tar.zst` (`mix ankusa.store.fixture`) holds a source with an ordered sink: a busy lane (head claimed), a lane whose head waits out a retry (kind 3) with members behind it, a keyless lane, and members of an unordered sink.

### 6. Sinks

`ctx` gains `ordering_key` (the row's key, or the default `"<tenant>/<source>"` for the keyless lane) and `lane` (the hex of the lane id, for logs). Ankusa hands a lane's hooks to the sink **one at a time, each after the previous returned `:ok`**; what the destination then guarantees is its own:

| Sink | Ordered mode |
| --- | --- |
| `Sink.SQS` (FIFO) | `MessageGroupId` = the key (not `message_group_id`), `MessageDeduplicationId` as today |
| `Sink.GooglePubSub` | `orderingKey` = the key; the subscription must have message ordering enabled and the publisher a regional endpoint (documented, as today) |
| `Sink.Kafka` | record key = the key (partition = hash of the key, brod's `:hash` partitioner) |
| `Sink.Http` | sequential per lane, plus the header `x-ankusa-ordering-key` |
| `Sink.RabbitMQ`, `Sink.NATS`, `Sink.Redis` | consumer-side: publishes are sequential, one queue/subject/stream keeps their order, and the key travels as the `ankusa_ordering_key` message attribute (additive, `ankusa_message_version` stays `"1"`); documented, not enforced |

The new attribute and header count toward `DEL-5`'s header and message-size budgets.

### 7. Observability

`ankusa_dispatch_lanes` (gauge; node aggregate of counter 10), `ankusa_dispatch_lanes_blocked` (gauge; counter 11), `ankusa_dispatch_lane_key_missing_total{sink}` (counter), and `ordered` in the metadata of `[:ankusa, :dispatch, :stop]`. Gauges come from maintained counters, never scans (ground rule 8); no label comes from a key, a tenant or a lane, so the series count is independent of tenants and keys. `lanes_blocked` counts a head waiting out a retry whether or not anything is behind it yet: it is the lanes that will hold the next hook.

### 8. Docs

`DEL-1.b` rewrites the `delivery.md` ordering paragraphs (the two "Deliveries are not ordered" passages and the Kafka/NATS/Pub/Sub notes), the `architecture.md` Dispatch row ("ordering lanes are gone"), the `Ankusa.Sink` and `Dispatch.Pipeline` moduledocs and the `configuration.md` sink rows, and adds the ordering tests to `testing.md`.

## Consequences

- **Throughput of a lane is `1 / (delivery round trip)`**: a key with a 20 ms sink does at most about 50 hooks/s. Other keys are unaffected, up to the sink's `sink_concurrency` (8) lanes at once per sink and tenant. An ordered source is the operator's choice of that ceiling.
- **A lane is blocked by its head for as long as its head keeps retrying**: up to the retry horizon (`retry.max_elapsed_ms`, 24 h by default) and breaker parks, then the head dead-letters and the lane moves on (ordering is *broken* at that point, deliberately). A blocked lane costs a few index keys and no slot, window room or process. Operators see it in `ankusa_dispatch_lanes_blocked` and the DLQ.
- **Order is per node and by `seq`** (commit order on the node that accepted the hook). Two nodes accepting hooks of one key have no common order; a deployment that needs one key ordered must route the key to one node (consistent hashing at the load balancer). Provider retries of an already-acked hook are dedupe hits and add nothing.
- **Delivery stays at-least-once.** A crash after the destination accepted the head and before the row was deleted re-sends the head before any later hook (a duplicate, in order). A timed-out attempt that the destination still completes late can land after a *later* hook (the case `Sink.Kafka` documents); consumers dedupe on `idempotency_key`.
- **Lanes change what `pending` hides**: members are pending rows that no scan claims until promoted, so `oldest_due_age` is the head's, not a member's. A backlog of members behind one key is bounded only by the tenant's queue ceilings (`503`/`429` at the edge), not per lane.
- **Order is by hook `seq`**: a hook ingested before `ordered` was enabled, or a row revived from the DLQ after later hooks started, can run out of order (sections 2 and 4.8). Turning `ordered` off drains existing lanes in order.
- **Ack path.** `Edge.Ingest` gains the key extraction for ordered sources only (a JSON decode up to 1 MiB when the key is `{:json, _}`; user code when it is an MFA); the writer puts `?O`/`?P` instead of `?D`. Hot-path files change (`edge/`, `queue/`, `dispatch/`, `store/`), so `DEL-1.b` runs `mise run bench:gate` and `mise run test:crash` and posts before/after numbers; an unordered source's path must not change measurably.
- **Later work must keep true**: commits of one lane happen in `seq` order (`DUR-4.b.b`, writer offload, must keep that per lane or partition lanes by hash to one writer); every terminal transition of a row goes through `Deliveries`' release (a new transition that bypasses it strands a lane); a new family keyed by tenant is purged by `purge_tenant`.
- **Rollback**: format 8 is a one-way door at its first start, even for a deployment that never enables `ordered`.

## Proof

`DEL-1.b` owns these; each fails if the decision is not honoured.

- `test/ankusa/dispatch/ordering_test.exs` (new): `ordered lane` (100 hooks for one key with a mid-stream retry arrive in order, one attempt of the lane in flight at a time), `other keys concurrent` (a slow key does not delay another key or another tenant; the slow lane holds no slot and no window room), `survives restart` (reopen with a busy lane, a retrying head and members: the order, and the duplicate-before-successor behaviour of 4.7), `dead head releases lane`, `replay respects lanes` (4.8), `keyless lane`, `key missing counter`, `lane invariant` (a property over random interleavings of ingest, claim, retry, delivered, dead, revive and tenant purge: every `?O` has an `?H` or `?P`, every `?H` names one row in `?D`/`?F`, counters equal the key counts), `racing writer` (members committed between a release's read and its write are promoted by the sweep).
- `test/ankusa/source_ordering_test.exs` (new): the option grammar of section 1 (every rejected form, the SQS standard-queue rejection, the Pub/Sub legacy `ordering_key`), and the YAML shapes in `config_test.exs`.
- `test/ankusa/store_upgrade_test.exs`: the 7 → 8 migration and every checked-in fixture including `store-v8`, which drains its lanes in order; a format 7 image refuses a format 8 store.
- `test/ankusa/queue/deliveries_test.exs` and `test/ankusa/property/`: kind 2/3 claim and retry counter deltas; `purge_tenant` removes every lane family and counter.
- Adapter suites (`sink/sqs_test.exs`, `sink/google_pubsub_test.exs`, `sink/http_test.exs`, `ankusa_kafka` `sink/kafka_test.exs`, and the RabbitMQ, NATS and Redis `sink/*_test.exs`): the key in `MessageGroupId`, `orderingKey`, the record key, the header and the attribute.
- `mise run bench:gate` → PASS with no lanes configured (the `lane_members == 0` gate) and with a new cell for one ordered source; `mise run test:crash` (the ack path changed); `mise run check:package ankusa` and every adapter and `ankusa_server`; `mise run check:links`.

**This ADR's own proof:** the PR merges, and `mise run check:links` passes.
