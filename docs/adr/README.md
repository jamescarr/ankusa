# Architecture decision records

A change to the ack path, the store key layout, a wire format, the isolation model or the durability mode needs an ADR merged before the code that makes the change (ground rule 14 of `ANKUSA_HARDENING_PLAN.md`; tasks marked `[design-gate]` are split into `.a`, the ADR, and `.b`, the implementation). Each record is one file, `NNNN-<slug>.md`, numbered in the order they are merged, with four sections: `Context` (what is true today and what is wrong with it), `Decision` (what will be built, precisely enough that the implementation task has no design question left), `Consequences` (what it costs, what it changes for operators, and what later work must keep true) and `Proof` (the named tests, benches and commands that fail if the decision is not honoured). An implementation that has to deviate from its ADR amends the ADR in the same PR.

| ADR | Decision |
| --- | --- |
| [`0001-tenant-key-layout.md`](0001-tenant-key-layout.md) | tenant ordinals in the index keys, per-tenant counters kept by merge operands, an active-tenant index, and the store format 2 → 3 migration |
| [`0002-dual-commit-intake-log.md`](0002-dual-commit-intake-log.md) | `wal.type: dual`: ack after local fsync and a confirmed publish to a replicated intake log (Kafka first, JetStream second), parallel halves, a half-persisted hook is carried to completion by a live reconciler, boot and offline recovery by seq and hook id |
