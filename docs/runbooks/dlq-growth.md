# The dead-letter queue is growing or full

Alerts: `AnkusaDLQGrowing`, `AnkusaDLQFull`

## Symptom

`AnkusaDLQGrowing` means the number of dead-lettered deliveries has gone up for 15 minutes. Deliveries are giving up at the end of their retry horizon, or being dead-lettered at once as a permanent failure such as a refused credential or a deleted source.

`AnkusaDLQFull` is the state after that: the DLQ is over `queue.dlq.max_rows` or `max_bytes` with no row whose hook is archived left to evict, so new hooks are refused `503 dlq_full` with `Retry-After: 60`. An acked hook is never deleted to make room.

## Metric

Is it growing, and is it full?

```promql
ankusa_dlq_rows
```

`ankusa_dlq_bytes` and `ankusa_dlq_full` (1 while the node refuses hooks) sit beside it. `ankusa_dispatch_dead_lettered_total{sink}` says which sink is giving up, and `ankusa_dispatch_config_errors_total{sink}` is the share of those that is a refused credential or destination. `ankusa_dlq_evicted_total{reason}` reports eviction by `retention`, `max_rows`, `max_bytes` or `purge`, with `tenant_id` when `metrics.detail: tenant`.

For `dlq_full`, the question is whether the compactor is archiving, because a DLQ row is evicted only once its hook is archived and `Storage.fetch/2` finds it. `ankusa_queue_archive_pending` and `ankusa_archive_oldest_unarchived_age_seconds` answer that. `ankusa_compact_failures_total{stage}` and `ankusa_archive_failures_total` say whether the compactor is failing.

## Action

1. Growing: list what is dying and why. `GET /v1/dlq` (scope `dlq:read`) gives the reason per row. Fix the cause, which is the sink, a credential or a deleted source, then revive the rows with `POST /v1/replays` and `{"kind":"dlq","source_id":"<id>"}` ([delivery](../delivery.md#dead-letters-and-replay)).
2. Full with the archive not draining: the node needs the `storage` role and a working object store. Read `ankusa_compact_failures_total{stage="segment"}` and the log for the blob store error, which is usually credentials, bucket or network, and see [storage](../storage.md). Once segments are written, the reaper evicts and `dlq_full` clears itself.
3. Full with rows already archived: relieve the ceiling. `DELETE /v1/dlq?tenant=<id>&source_id=<id>&until=<time>&limit=<n>` (scope `dlq:write`) runs the same archived-only eviction and answers `{deleted, bytes, unarchived, more}`. Or raise `queue.dlq.max_rows` / `max_bytes`, or shorten `queue.dlq.retention_days` (30).
4. Rows that are garbage, such as those of a source that no longer exists: purge them, after confirming nothing needs replaying.

## Verification

- `ankusa_dlq_full` is 0, `ankusa_dlq_rows` is flat or falling, and `delta(ankusa_dlq_rows[1h]) > 0` no longer holds.
- Ingest answers `201` again for a probe hook, and `ankusa_ingest_requests_total{outcome="dlq_full"}` stops rising.
- A replayed row is delivered: `GET /v1/replays/{id}` shows it finished and the sink's delivery counter moves.
