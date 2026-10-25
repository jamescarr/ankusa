# The dead-letter queue is growing or full

Alerts: `AnkusaDLQGrowing`, `AnkusaDLQFull`

## Symptom

`AnkusaDLQGrowing`: for 15 minutes the number of dead-lettered deliveries has gone up. Deliveries are giving up after their retry horizon, or being dead-lettered at once as a permanent failure (a refused credential, a deleted source).

`AnkusaDLQFull`: the DLQ is over `queue.dlq.max_rows` or `max_bytes`, with no row whose hook is archived left to evict. New hooks are refused `503 dlq_full` with `Retry-After: 60`: an acked hook is never deleted to make room.

## Metric

- `ankusa_dlq_rows`, `ankusa_dlq_bytes`, `ankusa_dlq_full` (1 while refusing hooks).
- `ankusa_dispatch_dead_lettered_total{sink}`: which sink is giving up; `ankusa_dispatch_config_errors_total{sink}` is the share that is a refused credential or destination.
- `ankusa_dlq_evicted_total{reason}` (with `tenant_id` from `metrics.detail: tenant`): eviction by `retention`, `max_rows`, `max_bytes` or `purge`.
- For `dlq_full`: `ankusa_queue_archive_pending` and `ankusa_archive_oldest_unarchived_age_seconds` show whether the compactor is archiving. A DLQ row is evicted only once its hook is archived and `Storage.fetch/2` finds it, so the archive stalling stalls the DLQ.
- `ankusa_compact_failures_total{stage}` and `ankusa_archive_failures_total`: the compactor failing.

## Action

1. Growth: list what is dying and why. `GET /v1/dlq` (scope `dlq:read`) gives the reason per row. Fix the cause (the sink, a credential, a deleted source), then revive the rows: `POST /v1/replays` with `{"kind":"dlq","source_id":"<id>"}` ([delivery](../delivery.md#dead-letters-and-replay)).
2. Full, archive not draining: the node needs the `storage` role and a working object store. Read `ankusa_compact_failures_total{stage="segment"}` and the log for the blob store error (credentials, bucket, network); see [storage](../storage.md). Once segments are written the reaper evicts and `dlq_full` clears itself.
3. Full, rows archived: lower the ceilings' pressure with `DELETE /v1/dlq?tenant=<id>&source_id=<id>&until=<time>&limit=<n>` (scope `dlq:write`; it runs the same archived-only eviction and answers `{deleted, bytes, unarchived, more}`), or raise `queue.dlq.max_rows` / `max_bytes`, or shorten `queue.dlq.retention_days` (30).
4. Rows that are garbage (a source that no longer exists): purge them after confirming nothing needs replaying.

## Verification

- `ankusa_dlq_full` is 0 and `ankusa_dlq_rows` is flat or falling; `delta(ankusa_dlq_rows[1h]) > 0` no longer holds.
- Ingest answers `201` again for a probe hook (no `503 dlq_full`); `ankusa_ingest_requests_total{outcome="dlq_full"}` stops rising.
- A replayed row is delivered: `GET /v1/replays/{id}` shows it finished, and the sink's delivery counter moves.
