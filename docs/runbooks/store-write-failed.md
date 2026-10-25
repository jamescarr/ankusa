# The store failed a write, or is closed

Alerts: `AnkusaStoreWriteFailures`, `AnkusaStoreUnavailable`

## Symptom

A write to the node's RocksDB store failed (ingest commit, dispatch, the archive, the quarantine pen, a source or rate-limit edit), or the store is closed and reopening. Hooks it could not commit are answered `503 store_unavailable`: they were not acked, so nothing is lost, and the provider retries. `/ready` answers `503` with `store: "failing"` and `reasons.store`.

## Metric

- `ankusa_store_write_failures_total{class}`: `no_space`, `io_error`, `read_only`, `corruption`, `busy` or `other` (`Ankusa.Store.failure_class/1`). The class decides the action.
- `ankusa_store_unavailable_seconds`: how long the store has been closed. It is 0 while open, and at most 60 s stale.
- `ankusa_store_reopen_total{result}` (`ok`, `error`, `deferred`: put off because the disk still refuses a synced write) and `ankusa_store_wal_replay_seconds` (how long the reopen took).
- `GET /ready`: `reasons.store` has the same class, from the node's own probe.

## Action

| Class | Cause | Do |
| --- | --- | --- |
| `no_space` | the volume is full | [disk-full](disk-full.md). Writers resume by themselves once space frees. |
| `io_error` | the device failed a write (EIO) | check the host's kernel log and the device; a failing disk is replaced, not retried. The node leaves the load balancer by itself. Replace the host: with `wal.type: disk` the backup is the other copy ([backup-stale](backup-stale.md)); with `dual`, restore replays the intake log ([intake-down](intake-down.md)). |
| `read_only` | the file system was remounted read-only | the kernel did it after an error; find the error in the host log, then repair or replace the volume. |
| `corruption` | RocksDB found damage | do not restart over it repeatedly. Boot refuses a damaged store (`{:store_open_failed, path, reason}`) rather than treat it as empty. Restore on a new volume from the backup ([storage](../storage.md#backup-and-restore)). |
| `busy`, `other` | contention or an unclassified error | read the log line; a recurrence means open an issue with it. |

1. A closed store retries its open by itself: after 5 s, then doubling to 60 s, jittered. A fleet whose disks come back together does not replay in step. You do not need to restart the node.
2. If `ankusa_store_reopen_total{result="deferred"}` rises, the disk still fails a synced write and the store stays open and readable: fix the disk.
3. Do not delete files from `data_dir`.

## Verification

- `increase(ankusa_store_write_failures_total[5m])` is 0 and `ankusa_store_unavailable_seconds` is 0.
- `GET /ready` answers `200` with `"store": "ok"`.
- A probe hook is acked `201` and `ankusa_ingest_requests_total{outcome="store_unavailable"}` stops rising.
- `ankusa_store_reopen_total{result="ok"}` rose by the reopen that healed it.
