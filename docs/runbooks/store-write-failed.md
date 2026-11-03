# The store failed a write, or is closed

Alerts: `AnkusaStoreWriteFailures`, `AnkusaStoreUnavailable`

## Symptom

A write to the node's RocksDB store failed, or the store is closed and reopening. The write could have been an ingest commit, a dispatch outcome, an archive row, the quarantine pen, a source edit or a rate-limit edit. Hooks the store could not commit were answered `503 store_unavailable`: they were never acked, so nothing is lost, and the provider retries. `/ready` answers `503` with `store: "failing"` and a `reasons.store`.

## Metric

The class names the cause:

```promql
ankusa_store_write_failures_total
```

`ankusa_store_write_failures_total{class}` is one of `no_space`, `io_error`, `read_only`, `corruption`, `busy` or `other`, from `Ankusa.Store.failure_class/1`. `ankusa_store_unavailable_seconds` is how long the store has been closed; it is 0 while the store is open and at most 60 s stale. `ankusa_store_reopen_total{result}` reports `ok`, `error` or `deferred`, the last meaning the reopen was put off because the disk still refuses a synced write, and `ankusa_store_wal_replay_seconds` is how long a reopen took. `GET /ready` carries the same class in `reasons.store`, from the node's own probe.

## Action

| Class | Cause | Do |
| --- | --- | --- |
| `no_space` | the volume is full | [disk-full](disk-full.md). Writers resume by themselves once space frees. |
| `io_error` | the device failed a write (EIO) | check the host's kernel log and the device. A failing disk is replaced, not retried. The node leaves the load balancer by itself. Replace the host: with `wal.type: disk` the backup is the other copy ([backup-stale](backup-stale.md)), and with `dual` a restore replays the intake log ([intake-down](intake-down.md)). |
| `read_only` | the file system was remounted read-only | the kernel did it after an error. Find the error in the host log, then repair or replace the volume. |
| `corruption` | RocksDB found damage | do not restart over it repeatedly. Boot refuses a damaged store (`{:store_open_failed, path, reason}`) rather than treat it as empty. Restore on a new volume from the backup ([storage](../storage.md#backup-and-restore)). |
| `busy`, `other` | contention, or an unclassified error | read the log line. A recurrence means opening an issue with it. |

A closed store retries its open by itself, after 5 s and then doubling to 60 s, jittered, so a fleet whose disks come back together does not replay in step. You do not need to restart the node. If `ankusa_store_reopen_total{result="deferred"}` rises, the disk still fails a synced write and the store stays open and readable, so fix the disk. Do not delete files from `data_dir`.

## Verification

- `increase(ankusa_store_write_failures_total[5m])` is 0 and `ankusa_store_unavailable_seconds` is 0.
- `GET /ready` answers `200` with `"store": "ok"`.
- A probe hook is acked `201`, and `ankusa_ingest_requests_total{outcome="store_unavailable"}` stops rising.
- `ankusa_store_reopen_total{result="ok"}` rose by the reopen that healed it.
