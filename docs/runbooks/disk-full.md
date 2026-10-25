# The data volume is nearly full

Alerts: `AnkusaDiskLow`

## Symptom

The file system holding `data_dir` has less than 10 % free. At zero the store cannot commit: every hook is `503 store_unavailable` (nothing is acked), `/ready` answers `503` and the load balancer drains the node. Acked hooks are safe; RocksDB writes nothing it cannot sync.

## Metric

- `ankusa_disk_free_bytes / ankusa_disk_total_bytes`: the alert's ratio. Reported only while `:disksup` runs, which the server image starts; an embedder without `:os_mon` has no disk series and no alert.
- `ankusa_store_disk_bytes`: the store's own size, against what else is on the volume.
- What grows the store: `ankusa_queue_pending` and `ankusa_queue_scheduled` (a backlog), `ankusa_dlq_rows`/`ankusa_dlq_bytes`, `ankusa_queue_archive_pending` and `ankusa_archive_oldest_unarchived_age_seconds` (hooks waiting for an archive segment), `ankusa_quarantine_bytes`, `ankusa_audit_bytes`.
- `ankusa_store_write_failures_total{class="no_space"}` once it is full.

## Action

1. **Find what grows.** A backlog: [backlog-age](backlog-age.md). Archive not draining (the compactor cannot write segments to the object store): `ankusa_compact_failures_total{stage="segment"}`, see [dlq-growth](dlq-growth.md) and [storage](../storage.md). DLQ: [dlq-growth](dlq-growth.md). Quarantine: `DELETE /v1/quarantine` (scope `quarantine:write`) after replaying what matters.
2. **Make room first, cleanly.** Grow the volume (resize the PVC or the disk), or move the store to a bigger one with the node stopped. Never delete files from `data_dir` by hand: RocksDB's files are not independent, and `segments/` under `LocalFS` is the archive.
3. **Do not wait for zero.** Set `node.min_free_bytes` so `/ready` flips at the floor, before the store hits `ENOSPC`: the node leaves the load balancer with its data intact ([deployment](../deployment.md#running-the-container)).
4. A full disk heals itself: commits fail with `503` and resume once space frees; the store closes and reopens (at most every 5 s, then backing off to 60 s) to clear a latched write error ([storage](../storage.md#durability-corruption-full-disk)). No restart is needed.

## Verification

- `ankusa_disk_free_bytes / ankusa_disk_total_bytes` is above 0.10 and not falling.
- `GET /ready` answers `200` with `"store": "ok"` and, with `node.min_free_bytes` set, `"disk": "ok"`.
- `ankusa_store_write_failures_total{class="no_space"}` stops rising; `ankusa_store_unavailable_seconds` is 0.
- A probe hook is acked `201`.
