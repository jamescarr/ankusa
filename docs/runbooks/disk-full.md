# The data volume is nearly full

Alerts: `AnkusaDiskLow`

## Symptom

The file system holding `data_dir` has less than 10 % free. At zero the store cannot commit: every hook becomes `503 store_unavailable`, nothing is acked, `/ready` answers `503` and the load balancer drains the node. Acked hooks are safe, because RocksDB writes nothing it cannot sync.

## Metric

The alert's own ratio:

```promql
ankusa_disk_free_bytes / ankusa_disk_total_bytes
```

It is reported only while `:disksup` runs, which the server image starts; an embedder without `:os_mon` has no disk series and this alert never fires. `ankusa_store_disk_bytes` is the store's own size, for comparing with everything else on the volume.

Four things grow the store, and all four have their own series. A backlog shows in `ankusa_queue_pending` and `ankusa_queue_scheduled`. Dead letters show in `ankusa_dlq_rows` and `ankusa_dlq_bytes`. Hooks waiting for an archive segment show in `ankusa_queue_archive_pending` and `ankusa_archive_oldest_unarchived_age_seconds`, and the quarantine pen and audit trail in `ankusa_quarantine_bytes` and `ankusa_audit_bytes`. Once the volume is full, `ankusa_store_write_failures_total{class="no_space"}` starts counting.

## Action

1. Find what grows. A backlog is [backlog-age](backlog-age.md). An archive that is not draining, which means the compactor cannot write segments to the object store, shows in `ankusa_compact_failures_total{stage="segment"}` ([dlq-growth](dlq-growth.md), [storage](../storage.md)). The DLQ is [dlq-growth](dlq-growth.md). The quarantine pen is `DELETE /v1/quarantine` (scope `quarantine:write`), once you have replayed what matters.
2. Make room cleanly before anything else. Grow the volume, by resizing the PVC or the disk, or move the store to a bigger one with the node stopped. Never delete files from `data_dir` by hand: RocksDB's files are not independent, and `segments/` under `LocalFS` is the archive.
3. Do not wait for zero. Set `node.min_free_bytes` so `/ready` flips at the floor, before the store hits `ENOSPC`; the node then leaves the load balancer with its data intact ([deployment](../deployment.md#running-the-container)).
4. A full disk also heals itself. Commits fail with `503` and resume once space frees, and the store closes and reopens (at most every 5 s, then backing off to 60 s) to clear a latched write error ([storage](../storage.md#durability-corruption-full-disk)). No restart is needed.

## Verification

- `ankusa_disk_free_bytes / ankusa_disk_total_bytes` is above 0.10 and not falling.
- `GET /ready` answers `200` with `"store": "ok"`, and with `node.min_free_bytes` set, `"disk": "ok"`.
- `ankusa_store_write_failures_total{class="no_space"}` stops rising and `ankusa_store_unavailable_seconds` is 0.
- A probe hook is acked `201`.
