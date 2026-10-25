# The last successful backup is stale

Alerts: `AnkusaBackupStale`

## Symptom

The node's last successful checkpoint upload is more than 5 minutes old. The backup is the only cross-host copy of an acked hook under `wal.type: disk`: the data a lost host would take with it grows by the minute. Under `wal.type: dual` the intake log covers the gap, but the replay on restore gets longer. With `backup.require_fresh: true` a stale backup also makes `/ready` answer `503`.

## Metric

- `ankusa_backup_age_seconds`: seconds since the last successful backup (or since the uploader started). With `backup.interval_ms` at 60 s it normally stays under that.
- `ankusa_backup_runs_total{result}` and `ankusa_backup_failures_total`: whether attempts are failing or not happening. An age that grows with no failures means the uploader is not running.
- `ankusa_backup_verify_failed_total{target}`: `backup.verify_interval_ms` found a backup that does not verify (signature, size or sha256).
- `ankusa_blob_store_retries_total` and the log: the object store's errors (credentials, bucket, throttling, network).

## Action

1. Read the node's log for the upload error. The usual causes: expired or rotated object-store credentials, a deleted or re-permissioned bucket, a network policy, throttling by the store.
2. Fix the cause. The next interval uploads (only files the last backup does not have), and the age drops.
3. If `backup.secondary` is configured, the primary and secondary are separate: check which `target` fails.
4. A backup that fails verification is a worse state than a stale one: do not rely on it. Take a fresh upload (the next successful run), then re-check `ankusa_backup_verify_failed_total`.
5. An age that grows with no failed runs: the uploader is not running. Check the node started with `backup.enabled: true`, and that its log shows no stopped failure domain ([node-unready](node-unready.md)).

## Verification

- `ankusa_backup_age_seconds` is under `backup.interval_ms` plus an upload's duration.
- `rate(ankusa_backup_runs_total{result="ok"}[10m]) > 0` and `ankusa_backup_failures_total` is flat.
- `GET /ready` shows `"backup": "ok"` (with `backup.max_age_ms` set).
- A restore drill against the backup (`docker run … recover --dry-run` on an empty volume reads it and counts) succeeds: [storage](../storage.md#backup-and-restore).
