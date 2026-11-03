# The last successful backup is stale

Alerts: `AnkusaBackupStale`

## Symptom

The node's last successful checkpoint upload is more than 5 minutes old. Under `wal.type: disk` the backup is the only copy of an acked hook that survives the host, so the data a lost host would take with it grows by the minute. Under `wal.type: dual` the intake log covers the gap, but the replay on restore gets longer. With `backup.require_fresh: true` a stale backup also makes `/ready` answer `503`.

## Metric

How old, and whether attempts are even happening:

```promql
ankusa_backup_age_seconds
```

The age counts from the last successful backup, or from when the uploader started. With `backup.interval_ms` at 60 s it normally stays under that. `ankusa_backup_runs_total{result}` and `ankusa_backup_failures_total` separate a failing upload from one that is not running: an age that grows with no failures means the uploader is not running. `ankusa_backup_verify_failed_total{target}` counts backups that `backup.verify_interval_ms` found bad, by signature, size or sha256. `ankusa_blob_store_retries_total` and the log carry the object store's errors, which are usually credentials, bucket, throttling or network.

## Action

1. Read the node's log for the upload error. The usual causes are expired or rotated object-store credentials, a deleted or re-permissioned bucket, a network policy, and throttling by the store.
2. Fix the cause. The next interval uploads only the files the last backup does not have, and the age drops.
3. With `backup.secondary` configured, the primary and the secondary are separate uploads: check which `target` fails.
4. A backup that fails verification is a worse state than a stale one, so do not rely on it. Let the next successful run take a fresh upload, then re-check `ankusa_backup_verify_failed_total`.
5. An age that grows with no failed runs means the uploader is not running. Check that the node started with `backup.enabled: true`, and that its log shows no stopped failure domain ([node-unready](node-unready.md)).

## Verification

- `ankusa_backup_age_seconds` is under `backup.interval_ms` plus the duration of an upload.
- `rate(ankusa_backup_runs_total{result="ok"}[10m]) > 0` and `ankusa_backup_failures_total` is flat.
- `GET /ready` shows `"backup": "ok"` (with `backup.max_age_ms` set).
- A restore drill against the backup succeeds: `docker run … recover --dry-run` on an empty volume reads it and counts ([storage](../storage.md#backup-and-restore)).
