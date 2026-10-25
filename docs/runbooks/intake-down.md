# The intake log is failing (`wal.type: dual`)

Alerts: `AnkusaIntakeFailures`, `AnkusaIntakeNotReady`, `AnkusaIntakeReconcileStuck`

## Symptom

Under `wal.type: dual` a hook is acked only after it is both fsynced on this host and confirmed in the replicated intake log. When the log does not answer, publishes fail; after `intake.breaker_failures` (3) failures in a row the breaker opens, and every new hook is `503 intake_unavailable` with `Retry-After: intake.retry_after_s` (2 s). `/ready` answers `503` with `intake: failing` (reason `down`), so the load balancer drains the node. After a restart `/ready` is `503` with reason `recovering` while the boot replay runs.

`AnkusaIntakeReconcileStuck`: a hook persisted in the store but not confirmed in the log that the reconciler cannot finish. It is not acked, but it is not resolved either.

## Metric

- `ankusa_intake_ready`: 1 when the breaker is closed, 0 when it is open or probing. `ankusa_intake_failures_total{class}` names the failure class.
- `ankusa_intake_publish_duration_seconds` by `outcome`: whether the log is slow or refusing.
- `ankusa_intake_pending_records`, `ankusa_intake_reconcile_oldest_age_seconds` and `ankusa_intake_reconcile_stuck`: the reconciler's backlog.
- `ankusa_intake_offset_lag_seconds`: how long since the watermark was written. Above `3 * intake.offset_flush_ms` is worth a look.
- `GET /v1/wal` carries an `intake` object (`node`, `partition`, `offset`, `watermark`, `pending`, `state`).

## Action

1. **The log is down or unreachable**: check the brokers (for Kafka: all brokers up, `min.insync.replicas` satisfied, the topic exists), the network path, and credentials (`intake.kafka`). The breaker lets one batch probe every `intake.probe_ms` (1 s) and closes by itself when a publish confirms. Nothing to restart.
2. **A whole fleet affected**: the log is a shared dependency, so every `dual` node refuses hooks at once. That is the availability cost of the host-loss guarantee. Providers retry; restore the log first.
3. **Stuck hooks**: read the node's log for the reconciler's error. A half-persisted hook the reconciler cannot finish usually needs the log back; if it stays stuck after the log recovers, run `recover` against the stopped node ([deployment](../deployment.md#host-loss-and-the-intake-log)).
4. **A replacement node**: it must start with the lost node's `intake.node_id` on an empty volume. `/ready` stays `503` (`recovering`) until its replay is done. Never give two nodes one id.
5. Do not switch `wal.type` to ride out an outage: `dual` is a durability promise; `disk` is a weaker one.

## Verification

- `ankusa_intake_ready` is 1; `increase(ankusa_intake_failures_total[5m])` is 0.
- `GET /ready` answers `200` with `"intake": "ok"`.
- `ankusa_intake_reconcile_stuck` is 0 and `ankusa_intake_pending_records` falls.
- A probe hook is acked `201`, and `ankusa_ingest_requests_total{outcome="intake_unavailable"}` stops rising.
