# The intake log is failing (`wal.type: dual`)

Alerts: `AnkusaIntakeFailures`, `AnkusaIntakeNotReady`, `AnkusaIntakeReconcileStuck`

## Symptom

Under `wal.type: dual` a hook is acked only after it is both fsynced on this host and confirmed in the replicated intake log. When the log does not answer, publishes fail. After `intake.breaker_failures` (default 3) failures in a row the breaker opens and every new hook is `503 intake_unavailable` with `Retry-After: intake.retry_after_s` (2 s). `/ready` answers `503` with `intake: failing` and reason `down`, so the load balancer drains the node. After a restart `/ready` answers `503` with reason `recovering` while the boot replay runs.

`AnkusaIntakeReconcileStuck` is the quieter case: a hook persisted in the store but not confirmed in the log, which the reconciler cannot finish. It is not acked, but it is not resolved either.

## Metric

Whether the breaker is closed, and how far behind the log you are:

```promql
ankusa_intake_ready
```

`ankusa_intake_ready` is 1 when the breaker is closed, and 0 when it is open or probing. `ankusa_intake_failures_total{class}` names the failure class. `ankusa_intake_publish_duration_seconds` by `outcome` says whether the log is slow or refusing. The reconciler's backlog is `ankusa_intake_pending_records`, `ankusa_intake_reconcile_oldest_age_seconds` and `ankusa_intake_reconcile_stuck`. `ankusa_intake_offset_lag_seconds` is how long since the watermark was written, and above `3 * intake.offset_flush_ms` it is worth a look. `GET /v1/wal` carries an `intake` object with `node`, `partition`, `offset`, `watermark`, `pending` and `state`.

## Action

1. The log is down or unreachable: check the brokers (for Kafka, all brokers up, `min.insync.replicas` satisfied and the topic present), the network path, and the credentials in `intake.kafka`. The breaker lets one batch probe every `intake.probe_ms` (1 s) and closes by itself when a publish confirms. There is nothing to restart.
2. A whole fleet is affected: the log is a shared dependency, so every `dual` node refuses hooks at once. That is the availability cost of the host-loss guarantee. Providers retry, and you restore the log first.
3. Stuck hooks: read the node's log for the reconciler's error. A half-persisted hook the reconciler cannot finish usually needs the log back; if it stays stuck after the log recovers, run `recover` against the stopped node ([deployment](../deployment.md#host-loss-and-the-intake-log)).
4. A replacement node must start with the lost node's `intake.node_id` on an empty volume. `/ready` stays `503` with `recovering` until its replay is done, and two nodes must never share one id.
5. Do not switch `wal.type` to ride out an outage. `dual` is a durability promise and `disk` is a weaker one.

## Verification

- `ankusa_intake_ready` is 1 and `increase(ankusa_intake_failures_total[5m])` is 0.
- `GET /ready` answers `200` with `"intake": "ok"`.
- `ankusa_intake_reconcile_stuck` is 0 and `ankusa_intake_pending_records` falls.
- A probe hook is acked `201`, and `ankusa_ingest_requests_total{outcome="intake_unavailable"}` stops rising.
