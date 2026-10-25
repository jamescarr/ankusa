# Runbooks

One page per alert family in [`deploy/monitoring/prometheus-rules.yaml`](https://github.com/jamescarr/ankusa/blob/main/deploy/monitoring/prometheus-rules.yaml). Every alert's `runbook_url` points at one of these, and every page lists its alerts on an `Alerts:` line. Each page has the same four sections: **Symptom** (what the operator or the provider sees), **Metric** (what to look at to tell causes apart), **Action** (what to do, cause by cause) and **Verification** (how to know it worked). `mise run check:monitoring` fails when an alert has no page, a page lacks a section, or the two disagree.

The dashboard is [`deploy/monitoring/grafana/ankusa.json`](https://github.com/jamescarr/ankusa/blob/main/deploy/monitoring/grafana/ankusa.json); its sections are named for the runbooks they lead to. The thresholds are the SLO table's ([architecture](../architecture.md#slis-and-slos)), and how to load all three is in [deployment](../deployment.md#monitoring).

| Runbook | Alerts | What it covers |
| --- | --- | --- |
| [ack-slo](ack-slo.md) | `AnkusaAckErrorRatioHigh`, `AnkusaAckLatencySLOBreach`, `AnkusaAckLatencyDegraded`, `AnkusaAckLatencyCritical`, `AnkusaIngestNearCapacity`, `AnkusaDrainBudgetAtRisk` | 503s, slow acks, capacity, the SIGTERM drain's commit budget |
| [backlog-age](backlog-age.md) | `AnkusaBacklogStale`, `AnkusaDeliveryLatencySLOBreach` | deliveries reaching their sinks late |
| [breaker-open](breaker-open.md) | `AnkusaBreakerOpen` | a sink's circuit breaker open |
| [dlq-growth](dlq-growth.md) | `AnkusaDLQGrowing`, `AnkusaDLQFull` | dead letters piling up; the DLQ ceiling refusing hooks |
| [disk-full](disk-full.md) | `AnkusaDiskLow` | the data volume running out |
| [backup-stale](backup-stale.md) | `AnkusaBackupStale` | the checkpoint upload failing or not running |
| [store-write-failed](store-write-failed.md) | `AnkusaStoreWriteFailures`, `AnkusaStoreUnavailable` | the store failing writes, or closed |
| [node-unready](node-unready.md) | `AnkusaSubtreeDown` | a failure domain out of restart budget; reading `/ready` |
| [intake-down](intake-down.md) | `AnkusaIntakeFailures`, `AnkusaIntakeNotReady`, `AnkusaIntakeReconcileStuck` | the intake log of `wal.type: dual` |
| [tenant-overload](tenant-overload.md) | `AnkusaTenantOverload` | one tenant refused with `429 tenant_overload` |
| [memory-growth](memory-growth.md) | `AnkusaMemoryGrowth` | the VM's memory climbing |
| [metrics-cardinality](metrics-cardinality.md) | `AnkusaMetricsSeriesOverBudget` | more than 10 000 series on a node |
