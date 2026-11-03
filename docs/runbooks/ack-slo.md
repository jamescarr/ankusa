# Acknowledgement SLO: 503s, slow acks, capacity, drain budget

Alerts: `AnkusaAckErrorRatioHigh`, `AnkusaAckLatencySLOBreach`, `AnkusaAckLatencyDegraded`, `AnkusaAckLatencyCritical`, `AnkusaIngestNearCapacity`, `AnkusaDrainBudgetAtRisk`

## Symptom

Providers get `503`s, or answers slower than their timeout, and retry. Nothing that was acked is lost, because an ack already means the hook is durable in the configured mode ([architecture](../architecture.md#the-core-invariant)). What is at risk is a provider that stops retrying.

Two of these alerts fire before anything is broken. `AnkusaIngestNearCapacity` says the node is above 80 % of the 10 000 hooks/s per node the in-process bench sustains. `AnkusaDrainBudgetAtRisk` says a store commit takes longer than the 2 s a SIGTERM drain budgets for it.

## Metric

Split the refusals by outcome on the node that paged:

```promql
sum by (outcome) (rate(ankusa_ingest_requests_total{instance="<node>"}[5m]))
```

| Outcome | What it means | Where to go |
| --- | --- | --- |
| `store_unavailable` | the store is closed, or could not take a synced write | [store-write-failed](store-write-failed.md), [disk-full](disk-full.md) |
| `overload` | a batcher partition holds `batcher.max_queue` records or `max_queue_bytes`, so the commit path is slower than arrivals | step 2 below |
| `intake_unavailable`, `intake_pending` | `wal.type: dual`, and the intake log is not answering | [intake-down](intake-down.md) |
| `dlq_full` | the dead-letter queue is over its ceiling with nothing archived left to evict | [dlq-growth](dlq-growth.md) |

For latency, compare three numbers. `histogram_quantile(0.99, sum by (le) (rate(ankusa_wal_sync_duration_seconds_bucket[5m])))` is the fsync every ack waits for, shared by a whole group commit. `ankusa_wal_commit_duration_seconds` is the write around it, and `ankusa_ingest_duration_seconds` is the whole request. `ankusa_load_shed_total` counts what the batchers refused.

## Action

1. A slow fsync, with `ankusa_wal_sync_duration_seconds` p99 above 10 ms on local SSD, means the volume is the bottleneck: a noisy neighbour, a throttled network disk, or a full or failing device. Check the host's disk latency, move the store to faster storage, or add edge nodes so each takes less. There is no config knob to turn, because a hook is acked after one fsync.
2. `overload` means the node takes more than it commits. Add edge nodes behind the load balancer, each keeping its own volume ([Scaling the ingest fleet](../deployment.md#scaling-the-ingest-fleet)). If one tenant is the load, the alert to work from is [tenant-overload](tenant-overload.md).
3. High `ankusa_ingest_duration_seconds` with a fine fsync and a saturated CPU means the node is undersized. Scale out before you raise any limit.
4. `AnkusaIngestNearCapacity` is the warning to act on early: add an edge node before the latency objective goes. Throughput per node depends on body size, and the bench that measures it is [testing.md](../testing.md#core-bench-benchcore_benchexs).
5. `AnkusaDrainBudgetAtRisk` means a commit is slower than the stop's budget. Find the slow commit as in step 1, and until it is fixed give pods more time to stop: `terminationGracePeriodSeconds` at or above `listener.drain_delay_ms` + `listener.drain_timeout_ms` + 10 s ([deployment](../deployment.md#running-the-container)). Otherwise a SIGTERM can reset a request whose hook is committed.

## Verification

- The alert clears after its `for` period.
- `sum(rate(ankusa_ingest_requests_total{outcome=~"overload|store_unavailable|intake_unavailable|intake_pending|dlq_full"}[5m])) / sum(rate(ankusa_ingest_requests_total[5m]))` is below 0.01.
- `histogram_quantile(0.99, sum by (le) (rate(ankusa_ingest_duration_seconds_bucket[5m])))` is below 0.05.
- `GET /ready` answers `200` on every node.
