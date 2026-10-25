# Acknowledgement SLO: 503s, slow acks, capacity, drain budget

Alerts: `AnkusaAckErrorRatioHigh`, `AnkusaAckLatencySLOBreach`, `AnkusaAckLatencyDegraded`, `AnkusaAckLatencyCritical`, `AnkusaIngestNearCapacity`, `AnkusaDrainBudgetAtRisk`

## Symptom

Providers see `503`s or answers slower than their timeout, and retry. Nothing acked is lost: an ack means the hook is durable in the configured mode ([architecture](../architecture.md#the-core-invariant)). What is at risk is a provider that gives up retrying.

`AnkusaIngestNearCapacity` is the early form: the node is above 80 % of the 10 000 hooks/s per node the in-process bench sustains, and nothing is wrong yet. `AnkusaDrainBudgetAtRisk` says a store commit takes longer than the 2 s a SIGTERM drain budgets for it.

## Metric

Which refusal is it? Split by outcome on any node:

```promql
sum by (outcome) (rate(ankusa_ingest_requests_total{instance="<node>"}[5m]))
```

| Outcome | It means | Go to |
| --- | --- | --- |
| `store_unavailable` | the store is closed or could not take a synced write | [store-write-failed](store-write-failed.md), [disk-full](disk-full.md) |
| `overload` | a batcher partition holds `batcher.max_queue` records or `max_queue_bytes`: the commit path is slower than arrivals | below |
| `intake_unavailable`, `intake_pending` | `wal.type: dual` and the intake log is not answering | [intake-down](intake-down.md) |
| `dlq_full` | the dead-letter queue is over its ceiling with nothing archived left to evict | [dlq-growth](dlq-growth.md) |

For latency, compare `histogram_quantile(0.99, sum by (le) (rate(ankusa_wal_sync_duration_seconds_bucket[5m])))` (the fsync every ack waits for, shared by a whole group commit) with `ankusa_wal_commit_duration_seconds` (the write) and `ankusa_ingest_duration_seconds` (the whole request). `ankusa_load_shed_total` counts what the batchers refused.

## Action

1. **Slow fsync** (`ankusa_wal_sync_duration_seconds` p99 above 10 ms on local SSD): the volume is the bottleneck: a noisy neighbour, a throttled network disk, a full or failing device. Check the host's disk latency, move the store to faster storage, or add edge nodes so each takes less. Not a config knob: a hook is acked after one fsync.
2. **`overload`**: the node takes more than it commits. Add edge nodes behind the load balancer (every node keeps its own volume: [Scaling the ingest fleet](../deployment.md#scaling-the-ingest-fleet)). If one tenant is the load, it is [tenant-overload](tenant-overload.md), not this.
3. **CPU**: `ankusa_ingest_duration_seconds` high with fsync fine and CPU saturated means the node is undersized; scale out before raising any limit.
4. **`AnkusaIngestNearCapacity`**: add an edge node before the latency objective goes. Per-node throughput depends on body size; the bench is in [testing.md](../testing.md#core-bench-benchcore_benchexs).
5. **`AnkusaDrainBudgetAtRisk`**: find the slow commit as in 1. Until it is fixed, give pods more time to stop: `terminationGracePeriodSeconds` at or above `listener.drain_delay_ms` + `listener.drain_timeout_ms` + 10 s ([deployment](../deployment.md)), or a SIGTERM can reset a request whose hook is committed.

## Verification

- The alert resolves after its `for` period.
- `sum(rate(ankusa_ingest_requests_total{outcome=~"overload|store_unavailable|intake_unavailable|intake_pending|dlq_full"}[5m])) / sum(rate(ankusa_ingest_requests_total[5m]))` is below 0.01.
- `histogram_quantile(0.99, sum by (le) (rate(ankusa_ingest_duration_seconds_bucket[5m])))` is below 0.05.
- `GET /ready` answers `200` on every node.
