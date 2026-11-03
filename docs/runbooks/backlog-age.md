# Backlog age and delivery latency

Alerts: `AnkusaBacklogStale`, `AnkusaDeliveryLatencySLOBreach`

## Symptom

Hooks are acked and durable, but they reach their sinks late. `AnkusaBacklogStale` means the oldest due delivery on a node has waited more than 5 minutes. `AnkusaDeliveryLatencySLOBreach` means deliveries to one sink settle with p99 above 5 s, where settling runs from receipt to confirmation and includes every retry and its backoff.

## Metric

Age first, then whether dispatch is moving:

```promql
ankusa_queue_oldest_due_age_seconds
```

`ankusa_queue_pending` is the first attempts waiting and `ankusa_queue_scheduled` the retries waiting. Then read `ankusa_dispatch_running`, `ankusa_dispatch_claimed` and `ankusa_dispatch_runnable` together. Zero running with a backlog means the dispatch domain is down: check `ankusa_instance_subtree_down_total{domain="dispatch"}` ([node-unready](node-unready.md)) and that the node's `roles` include `dispatch`.

A dispatch that is running narrows it to one of four things. A failing sink shows up in `ankusa_dispatch_attempt_duration_seconds` by `sink` and `result` (`error`, `timeout`), and in `ankusa_dispatch_breakers_open` ([breaker-open](breaker-open.md)). A saturated pipeline holds `ankusa_dispatch_claimed` at `dispatch.max_inflight`, or `ankusa_dispatch_claimed_bytes` at `dispatch.max_inflight_bytes`, while the sinks stay healthy; `ankusa_dispatch_tenant_inflight` shows a tenant at its own window (`dispatch.tenants`). An ordered sink reports lane heads in `ankusa_dispatch_lanes_blocked`, each waiting out a retry, and one stuck message holds its lane only ([delivery](../delivery.md#ordering-lanes)). A store that is down shows `ankusa_store_unavailable_seconds` above 0 ([store-write-failed](store-write-failed.md)).

## Action

1. A sink that errors or times out: fix the destination, or its credentials. `ankusa_dispatch_config_errors_total` counts the rows dead-lettered for a refused credential. Rows back off and retry by themselves.
2. A saturated pipeline with healthy sinks: raise `dispatch.concurrency` (default 32), `dispatch.sink_concurrency` (8) or `dispatch.max_inflight` (4096), within what the sinks tolerate, or add a node.
3. One slow sink starving the others: set `dispatch.sink_concurrency` so it cannot take every slot.
4. A stopped dispatch domain: read the node's log for the `subtree stopped` error and fix the cause, usually a sink that raises or a port someone else took. The domain restarts itself after a backoff of at most 60 s and the backlog drains.
5. Dead rows that should be delivered once the sink is back: `POST /v1/replays` with `kind: dlq` revives them ([delivery](../delivery.md#dead-letters-and-replay)).

## Verification

- `max(ankusa_queue_oldest_due_age_seconds)` falls below 300 and keeps falling, and `ankusa_queue_pending` trends back to its normal level.
- `histogram_quantile(0.99, sum by (le, sink) (rate(ankusa_delivery_latency_seconds_bucket{result="ok"}[10m])))` is under 5 for every sink.
- With `queue.max_oldest_due_ms` set, `GET /ready` no longer lists `"backlog": "failing"`.
