# Backlog age and delivery latency

Alerts: `AnkusaBacklogStale`, `AnkusaDeliveryLatencySLOBreach`

## Symptom

Hooks are acked and durable but reach their sinks late. `AnkusaBacklogStale`: the oldest due delivery on a node has waited more than 5 minutes. `AnkusaDeliveryLatencySLOBreach`: deliveries to one sink settle (receipt to confirmation, retries and backoff included) with p99 above 5 s.

## Metric

- `ankusa_queue_oldest_due_age_seconds` is the backlog's age, `ankusa_queue_pending` the first attempts waiting, `ankusa_queue_scheduled` the retries waiting.
- Is dispatch running? `ankusa_dispatch_running`, `ankusa_dispatch_claimed` and `ankusa_dispatch_runnable`. Zero running with a backlog means the dispatch domain is down: check `ankusa_instance_subtree_down_total{domain="dispatch"}` ([node-unready](node-unready.md)) and that the node's `roles` include `dispatch`.
- Is a sink failing? `ankusa_dispatch_attempt_duration_seconds` by `sink` and `result` (`error`, `timeout`) and `ankusa_dispatch_breakers_open` ([breaker-open](breaker-open.md)).
- Is it saturated? `ankusa_dispatch_claimed` at `dispatch.max_inflight`, or `ankusa_dispatch_claimed_bytes` at `dispatch.max_inflight_bytes`, with healthy sinks: the pipeline is the limit. `ankusa_dispatch_tenant_inflight` shows a tenant at its window (`dispatch.tenants`).
- Ordered sinks: `ankusa_dispatch_lanes_blocked` counts lane heads waiting out a retry; one stuck message holds its lane only ([delivery](../delivery.md#ordering-lanes)).
- Is the store down? `ankusa_store_unavailable_seconds` above 0: [store-write-failed](store-write-failed.md).

## Action

1. A sink that errors or times out: fix the destination, or its credentials (`ankusa_dispatch_config_errors_total` counts rows dead-lettered for a refused credential). Rows back off and retry by themselves.
2. A saturated pipeline with healthy sinks: raise `dispatch.concurrency` (default 32), `dispatch.sink_concurrency` (8) or `dispatch.max_inflight` (4096), within what the sinks tolerate, or add a node.
3. One slow sink starving others: set `dispatch.sink_concurrency` so it cannot take every slot.
4. A stopped dispatch domain: read the node's log for the `subtree stopped` error, fix the cause (a sink that raises, a port taken). The domain restarts itself after a backoff of at most 60 s and the backlog drains.
5. Dead rows that should be delivered once the sink is back: `POST /v1/replays` with `kind: dlq` revives them ([delivery](../delivery.md#dead-letters-and-replay)).

## Verification

- `max(ankusa_queue_oldest_due_age_seconds)` falls below 300 and keeps falling; `ankusa_queue_pending` trends to its normal level.
- `histogram_quantile(0.99, sum by (le, sink) (rate(ankusa_delivery_latency_seconds_bucket{result="ok"}[10m])))` is under 5 for every sink.
- With `queue.max_oldest_due_ms` set, `GET /ready` no longer lists `"backlog": "failing"`.
