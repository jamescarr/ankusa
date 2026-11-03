# A sink's circuit breaker is open

Alerts: `AnkusaBreakerOpen`

## Symptom

At least one destination has had its circuit breaker open for 5 minutes. Its deliveries are parked, not attempted: they are written back due when the breaker's period ends, with no attempt spent. Every other sink is unaffected. The node logs a warning when a breaker opens.

## Metric

Which sink, and how many times it has opened:

```promql
ankusa_dispatch_breaker_transitions_total{state="open"}
```

`ankusa_dispatch_breakers_open` is how many breakers are open on the node now. `ankusa_dispatch_attempt_duration_seconds` by `sink` and `result` shows what the failing attempts look like: `error`, or `timeout` at `dispatch.attempt_timeout_ms`. `ankusa_queue_oldest_due_age_seconds` is the cost of the park, which goes up behind a breaker.

A breaker opens after `dispatch.breaker_failures` (default 5) consecutive failures, stays open for `dispatch.breaker_open_ms` (30 s), then doubles per consecutive open up to `dispatch.breaker_max_open_ms` (5 min). After that one attempt probes.

## Action

1. Name the sink from the transitions series, then find out why it fails. `GET /v1/dlq` shows the exact reason string of the rows that gave up, and the log has the first failures. The usual causes are a destination that is down, a credential that expired or was rotated, a changed network policy, an expired certificate, and a destination that is rate limiting us.
2. Fix the destination, or the source's sink configuration (`PUT /v1/tenants/{tenant}/sources/{name}`). The next probe closes the breaker; one that last opened long ago re-probes within `breaker_max_open_ms` (5 min).
3. Breakers live in dispatch's memory, so restarting the node closes them all and probes again at once. Do that only when the destination is known to be healthy.
4. A sink that is down for good: remove it from the source, or let its rows dead-letter after the retry horizon (`dispatch.retry.max_elapsed_ms`, 24 h) and replay them later.

## Verification

- `ankusa_dispatch_breakers_open` returns to 0, and `ankusa_dispatch_breaker_transitions_total{state="closed"}` rose for the sink.
- The attempt series for that sink shows `result="ok"` and no `error` or `timeout`.
- `ankusa_queue_oldest_due_age_seconds` falls, which means the parked rows are being delivered.
