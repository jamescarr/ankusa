# A sink's circuit breaker is open

Alerts: `AnkusaBreakerOpen`

## Symptom

At least one destination has had its circuit breaker open for 5 minutes. Its deliveries are parked: written back due when the breaker's period ends, attempts not spent. Every other sink is unaffected. The node logs a warning when a breaker opens.

## Metric

- `ankusa_dispatch_breakers_open`: how many are open on the node now.
- `ankusa_dispatch_breaker_transitions_total{sink,state}`: which sink opened (`state="open"`) and how often it has closed again.
- `ankusa_dispatch_attempt_duration_seconds` by `sink` and `result`: what the failing attempts look like (`error`, or `timeout` at `dispatch.attempt_timeout_ms`).
- `ankusa_queue_oldest_due_age_seconds`: the cost of the park, which goes up behind a breaker.

A breaker opens after `dispatch.breaker_failures` (5) consecutive failures, stays open for `dispatch.breaker_open_ms` (30 s), then doubles per consecutive open up to `dispatch.breaker_max_open_ms` (5 min). Then one attempt probes.

## Action

1. Name the sink from the transitions series, then find why it fails: `GET /v1/dlq` shows the exact reason string of rows that gave up; the log has the first failures. Typical causes: the destination is down, a credential expired or was rotated, a network policy changed, a certificate expired, the destination is rate limiting us.
2. Fix the destination or the source's sink configuration (`PUT /v1/tenants/{tenant}/sources/{name}`). The next probe closes the breaker; a breaker that last opened long ago re-probes within `breaker_max_open_ms` (5 min).
3. Breakers live in dispatch's memory, so restarting the node closes them all and probes again at once. Do that only when the destination is known healthy.
4. A sink that is down for good: remove it from the source, or let its rows dead-letter after the retry horizon (`dispatch.retry.max_elapsed_ms`, 24 h) and replay them later.

## Verification

- `ankusa_dispatch_breakers_open` returns to 0, and `ankusa_dispatch_breaker_transitions_total{state="closed"}` rose for the sink.
- The attempt series for that sink shows `result="ok"` and no `error` or `timeout`.
- `ankusa_queue_oldest_due_age_seconds` falls: the parked rows are being delivered.
