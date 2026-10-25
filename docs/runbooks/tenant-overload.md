# One tenant is being refused with `429 tenant_overload`

Alerts: `AnkusaTenantOverload`

## Symptom

One tenant has used its whole share of the edge's batchers for 10 minutes and is answered `429 tenant_overload` with `Retry-After`. Other tenants are unaffected: this is the isolation working, not the node's `503 overload`, which is every tenant's problem. The tenant's provider retries, so the tenant's hooks arrive late (or, if its provider stops retrying, not at all).

## Metric

- `sum by (tenant_id) (rate(ankusa_ingest_requests_total{outcome="tenant_overload"}[5m]))`: who, and how hard. The `tenant_id` label is present at `metrics.detail: tenant` (the default) and `source`; at `none` there is no tenant label and the alert is per node.
- `ankusa_ingest_requests_total{outcome="rate_limited"}`: the tenant's hooks-per-second or bytes-per-second quota, a different refusal (`429 rate_limited`).
- `ankusa_tenant_pending{tenant}` and `ankusa_dispatch_tenant_inflight{tenant}`: whether the tenant also has a delivery backlog (a slow tenant sink makes dispatch the bottleneck, not ingest).
- `ankusa_queue_oldest_due_age_seconds`: whether the tenant's load is hurting delivery for others.

## Action

1. Confirm it is a real surge and not a retry storm from a provider that saw our `429`s: look at the request rate before the first refusal.
2. A legitimate tenant outgrowing its share: raise its limits with `PUT /v1/tenants/{tenant}/rate-limit` (scope `rate_limits:write`; audited), including `max_queue` and `max_queue_bytes` (its share of the batchers), or set `rate_limits.tenants.<id>` in the config. `GET /v1/rate-limits` lists what is in force ([configuration](../configuration.md#rate-limits)).
3. A tenant that should be throttled: leave the limit and tell the tenant. The `429` is the contract: providers back off on `Retry-After`.
4. Do not raise `batcher.max_queue` for one tenant: it is the node's own ceiling and the reason other tenants are protected. Add edge nodes if the total is the problem ([ack-slo](ack-slo.md)).
5. With `rate_limits.scope: fleet` the tenant's budget is shared across edge nodes through `fleet.store`: check `ankusa_fleet_limiter_degraded` before concluding the limit is wrong.

## Verification

- `rate(ankusa_ingest_requests_total{outcome="tenant_overload",tenant_id="<id>"}[5m])` falls to 0.
- A probe hook for that tenant is acked `201`.
- Other tenants' `ankusa_ingest_duration_seconds` was never affected: the per-node p99 stays below 0.05 s.
