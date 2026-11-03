# One tenant is being refused with `429 tenant_overload`

Alerts: `AnkusaTenantOverload`

## Symptom

One tenant has used its whole share of the edge's batchers for 10 minutes and is answered `429 tenant_overload` with `Retry-After`. The other tenants are unaffected, which is the isolation working: this is not the node's `503 overload`, which is every tenant's problem. The tenant's provider retries, so its hooks arrive late or, if the provider stops retrying, not at all.

## Metric

Who, and how hard:

```promql
sum by (tenant_id) (rate(ankusa_ingest_requests_total{outcome="tenant_overload"}[5m]))
```

The `tenant_id` label is present at `metrics.detail: tenant` (the default) and `source`. At `none` there is no tenant label and the alert is per node.

`ankusa_ingest_requests_total{outcome="rate_limited"}` is a different refusal: the tenant's hooks-per-second or bytes-per-second quota, answered `429 rate_limited`. `ankusa_tenant_pending{tenant}` and `ankusa_dispatch_tenant_inflight{tenant}` say whether the tenant also has a delivery backlog, which would mean the tenant's sink is slow and dispatch is the bottleneck, not ingest. `ankusa_queue_oldest_due_age_seconds` says whether the tenant's load is hurting delivery for everyone else.

## Action

1. Confirm it is a real surge and not a retry storm from a provider that saw our `429`s. Look at the request rate before the first refusal.
2. A legitimate tenant outgrowing its share: raise its limits with `PUT /v1/tenants/{tenant}/rate-limit` (scope `rate_limits:write`, audited), including `max_queue` and `max_queue_bytes`, which are its share of the batchers. You can also set `rate_limits.tenants.<id>` in the config, and `GET /v1/rate-limits` lists what is in force ([configuration](../configuration.md#rate-limits)).
3. A tenant that should be throttled: leave the limit and tell the tenant. The `429` is the contract, and providers back off on `Retry-After`.
4. Do not raise `batcher.max_queue` for one tenant. It is the node's own ceiling, and the reason the other tenants are protected. If the total is the problem, add edge nodes ([ack-slo](ack-slo.md)).
5. With `rate_limits.scope: fleet` the tenant's budget is shared across edge nodes through `fleet.store`, so check `ankusa_fleet_limiter_degraded` before concluding the limit is wrong.

## Verification

- `rate(ankusa_ingest_requests_total{outcome="tenant_overload",tenant_id="<id>"}[5m])` falls to 0.
- A probe hook for that tenant is acked `201`.
- The other tenants were never affected: the per-node p99 of `ankusa_ingest_duration_seconds` stays below 0.05 s.
