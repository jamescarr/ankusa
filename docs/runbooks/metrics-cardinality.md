# `/metrics` exposes too many series

Alerts: `AnkusaMetricsSeriesOverBudget`

## Symptom

One node's `/metrics` carries more than 10 000 `ankusa_` series. The objective is at most 10 000 per node, **independent of the number of tenants**: a scrape that grows with tenants or sources slows the node's scrape, costs the monitoring system memory, and in the worst case gets the target dropped by `sample_limit`.

## Metric

- `count by (instance) ({__name__=~"ankusa_.+"})`: the alert's number.
- Which metric: `topk(10, count by (__name__) ({__name__=~"ankusa_.+", instance="<node>"}))`.
- Which label: `count(count by (tenant_id) (<metric>))`, then `source_id`, `tenant`, `sink`.
- `metrics.detail` and `metrics.max_series_per_metric` in `GET /v1/config`: the policy in force.

## Action

1. `metrics.detail` decides which identity labels a per-hook metric carries: `tenant` (default: `tenant_id` on counters and per-tenant gauges, none on histograms), `source` (`tenant_id` and `source_id`, on histograms too) or `none`. Set `tenant` or `none` unless you need more; every histogram label set is a dozen series.
2. `metrics.max_series_per_metric` caps what one metric keeps: the first `max - 1` label sets, then `"other"`. Lower it to bound the worst case. A gauge past the cap drops tenants instead of folding them.
3. Distinct series per sink or per reason are bounded by configuration (one per sink module, a fixed set of outcomes). A metric that is large with neither `tenant_id` nor `source_id` nor `tenant` in it is a **bug**: no label may come from unauthenticated input. Open an issue with the metric name and the label, and drop the label at your scrape with `metric_relabel_configs` meanwhile.
4. Serve `/metrics` from the metrics listener (`metrics.enabled`) so a scrape does not share the admin port.

## Verification

- `count by (instance) ({__name__=~"ankusa_.+"})` is under 10 000 on every node and does not move when a tenant is added.
- `metrics_test.exs` (`bounded series under 10k sources`) passes in CI: the series set is independent of tenant count.
