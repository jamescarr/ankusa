# `/metrics` exposes too many series

Alerts: `AnkusaMetricsSeriesOverBudget`

## Symptom

One node's `/metrics` carries more than 10 000 `ankusa_` series. The objective is at most 10 000 per node, independent of the number of tenants. A scrape that grows with tenants or sources slows the node's scrape, costs the monitoring system memory, and in the worst case gets the target dropped by `sample_limit`.

## Metric

The alert's own number:

```promql
count by (instance) ({__name__=~"ankusa_.+"})
```

Which metric is responsible: `topk(10, count by (__name__) ({__name__=~"ankusa_.+", instance="<node>"}))`. Which label makes it large: `count(count by (tenant_id) (<metric>))`, then the same query by `source_id`, `tenant` and `sink`. `GET /v1/config` shows the policy in force, `metrics.detail` and `metrics.max_series_per_metric`.

## Action

1. `metrics.detail` decides which identity labels a per-hook metric carries. `tenant` is the default: `tenant_id` on counters and per-tenant gauges, and none on histograms. `source` adds `tenant_id` and `source_id`, on histograms too, and `none` carries neither. Set `tenant` or `none` unless you need more, because every histogram label set is a dozen series.
2. `metrics.max_series_per_metric` caps what one metric keeps: the first `max - 1` label sets, then `"other"`. Lower it to bound the worst case. A gauge past the cap drops tenants instead of folding them.
3. Distinct series per sink or per reason are bounded by configuration, one per sink module and a fixed set of outcomes. A metric that is large with neither `tenant_id` nor `source_id` nor `tenant` in it is a bug, because no label may come from unauthenticated input. Open an issue with the metric name and the label, and drop the label at your scrape with `metric_relabel_configs` meanwhile.
4. Serve `/metrics` from the metrics listener (`metrics.enabled`), so a scrape does not share the admin port.

## Verification

- `count by (instance) ({__name__=~"ankusa_.+"})` is under 10 000 on every node and does not move when a tenant is added.
- `metrics_test.exs` (`bounded series under 10k sources`) passes in CI, which shows the series set is independent of tenant count.
