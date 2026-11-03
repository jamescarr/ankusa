# A node is unready, or a failure domain went down

Alerts: `AnkusaSubtreeDown`

## Symptom

A failure domain of an Ankusa instance exhausted its restart budget and stays down until a backoff of 1 s, doubling to 60 s, ends. The domains are `dispatch`, `storage`, `backup`, `lifecycle`, `metrics`, `auth`, `tenant_erase`, and the `admin`, `routes_admin`, `claim_check` and `metrics_listener` listeners.

The node keeps acking hooks, because the core is a separate domain, and hooks wait in the store until the domain returns. A listener domain that is down means that port does not answer. The `metrics` domain is the one that produces the series every other alert reads, so alert on `up == 0` for the scrape target as well.

A node whose `/ready` answers `503` is the other face of this: the load balancer has already taken it out. That is a probe alert in your platform, and this runbook says what to look at.

## Metric

Which domain, and how often:

```promql
ankusa_instance_subtree_down_total
```

`ankusa_instance_subtree_down_total{domain}` names it. `GET /ready` lists every applicable check as `ok`, `failing` or `draining`, with `reasons` for each one that is not `ok` ([deployment](../deployment.md#running-the-container)):

| `/ready` shows | Look at |
| --- | --- |
| `store: failing` | [store-write-failed](store-write-failed.md), [disk-full](disk-full.md) |
| `disk: failing` | [disk-full](disk-full.md) |
| `intake: failing` | [intake-down](intake-down.md) |
| `sinks: failing` (`wal.type: none`) | the broker: a sink failed its last publish within `health.sink_error_window_ms` |
| `backlog: failing` | a label only, never a `503`: [backlog-age](backlog-age.md) |
| `backup: failing` | [backup-stale](backup-stale.md) |
| `status: draining` | a stop began (SIGTERM); it ends with the pod |

`ankusa_dispatch_running` at 0 with a backlog is a dispatch domain that is down.

## Action

1. Read the log line `[ankusa] <domain> subtree stopped (<reason>) after exhausting its restart budget; restarting it in <n> ms`. The reason is the first fault: a sink that keeps raising stops dispatch, and a port someone else took stops a listener.
2. Fix the cause. The domain restarts itself, and you do not restart the node. A child that cannot start at boot fails the boot instead, and the pod crash-loops: the log shows `{:store_open_failed, …}` or the config error ([deployment](../deployment.md#when-a-node-refuses-to-start)).
3. A core crash loop stops the instance for its supervisor to restart. Look at the store first.
4. A node that must leave the fleet now: `docker stop` it or delete the pod. SIGTERM drains it ([deployment](../deployment.md#running-the-container)).

## Verification

- `increase(ankusa_instance_subtree_down_total[10m])` is 0 and stays 0.
- `GET /ready` answers `200` and lists no check as `failing`.
- `ankusa_dispatch_running` is above 0 on a dispatch node, and `ankusa_queue_oldest_due_age_seconds` is falling.
