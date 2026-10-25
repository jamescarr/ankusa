# Ankusa Helm chart

Runs `jamescarr/ankusa` on Kubernetes in any of its three durability modes, with a
hardened pod spec. The guide (probes, drain and grace period, PVC sizing, topology,
multi-region) is [`docs/deployment.md`](../../../docs/deployment.md#kubernetes-the-helm-chart);
every value is commented in [`values.yaml`](values.yaml).

```sh
helm install ankusa deploy/helm/ankusa -f my-values.yaml
```

| `wal.type` | Renders | Scales |
| --- | --- | --- |
| `disk` (default) | a StatefulSet, a PVC per pod | `replicas`; never automatically |
| `dual` | the same, `intake.node_id` = the pod name; needs `config.intake` and `config.backup` | `replicas` |
| `none` | a Deployment, stateless, behind an HPA | `autoscaling.*` |

Every mode also gets: a Service, a PodDisruptionBudget (`maxUnavailable: 1`), a
NetworkPolicy (ingest open, 4001-4004 to the operator namespaces), a ServiceAccount
with no token, spread constraints over zones and nodes, and a
`terminationGracePeriodSeconds` computed from the drain (`drain.*`, plus
`backup.final_timeout_ms` when the node backs up). `claimCheck.enabled` adds the
claim-check gateway as its own Deployment.

The pod runs as uid 100 / gid 101 (the image's `ankusa` user), with a read-only root
file system (writable: the data volume and a 256 MiB `/tmp`), `allowPrivilegeEscalation:
false`, `capabilities.drop: [ALL]` and the runtime's default seccomp profile. Probes:
`/health` for startup and liveness, `/ready` for readiness, all on the ingest port.

`ci/*-values.yaml` are the worked examples `mise run check:examples` lints, renders
and validates with `kubeconform` for each mode. The F5 fleet runs from this chart on
kind: `mise run e2e fleet` ([`examples/fleet-e2e/kind/`](../../../examples/fleet-e2e/kind/)).

The chart's `version` is its own; `appVersion` is the server image it runs by default
and moves with `packages/ankusa_server`'s version.
