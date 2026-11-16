# Ankusa Helm chart

Runs `jamescarr/ankusa` on Kubernetes in any of its three durability modes, with a
hardened pod spec. The guide (probes, drain and grace period, PVC sizing, topology,
multi-region) is [`docs/deployment.md`](../../../docs/deployment.md#kubernetes-the-helm-chart);
every value is commented in [`values.yaml`](values.yaml).

![One chart release renders a StatefulSet with a PVC per pod for wal.type disk or dual (dual also confirming each hook in a Kafka intake topic) or a Deployment with an emptyDir and an HPA for none; every mode gets the Service, PDB, spread and NetworkPolicy, plus an optional claim-check Deployment.](../../../docs/diagrams/helm-chart.png)

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
`backup.final_timeout_ms` when the node backs up).

`claimCheck.enabled` adds the claim-check gateway as its own Deployment. It runs
the gateway image (`claimCheck.image`, `jamescarr/ankusa-gateway`, released on its
own from `packages/ankusa_gateway_server`), not the server image: a process with no
core role that reads the claim bucket named by `config.storage` /
`config.claim_check.store`. The chart derives its `gateway.yml` (a ConfigMap of its
own: `node.data_dir`, JSON logs, the drain, `gateway.port: 4001`, and the `storage`
and `claim_check` blocks of `config`), with `claimCheck.config` merged over it; a
`claim_check:` block a values file still carries is migrated as a version-1
document. Like the admin listener, the gateway refuses to bind the pod address
without `claimCheck.tokenSecret.name` unless `claimCheck.allowInsecure: true`.
Readiness is `GET /ready`; startup and liveness stay `GET /health`.

The pod runs as uid 100 / gid 101 (the image's `ankusa` user), with a read-only root
file system (writable: the data volume and a 256 MiB `/tmp`), `allowPrivilegeEscalation:
false`, `capabilities.drop: [ALL]` and the runtime's default seccomp profile. Probes:
`/health` for startup and liveness, `/ready` for readiness, all on the ingest port.

`ci/*-values.yaml` are the worked examples `mise run check:examples` lints, renders
and validates with `kubeconform` for each mode. The F5 fleet runs from this chart on
kind: `mise run e2e fleet` ([`examples/fleet-e2e/kind/`](../../../examples/fleet-e2e/kind/)).

The chart's `version` is its own; `appVersion` is the server image it runs by default
and moves with `packages/ankusa_server`'s version.
