# Memory keeps growing

Alerts: `AnkusaMemoryGrowth`

## Symptom

The low-water mark of the Erlang VM's memory on a node is more than 5 % above where it was an hour earlier, and has been for an hour. The objective is at most 5 % RSS growth over a one-hour soak at the target rate. Left alone, the node is OOM-killed: the store recovers from its write-ahead log, but the node drops out of the fleet and its in-flight requests are reset.

## Metric

The low-water mark is the alert's number, because a spike does not matter:

```promql
min_over_time(ankusa_vm_memory_bytes[30m])
```

`ankusa_vm_memory_bytes` is `:erlang.memory(:total)`, which is the VM only. RocksDB's block cache and memtables are native memory outside it, so compare with the container's working set as well.

The series that hold memory in the VM are `ankusa_dedupe_keys`, `ankusa_http_pools`, `ankusa_sink_batch_queued_bytes`, `ankusa_dispatch_claimed_bytes`, `ankusa_quarantine_bytes`, `ankusa_claim_check_read_queued` and `ankusa_tenants`. Each has a ceiling, and one that climbs without reaching its ceiling is the finding. Growing `ankusa_queue_pending` with `ankusa_dispatch_claimed` is load, not a leak.

## Action

1. Rule out a backlog ([backlog-age](backlog-age.md)). A deep queue holds hook bodies in flight up to `dispatch.max_inflight_bytes` (128 MiB), and the batchers hold up to `batcher.max_queue_bytes` (256 MiB) per partition.
2. Inspect the running node with `docker-entrypoint rpc 'IO.inspect(:erlang.memory())'` for the split between binaries, ETS and processes, and `:recon.proc_count(:memory, 10)` if `:recon` is present. Binaries growing while ETS stays flat is a held reference. ETS growing is a table without its pruner.
3. A native-memory problem looks different: `ankusa_vm_memory_bytes` is flat while container memory rises. Check `queue.rocksdb` (block cache, `max_open_files`, memtable sizes) in [configuration](../configuration.md).
4. Buy time by rolling the node, one at a time, with a drained SIGTERM and then a new pod ([deployment](../deployment.md#running-the-container)). That is a mitigation: capture the `:erlang.memory()` output and the series above first, and open an issue with them.

## Verification

- `min_over_time(ankusa_vm_memory_bytes[30m])` is within 5 % of its value an hour earlier.
- The container's working set stops rising across a full traffic cycle.
- The soak job ([testing](../testing.md#soak-mise-run-benchsoak)) holds RSS growth at or under 5 %.
