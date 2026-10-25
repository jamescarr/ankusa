# Memory keeps growing

Alerts: `AnkusaMemoryGrowth`

## Symptom

The low-water mark of the Erlang VM's memory on a node is more than 5 % above where it was an hour earlier, and has been for an hour. The objective is at most 5 % RSS growth over a one-hour soak at the target rate. Left alone, the node is OOM-killed; the store recovers from its write-ahead log, but the node drops out of the fleet and its in-flight requests are reset.

## Metric

- `ankusa_vm_memory_bytes`: `:erlang.memory(:total)`. It is the VM only: RocksDB's block cache and memtables are native memory outside it, so compare with the container's working set as well.
- What holds memory in the VM, by series: `ankusa_dedupe_keys`, `ankusa_http_pools`, `ankusa_sink_batch_queued_bytes`, `ankusa_dispatch_claimed_bytes`, `ankusa_quarantine_bytes`, `ankusa_claim_check_read_queued` and `ankusa_tenants`. Each is bounded by a ceiling; one that climbs without a ceiling is the finding.
- Is it load? `ankusa_queue_pending` and `ankusa_dispatch_claimed` growing with the backlog is load, not a leak.

## Action

1. Rule out a backlog: [backlog-age](backlog-age.md). A deep queue holds hook bodies in flight up to `dispatch.max_inflight_bytes` (128 MiB) and batchers hold up to `batcher.max_queue_bytes` (256 MiB) per partition.
2. Inspect the running node: `docker-entrypoint rpc 'IO.inspect(:erlang.memory())'` for the split (binaries, ETS, processes), and `:recon.proc_count(:memory, 10)` if `:recon` is present. Binaries growing with ETS flat is a held reference; ETS growing is a table without its pruner.
3. A native-memory problem looks different: `ankusa_vm_memory_bytes` flat, container memory rising. Check `queue.rocksdb` (block cache, `max_open_files`, memtable sizes) in [configuration](../configuration.md).
4. Buy time: roll the node (a drained SIGTERM, then a new pod, one at a time: [deployment](../deployment.md#running-the-container)). It is a mitigation. Capture the `:erlang.memory()` output and the series above before you do, and open an issue with them.

## Verification

- `min_over_time(ankusa_vm_memory_bytes[30m])` is within 5 % of its value an hour earlier.
- The container's working set stops rising across a full traffic cycle.
- The soak job ([testing](../testing.md#soak-mise-run-benchsoak)) holds RSS growth at or under 5 %.
