# ankusa

A loosely coupled, high-throughput webhook ingestion framework: durable local RocksDB store,
pluggable verification/storage/delivery, multi-tenant catch-URL routing.

![Provider posts to the edge; the batcher group-commits the hook to the local store and the provider gets 201 only after the fsync. Dispatch delivers due rows to sinks, dead-lettering give-ups; the compactor archives to the blob store.](https://raw.githubusercontent.com/jamescarr/ankusa/main/docs/diagrams/overview.png)

## Installation

```elixir
def deps do
  [
    {:ankusa, "~> 0.7"}
  ]
end
```

See [`https://hexdocs.pm/ankusa`](https://hexdocs.pm/ankusa) for the full
documentation and [`https://github.com/jamescarr/ankusa`](https://github.com/jamescarr/ankusa)
for the source.
