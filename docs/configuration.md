# Configuration reference

Two structs, both built once at boot and passed down the supervision tree —
never `Application.get_env/2` scattered through call sites:

- **`%Hook.Config{}`** — instance-wide: roles, ports, adapters, tuning.
- **`%Hook.Source{}`** — per catch-URL: verification, dedup, sinks, tenant.

## `%Hook.Config{}`

Built with `Hook.Config.new/1` from a keyword list; unknown keys raise
`ArgumentError` at boot (fail fast on a typo, not at 3am). `:batcher`,
`:dispatch`, and `:storage` are maps and get **deep-merged** over the
defaults — pass only the keys you want to change.

```elixir
config :hook,
  instance: :default,
  port: 4000,
  data_dir: "./data",
  roles: [:edge, :dispatch, :storage],
  max_body_bytes: 8_000_000,
  route_resolver: {Hook.RouteResolver.Path, []},
  source_store: {Hook.SourceStore.Static, sources: %{}},
  wal: {Hook.WAL.DiskLog, []},
  batcher: %{partitions: System.schedulers_online(), max_batch: 256, max_delay_ms: 5, max_queue: 10_000},
  dispatch: %{poll_ms: 200, batch: 128, retry: {Hook.RetryPolicy.Exponential, []}},
  storage: %{
    blob_store: {Hook.BlobStore.LocalFS, []},
    codec: {Hook.Codec.Raw, []},
    roll_bytes: 16 * 1024 * 1024,
    roll_ms: 30_000,
    interval_ms: 1_000
  }
```

| Key | Default | Meaning |
| --- | --- | --- |
| `instance` | `:default` | Registry namespace — see [`architecture.md#instance-model`](architecture.md#instance-model). Two instances with different names run independently in one VM. |
| `data_dir` | `"./data"` | Root for on-disk state; actual paths are `<data_dir>/<instance>/{wal,segments,quarantine,dlq}`. |
| `roles` | `[:edge, :dispatch, :storage]` | Which children boot. `HOOK_ROLES=edge,dispatch` (comma-separated) overrides this at runtime in `Hook.Application`. See [`deployment.md`](deployment.md). |
| `port` | `4000` | Bandit HTTP port. `PORT` env var overrides in `Hook.Application`. |
| `max_body_bytes` | `8_000_000` | Hard cap enforced while streaming the request body; over it is `413` without buffering the whole thing. |
| `route_resolver` | `{Hook.RouteResolver.Path, []}` | `{module, opts}` implementing `Hook.RouteResolver` — catch-URL scheme. See [`multi-tenancy.md`](multi-tenancy.md). |
| `source_store` | `{Hook.SourceStore.Static, sources: %{}}` | `{module, opts}` implementing `Hook.SourceStore`. |
| `wal` | `{Hook.WAL.DiskLog, []}` | `{module, opts}` implementing `Hook.WAL`. See [`storage.md`](storage.md). |
| `batcher.partitions` | `System.schedulers_online()` | One group-commit `GenServer` per partition; a single commit process is a throughput ceiling. |
| `batcher.max_batch` | `256` | Flush once this many envelopes have queued. |
| `batcher.max_delay_ms` | `5` | Flush at least this often even under low load. |
| `batcher.max_queue` | `10_000` | Bound per partition; full means `{:error, :overload}` → `503`. |
| `dispatch.poll_ms` | `200` | How often the dispatch pipeline polls the WAL past its cursor. |
| `dispatch.batch` | `128` | Max envelopes read per poll. |
| `dispatch.retry` | `{Hook.RetryPolicy.Exponential, []}` | `{module, opts}` implementing `Hook.RetryPolicy` — the **default**, overridable per source (see below). |
| `storage.blob_store` | `{Hook.BlobStore.LocalFS, []}` | `{module, opts}` implementing `Hook.BlobStore`. See [`storage.md`](storage.md). |
| `storage.codec` | `{Hook.Codec.Raw, []}` | `{module, opts}` implementing `Hook.Codec` — segment record framing. |
| `storage.roll_bytes` | `16 MiB` | Roll a new segment past this size. |
| `storage.roll_ms` | `30_000` | ...or after this long, whichever comes first. |
| `storage.interval_ms` | `1_000` | Compactor tick interval. |

## Configuring a source

Sources are what `Hook.SourceStore.Static` (the default store) returns for a
given `source_id`; every field has a default, so `%{"demo" => []}` is valid
(everything default: `Verifier.None`, `DedupKey.Rules`, `:reject`,
`Sink.Log`, tenant `"default"`).

```elixir
config :hook,
  sources: %{
    "stripe" => [
      tenant_id: "acme",                                       # default: "default"
      verifier: {Hook.Verifier.Stripe, secret: System.get_env("STRIPE_WHSEC")},
      dedup: {Hook.DedupKey.Stripe, []},
      on_verify_failure: :quarantine,                           # :reject | :quarantine | :accept_flag
      sinks: [{Hook.Sink.Http, url: "https://example.internal/stripe"}]
    ]
  }
```

| Field | Default | Meaning |
| --- | --- | --- |
| `tenant_id` | `"default"` | The dedup/storage/retention scope — `(tenant_id, source_id, dedup_key)` is the uniqueness triple. See [`multi-tenancy.md`](multi-tenancy.md). |
| `verifier` | `{Hook.Verifier.None, []}` | `{module, opts}` implementing `Hook.Verifier`. |
| `dedup` | `{Hook.DedupKey.Rules, []}` | `{module, opts}` implementing `Hook.DedupKey`. |
| `on_verify_failure` | `:reject` | `:reject` (`401`, nothing stored) / `:quarantine` (`202`, durable pen) / `:accept_flag` (commits, envelope flagged). |
| `sinks` | `[{Hook.Sink.Log, []}]` | `[{module, opts}]` implementing `Hook.Sink`, delivered to in order, independently retried. |

A source can override the dispatch-wide retry policy by putting a
`:retry` opt directly in a sink tuple's opts if that sink's module reads it
(none of the shipped sinks do — `Sink.Http`/`Sink.RabbitMQ` retries are all
driven by `config.dispatch.retry`, applied uniformly per source by
`Hook.Dispatch.Pipeline`). Per-source retry policy override is not currently
supported; it's dispatch-wide.

## Every behaviour, at a glance

Full detail (options, guarantees, how to write your own) lives in
[`storage.md`](storage.md) and [`delivery.md`](delivery.md); this table is
the map.

| Behaviour | Job | Default | Also shipped |
| --- | --- | --- | --- |
| `Hook.RouteResolver` | Catch-URL scheme → `%Route{tenant_id, source_id}` | `RouteResolver.Path` (`/hooks/:source_id`) | `RouteResolver.TenantPath` (`/hooks/:tenant/:source`) |
| `Hook.WAL` | Durable ack, ordered log, truncation | `WAL.DiskLog` (fsync group commit) | `WAL.Postgres` (shared, multi-node — `hook_postgres` package) |
| `Hook.Verifier` | Signature/timestamp checks | `Verifier.None` | `StandardWebhooks`, `Stripe`, `GitHub` |
| `Hook.DedupKey` | Extract provider event id | `DedupKey.Rules` (header/JSON path) | `Stripe`, `GitHub` |
| `Hook.SourceStore` | Source config, secrets, policy | `SourceStore.Static` | — |
| `Hook.Sink` | What happens to a delivered hook | `Sink.Log` | `Sink.Http` (`:httpc` forward), `Sink.RabbitMQ` (exchange publish — `hook_rabbitmq` package) |
| `Hook.RetryPolicy` | Backoff / give-up | `RetryPolicy.Exponential` (jitter) | — |
| `Hook.BlobStore` | Segment PUT / range GET / delete | `BlobStore.LocalFS` | `BlobStore.S3` (+R2/MinIO), `BlobStore.GCS` |
| `Hook.Codec` | Segment record framing | `Codec.Raw` (len-prefixed, CRC32) | — |

Swapping any of these is a one-line config change — `wal: {Hook.WAL.Postgres,
hostname: "...", database: "..."}` — because every layer is a behaviour with
`{module, opts}` config, resolved at the call site, never hardcoded.

## Runtime environment overrides

`Hook.Application` (the default OTP application boot path) reads two env
vars on top of whatever `config.exs` sets:

- `PORT` — overrides `config.port`.
- `HOOK_ROLES` — comma-separated, overrides `config.roles` (e.g.
  `HOOK_ROLES=edge,dispatch`).

This is deliberately the *only* place env vars are read inside `hook` core —
everything else is `%Hook.Config{}` passed explicitly. A deployment wrapper
(like [`examples/rabbitmq-consumer/ingest_app`](../examples/rabbitmq-consumer/ingest_app))
is free to read as many env vars as it wants and build the config struct
itself; that's the intended extension point, not a gap.
