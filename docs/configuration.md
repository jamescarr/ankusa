# Configuration reference

Two structs, both built once at boot and passed down the supervision tree —
never `Application.get_env/2` scattered through call sites:

- **`%Ankusa.Config{}`** — instance-wide: roles, ports, adapters, tuning.
- **`%Ankusa.Source{}`** — per catch-URL: verification, dedup, sinks, tenant.

## `%Ankusa.Config{}`

Built with `Ankusa.Config.new/1` from a keyword list; unknown keys raise
`ArgumentError` at boot (fail fast on a typo, not at 3am). `:batcher`,
`:dispatch`, `:storage`, and `:claim_check` are maps and get **deep-merged**
over the defaults — pass only the keys you want to change.

```elixir
config :ankusa,
  instance: :default,
  port: 4000,
  data_dir: "./data",
  roles: [:edge, :dispatch, :storage],
  max_body_bytes: 8_000_000,
  route_resolver: {Ankusa.RouteResolver.Path, []},
  source_store: {Ankusa.SourceStore.Static, sources: %{}},
  wal: {Ankusa.WAL.DiskLog, []},
  batcher: %{partitions: System.schedulers_online(), max_batch: 256, max_delay_ms: 5, max_queue: 10_000},
  dispatch: %{poll_ms: 200, batch: 128, retry: {Ankusa.RetryPolicy.Exponential, []}},
  storage: %{
    blob_store: {Ankusa.BlobStore.LocalFS, []},
    codec: {Ankusa.Codec.Raw, []},
    roll_bytes: 16 * 1024 * 1024,
    roll_ms: 30_000,
    interval_ms: 1_000
  },
  claim_check: %{
    adapter: {Ankusa.ClaimCheck.Direct, []},
    max_bytes: 8_000_000,
    port: 4001,
    api_tokens: %{},
    retention_days: nil,
    sweep_interval_ms: 3_600_000
  }
```

| Key | Default | Meaning |
| --- | --- | --- |
| `instance` | `:default` | Registry namespace — see [`architecture.md#instance-model`](architecture.md#instance-model). Two instances with different names run independently in one VM. |
| `data_dir` | `"./data"` | Root for on-disk state; actual paths are `<data_dir>/<instance>/{wal,segments,quarantine,dlq}`. |
| `roles` | `[:edge, :dispatch, :storage]` | Which children boot. `:claim_check` is a fourth, **opt-in** role — see [`claim-check.md`](claim-check.md). `ANKUSA_ROLES=edge,dispatch` (comma-separated) overrides this at runtime in the default Ankusa.Application. See [`deployment.md`](deployment.md). |
| `port` | `4000` | Bandit HTTP port. `PORT` env var overrides in the default Ankusa.Application. |
| `max_body_bytes` | `8_000_000` | Hard cap enforced while streaming the request body; over it is `413` without buffering the whole thing. |
| `route_resolver` | `{Ankusa.RouteResolver.Path, []}` | `{module, opts}` implementing `Ankusa.RouteResolver` — catch-URL scheme. See [`multi-tenancy.md`](multi-tenancy.md). |
| `source_store` | `{Ankusa.SourceStore.Static, sources: %{}}` | `{module, opts}` implementing `Ankusa.SourceStore`. |
| `wal` | `{Ankusa.WAL.DiskLog, []}` | `{module, opts}` implementing `Ankusa.WAL`. See [`storage.md`](storage.md). |
| `batcher.partitions` | `System.schedulers_online()` | One group-commit `GenServer` per partition; a single commit process is a throughput ceiling. |
| `batcher.max_batch` | `256` | Flush once this many envelopes have queued. |
| `batcher.max_delay_ms` | `5` | Flush at least this often even under low load. |
| `batcher.max_queue` | `10_000` | Bound per partition; full means `{:error, :overload}` → `503`. |
| `dispatch.poll_ms` | `200` | How often the dispatch pipeline polls the WAL past its cursor. |
| `dispatch.batch` | `128` | Max envelopes read per poll. |
| `dispatch.retry` | `{Ankusa.RetryPolicy.Exponential, []}` | `{module, opts}` implementing `Ankusa.RetryPolicy` — the **default**, overridable per source (see below). |
| `storage.blob_store` | `{Ankusa.BlobStore.LocalFS, []}` | `{module, opts}` implementing `Ankusa.BlobStore`. See [`storage.md`](storage.md). |
| `storage.codec` | `{Ankusa.Codec.Raw, []}` | `{module, opts}` implementing `Ankusa.Codec` — segment record framing. |
| `storage.roll_bytes` | `16 MiB` | Roll a new segment past this size. |
| `storage.roll_ms` | `30_000` | ...or after this long, whichever comes first. |
| `storage.interval_ms` | `1_000` | Compactor tick interval. |
| `claim_check.adapter` | `{Ankusa.ClaimCheck.Direct, []}` | `{module, opts}` implementing `Ankusa.ClaimCheck`. See [`claim-check.md`](claim-check.md). |
| `claim_check.max_bytes` | `8_000_000` | Hard cap on a checked-in body. |
| `claim_check.port` | `4001` | The `:claim_check` role's Bandit port. |
| `claim_check.api_tokens` | `%{}` | `%{token => :all \| [tenant_id, ...]}`; required (non-empty) on a `:claim_check`-role node. |
| `claim_check.retention_days` | `nil` | LocalFS-only sweeper retention; `nil` disables the sweeper. |
| `claim_check.sweep_interval_ms` | `3_600_000` | Sweeper tick interval. |

## Configuring a source

Sources are what `Ankusa.SourceStore.Static` (the default store) returns for a
given `source_id`; every field has a default, so `%{"demo" => []}` is valid
(everything default: `Verifier.None`, `DedupKey.Rules`, `:reject`,
`Sink.Log`, tenant `"default"`).

```elixir
config :ankusa,
  sources: %{
    "stripe" => [
      tenant_id: "acme",                                       # default: "default"
      verifier: {Ankusa.Verifier.Stripe, secret: System.get_env("STRIPE_WHSEC")},
      dedup: {Ankusa.DedupKey.Stripe, []},
      on_verify_failure: :quarantine,                           # :reject | :quarantine | :accept_flag
      sinks: [{Ankusa.Sink.Http, url: "https://example.internal/stripe"}]
    ]
  }
```

| Field | Default | Meaning |
| --- | --- | --- |
| `tenant_id` | `"default"` | The dedup/storage/retention scope — `(tenant_id, source_id, dedup_key)` is the uniqueness triple. See [`multi-tenancy.md`](multi-tenancy.md). |
| `verifier` | `{Ankusa.Verifier.None, []}` | `{module, opts}` implementing `Ankusa.Verifier`. |
| `dedup` | `{Ankusa.DedupKey.Rules, []}` | `{module, opts}` implementing `Ankusa.DedupKey`. |
| `on_verify_failure` | `:reject` | `:reject` (`401`, nothing stored) / `:quarantine` (`202`, durable pen) / `:accept_flag` (commits, envelope flagged). |
| `sinks` | `[{Ankusa.Sink.Log, []}]` | `[{module, opts}]` implementing `Ankusa.Sink`, delivered to in order, independently retried. |

A source can override the dispatch-wide retry policy by putting a
`:retry` opt directly in a sink tuple's opts if that sink's module reads it
(none of the shipped sinks do — `Sink.Http`/`Sink.RabbitMQ`/`Sink.Kafka` retries are all
driven by `config.dispatch.retry`, applied uniformly per source by
`Ankusa.Dispatch.Pipeline`). Per-source retry policy override is not currently
supported; it's dispatch-wide.

## Every behaviour, at a glance

Full detail (options, guarantees, how to write your own) lives in
[`storage.md`](storage.md) and [`delivery.md`](delivery.md); this table is
the map.

| Behaviour | Job | Default | Also shipped |
| --- | --- | --- | --- |
| `Ankusa.RouteResolver` | Catch-URL scheme → `%Route{tenant_id, source_id}` | `RouteResolver.Path` (`/webhooks/:source_id`) | `RouteResolver.TenantPath` (`/webhooks/:tenant/:source`) |
| `Ankusa.WAL` | Durable ack, ordered log, truncation | `WAL.DiskLog` (fsync group commit) | `WAL.Postgres` (shared, multi-node — `ankusa_postgres` package) |
| `Ankusa.Verifier` | Signature/timestamp checks | `Verifier.None` | `StandardWebhooks`, `Stripe`, `GitHub` |
| `Ankusa.DedupKey` | Extract provider event id | `DedupKey.Rules` (header/JSON path) | `Stripe`, `GitHub` |
| `Ankusa.SourceStore` | Source config, secrets, policy | `SourceStore.Static` | — |
| `Ankusa.Sink` | What happens to a delivered hook | `Sink.Log` | `Sink.Http` (Req forward), `Sink.RabbitMQ` (exchange publish — `ankusa_rabbitmq` package), `Sink.Kafka` (topic produce — `ankusa_kafka` package) |
| `Ankusa.RetryPolicy` | Backoff / give-up | `RetryPolicy.Exponential` (jitter) | — |
| `Ankusa.BlobStore` | Segment PUT / range GET / delete | `BlobStore.LocalFS` | `BlobStore.S3` (+R2/MinIO), `BlobStore.GCS` |
| `Ankusa.ClaimCheck` | Check bytes in, redeem by ticket | `ClaimCheck.Direct` (in-process `BlobStore`) | `ClaimCheck.Remote` (HTTP, `:claim_check` role) |
| `Ankusa.Codec` | Segment record framing | `Codec.Raw` (len-prefixed, CRC32) | — |

Swapping any of these is a one-line config change — `wal: {Ankusa.WAL.Postgres,
hostname: "...", database: "..."}` — because every layer is a behaviour with
`{module, opts}` config, resolved at the call site, never hardcoded.

## Runtime environment overrides

Ankusa.Application (the default OTP application boot path) reads two env
vars on top of whatever `config.exs` sets:

- `PORT` — overrides `config.port`.
- `ANKUSA_ROLES` — comma-separated, overrides `config.roles` (e.g.
  `ANKUSA_ROLES=edge,dispatch`). Parsed with `Ankusa.Config.parse_roles!/1`:
  an unknown role name (anything other than `edge`, `dispatch`, `storage`,
  `claim_check`) raises `ArgumentError` and fails boot rather than silently
  starting with the wrong roles.

`autostart` (application env, default `false`) gates whether
Ankusa.Application boots its built-in default instance at all — a library
must not bind a port just because it's a dependency. Set `config :ankusa,
autostart: true` in a deployment that wants the zero-config default instance
(the repo's own `config/config.exs` does this outside `:test`).

This is deliberately the *only* place env vars are read inside `ankusa` core —
everything else is `%Ankusa.Config{}` passed explicitly. A deployment wrapper
(like [`examples/rabbitmq-consumer/ingest_app`](https://github.com/jamescarr/ankusa/tree/main/examples/rabbitmq-consumer/ingest_app))
is free to read as many env vars as it wants and build the config struct
itself; that's the intended extension point, not a gap.
