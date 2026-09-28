# Configuration

The container reads a YAML file; the library takes a `%Ankusa.Config{}`, and
both describe the same pipeline.

## Container configuration (YAML)

The image starts with a baked-in demo config at `/etc/ankusa/ankusa.yml`. Mount
yours over it:

```sh
-v "$PWD/ankusa.yml:/etc/ankusa/ankusa.yml:ro"
```

or point `ANKUSA_CONFIG` at another path.

`${VAR}` and `${VAR:-default}` are interpolated from the environment. A
`${VAR}` with no value and no default stops the container at startup and names
the field, so an empty secret can never quietly accept everything an attacker
signs. An invalid file exits `78` (`EX_CONFIG`), a message, not a crash dump.
Check a file before you start the container:

```sh
docker run --rm -v "$PWD/ankusa.yml:/etc/ankusa/ankusa.yml:ro" \
  -e STRIPE_WHSEC jamescarr/ankusa:edge check-config
# => config OK: roles=[:edge, :dispatch, :storage] sources=demo,stripe \
#      wal=Ankusa.WAL.DiskLog storage=Ankusa.BlobStore.LocalFS

docker run --rm -v "$PWD/ankusa.yml:/etc/ankusa/ankusa.yml:ro" \
  -e STRIPE_WHSEC jamescarr/ankusa:edge print-config
# => the effective config as JSON, with every secret redacted
```

`version` prints the server and core versions.

A minimal file, one open source and one HTTP sink:

```yaml
sources:
  demo:
    verify: {type: none}
    on_verify_failure: accept_flag
    sinks:
      - type: http
        url: http://worker:8080/hooks
        timeout_ms: 5000
```

### Sections

Every top-level section, with its keys and defaults:

| Section | Keys (default) |
| --- | --- |
| `node` | `roles` (`[edge, dispatch, storage]`), `data_dir` (`/var/lib/ankusa`) |
| `log` | `level` (`info`) |
| `http` | `port` (4000), `max_body_bytes` (8000000), `routing` (`path` \| `tenant_path`), `prefix` (`/webhooks`) |
| `admin` | `enabled` (`true` in the image, `false` in core), `port` (4002) |
| `batcher` | `partitions` (2), `max_batch` (256), `max_delay_ms` (0), `max_queue` (10000) |
| `dispatch` | `poll_ms` (200), `batch` (128), `concurrency` (32), `max_inflight` (4096), `max_inflight_bytes` (134217728), `retry.base_ms` (100), `retry.max_ms` (30000), `retry.max_attempts` (12), `retry.jitter` (`true`) |
| `wal` | `type` (`disk`) |
| `storage` | `type` (`local` \| `s3` \| `gcs`), `roll_bytes` (16777216), `roll_ms` (30000), `s3.*` (`bucket`, `region`, `endpoint`, keys), `gcs.*` (`bucket`, `endpoint`, `auth` = `metadata` \| `token` \| `none`) |
| `claim_check` | `port` (4001), `pack_max_bytes` (16777216), `retention_days` (null disables the sweeper) |
| `routes` | `enabled` (`false`), `max_routes` (10000), `store.type` (`ets` \| `redis`), `cache.*`, `trusted_proxies` (`[]`), `ip_rules.*`, `admin.token`/`admin.port` (4003), `log_sample` (100), `ip_denied_status` (403), `seed` (`[]`). See [Route management](#route-management) |
| `sources` | One entry per catch-URL source. See below |

`storage.s3`/`storage.gcs` are read only when the matching
`type` is set. See
[`config-examples/reference.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/config-examples/reference.yml)
for every key with its comment and the alternatives.

### Sources

One source per provider endpoint; the key is the catch-URL segment
(`POST /webhooks/stripe`).

| Key | Meaning |
| --- | --- |
| `tenant` | The storage/retention scope. With `routing: tenant_path` the URL wins. Default `default`. See [`multi-tenancy.md`](multi-tenancy.md). |
| `verify.type` | `none` \| `stripe` \| `github` \| `standard_webhooks` \| `shopify` \| `slack` \| `hmac`. Every type except `none` requires `secret`. `stripe`, `standard_webhooks`, and `slack` also take `tolerance_seconds` (default 300). `hmac` takes the descriptor keys below. |
| `on_verify_failure` | `reject` \| `quarantine` \| `accept_flag`: what happens when verification fails. |
| `sinks` | At least one; every sink is tried on every delivered hook. |

### Custom HMAC schemes

The five named verifier types (`stripe`, `github`, `standard_webhooks`, `shopify`,
`slack`) are presets for the same engine. Any other body-HMAC provider is
described inline with `type: hmac` and the descriptor keys:

```yaml
sources:
  acme:
    verify:
      type: hmac
      secret: "${ACME_WEBHOOK_SECRET}"
      signature_header: "X-Acme-Signature"   # required. Where the signature lives
      signed: "{body}"                        # template over {body}, {header:NAME}, {ts}
      hash: sha256                            # sha256 | sha512 | sha1
      encoding: hex                           # hex | base64
      sig_prefix: "sha256="                   # optional. Stripped from the header value
      timestamp_header: "X-Acme-Timestamp"    # optional. Also signs {ts} + replay window
      tolerance_seconds: 300
    sinks: [{type: log}]
```

| Key | Default | Meaning |
| --- | --- | --- |
| `signature_header` | none | Required. The header carrying the signature(s). |
| `signed` | `"{body}"` | Template over `{body}`, `{header:NAME}` (another header's value), and `{ts}` (the timestamp below). Quoted: `{...}` would otherwise parse as a YAML flow map. |
| `parse` | `whole` | `whole` (value is one signature) \| `csv_pairs` (`k=v,k=v`; `sig_key` names the signature pair) \| `space_versions` (space-separated `v1,<sig>` tokens; `version` is the token prefix). |
| `sig_prefix` | none | Literal prefix stripped from a `whole` header value, e.g. `sha256=`. |
| `sig_key` | none | With `parse: csv_pairs`, the key whose value is a signature, e.g. `v1`. |
| `version` | none | With `parse: space_versions`, the token prefix, e.g. `v1,`. |
| `hash` | `sha256` | `sha256` \| `sha512` \| `sha1`. |
| `encoding` | `hex` | `hex` (lowercase) \| `base64`. |
| `secret_decode` | `raw` | `raw` (use `secret` as the key) \| `whsec_base64` (strip `whsec_` then Base64-decode). |
| `timestamp_header` | none | Header carrying the Unix-seconds timestamp, both signed as `{ts}` and checked against `tolerance_seconds`. |

Providers that are not body-HMAC, Twilio (SHA1 over the full request URL plus
sorted form params) and PayPal (RSA over a certificate fetched from a provider
URL), cannot be described by this engine. They need a bespoke
`Ankusa.Verifier` module.

### Sinks

| `type` | Keys |
| --- | --- |
| `log` | none |
| `http` | `url`, `method` (`post` \| `put` \| `patch`), `headers`, `timeout_ms` (5000), `ordered` (`false`; `true` serializes deliveries per `{tenant_id, source_id}`, in `seq` order). The receiver contract is in [`integrations.md#http-handoff-any-language`](integrations.md#http-handoff-any-language). |
| `rabbitmq` | `url`, `exchange`, `exchange_type` (`topic` \| `direct` \| `fanout` \| `headers`), `routing_key`, `inline_max_bytes` (65536). |
| `kafka` | `brokers` (a list, or one comma-separated string), `topic`, `key` (a static string), `inline_max_bytes` (65536), `ssl`, `sasl` (`mechanism` = `plain` \| `scram_sha_256` \| `scram_sha_512`, `username`, `password`). |
| `nats` | `servers` (a list, or one comma-separated string, tried in order), `subject`, `inline_max_bytes` (65536), `publish_timeout_ms` (5000), `tls`, `auth` (one scheme: `username` + `password`, `token`, or `nkey_seed` + `jwt`). The stream must already exist. See [`delivery.md`](delivery.md#sinknats--subject-delivery). |

Bodies above a sink's `inline_max_bytes` are checked in to the object store and
the message carries a claim reference. See [`claim-check.md`](claim-check.md).

### Environment overrides

Anything you would normally read from the platform, so a container can be
reconfigured without a new file. Env wins over the file.

| Variable | Field |
| --- | --- |
| `ANKUSA_ROLES` | `node.roles`, comma-separated (`edge`, `dispatch`, `storage`, `claim_check`) |
| `ANKUSA_DATA_DIR` | `node.data_dir` |
| `ANKUSA_LOG_LEVEL` | `log.level` |
| `ANKUSA_HTTP_PORT`, else `PORT` | `http.port` |
| `ANKUSA_ADMIN_PORT` | `admin.port` |
| `ANKUSA_CLAIM_CHECK_PORT` | `claim_check.port` |
| `ANKUSA_ROUTES_ENABLED` | `routes.enabled` |
| `ANKUSA_ROUTES_ADMIN_TOKEN` | `routes.admin.token` |
| `ANKUSA_ROUTES_STORE_URL` | `routes.store.url` |
| `ANKUSA_WAL_TYPE` | `wal.type` (`disk`) |
| `ANKUSA_STORAGE_TYPE` | `storage.type` (`local`, `s3`, `gcs`) |
| `ANKUSA_S3_BUCKET`, `ANKUSA_S3_REGION`, `ANKUSA_S3_ENDPOINT` | `storage.s3.bucket/region/endpoint` |
| `ANKUSA_GCS_BUCKET` | `storage.gcs.bucket` |

`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` are read by the S3 adapter
directly when the config does not name static keys.

Sources and sinks are not env-overridable: they carry behaviour, so they live in
the file, with secrets injected through `${VAR}`.

### Starting points

All loadable as-is. Copy one, delete what you don't use, replace the `${VAR}`s:

| File | What it is |
| --- | --- |
| [`config-examples/reference.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/config-examples/reference.yml) | every key, at its default, with the alternatives |
| [`config-examples/single-node.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/config-examples/single-node.yml) | one box: disk WAL, Stripe + GitHub, HTTP sink |
| [`config-examples/kafka-fanout.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/config-examples/kafka-fanout.yml), [`rabbitmq-fanout.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/config-examples/rabbitmq-fanout.yml), [`nats-fanout.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/config-examples/nats-fanout.yml) | queue fan-out, with the claim-check gateway (`claim_check` role included) |
| [`config-examples/multi-tenant.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/config-examples/multi-tenant.yml) | one instance, many tenants, tenant in the URL |

## Library configuration (Elixir)

Two structs, both built once at boot and passed down the supervision tree.
Never `Application.get_env/2` scattered through call sites:

- **`%Ankusa.Config{}`**. Instance-wide: roles, ports, adapters, tuning.
- **`%Ankusa.Source{}`**. Per catch-URL: verification, sinks, tenant.

### `%Ankusa.Config{}`

Built with `Ankusa.Config.new/1` from a keyword list; unknown keys raise
`ArgumentError` at boot (fail fast on a typo, not at 3am). `:batcher`,
`:dispatch`, `:storage`, `:claim_check`, and `:admin` are maps and get **deep-merged**
over the defaults. Pass only the keys you want to change.

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
  batcher: %{partitions: 2, max_batch: 256, max_delay_ms: 0, max_queue: 10_000},
  dispatch: %{
    poll_ms: 200,
    batch: 128,
    concurrency: 32,
    max_inflight: 4096,
    max_inflight_bytes: 134_217_728,
    retry: {Ankusa.RetryPolicy.Exponential, []}
  },
  storage: %{
    blob_store: {Ankusa.BlobStore.LocalFS, []},
    codec: {Ankusa.Codec.Raw, []},
    roll_bytes: 16 * 1024 * 1024,
    roll_ms: 30_000,
    interval_ms: 1_000
  },
  claim_check: %{
    port: 4001,
    pack_max_bytes: 16_777_216,
    retention_days: nil,
    sweep_interval_ms: 3_600_000
  },
  admin: %{enabled: false, port: 4002},
  routes: %{
    enabled: false,
    max_routes: 10_000,
    store: {Ankusa.Routes.Store.ETS, []},
    cache: %{max_size: 50_000, ttl_ms: 30_000, negative_ttl_ms: 5_000, gc_interval_ms: 60_000},
    trusted_proxies: [],
    ip_rules: %{default: :allow, rules: []},
    admin: %{port: 4003, token: nil},
    log_sample: 100,
    ip_denied_status: 403,
    seed: []
  }
```

| Key | Default | Meaning |
| --- | --- | --- |
| `instance` | `:default` | Registry namespace. See [`architecture.md#instance-model`](architecture.md#instance-model). Two instances with different names run independently in one VM. |
| `data_dir` | `"./data"` | Root for on-disk state; actual paths are `<data_dir>/<instance>/{wal,segments,quarantine,dlq}`. |
| `roles` | `[:edge, :dispatch, :storage]` | Which children boot. `:claim_check` is a fourth, **opt-in** role. See [`claim-check.md`](claim-check.md). `ANKUSA_ROLES=edge,dispatch` (comma-separated) overrides this at runtime in the default Ankusa.Application. See [`deployment.md`](deployment.md). |
| `port` | `4000` | Bandit HTTP port. `PORT` env var overrides in the default Ankusa.Application. |
| `max_body_bytes` | `8_000_000` | Hard cap enforced while streaming the request body; over it is `413` without buffering the whole thing. |
| `route_resolver` | `{Ankusa.RouteResolver.Path, []}` | `{module, opts}` implementing `Ankusa.RouteResolver`: catch-URL scheme. See [`multi-tenancy.md`](multi-tenancy.md). |
| `source_store` | `{Ankusa.SourceStore.Static, sources: %{}}` | `{module, opts}` implementing `Ankusa.SourceStore`. |
| `wal` | `{Ankusa.WAL.DiskLog, []}` | `{module, opts}` implementing `Ankusa.WAL`. See [`storage.md`](storage.md). |
| `batcher.partitions` | `2` | One group-commit `GenServer` per partition. The DiskLog GenServer serializes commits itself, so more partitions only add contention. |
| `batcher.max_batch` | `256` | Flush once this many envelopes have queued. |
| `batcher.max_delay_ms` | `0` | Commit immediately. The WAL append runs in a task, so waiting is a scheduling hop rather than head-of-line blocking. Raise it to trade a little ack latency for larger batches. |
| `batcher.max_queue` | `10_000` | Bound per partition, counting buffered **and** in-flight records; full means `{:error, :overload}` → `503`. |
| `dispatch.poll_ms` | `200` | How often the dispatch pipeline polls the WAL past its cursor. |
| `dispatch.batch` | `128` | Max envelopes read per WAL read. |
| `dispatch.concurrency` | `32` | Max sink deliveries in flight at once. Keep Req's Finch pool (default 50) at least this large for `Sink.Http`. |
| `dispatch.max_inflight` | `4096` | Max admitted-but-unfinished envelopes. Bounds how much a stalled destination can hold. |
| `dispatch.max_inflight_bytes` | `134_217_728` (128 MiB) | ...and the max sum of their body bytes. |
| `dispatch.retry` | `{Ankusa.RetryPolicy.Exponential, []}` | `{module, opts}` implementing `Ankusa.RetryPolicy`: the **default**, overridable per source (see below). |
| `storage.blob_store` | `{Ankusa.BlobStore.LocalFS, []}` | `{module, opts}` implementing `Ankusa.BlobStore`. See [`storage.md`](storage.md). |
| `storage.codec` | `{Ankusa.Codec.Raw, []}` | `{module, opts}` implementing `Ankusa.Codec`: segment record framing. |
| `storage.roll_bytes` | `16 MiB` | Roll a new segment past this size. |
| `storage.roll_ms` | `30_000` | ...or after this long, whichever comes first. |
| `storage.interval_ms` | `1_000` | Compactor tick interval. |
| `claim_check.port` | `4001` | The `:claim_check` role's Bandit port. |
| `claim_check.pack_max_bytes` | `16_777_216` | Target size of one claim pack; a body larger than this still gets a pack of its own. Must be a positive integer. |
| `claim_check.retention_days` | `nil` | LocalFS-only sweeper retention; `nil` disables the sweeper. |
| `claim_check.sweep_interval_ms` | `3_600_000` | Sweeper tick interval. |
| `admin.enabled` | `false` | Start the admin API and `Ankusa.Metrics` on this instance. Off for embedded use; the `jamescarr/ankusa` image turns it on. |
| `admin.port` | `4002` | The admin API's Bandit port. |
| `routes.enabled` | `false` | Enforce route management. Off captures every `POST`, as before; on is **deny by default** — see [Route management](#route-management). |
| `routes.max_routes` | `10_000` | Hard cap on definitions. Creating one past it is a `409`; nothing is ever evicted. |
| `routes.store` | `{Ankusa.Routes.Store.ETS, []}` | `{module, opts}` implementing `Ankusa.Routes.Store`. `Ankusa.Routes.Store.Redis` (package `ankusa_redis`) shares definitions across nodes. |
| `routes.cache.*` | `max_size: 50_000`, `ttl_ms: 30_000`, `negative_ttl_ms: 5_000`, `gc_interval_ms: 60_000` | The per-request decision cache. `ttl_ms` must stay under `gc_interval_ms`. |
| `routes.trusted_proxies` | `[]` | CIDRs whose peers may set `X-Forwarded-For`. Empty means the header is never read. |
| `routes.ip_rules` | `%{default: :allow, rules: []}` | Ordered global rules, first match wins, plus the `default` when none match. |
| `routes.admin.token` | `nil` | Bearer token for the management API. **Required** when `routes.enabled` is true. |
| `routes.admin.port` | `4003` | The management API's own Bandit port. |
| `routes.log_sample` | `100` | 1 in N rejections is logged at `:debug`; `0` disables it. |
| `routes.ip_denied_status` | `403` | Status for an IP denial, or `404` for uniformity with a missing route. |
| `routes.seed` | `[]` | Route definitions loaded at boot (see below). |

#### The admin API

With `admin.enabled: true`, every node serves `GET /health`, `GET /metrics`
(Prometheus text), `GET /v1/config` (the effective config, secrets redacted),
`GET /v1/dlq` and `POST /v1/dlq/replay` (`:dispatch` role), and
`GET /v1/quarantine` (`:edge` role) on `admin.port`, independent of the node's
roles. It is **unauthenticated by design**: put it behind your own proxy, SSO,
or network policy. The HTTP contract is
[`priv/openapi/admin.v1.yaml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa/priv/openapi/admin.v1.yaml).

#### Route management

**Off by default, and off means "capture everything".** With `routes.enabled:
true` the edge becomes deny-by-default: a `POST` is captured only if it passes
the IP rules **and** its method and normalized path match an enabled route.
Anything else is answered `404` (`403` for an IP denial, unless
`ip_denied_status: 404`) and never reaches the WAL — no record, no dispatch, no
delivery.

Path patterns, no regex:

| Pattern | Matches |
| --- | --- |
| `/hooks/stripe` | exactly that path |
| `/hooks/:tenant/github` | one segment in the middle, any value |
| `/hooks/shopify/*` | one **or more** remaining segments (`/hooks/shopify` itself does not match) |

Requests are normalized before matching, and hardening is part of the contract:
a percent-encoded slash (`%2F`) and a `.`/`..` segment are rejections, not
segment boundaries or paths to resolve.

**IP rules.** `routes.ip_rules` is an ordered list (`%{action: :allow | :deny,
cidr: "10.0.0.0/8"}`) with a `default` for the unmatched case; the first match
wins. A route may declare its own `ip_rules`, which *replace* the global list for
that route — that is how one provider is pinned to its published ranges while a
global ban list still applies everywhere else. A global `deny` always wins.

Client addresses come from the socket peer. `X-Forwarded-For` is read **only**
when the peer is inside `routes.trusted_proxies`; from anyone else the header is
ignored, and one unparseable entry discards the whole header. IPv4-mapped IPv6
addresses (`::ffff:1.2.3.4`) are matched as IPv4.

**Definitions.** `ankusa`'s default store keeps them in this node's memory, which
is enough for a single node — pair it with `routes.seed`, which loads at boot
(and only at boot: a route deleted through the API is not resurrected by a
restart). To share definitions across edge nodes, use the `ankusa_redis` package:

```yaml
routes:
  enabled: true
  admin: {token: "${ANKUSA_ROUTES_ADMIN_TOKEN}"}
  store: {type: redis, url: redis://cache:6379, namespace: ankusa:routes}
```

Every node with the same `namespace` enforces the same routes: writes bump a
version counter and publish it, each node reloads on the broadcast, and a
periodic tick (`tick_ms`, default 30s) is the safety net for a missed one. A
node keeps serving its in-memory snapshot through a Redis outage; only writes
report `503 store_unavailable`.

**Management API**, on `routes.admin.port` (its own listener, never the ingest
port), authenticated with `authorization: Bearer
<routes.admin.token>` on every route:

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/admin/routes?enabled=&limit=&cursor=` | list, id-ordered, cursor-paginated |
| `POST` / `PUT` / `PATCH` / `DELETE` | `/admin/routes[/:id]` | create, replace, update, delete |
| `GET` / `PUT` | `/admin/ip-rules` | the global rules and default |
| `POST` | `/admin/routes/test` | dry run: `{method, path, ip}` → decision, reason, route id, and the rule that decided it |
| `GET` | `/health` | `{status, routes}` |

A route is `{id, path, methods: [POST], enabled, ip_rules, metadata,
inserted_at, updated_at}`. Ids are lowercase slugs, `path`/`id` are immutable
under `PATCH` (moving a route changes what it captures — that is a `PUT`), and
two enabled routes may not share a path and method. `POST /admin/routes/test`
answers "why was this rejected" without capturing anything, so a route change
can be checked before it goes live.

> **Rejections are not retried by Ankusa.** Some providers retry any `4xx`, some
> give up, and some disable an endpoint after enough failures — the provider's
> own policy decides, and `403`/`404` are the only signals it gets. Dry-run the
> change first.

Rejections are logged at `:debug`, sampled at `routes.log_sample` (1 in N, `0`
for silent), and emit `[:ankusa, :routes, :reject]`; captures emit
`[:ankusa, :routes, :match]` with `%{instance, route_id, cached}`.

### Configuring a source

Sources are what `Ankusa.SourceStore.Static` (the default store) returns for a
given `source_id`; every field has a default, so `%{"demo" => []}` is valid
(everything default: `Verifier.None`, `:reject`,
`Sink.Log`, tenant `"default"`).

```elixir
config :ankusa,
  sources: %{
    "stripe" => [
      tenant_id: "acme",                                       # default: "default"
      verifier: {Ankusa.Verifier.Hmac, scheme: :stripe, secret: System.get_env("STRIPE_WHSEC")},
      on_verify_failure: :quarantine,                           # :reject | :quarantine | :accept_flag
      sinks: [{Ankusa.Sink.Http, url: "https://example.internal/stripe"}]
    ]
  }
```

| Field | Default | Meaning |
| --- | --- | --- |
| `tenant_id` | `"default"` | The storage/retention scope. See [`multi-tenancy.md`](multi-tenancy.md). |
| `verifier` | `{Ankusa.Verifier.None, []}` | `{module, opts}` implementing `Ankusa.Verifier`. |
| `on_verify_failure` | `:reject` | `:reject` (`401`, nothing stored) / `:quarantine` (`202`, durable pen) / `:accept_flag` (commits, envelope flagged). |
| `sinks` | `[{Ankusa.Sink.Log, []}]` | `[{module, opts}]` implementing `Ankusa.Sink`, delivered to in order, independently retried. |

A source can override the dispatch-wide retry policy by putting a
`:retry` opt directly in a sink tuple's opts if that sink's module reads it
(none of the shipped sinks do. `Sink.Http`/`Sink.RabbitMQ`/`Sink.Kafka`/`Sink.NATS` retries are all
driven by `config.dispatch.retry`, applied uniformly per source by
`Ankusa.Dispatch.Pipeline`). Per-source retry policy override is not currently
supported; it's dispatch-wide.

### Every behaviour, at a glance

Full detail (options, guarantees, how to write your own) lives in
[`storage.md`](storage.md) and [`delivery.md`](delivery.md); this table is
the map.

| Behaviour | Job | Default | Also shipped |
| --- | --- | --- | --- |
| `Ankusa.RouteResolver` | Catch-URL scheme → `%Route{tenant_id, source_id}` | `RouteResolver.Path` (`/webhooks/:source_id`) | `RouteResolver.TenantPath` (`/webhooks/:tenant/:source`) |
| `Ankusa.WAL` | Durable ack, ordered log, truncation | `WAL.DiskLog` (fsync group commit) | none |
| `Ankusa.Verifier` | Signature/timestamp checks | `Verifier.None` | `Verifier.Hmac` (configurable HMAC engine; named schemes Stripe, GitHub, Standard Webhooks, Shopify, Slack) |
| `Ankusa.SourceStore` | Source config, secrets, policy | `SourceStore.Static` | none |
| `Ankusa.Sink` | What happens to a delivered hook | `Sink.Log` | `Sink.Http` (Req forward), `Sink.RabbitMQ` (exchange publish, `ankusa_rabbitmq` package), `Sink.Kafka` (topic produce, `ankusa_kafka` package), `Sink.NATS` (JetStream subject publish, `ankusa_nats` package) |
| `Ankusa.RetryPolicy` | Backoff / give-up | `RetryPolicy.Exponential` (jitter) | none |
| `Ankusa.BlobStore` | Segment PUT / range GET / delete | `BlobStore.LocalFS` | `BlobStore.S3` (+R2/MinIO), `BlobStore.GCS` |
| `Ankusa.ClaimCheck` | Pack claims into the object store, redeem by reference | none (the instance's `BlobStore`) | none |
| `Ankusa.Codec` | Segment record framing | `Codec.Raw` (len-prefixed, CRC32) | none |

Swapping any of these is a one-line config change, `route_resolver:
{Ankusa.RouteResolver.TenantPath, []}`, because every layer is a behaviour with
`{module, opts}` config, resolved at the call site, never hardcoded.

### Runtime environment overrides

Ankusa.Application (the default OTP application boot path) reads two env
vars on top of whatever `config.exs` sets:

- `PORT`: overrides `config.port`.
- `ANKUSA_ROLES`: comma-separated, overrides `config.roles` (e.g.
  `ANKUSA_ROLES=edge,dispatch`). Parsed with `Ankusa.Config.parse_roles!/1`:
  an unknown role name (anything other than `edge`, `dispatch`, `storage`,
  `claim_check`) raises `ArgumentError` and fails boot rather than silently
  starting with the wrong roles.

`autostart` (application env, default `false`) gates whether
Ankusa.Application boots its built-in default instance at all. A library
must not bind a port just because it's a dependency. Set `config :ankusa,
autostart: true` in a deployment that wants the zero-config default instance
(core's own `packages/ankusa/config/config.exs` does this outside `:test`).

This is deliberately the *only* place env vars are read inside `ankusa` core.
Everything else is `%Ankusa.Config{}` passed explicitly. A deployment wrapper
(like [`examples/rabbitmq-consumer/ingest_app`](https://github.com/jamescarr/ankusa/tree/main/examples/rabbitmq-consumer/ingest_app))
is free to read as many env vars as it wants and build the config struct
itself; that's the intended extension point, not a gap.
