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
#      wal=disk storage=Ankusa.BlobStore.LocalFS

docker run --rm -v "$PWD/ankusa.yml:/etc/ankusa/ankusa.yml:ro" \
  -e STRIPE_WHSEC jamescarr/ankusa:edge print-config
# => the effective config as JSON, with every secret redacted
```

`version` prints the server and core versions.

A minimal file, one open source and one HTTP sink. The worker here is plain
`http://` on a private network, which sinks may not reach unless the `egress`
block says so; a production sink is an `https://` URL on a public name and needs
no `egress` block (see [Egress](#egress)):

```yaml
egress:
  allow_insecure_http: true
  allow_hosts: [worker]

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
| `node` | `roles` (`[edge, dispatch, storage]`; under `wal.type: none` the queue's readers — `dispatch`, `storage` — are dropped, so an all-role node becomes `[edge]`), `data_dir` (`/var/lib/ankusa`) |
| `log` | `level` (`info`), `banner` (`true`; the ASCII-art banner on start) |
| `http` | `port` (4000), `max_body_bytes` (8000000), `routing` (`path` \| `tenant_path`), `prefix` (`/webhooks`), `store_headers` (`drop`: `authorization`, `proxy-authorization`, `cookie`, `set-cookie`, `x-api-key`, `x-auth-token`; `hash`: none). See [Stored headers](#stored-headers) |
| `listener` | Limits for every HTTP listener, and the ingest port's TLS: `max_connections` (20000), `num_acceptors` (null = two per scheduler), `read_timeout_ms` (10000), `body_read_timeout_ms` (15000), `request_deadline_ms` (30000), `max_requests_per_connection` (1000), `max_header_count` (64), `max_header_length` (16384), `http2` (`false`), `tls` (`certfile`, `keyfile`, `cacertfile`, `verify` = `verify_none` \| `verify_peer`, `versions`, `ciphers`). See [Listeners](#listeners-limits-tls-and-mtls) |
| `admin` | `enabled` (`true` in the image, `false` in core), `port` (4002), `ip` (`127.0.0.1`; `0.0.0.0` exposes it, and then needs `auth`), `gauge_interval_ms` (15000; how often the `/metrics` gauges are sampled), `tls` (a TLS block, like `listener.tls`), `auth` (who may call it: `mode` = `none` \| `token` \| `jwt` \| `mtls`, `allow_insecure`, `tokens`, `jwt`, `mtls`; see [Control-plane authentication](#control-plane-authentication)) |
| `metrics` | `enabled` (`false`), `port` (4004), `ip` (`127.0.0.1`), `tls`. A listener that serves only `/metrics`, `/health` and `/ready`. See [A separate metrics listener](#a-separate-metrics-listener) |
| `batcher` | `partitions` (2), `max_batch` (256), `max_delay_ms` (0), `max_queue` (10000), `max_queue_bytes` (268435456) |
| `dispatch` | `batch` (128), `concurrency` (32), `max_inflight` (4096), `max_inflight_bytes` (134217728), `attempt_timeout_ms` (30000), `sink_concurrency` (null = only `concurrency`), `breaker_failures` (5; 0 disables breakers), `breaker_open_ms` (30000), `breaker_max_open_ms` (300000), `retry.base_ms` (100), `retry.max_ms` (300000), `retry.max_attempts` (84), `retry.jitter` (`true`). See [`delivery.md`](delivery.md#a-slow-or-dead-sink) |
| `wal` | `type` (`disk` \| `none`; the queue's mode — the name is historical), `publish_timeout_ms` (`8000`; `wal.type: none` only: the overall deadline every sink must confirm under — keep it below the provider's own timeout) |
| `storage` | `type` (`local` \| `s3` \| `gcs`), `roll_bytes` (16777216), `roll_ms` (30000), `key_prefix` (`""`; `name/` parts prepended to every segment key, so nodes can share a bucket), `s3.*` (`bucket`, `region`, `endpoint`, `access_key_id`, `secret_access_key`, `session_token`), `gcs.*` (`bucket`, `endpoint`, `auth` = `metadata` \| `token` \| `none`; `token` is required when `auth: token`) |
| `storage.s3.sse`, `storage.gcs.sse` | `type` (`aes256` \| `kms`), `key_id` (the KMS key; required for `kms` on GCS, optional on S3). Server-side encryption sent on every object this store writes; the same `sse:` key is accepted by the `s3`/`gcs` block of `claim_check.store(s)` and `backup.store`. See [Encryption at rest](storage.md#encryption-at-rest) |
| `claim_check` | `port` (4001), `ip` (`127.0.0.1`), `tls` (a TLS block), `auth` (like `admin.auth`; reading a claim needs `claims:read` and the claim's tenant), `pack_max_bytes` (16777216), `retention_days` (null disables the sweeper; else ≥ 1), `store` (unset = the `storage` bucket; same shape as `storage` plus `root` for `type: local`), `stores` (a list of `store`-shaped entries, exclusive with `store`: failover between them), `placement` (`ordered` \| `hash`), `store_cooldown_ms` (30000), `key_layout` (`flat` \| `sharded`), and the gateway's reads: `read_deadline_ms` (5000), `read_timeout_ms` (3000), `read_retries` (1), `read_max_inflight` (48), `read_queue_max` (256), `read_cache_bytes` (134217728; 0 = no cache), `read_cache_max_entry_bytes` (1048576). See [`claim-check.md`](claim-check.md#a-dedicated-claim-store) and [Under load](claim-check.md#under-load) |
| `backup` | `enabled` (`false`), `interval_ms` (60000), `keep` (3), `store` (unset = the `storage` bucket; same shape as `claim_check.store`). Continuous backup of the node's store, restored at boot into an empty `data_dir`; keys go under `storage.key_prefix`. See [`storage.md`](storage.md#backup-and-restore) |
| `routes` | `enabled` (`false`), `max_routes` (10000), `store.type` (`ets` \| `redis`; `store.url`/`store.namespace`/`store.tick_ms` are Redis-only), `cache.*`, `trusted_proxies` (`[]`), `ip_rules.*`, `admin.port` (4003), `admin.ip` (`127.0.0.1`), `admin.tls` (a TLS block), `admin.auth` (like `admin.auth` of the operator API), `log_sample` (100), `ip_denied_status` (403), `seed` (`[]`; each entry takes `id`, `path`, `methods`, `enabled`, `ip_rules`, `metadata`, and any other key is a load error). See [Route management](#route-management) |
| `sources` | One entry per catch-URL source. See below |
| `source_store` | `type` (`static` \| `persistent` \| `redis`; `persistent` adds API-managed sources kept in this node's store, `redis` keeps them in Redis, shared by every node with the same namespace; a `url` with no `type` means `redis`), `url`, `namespace` (`ankusa:sources:<instance>`), `tick_ms` (30000) — the last three only with `type: redis`. See [`multi-tenancy.md#dynamic-sources`](multi-tenancy.md#dynamic-sources) |
| `secrets` | `kek` (a 32-byte key, base64, optionally `<id>:<base64>`; use `${VAR}`), `kek_previous` (a list, or one comma-separated string, of keys that may still decrypt). With a `kek`, the secrets of API-managed sources are encrypted before they are stored (`source_store.type: persistent` or `redis`); unset, they are stored as plain JSON. See [Encryption at rest](storage.md#encryption-at-rest) |
| `rate_limits` | `default` (`null` = unlimited), `tenants` (`{tenant_id: {rate, burst}}`). Per node, charged after verification. See [Rate limits](#rate-limits) |
| `quarantine` | `burst` (100), `rate` (20 per second), `max_bytes` (1 GiB). The pen for hooks whose source has `on_verify_failure: quarantine`: one token bucket per source (over it, `429 quarantine_rate_limited`), and a byte cap (a full pen answers `503 quarantine_full`; it never evicts). Per node. See [`delivery.md#quarantine`](delivery.md#quarantine) |
| `lifecycle` | `sinks` (required and non-empty when the section is present; omit the section to turn events off). Sinks, shaped like a source's, that receive a CloudEvent when a source or route is created, updated, or deleted. See [AsyncAPI and lifecycle events](asyncapi.md) |
| `egress` | `allow_insecure_http` (`false`: `http` sink URLs fail the load), `deny_cidrs` (loopback, RFC 1918, carrier-grade NAT, link-local, unique-local IPv6 and the unspecified addresses; setting it replaces the list), `allow_cidrs` (`[]`), `allow_hosts` (`[]`), `deny_hosts` (`[metadata.google.internal]`). Where `type: http` sinks may connect, checked on every attempt. See [Egress](#egress) |

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
| `verify.type` | `none` \| `stripe` \| `github` \| `standard_webhooks` \| `shopify` \| `slack` \| `hmac`. Every type except `none` requires `secret`: a non-empty string or a list of non-empty strings (at most 8), newest first — every key is tried, so a rotation is `secret: ["${NEW}", "${OLD}"]`. An empty key would verify whatever anyone signs with it, so an empty value (an unset `${VAR:-}` included) is a load error. `stripe`, `standard_webhooks`, and `slack` also take `tolerance_seconds` (default 300). `hmac` takes the descriptor keys below. |
| `on_verify_failure` | `reject` \| `quarantine` \| `accept_flag`: what happens when verification fails. |
| `trust_url_tenant` | `true` \| `false` (default `false`). With `routing: tenant_path`, a source with `tenant: default` and `verify: {type: none}` files hooks under whatever tenant the URL names, unverified: boot, `check-config` and an admin API write refuse it unless it sets `trust_url_tenant: true`. See [`multi-tenancy.md`](multi-tenancy.md#tenant-scoping-what-tenant_id-actually-does). |
| `sinks` | At least one; every sink is tried on every delivered hook. |
| `dedupe` | Collapse provider retries at ingest. A preset name (`github`, `standard_webhooks`, `svix`, `shopify`, `stripe`), or a mapping with exactly one of `preset` (same names), `header` (the event-id header, e.g. `X-GitHub-Delivery`) or `json` (a dot path into the body, e.g. `data.id`), plus an optional `ttl_seconds` (default 259 200 = 72 h). Hooks that share a key within the TTL get the same `201` and the original id, with `"duplicate": true` on the later ones. Keys are scoped to the tenant and the source, so two tenants behind one `tenant_path` source never collapse each other's events. The key travels into every delivery as `dedupe_key`. |
| `forward_headers` | Which provider request headers travel into sink messages (and `Sink.Http` requests): a list of names (lowercased; still minus the never-forwarded set below), or absent for the default set: every header except `authorization`, `proxy-authorization`, `cookie`, `x-api-key`, `host`, `content-length`, `content-type`, `connection`, `keep-alive`, `transfer-encoding`, `te`, `trailer`, `upgrade`, `expect`, and every `x-ankusa-*` header. (`:default` is the Elixir value only; YAML takes a list or nothing.) |
| `store_headers` | `drop` / `hash` lists of header names that override `http.store_headers` for this source, key by key (an absent list inherits). See [Stored headers](#stored-headers). |

#### Stored headers

A hook is stored with the request's headers, and the store, the archive
segments, the quarantine pen and every sink message all carry what was stored.
`store_headers` decides which headers survive, after the signature is verified
(the verifier and the source's `dedupe` read the complete list):

- `drop`: removed from the hook. Default: `authorization`, `proxy-authorization`,
  `cookie`, `set-cookie`, `x-api-key`, `x-auth-token`.
- `hash`: kept under the same name with the value replaced by `sha256:<hex>`,
  so a consumer can tell two requests carried the same value without being able
  to read it. Default: none.

Names are case-insensitive; a name in both lists is dropped. The count of
removed headers is `dropped_headers` on the `[:ankusa, :ingest]` telemetry
span. A source's `store_headers` overrides the `http` lists key by key, so a
source that sets only `hash` keeps the instance's `drop`. Do not drop a header a
source's verifier needs: a quarantine replay verifies the stored hook again.
Changing the policy affects hooks accepted afterwards; stored hooks are not
rewritten.

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
| `http` | `url` (`https://` unless `egress.allow_insecure_http`), `method` (`post` \| `put` \| `patch`), `headers`, `timeout_ms` (5000), `secret` (a `whsec_` secret, or a list while rotating: signs every delivery the Standard Webhooks way), `max_response_bytes` (65536; only the status is used), `cacertfile` (a PEM CA bundle to verify the receiver's certificate against, for a private CA; the file must exist at load), `allow_insecure_tls` (`false`; `true` skips certificate verification for this sink). Deliveries are not ordered; a consumer that needs order has to rebuild it from data it receives and tolerate redelivery. The receiver contract is in [`integrations.md#http-handoff-any-language`](integrations.md#http-handoff-any-language). |
| `rabbitmq` | `url`, `exchange`, `exchange_type` (`topic` \| `direct` \| `fanout` \| `headers`), `routing_key`, `inline_max_bytes` (65536), `max_inflight` (256 unconfirmed publishes per connection; past it a publish fails fast and is retried), `idle_timeout_ms` (600000; the connection closes after this long with no delivery, `0` never; at least 16000). |
| `kafka` | `brokers` (a list, or one comma-separated string), `topic`, `key` (a static string), `inline_max_bytes` (65536), `max_record_bytes` (1000000; a larger record is dead-lettered at once), `ssl`, `sasl` (`mechanism` = `plain` \| `scram_sha_256` \| `scram_sha_512`, `username`, `password`), `idle_timeout_ms` (600000; the client closes after this long with no delivery, `0` never; at least the produce timeout plus 1000). |
| `nats` | `servers` (a list, or one comma-separated string; failover, one server at a time, picked at random per connect), `subject`, `inline_max_bytes` (65536), `publish_timeout_ms` (5000), `tls`, `auth` (one scheme: `username` + `password`, `token`, or `nkey_seed` + `jwt`), `idle_timeout_ms` (600000; the connection closes after this long with no delivery, `0` never; at least `publish_timeout_ms` plus 1000). The stream must already exist. See [`delivery.md`](delivery.md#sinknats-subject-delivery). |
| `redis` | `url` (credentials and db go in it: `redis://:password@host:6379/0`, `rediss://` for TLS), `channel` (a static string), `inline_max_bytes` (65536), `publish_timeout_ms` (5000), `idle_timeout_ms` (600000; the connection closes after this long with no delivery, `0` never; at least `publish_timeout_ms` plus 1000). Pub/sub keeps no copy, so a publish nobody is subscribed to is an error and the sink is never durable. See [`delivery.md`](delivery.md#sinkredis-pubsub-delivery). |
| `sqs` | `queue_url`, `region`, `endpoint` (default: the queue URL's origin; set it for a VPC endpoint or an emulator), `message_group_id` (a static string), `inline_max_bytes` (65536), `max_message_bytes` (1048576, SQS's limit; body plus message attributes, a larger message is dead-lettered at once), `timeout_ms` (5000), `access_key_id` / `secret_access_key` / `session_token` (omit for the `AWS_*` env, IRSA, or the EC2 instance role). FIFO when `queue_url` ends `.fifo`: the group defaults to `tenant/source` and the deduplication id is the hook id (SQS dedupes for 5 minutes). The queue must already exist. See [`delivery.md`](delivery.md#sinksqs-queue-delivery). |
| `google_pubsub` | `project`, `topic` (the id, not `projects/…/topics/…`), `endpoint` (default `https://pubsub.googleapis.com`; set it for a regional endpoint or an emulator), `ordering_key` (a static string; default none), `inline_max_bytes` (65536), `max_message_bytes` (10000000, Pub/Sub's limit; data plus attributes plus ordering key, a larger message is dead-lettered at once), `timeout_ms` (5000), `auth` = `metadata` \| `token` \| `none` (default `metadata`: the GCE/GKE metadata server; `token` is required when `auth: token`; `none` sends no credential, for an emulator). The topic must already exist, and a topic with no subscription drops the message. See [`delivery.md`](delivery.md#sinkgooglepubsub-google-cloud-pubsub-delivery). |

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
| `ANKUSA_BANNER` | `log.banner` |
| `ANKUSA_HTTP_PORT`, else `PORT` | `http.port` |
| `ANKUSA_ADMIN_PORT` | `admin.port` |
| `ANKUSA_CLAIM_CHECK_PORT` | `claim_check.port` |
| `ANKUSA_ADMIN_IP` | `admin.ip` |
| `ANKUSA_CLAIM_CHECK_IP` | `claim_check.ip` |
| `ANKUSA_ADMIN_TOKEN` | `admin.auth`: one bearer token holding every scope and `tenant:*`; replaces the file's `admin.auth` block. Only its SHA-256 is kept. See [Control-plane authentication](#control-plane-authentication) |
| `ANKUSA_ROUTES_ADMIN_TOKEN` | `routes.admin.auth`: one token holding `routes:read`, `routes:write` and `tenant:*` |
| `ANKUSA_CLAIM_CHECK_TOKEN` | `claim_check.auth`: one token holding `claims:read` and `tenant:*` |
| `ANKUSA_ROUTES_ENABLED` | `routes.enabled` |
| `ANKUSA_ROUTES_STORE_URL` | `routes.store.url` |
| `ANKUSA_SOURCE_STORE_URL` | `source_store.url` |
| `ANKUSA_WAL_TYPE` | `wal.type` (`disk`, `none`; the queue's mode) |
| `ANKUSA_STORAGE_TYPE` | `storage.type` (`local`, `s3`, `gcs`) |
| `ANKUSA_S3_BUCKET`, `ANKUSA_S3_REGION`, `ANKUSA_S3_ENDPOINT` | `storage.s3.bucket/region/endpoint` |
| `ANKUSA_GCS_BUCKET` | `storage.gcs.bucket` |
| `ANKUSA_STORAGE_KEY_PREFIX` | `storage.key_prefix` |
| `ANKUSA_BACKUP_ENABLED` | `backup.enabled` |
| `ANKUSA_METRICS_ENABLED`, `ANKUSA_METRICS_PORT`, `ANKUSA_METRICS_IP` | `metrics.enabled/port/ip` |

When the config names no static keys, the S3 adapter looks for credentials in
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (+ `AWS_SESSION_TOKEN`), then web
identity (`AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`, as IRSA sets them),
then the EC2 instance profile (IMDSv2), caching each until shortly before it
expires.

A `${VAR}` the loader cannot resolve (an unset variable without a `:-`
default, or a `${` that is not a variable reference) is a load error naming the
key, never a literal string.

Sources and sinks are not env-overridable: they carry behaviour, so they live in
the file, with secrets injected through `${VAR}`.

### Starting points

All loadable as-is. Copy one, delete what you don't use, replace the `${VAR}`s:

| File | What it is |
| --- | --- |
| [`config-examples/reference.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/config-examples/reference.yml) | every key, at its default, with the alternatives |
| [`config-examples/single-node.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/config-examples/single-node.yml) | one box: a disk queue, Stripe + GitHub, HTTP sink |
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
`:dispatch`, `:storage`, `:claim_check`, `:backup`, `:admin`, `:rate_limits`,
`:quarantine`, `:lifecycle` and `:egress` are maps and get **deep-merged** over the
defaults. Pass only the keys you want to change.

The example lists the keys `Ankusa.Config.new/1` accepts, at their defaults. As `config :ankusa` app env, the autostarted default instance reads only `:autostart`, `:port`, `:data_dir`, `:roles` and `:sources`; pass the rest to `Ankusa.Config.new/1` or an `Ankusa.Instance` child spec.

```elixir
config :ankusa,
  instance: :default,
  port: 4000,
  data_dir: "./data",
  roles: [:edge, :dispatch, :storage],
  max_body_bytes: 8_000_000,
  store_headers: %{drop: ~w(authorization proxy-authorization cookie set-cookie x-api-key x-auth-token), hash: []},
  route_resolver: {Ankusa.RouteResolver.Path, []},
  source_store: {Ankusa.SourceStore.Static, sources: %{}},
  wal: :disk,
  direct_publish_timeout_ms: 8_000,
  batcher: %{
    partitions: 2,
    max_batch: 256,
    max_delay_ms: 0,
    max_queue: 10_000,
    max_queue_bytes: 268_435_456
  },
  dispatch: %{
    batch: 128,
    concurrency: 32,
    max_inflight: 4096,
    max_inflight_bytes: 134_217_728,
    attempt_timeout_ms: 30_000,
    sink_concurrency: nil,
    breaker_failures: 5,
    breaker_open_ms: 30_000,
    breaker_max_open_ms: 300_000,
    retry: {Ankusa.RetryPolicy.Exponential, []}
  },
  storage: %{
    blob_store: {Ankusa.BlobStore.LocalFS, []},
    key_prefix: "",
    codec: {Ankusa.Codec.Raw, []},
    roll_bytes: 16 * 1024 * 1024,
    roll_ms: 30_000,
    interval_ms: 1_000
  },
  claim_check: %{
    port: 4001,
    ip: "127.0.0.1",
    pack_max_bytes: 16_777_216,
    retention_days: nil,
    sweep_interval_ms: 3_600_000,
    blob_stores: nil,
    placement: :ordered,
    store_cooldown_ms: 30_000,
    key_layout: :flat,
    read_deadline_ms: 5_000,
    read_timeout_ms: 3_000,
    read_retries: 1,
    read_max_inflight: 48,
    read_queue_max: 256,
    read_cache_bytes: 128 * 1024 * 1024,
    read_cache_max_entry_bytes: 1024 * 1024
  },
  backup: %{enabled: false, interval_ms: 60_000, keep: 3, blob_store: nil},
  admin: %{enabled: false, port: 4002, ip: "127.0.0.1", gauge_interval_ms: 15_000},
  routes: %{
    enabled: false,
    max_routes: 10_000,
    store: {Ankusa.Routes.Store.ETS, []},
    cache: %{max_size: 50_000, ttl_ms: 30_000, negative_ttl_ms: 5_000, gc_interval_ms: 60_000},
    trusted_proxies: [],
    ip_rules: %{default: :allow, rules: []},
    admin: %{port: 4003, ip: "127.0.0.1"},
    log_sample: 100,
    ip_denied_status: 403,
    seed: []
  },
  rate_limits: %{default: nil, tenants: %{}},
  quarantine: %{burst: 100, rate: 20, max_bytes: 1_073_741_824},
  lifecycle: %{sinks: []},
  egress: %{
    allow_insecure_http: false,
    deny_cidrs: ~w(0.0.0.0/8 127.0.0.0/8 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16
                   100.64.0.0/10 169.254.0.0/16 ::/128 ::1/128 fc00::/7 fe80::/10),
    allow_cidrs: [],
    allow_hosts: [],
    deny_hosts: ["metadata.google.internal"]
  }
```

| Key | Default | Meaning |
| --- | --- | --- |
| `instance` | `:default` | Registry namespace. See [`architecture.md#instance-model`](architecture.md#instance-model). Two instances with different names run independently in one VM. |
| `data_dir` | `"./data"` | Root for on-disk state; actual paths are `<data_dir>/<instance>/{store,segments}`: the node's RocksDB store and the LocalFS blob root. |
| `roles` | `[:edge, :dispatch, :storage]` | Which children boot. `:claim_check` is a fourth, **opt-in** role. See [`claim-check.md`](claim-check.md). `ANKUSA_ROLES=edge,dispatch` (comma-separated) overrides this at runtime in the default Ankusa.Application. The roles divide the hook's life: `:edge` accepts and commits it, `:dispatch` delivers it, `:storage` archives it, and the hook is deleted once its last obligation clears — so `edge,dispatch` and `edge,dispatch,storage` both reclaim delivered hooks, and an archive that is off or behind never holds up delivery. All the roles share one node-local store, so they run in one BEAM (see [`deployment.md`](deployment.md)). With `wal: :none` the roles that only read the queue (`:dispatch`, `:storage`) are dropped from this list rather than rejected, so an existing all-role deployment can flip `wal.type` with no other change; `GET /health` on the admin port reports the effective roles. |
| `port` | `4000` | Bandit HTTP port. `PORT` env var overrides in the default Ankusa.Application. |
| `listener.max_connections`, `listener.num_acceptors` | `20_000`, `nil` (two per scheduler) | Open connections per listener, spread over the acceptors; past it new connections are closed. Positive integers. |
| `listener.read_timeout_ms` | `10_000` | A connection that sends nothing for this long is closed (slowloris, idle keep-alive). |
| `listener.body_read_timeout_ms`, `listener.request_deadline_ms` | `15_000`, `30_000` | One read of the request body, and the whole body, must complete within these; the ingest port answers `408` past either. |
| `listener.max_requests_per_connection` | `1_000` | A keep-alive connection is closed after this many requests. |
| `listener.max_header_count`, `listener.max_header_length` | `64`, `16_384` | Request-head limits, enforced before any body is read. |
| `listener.http2` | `false` | Serve HTTP/2 (off: HTTP/1.1 only). |
| `listener.tls` | `nil` | The ingest listener's TLS block (`certfile`, `keyfile`, `cacertfile`, `verify`, `versions`, `ciphers`). The `listener` limits apply to every listener; this key is the ingest port's alone. See [Listeners](#listeners-limits-tls-and-mtls). |
| `max_body_bytes` | `8_000_000` | Hard cap on a request body. A `Content-Length` over it is `413` before any of the body is read; a body without one (chunked) is cut off at the cap while streaming. |
| `store_headers` | `%{drop: [...the six credential headers above], hash: []}` | Which request headers a hook is stored with (`Ankusa.Edge.StoredHeaders`), applied after verification. `drop` removes a header from the stored hook, `hash` keeps it with the value replaced by `"sha256:<hex>"`; names are case-insensitive and a name in both lists is dropped. An absent `drop` or `hash` keeps the default's list. See [Stored headers](#stored-headers). |
| `route_resolver` | `{Ankusa.RouteResolver.Path, []}` | `{module, opts}` implementing `Ankusa.RouteResolver`: catch-URL scheme. See [`multi-tenancy.md`](multi-tenancy.md). |
| `source_store` | `{Ankusa.SourceStore.Static, sources: %{}}` | `{module, opts}` implementing `Ankusa.SourceStore`. `SourceStore.Persistent` adds API-managed, tenant-scoped sources persisted in this node's store (the image's `source_store.type: persistent`); `Ankusa.SourceStore.Redis` (`ankusa_redis`, the image's `source_store.type: redis`) keeps them in Redis, shared across nodes; the rest is read-only. |
| `wal` | `:disk` | The queue's mode; the name is historical. `:disk` commits every hook into this node's RocksDB store before the ack (`Ankusa.Queue`). `:none` commits nothing: ingest acks on the sink's confirm, and every statically configured source needs at least one sink whose `:ok` means durable (`c:Ankusa.Sink.durable?/1`), so boot refuses a config that cannot make that promise. Under `:none` the queue's reader roles (`:dispatch`, `:storage`) are dropped from `roles`. See [`delivery.md`](delivery.md#direct-mode). |
| `direct_publish_timeout_ms` | `8_000` | `wal: :none` only: the one deadline every sink must confirm under (YAML `wal.publish_timeout_ms`). Keep it below the provider's own timeout. |
| `batcher.partitions` | `2` | One group-commit `GenServer` per partition. The queue writer serializes commits itself, so more partitions only add contention. |
| `batcher.max_batch` | `256` | Flush once this many envelopes have queued. |
| `batcher.max_delay_ms` | `0` | Commit immediately. The store commit runs in a task, so waiting is a scheduling hop rather than head-of-line blocking. Raise it to trade a little ack latency for larger batches. |
| `batcher.max_queue` | `10_000` | Bound per partition, counting buffered **and** in-flight records; full means `{:error, :overload}` → `503`. |
| `batcher.max_queue_bytes` | `268_435_456` (256 MiB) | The same bound in body bytes, buffered and in flight, so a burst of large hooks sheds load (`503`) before it exhausts memory. |
| `dispatch.batch` | `128` | Delivery rows claimed per store scan. |
| `dispatch.concurrency` | `32` | Max sink deliveries in flight at once. Keep Req's Finch pool (default 50) at least this large for `Sink.Http`. |
| `dispatch.max_inflight` | `4096` | Max claimed, unfinished deliveries. Bounds how much a stalled destination can hold. |
| `dispatch.max_inflight_bytes` | `134_217_728` (128 MiB) | ...and the max sum of their stored hook sizes. |
| `dispatch.attempt_timeout_ms` | `30_000` | A delivery attempt that has not returned after this is killed and counts as a failed attempt (`{:attempt_timeout, ms}`). Keep it above every sink's own timeout. |
| `dispatch.sink_concurrency` | `nil` | Each sink of each source (`{source_id, sink index, module}`) has its own queue; at most this many of its attempts run at once. `nil` leaves only the global `concurrency` cap. |
| `dispatch.breaker_failures` | `5` | Consecutive failures that open a sink's breaker; `0` never opens one. An open sink's rows wait without spending attempts. |
| `dispatch.breaker_open_ms`, `dispatch.breaker_max_open_ms` | `30_000`, `300_000` | How long a breaker stays open, doubling per consecutive open up to the max; one probe attempt then closes it or reopens it. In memory: a restart closes every breaker. |
| `dispatch.retry` | `{Ankusa.RetryPolicy.Exponential, []}` | `{module, opts}` implementing `Ankusa.RetryPolicy`: dispatch-wide; there is no per-source retry policy. |
| `storage.blob_store` | `{Ankusa.BlobStore.LocalFS, []}` | `{module, opts}` implementing `Ankusa.BlobStore`. See [`storage.md`](storage.md). |
| `storage.key_prefix` | `""` | Prepended to the key of every new segment: `""`, or one or more `name/` parts (`[A-Za-z0-9._-]`). Nodes sharing one bucket each take their own. The archive catalogue records each segment's full key, so changing the prefix moves only new segments; archived ones are still read where they were written. Claims are never prefixed. |
| `storage.codec` | `{Ankusa.Codec.Raw, []}` | `{module, opts}` implementing `Ankusa.Codec`: segment record framing. |
| `storage.roll_bytes` | `16 MiB` | Roll a new segment past this size. |
| `storage.roll_ms` | `30_000` | ...or after this long, whichever comes first. |
| `storage.interval_ms` | `1_000` | Compactor tick interval. |
| `storage.blob_store` `:sse` | not set | `sse: %{type: :aes256}` or `%{type: :kms, key_id: key}` in the options of `Ankusa.BlobStore.S3`, `GCS`, `Azure` (`key_id` = the encryption scope) and `OCI`: server-side encryption, sent on every `put/4` of that store (segments, backups, claim packs: whichever are configured on it). Checked at boot; `LocalFS` refuses it. See [`Ankusa.BlobStore.SSE`](storage.md#encryption-at-rest). |
| `secrets.kek` | `nil` | The key-encryption key for stored source secrets: 32 bytes, base64 (`openssl rand -base64 32`), optionally `"<id>:<base64>"` (the id is written into every value it encrypts; without one it is a digest of the key). Encrypts the secret-bearing fields (those `Ankusa.Admin.Redact` hides) of every spec `Ankusa.SourceStore.Persistent` or `Ankusa.SourceStore.Redis` stores, with AES-256-GCM. A stored source no configured key opens refuses the boot. `/v1/config` shows it as `[REDACTED]`. |
| `secrets.kek_previous` | `[]` | Keys that may still decrypt, never encrypt: the old key while a rotation is rolled out. Needs `kek` or `kek_provider`. |
| `secrets.kek_provider` | `nil` | `{module, fun, args}` instead of `kek`: called once when the source store starts, returning `{:ok, [kek \| previous]}` (each a `kek` string or `{id, key}`; the first is current). Exclusive with `kek`. A provider that fails refuses the boot. |
| `claim_check.port` | `4001` | The `:claim_check` role's Bandit port. |
| `claim_check.ip` | `"127.0.0.1"` | The address the gateway binds: a strict IPv4/IPv6 literal (or an `:inet` tuple). Loopback by default; `"0.0.0.0"` exposes it on every interface, which a container needs before a published port reaches it. |
| `claim_check.pack_max_bytes` | `16_777_216` | Target size of one claim pack; a body larger than this still gets a pack of its own. Must be a positive integer. |
| `claim_check.retention_days` | `nil` | LocalFS-only sweeper retention; `nil` disables the sweeper. |
| `claim_check.sweep_interval_ms` | `3_600_000` | Sweeper tick interval. |
| `claim_check.blob_stores` | `nil` | `[{module, opts}, ...]`, the stores claim packs are written to and read from; `nil` uses `[storage.blob_store]`. A `LocalFS` claim store takes `root:` (an absolute path). A failed write moves to the next store; a read searches every one. Removing a store makes its claims unreadable. See [`claim-check.md`](claim-check.md#several-stores-and-failover). |
| `claim_check.placement` | `:ordered` | Which store a new pack goes to: `:ordered` the first, the others only while it fails; `:hash` the pack id's random bits pick one, spreading packs over every store. |
| `claim_check.store_cooldown_ms` | `30_000` | A store that failed a write, or could not answer a read, is skipped by later writes and searched last by later reads for this long, then tried again. Positive integer. |
| `claim_check.key_layout` | `:flat` | `:flat` = `claims/tenant=<t>/dt=<d>/<pack>`; `:sharded` adds `shard=<c>/` under the tenant (32 prefixes per tenant, against per-prefix request limits like S3's `503 SlowDown`). Reads try both layouts. See [`claim-check.md`](claim-check.md#sharded-keys). |
| `claim_check.read_deadline_ms` | `5_000` | The gateway answers every claim request within this: past it, `503 store_unavailable`. A store read that outlives it still finishes and fills the cache. Also bounds `redeem/3` on a `:claim_check` node. See [`claim-check.md`](claim-check.md#under-load). |
| `claim_check.read_timeout_ms`, `claim_check.read_retries` | `3_000`, `1` | Per store request while reading a claim, and the retries after a failed one. A claim store's own `timeout_ms`/`retries` options are for writes. |
| `claim_check.read_max_inflight` | `48` | Per store, the ceiling of the gateway's adaptive limit on concurrent store reads: it halves when the store throttles (`429`, `503`) or does not answer, and grows back by one per round of successes. The HTTP stores read through a client connection pool of 50 per host (claim reads get a pool of their own, keyed by `read_timeout_ms`), which is why the default sits under 50: a ceiling above it only queues reads inside the client, and one that waits past the client's 5 s checkout timeout fails as a transport error. Two claim stores on one host share that pool. |
| `claim_check.read_queue_max` | `256` | Per store, reads waiting for a free slot; past it a read is answered `503` at once. `0` never waits. |
| `claim_check.read_cache_bytes` | `134_217_728` | Memory for the gateway's cached pack indexes and claims; `0` turns the cache off. |
| `claim_check.read_cache_max_entry_bytes` | `1_048_576` | A claim larger than this is served but never cached. |
| `backup.enabled` | `false` | Back the store up to an object store while it runs, and restore the latest backup at boot when the store directory holds no database. A backup location that can't be read then refuses the boot (`{:store_restore_failed, path, reason}`) instead of starting empty. See [`storage.md`](storage.md#backup-and-restore). |
| `backup.interval_ms` | `60_000` | How often a checkpoint is uploaded: the most a lost host loses. Only files the last backup doesn't already have are uploaded. |
| `backup.keep` | `3` | Backups kept; older ones, and the files only they used, are deleted after each successful backup. |
| `backup.blob_store` | `nil` | `{module, opts}` for backup objects; `nil` uses `storage.blob_store`. Keys are under `storage.key_prefix` either way: a backup is one node's, and a node whose prefix holds another store's backups refuses to back up. With `LocalFS` the backup shares the host's fate, and the node logs a warning saying so. |
| `admin.enabled` | `false` | Start the admin API and `Ankusa.Metrics` on this instance. Off for embedded use; the `jamescarr/ankusa` image turns it on. |
| `admin.port` | `4002` | The admin API's Bandit port. |
| `admin.ip` | `"127.0.0.1"` | The address the admin API binds, same rules as `claim_check.ip`. |
| `admin.gauge_interval_ms` | `15_000` | How often `Ankusa.Metrics.Gauges` samples store/queue/quarantine depth, disk free, dispatch in-flight and open breakers for `/metrics`. |
| `admin.tls` | `nil` | A TLS block for the admin listener; see [Listeners](#listeners-limits-tls-and-mtls). |
| `admin.auth` | `%{mode: :none}` | Who may call the admin API: `mode` (`:none`, `:token`, `:jwt`, `:mtls`), `allow_insecure`, `tokens`, `jwt`, `mtls`. `mode: :none` is accepted only when `admin.ip` is loopback (or `allow_insecure: true`). See [Control-plane authentication](#control-plane-authentication). |
| `metrics.enabled`, `metrics.port`, `metrics.ip` | `false`, `4004`, `"127.0.0.1"` | A listener serving only `/metrics`, `/health` and `/ready` (`Ankusa.Metrics.Router`); see [A separate metrics listener](#a-separate-metrics-listener). |
| `metrics.tls` | `nil` | A TLS block for the metrics listener. |
| `claim_check.tls`, `routes.admin.tls` | `nil` | A TLS block for the claim-check gateway and the route-management API. |
| `routes.admin.auth`, `claim_check.auth` | `%{mode: :none}` | The same block for the route-management API and the claim-check gateway. |
| `routes.enabled` | `false` | Enforce route management. Off captures every `POST`, as before; on is **deny by default** — see [Route management](#route-management). |
| `routes.max_routes` | `10_000` | Hard cap on definitions. Creating one past it is a `409`; nothing is ever evicted. |
| `routes.store` | `{Ankusa.Routes.Store.ETS, []}` | `{module, opts}` implementing `Ankusa.Routes.Store`. `Ankusa.Routes.Store.Redis` (package `ankusa_redis`) shares definitions across nodes. |
| `routes.cache.*` | `max_size: 50_000`, `ttl_ms: 30_000`, `negative_ttl_ms: 5_000`, `gc_interval_ms: 60_000` | The per-request decision cache. Entries are keyed by the published snapshot's `epoch`, so publishing a snapshot retires every earlier decision at once. Only short requests are cached — at most 16 path segments and 256 bytes of path — because the key is the sender's own path; a longer one is still decided correctly, but by a scan of the whole route table on every request, and its `[:ankusa, :routes, :match]` event carries `cacheable: false` so that cost is visible. `max_size` is approximate between the adapter's 1s memory checks, and an eviction only costs a re-scan: it can never change a decision. `ttl_ms` must stay under `gc_interval_ms`. |
| `routes.trusted_proxies` | `[]` | CIDRs whose peers may set `X-Forwarded-For`. Empty means the header is never read. |
| `routes.ip_rules` | `%{default: :allow, rules: []}` | Ordered global rules, first match wins, plus the `default` when none match. |
| `routes.admin.port` | `4003` | The management API's own Bandit port. Authenticated by `routes.admin.auth`, like `admin.port`. |
| `routes.admin.ip` | `"127.0.0.1"` | The address the management API binds, same rules as `claim_check.ip`. |
| `routes.log_sample` | `100` | 1 in N rejections is logged at `:debug`; `0` disables it. |
| `routes.ip_denied_status` | `403` | Status for an IP denial, or `404` for uniformity with a missing route. With `403` a sender can tell a route that has its own `ip_rules` (which denied it) from a path that does not exist (`404`); `404` removes that distinction. |
| `routes.seed` | `[]` | Route definitions loaded at boot (see below). With the ETS store they are loaded on **every** boot. |
| `rate_limits.default`, `rate_limits.tenants` | `nil`, `%{}` | Per-tenant ingest limits, `%{rate: hooks_per_second, burst: hooks}`; fractions are allowed (`0.5` is one every two seconds) and both keys are required. Precedence is a runtime override, then the tenant's own entry, then `default`; no limit means unlimited. See [Rate limits](#rate-limits). |
| `quarantine.burst`, `quarantine.rate`, `quarantine.max_bytes` | `100`, `20`, `1_073_741_824` | The quarantine pen: one token bucket per source (`burst` hooks back to back, refilled `rate` per second; over it, `429 quarantine_rate_limited`) and a cap on the pen's total bytes (a hook that would cross it is `503 quarantine_full`; the pen never evicts). See [`delivery.md#quarantine`](delivery.md#quarantine). |

Boot checks ranges, not only types, and fails with `"<dotted.key> must be
<constraint>, got <value>"`: every port is `0..65535`; `max_body_bytes`,
`direct_publish_timeout_ms`, the `batcher` and `dispatch` sizes and counts,
`storage.roll_bytes`, `claim_check.sweep_interval_ms`,
`claim_check.store_cooldown_ms`, `claim_check.read_deadline_ms`,
`claim_check.read_timeout_ms`, `claim_check.read_max_inflight`,
`claim_check.read_cache_max_entry_bytes`, `backup.interval_ms`,
`backup.keep`, `admin.gauge_interval_ms`, `routes.max_routes` and every
numeric `listener.*` key (`max_connections`, `num_acceptors`, the three
timeouts, `max_requests_per_connection`, `max_header_count`,
`max_header_length`) must be positive; `listener.http2` and `metrics.enabled`
are booleans, `metrics.ip` an IP address, and a `tls` block names existing
`certfile`/`keyfile`, a `verify` of `verify_none`/`verify_peer` (the latter
with a `cacertfile`), non-empty `versions` from `tlsv1.2`/`tlsv1.3` and known
`ciphers`;
`batcher.max_delay_ms`, `storage.roll_ms`, `storage.interval_ms`,
`claim_check.read_retries`, `claim_check.read_queue_max` and
`claim_check.read_cache_bytes` may be `0` (no linger, roll at once, a
compactor that runs only when asked, no read retry, no waiting for a slot, no
claim cache);
`claim_check.retention_days` is `nil` or ≥ 1; `storage.key_prefix` is `""` or
`name/` parts; `dispatch.sink_concurrency` is `nil` or `1..concurrency`,
`dispatch.breaker_failures` ≥ 0, `breaker_open_ms` positive and
`breaker_max_open_ms` ≥ `breaker_open_ms`. An embedded source whose verifier secret is missing or
undecodable fails boot too (`source <id>: verifier secret is missing or
undecodable`), instead of rejecting every hook later.

#### Listeners: limits, TLS and mTLS

Bandit's own defaults suit a browser-facing app, not a webhook edge, so every
HTTP listener the instance runs (ingest, admin, route management, claim-check
gateway, metrics) is started with the `listener` limits instead:

- **Idle and slow clients.** A connection that sends nothing for
  `listener.read_timeout_ms` (10 s) is closed: this bounds a request head
  dribbled in a byte at a time (slowloris) and an idle keep-alive. One read of
  a request body must complete within `body_read_timeout_ms` (15 s), and the
  whole body within `request_deadline_ms` (30 s) however it is chunked; the
  ingest port answers `408` past either.
- **Connections.** `max_connections` (20 000) is spread over `num_acceptors`
  (default: two per scheduler); past it new connections are closed, not queued.
  A keep-alive connection is closed after `max_requests_per_connection` (1 000)
  requests, so a load balancer re-spreads long-lived clients.
- **Request heads.** More than `max_header_count` (64) headers, or a header
  line over `max_header_length` (16 384 bytes), is refused before any body is
  read.
- **HTTP/2** is off (`http2: false`): the listeners speak HTTP/1.1, which is
  what webhook providers send.

`listener.tls` is the ingest listener's TLS block; `admin.tls`,
`routes.admin.tls`, `claim_check.tls` and `metrics.tls` give each of the other
listeners its own, with the same keys:

| Key | Default | Meaning |
| --- | --- | --- |
| `certfile`, `keyfile` | required | PEM server certificate and its private key. Relative paths resolve against the working directory; both must exist at boot (`check-config` checks them too) |
| `cacertfile` | none | PEM bundle of the CAs that sign *client* certificates; required by `verify: verify_peer` |
| `verify` | `verify_none` | `verify_peer` is mutual TLS: a client without a certificate that chains to `cacertfile` is refused at the handshake, and the accepted certificate is the request's `Plug.Conn.get_peer_data/1` `:ssl_cert`, which `auth.mode: mtls` maps to a principal |
| `versions` | `[tlsv1.2, tlsv1.3]` | protocol versions offered |
| `ciphers` | Erlang/OTP defaults | cipher suites by OpenSSL name (`ECDHE-RSA-AES256-GCM-SHA384`) or TLS 1.3 name (`TLS_AES_256_GCM_SHA384`); an unknown name fails boot |

A listener with a `tls` block is HTTPS only (plain HTTP to it gets no answer);
one without is plain HTTP. The private key's file path is shown by
`GET /v1/config` (a path is not a secret); the key itself is never read into
the config. The boot log's "no authentication" warning for the admin,
route-management and claim-check listeners is skipped when that listener's
`auth.mode` is not `none`, or when it demands client certificates.

```yaml
listener:
  tls:
    certfile: /etc/ankusa/tls/server.pem
    keyfile: /etc/ankusa/tls/server-key.pem
    cacertfile: /etc/ankusa/tls/ca.pem   # providers present a client cert
    verify: verify_peer
admin:
  tls: {certfile: /etc/ankusa/tls/server.pem, keyfile: /etc/ankusa/tls/server-key.pem,
        cacertfile: /etc/ankusa/tls/ops-ca.pem, verify: verify_peer}
```

Certificates are read when the listener starts: replacing one takes a restart.

#### A separate metrics listener

`GET /metrics` is also served by the admin API, which mutates sources, replays
and the quarantine pen. `metrics.enabled: true` starts one more listener
(`metrics.port`, default 4004, on `metrics.ip`, default `127.0.0.1`) that
serves **only** `GET /metrics`, `GET /health` and `GET /ready` and answers
`404` to everything else, so a Prometheus job or a kubelet probe can be given a
route to it and none to the operator API. It works with `admin.enabled: false`
(it then also starts the metrics reporter and the gauge poller), and the admin
listener keeps serving `/metrics` either way. `metrics.tls` secures it like any
other listener.

#### The admin API

With `admin.enabled: true`, every node serves `GET /health` (the process
answers), `GET /ready` (this node's store takes a synced write, checked at
most once a second, and no write failed in the last 5 s; `503` with
`Retry-After: 1` otherwise — the ingest
port serves the same `/ready`), `GET /metrics`
(Prometheus text), `GET /v1/config` (the effective config; adapter option
values are hidden unless on an allowlist of non-secret keys such as `region`,
`bucket` or `topic`, and URL passwords and query values are hidden),
`GET /v1/wal` (this node's store stats: `next_seq`, and the `hooks`,
`deliveries` and `disk_bytes` estimates; `{}` when the store cannot be read,
`409 wal_disabled` under `wal.type: none`), `GET /v1/dlq`, the replay jobs API
— `POST /v1/replays`, `GET /v1/replays`, `GET|PATCH /v1/replays/{id}`; `kind`
is `dlq`, `archive` or `quarantine` — and `GET|DELETE /v1/quarantine` (`:edge`
role: list the pen, or purge entries by `source_id`, `id`, `since`/`until`,
`limit`) on `admin.port`, independent of the node's roles. Replay jobs are per
node, so a fleet replay is one `POST /v1/replays` per dispatch node; a
`quarantine` job also needs the `:edge` role, since it releases this node's
pen into this node's queue. Source management is
node-agnostic: `GET|POST /v1/tenants/{tenant}/sources` and
`GET|PUT|DELETE /v1/tenants/{tenant}/sources/{name}`, on a writable source
store (`409 source_store_read_only` otherwise; a write the store could not
persist, such as during a Redis outage, is `503 store_unavailable` with
`Retry-After: 1`). `DELETE` of a source that still has undelivered rows in
this node's queue (pending or in flight) is `409 source_has_deliveries`
(`{"pending": n, "inflight": m}`) and deletes nothing;
`?deliveries=dead_letter` dead-letters its pending rows as
`{:source_gone, source_id}` first (needs the `:dispatch` role,
`409 role_not_enabled` otherwise), then deletes; rows in flight settle on
their own. The check is this node's queue only, like the DLQ. The `:edge`
role also gets
`GET /v1/rate-limits` and `GET|PUT|DELETE /v1/tenants/{tenant}/rate-limit`,
which read and adjust this node's per-tenant ingest limits
([Rate limits](#rate-limits)); a store that cannot be read or written answers
`503 store_unavailable`. Every route but `GET /health` and `GET /ready` needs a
credential and a scope: see [Control-plane
authentication](#control-plane-authentication). The HTTP contract is
[`priv/openapi/admin.v1.yaml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa/priv/openapi/admin.v1.yaml).

With `admin.enabled: true` it also serves `GET /asyncapi.json`, the AsyncAPI
document of the channels this node publishes to ([AsyncAPI and lifecycle
events](asyncapi.md)).

#### Control-plane authentication

The admin API, the route-management API and the claim-check gateway each carry
an `auth` block (`admin.auth`, `routes.admin.auth`, `claim_check.auth`). A
request is authenticated into a principal (an id, a list of scopes, and the
tenants it may act on), and each route needs a scope: `401` when there is no
accepted credential (with `www-authenticate: Bearer realm="ankusa"`, the same
body whatever was wrong), `403` `{"error":"forbidden","scope":"…"}` when the
principal lacks the scope. Scopes are checked before anything else, so a caller
without them learns nothing about the node's roles or data. `GET /health` and
`GET /ready` need nothing; the [metrics listener](#a-separate-metrics-listener)
is the unauthenticated way to scrape.

| `mode` | Credential | Use it for |
| --- | --- | --- |
| `none` (default) | none | loopback only: `Ankusa.Config.new/1` refuses it on any other `ip` unless `allow_insecure: true` says a mesh or proxy in front of the port authenticates every caller (a `verify_peer` listener is not enough: use `mtls`) |
| `token` | `Authorization: Bearer <token>`, matched against SHA-256 hashes | development, CI and scripts. Hash a random 32-byte value, not a password |
| `jwt` | `Authorization: Bearer <access token>` from your OIDC provider | people and services that sign in through SSO; revocation is the provider's |
| `mtls` | the client certificate of a `verify_peer` listener | service-to-service inside your own PKI |

Scopes (`Ankusa.Principal`): `metrics:read` (`/metrics`, `/v1/wal`),
`config:read` (`/v1/config`, `/asyncapi.json`), `sources:read` and
`sources:write`, `dlq:read`, `replays:read` and `replays:write`,
`quarantine:read` and `quarantine:write`, `rate_limits:read` and
`rate_limits:write`, `routes:read` and `routes:write`, `claims:read`, and
`dlq:write`, which starting a `dlq` replay also needs (`quarantine:write` for a
`quarantine` replay). `config:write` and `audit:read` are reserved: no route
uses them yet. Tenants are scopes too: `tenant:<id>` for one, `tenant:*` for all.
A tenant's resources (its sources and rate limit, its claims) need that tenant;
resources that span tenants (the DLQ, replay jobs, the quarantine pen, the
rate-limit listing, route definitions and IP rules) need `tenant:*` until they
are filtered by tenant, so a tenant-limited token cannot read another tenant's
rows.

```yaml
admin:
  ip: 0.0.0.0
  auth:
    mode: token
    tokens:
      - id: ci
        token: "${CI_ADMIN_TOKEN}"        # hashed at load; or sha256: <hex>
        scopes: [dlq:read, replays:write, dlq:write, "tenant:*"]
      - id: acme-support
        sha256: "<64 hex characters: printf %s \"$TOKEN\" | shasum -a 256>"
        scopes: [sources:read, sources:write, "tenant:acme"]
```

```bash
curl -H "Authorization: Bearer $TOKEN" http://localhost:4002/v1/dlq
```

**JWT.** `jwt.issuer` and `jwt.audience` (a string, or a list: any one is
accepted) are required, with `jwt.jwks_url` (https; http only to a loopback
host) and/or `jwt.keys` (public JWKs or PEM keys). A token is accepted when it
is signed with an asymmetric algorithm (`RS*`, `PS*`, `ES*`, `EdDSA`; never
`none` or `HS*`) by one of those keys, its `iss` and `aud` match, `exp` is in
the future and `nbf`, when present, is not (`jwt.leeway_s`, default 30,
forgives clock skew), and `sub` is set. The scopes come from the
`jwt.scope_claim` claim (default `scope`: a space-delimited string or a list;
scopes this system does not define, such as `openid`, are ignored) and the
tenants from `jwt.tenants_claim` (default `ankusa_tenants`: `["acme"]`, or
`["*"]` for all) together with any `tenant:<id>` scope. The JWKS is fetched
after boot (a slow provider does not delay it) and every `jwt.refresh_ms`
(default 5 minutes); a token naming an unknown `kid` refetches it, at most once
per `jwt.min_refresh_ms` (default 10 s). A failed fetch is a warning and keeps
the keys already loaded; until the first fetch succeeds every token is refused.

**mTLS.** `mode: mtls` needs the listener's `tls` block with `verify:
verify_peer`. `mtls.subject_map` maps a certificate identity to scopes:
`"cn:<common name>"`, or one of its subject alternative names
(`"dns:…"`, `"email:…"`, `"uri:…"`, `"ip:…"`). The first identity of the
certificate (SANs, then the common name) found in the map is the principal. A
valid certificate nobody mapped is `401`.

Tokens and keys are validated at load, and the redacted `GET /v1/config` shows
each token's `id` and scopes but never its hash. The claim-check gateway's
readers are your SDK clients: send the token in `Authorization` (the
TypeScript `createClaimCheckClient({ baseUrl, headers })` and the Elixir SDK's
`:headers` option take it as is).

#### Route management

**Off by default, and off means "capture everything".** With `routes.enabled:
true` the edge becomes deny-by-default: a `POST` is captured only if it passes
the IP rules **and** its method and normalized path match an enabled route.
Anything else is answered `404` (`403` for an IP denial, unless
`ip_denied_status: 404`) and never reaches the queue — no record, no dispatch,
no delivery.

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
global ban list still applies everywhere else. A global `deny` always wins. A
CIDR is written as IPv4 or IPv6; one inside `::ffff:0:0/96` (`::ffff:10.0.0.0/104`)
is refused when it is written, because client addresses are matched as IPv4 and
such a range could never match — as a `deny` rule it would silently let its own
traffic through. Write `10.0.0.0/8` instead.

Client addresses come from the socket peer. `X-Forwarded-For` is read **only**
when the peer is inside `routes.trusted_proxies`; from anyone else the header is
ignored, whatever it says. Every `x-forwarded-for` header value is joined in
order and the chain is walked right to left, because proxies append: the
rightmost entries are the ones this node's own proxies wrote. An entry is an
address with an optional port (`1.2.3.4`, `1.2.3.4:5678`, `::1`, `[::1]`,
`[::1]:443`), normalized so IPv4-mapped IPv6 (`::ffff:1.2.3.4`) is matched as
IPv4. The first entry not in the trusted set is the client, and the walk stops
there — entries to its left are never read. An entry the walk cannot read
before it finds the client denies the request rather than being skipped: a
chain that is partly unreadable is not evidence of anything. If every entry is
trusted, the chain's leftmost entry is the client; no header, or an empty one,
means the peer.

**Definitions.** `ankusa`'s default store keeps them in this node's memory, which
is enough for a single node — pair it with `routes.seed`. The in-memory store is
seeded on **every** boot, so a route deleted through the API returns after a
restart unless it is also removed from the seed: with `store.type: ets` the seed
is what the node comes up with, not a one-time import. To share definitions
across edge nodes, use the `ankusa_redis` package:

```yaml
routes:
  enabled: true
  store: {type: redis, url: redis://cache:6379, namespace: ankusa:routes}
```

`store.url`, `store.namespace`, and `store.tick_ms` are Redis-store keys: with
`type: ets` any of them is a config error, not a silently ignored key (a `url`
with no `type` at all still means Redis).

Every node with the same `namespace` enforces the same routes: writes bump a
version counter and publish it, each node reloads on the broadcast, and a
periodic tick (`tick_ms`, default 30s) is the safety net for a missed one. A
node keeps serving its in-memory snapshot through a Redis outage; only writes
report `503 store_unavailable`. The Redis store seeds a namespace once, on its
first boot, so a route deleted there is not resurrected by a restart.

**Management API**, on `routes.admin.port` (its own listener, never the ingest
port), authenticated by `routes.admin.auth` like the operator admin API (see
[Control-plane authentication](#control-plane-authentication)): `routes:read`
for `GET` and the dry run, `routes:write` for everything else, and in both
cases `tenant:*`, since route definitions and IP rules belong to no tenant.
The HTTP contract is the `routes` tag of
[`priv/openapi/admin.v1.yaml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa/priv/openapi/admin.v1.yaml),
examples included — and
[`test/ankusa/routes/router_openapi_test.exs`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa/test/ankusa/routes/router_openapi_test.exs)
executes those examples against the implementation, so the document and the code
cannot drift apart:

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
answers "why was this rejected" without capturing anything and **without
reading or writing the decision cache**, so a dry run can never change what
live requests decide, and a route change can be checked before it goes live.

`PUT /admin/ip-rules` requires both `default` and `rules`. Omitting either is a
`400 invalid_ip_rules` that names the field, because an omitted `default` used
to mean `allow`, and a guard's default is not a value to infer.

Writes are serialized against each other: each one is validated against the
snapshot it read, and a write that keeps losing to a concurrent writer answers
`503 store_unavailable` instead of overwriting a definition it never saw. That
response is safe to retry.

> **Rejections are not retried by Ankusa.** Some providers retry any `4xx`, some
> give up, and some disable an endpoint after enough failures — the provider's
> own policy decides, and `403`/`404` are the only signals it gets. Dry-run the
> change first.

Rejections are logged at `:debug`, sampled at `routes.log_sample` (1 in N, `0`
for silent), and emit `[:ankusa, :routes, :reject]`; captures emit
`[:ankusa, :routes, :match]` with `%{instance, route_id, cached, cacheable}`.

#### Rate limits

Per-tenant ingest limits, enforced by each edge node **in its own memory**: a
fleet of N edges admits up to N × the limit, so size the numbers per node, not
for the fleet. A catch-URL scheme that lets the sender choose its tenant
(`routing: tenant_path`) has as many budgets as there are tenant names.

```yaml
rate_limits:
  default: {rate: 50, burst: 100}
  tenants:
    acme: {rate: 500, burst: 1000}
```

A hook is charged **after verification and before the durable write**, so only
hooks verification accepted spend a tenant's budget: a flood of forged requests
is free and can never lock a tenant out. Over the limit, the sender gets `429`
with a `Retry-After` header and nothing is stored — no hook record, no
dispatch, no claim. The check costs one ETS compare-and-swap per accepted hook, and a
denial writes nothing; the `[:ankusa, :rate_limit, :rejected]` event and its
`ankusa_rate_limit_rejected_total` counter (tagged by tenant) are the signal,
so rejections are not logged.

Limits can also be adjusted per tenant at runtime, with no restart:
`GET /v1/rate-limits` and `GET|PUT|DELETE /v1/tenants/{tenant}/rate-limit` on
`admin.port` (`:edge` role only, `409 role_not_enabled` elsewhere). An override
lives on **the node that accepted it** and is persisted in this node's store
(one key per tenant), the same node-local model as `source_store.type:
persistent`'s API-managed sources — put
durable limits in the config. `PUT` and `DELETE`
reset that tenant's bucket, so a raised limit is not held back by the old
limit's accumulated debt. There is no "unlimited" value: to exempt a tenant
from a `default`, give it a high limit of its own.

#### Egress

An HTTP sink forwards the webhook body to a URL that an operator, or a tenant
through the admin API, chose. Left alone that URL could be Ankusa's own admin
listener (`http://127.0.0.1:4003/...`), the cloud metadata service
(`http://169.254.169.254/...`, which hands out the node's credentials) or any
service on the private network. The `egress` section decides where
`Sink.Http` may connect (`Ankusa.HttpClient.Egress`; source sinks and
`lifecycle.sinks` alike):

```yaml
egress:
  allow_insecure_http: false      # true also allows http:// sink URLs
  deny_cidrs: [...]               # the default list above; setting it replaces it
  allow_cidrs: ["10.20.0.0/16"]   # an internal range sinks may reach despite deny_cidrs
  allow_hosts: [jobs.internal]    # names exempt from the address check
  deny_hosts: [metadata.google.internal]   # names refused before any lookup
```

- **At load**, a `type: http` sink whose URL is not `https://` (or is not an
  absolute http(s) URL) fails the config load, naming the key, unless
  `allow_insecure_http` is on. The same rule applies when the admin API writes a
  source (`400`), because the image's source decoder runs the same check.
- **On every attempt**, the name is resolved for both address families
  (the OTP resolver, `:inet.getaddrs`) and the delivery is refused when *any* address is in
  `deny_cidrs` and not in `allow_cidrs`, or when the name is in `deny_hosts`
  (`allow_hosts` exempts a name from the address check, `deny_hosts` wins). A name
  with one public and one private record is refused as a rebinding setup.
  IPv4-mapped IPv6 (`::ffff:127.0.0.1`) is matched as the IPv4 address. A literal
  address, in any spelling inet parses (`http://2130706433/`), is checked as the
  address it is.
- **A denial is a dead letter**, not a retry: `{:permanent, {:egress_denied,
  reason}}`, like a `401`. Fix the policy or the URL, then replay. A name that
  does not resolve is retried. The reason names the host and the address, never
  the URL, which may carry credentials.
- **Pinning.** An `http` delivery connects to the address that was checked (the
  URL is rewritten to it, with the name in the `host` header), so nothing is left
  to rebind. An `https` delivery keeps the name and the client resolves it again
  when it connects, so a name rebound to an internal address in that gap still has
  to present a certificate valid for the name, which the service behind it does
  not hold: peer verification is what closes the window. A sink with
  `allow_insecure_tls: true` has no such check, so its connection is pinned too
  (pinning every `https` URL would cost Req a connection pool per hostname, which
  tenant-supplied URLs make unbounded).

TLS verification is on for every outbound `https` connection (object stores, SQS,
Pub/Sub, HTTP sinks): the peer's chain and hostname, TLS 1.2 or 1.3, against the
system trust store with `castore`'s bundle behind it. A sink's `cacertfile` names a
private CA instead; `allow_insecure_tls: true` on one sink is the only way to turn
verification off, and a `verify: :verify_none` in `:req_options` fails the config
load without it. `/v1/config` shows the policy and whether a sink skips
verification, and hides a sink's `cacertfile` path like any other adapter option.

The policy covers `Sink.Http`. The queue sinks (`rabbitmq`, `kafka`, `nats`,
`redis`, `sqs`, `google_pubsub`) connect to the broker endpoints in their own
config, and the object stores, SQS credentials and the cloud metadata clients to
their configured endpoints; none of those URLs comes from a tenant.

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
| `verifier` | `{Ankusa.Verifier.None, []}` | `{module, opts}` implementing `Ankusa.Verifier`. `Ankusa.Verifier.Hmac`'s `:secret` is a binary or a list of binaries, newest first — every key is tried, so a rotation is `secret: [new, old]`. |
| `on_verify_failure` | `:reject` | `:reject` (`401`, nothing stored) / `:quarantine` (`202`, durable pen; see [`delivery.md#quarantine`](delivery.md#quarantine)) / `:accept_flag` (commits, envelope flagged). |
| `trust_url_tenant` | `false` | Under a `route_resolver` other than `Ankusa.RouteResolver.Path`, a source with `tenant_id: "default"` and `Ankusa.Verifier.None` must set it to `true` (`Ankusa.Verifier.validate_shared!/1`); see [`multi-tenancy.md`](multi-tenancy.md#tenant-scoping-what-tenant_id-actually-does). |
| `sinks` | `[{Ankusa.Sink.Log, []}]` | `[{module, opts}]` implementing `Ankusa.Sink`: one delivery per sink, each retried independently. |
| `dedupe` | `nil` | Collapse provider retries at ingest: `:github \| :standard_webhooks \| :svix \| :shopify \| :stripe`, or `%{header: "X-Event-Id"}` / `%{json: "data.id"}`, each with an optional `ttl_ms` (default `259_200_000`). |
| `forward_headers` | `:default` | `:default` (every header except the never-forwarded set; see [Sources](#sources)) or a list of header names. |
| `store_headers` | `nil` | `nil` (the instance's `store_headers`) or `%{drop: [..], hash: [..]}` overriding it per key; an absent `drop`/`hash` inherits the instance's list. |

A source cannot override the dispatch-wide retry policy: there is no per-source
retry policy. `Sink.Http`/`Sink.RabbitMQ`/`Sink.Kafka`/`Sink.NATS`/`Sink.Redis`
retries are all driven by `config.dispatch.retry`, applied uniformly per source
by `Ankusa.Dispatch.Pipeline`.

### Every behaviour, at a glance

Full detail (options, guarantees, how to write your own) lives in
[`storage.md`](storage.md) and [`delivery.md`](delivery.md); this table is
the map.

| Behaviour | Job | Default | Also shipped |
| --- | --- | --- | --- |
| `Ankusa.RouteResolver` | Catch-URL scheme → `%Route{tenant_id, source_id}` | `RouteResolver.Path` (`/webhooks/:source_id`) | `RouteResolver.TenantPath` (`/webhooks/:tenant/:source`) |
| `Ankusa.Queue` | Durable ack and seq assignment over this node's store | `wal: :disk` (one synced commit per batch) | none (`wal: :none` drops the queue entirely: ingest acks on the sinks' confirm, see [`delivery.md`](delivery.md#direct-mode)) |
| `Ankusa.Verifier` | Signature/timestamp checks | `Verifier.None` | `Verifier.Hmac` (configurable HMAC engine; named schemes Stripe, GitHub, Standard Webhooks, Shopify, Slack) |
| `Ankusa.SourceStore` | Source config, secrets, policy | `SourceStore.Static` | `SourceStore.Persistent` (API-managed sources in this node's store) |
| `Ankusa.Sink` | What happens to a delivered hook | `Sink.Log` | `Sink.Http` (Req forward), `Sink.SQS` (`SendMessage`, in core), `Sink.RabbitMQ` (exchange publish, `ankusa_rabbitmq` package), `Sink.Kafka` (topic produce, `ankusa_kafka` package), `Sink.NATS` (JetStream subject publish, `ankusa_nats` package), `Sink.Redis` (pub/sub channel publish, `ankusa_redis` package) |
| `Ankusa.RetryPolicy` | Backoff / give-up | `RetryPolicy.Exponential` (jitter) | none |
| `Ankusa.BlobStore` | Segment PUT / range GET / delete | `BlobStore.LocalFS` | `BlobStore.S3` (+R2/MinIO), `BlobStore.GCS` |
| `Ankusa.ClaimCheck` | Pack claims into the object store, redeem by reference | none (the instance's `BlobStore`) | none |
| `Ankusa.Codec` | Segment record framing | `Codec.Raw` (len-prefixed, CRC32) | none |

Swapping a behaviour is a one-line config change, `route_resolver:
{Ankusa.RouteResolver.TenantPath, []}`, because every pluggable layer is a
behaviour with `{module, opts}` config, resolved at the call site, never
hardcoded. The queue is the exception: its mode is `wal: :disk | :none`, not a
module.

### Runtime environment overrides

Ankusa.Application (the default OTP application boot path) reads two env
vars on top of whatever `config.exs` sets:

- `PORT`: overrides `config.port`.
- `ANKUSA_ROLES`: comma-separated, overrides `config.roles` (e.g.
  `ANKUSA_ROLES=edge,dispatch`). Parsed with `Ankusa.Config.parse_roles!/1`:
  an unknown role name (anything other than `edge`, `dispatch`, `storage`,
  `claim_check`) raises `ArgumentError` and fails boot rather than silently
  starting with the wrong roles. `edge` commits hooks, `dispatch` delivers
  them, `storage` archives them, and a hook is deleted once its last
  obligation clears: `edge,dispatch` reclaims delivered hooks without the
  archive, so dropping `storage` no longer strands them.

The autostarted default instance reads only `:autostart`, `:port`, `:data_dir`, `:roles` and `:sources` from the application env; every other `Ankusa.Config` key takes effect only when passed to `Ankusa.Config.new/1` or an `Ankusa.Instance` child spec.

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
