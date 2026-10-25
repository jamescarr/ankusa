# Configuration

The container reads one YAML file; the library takes an `%Ankusa.Config{}` and
per-source `%Ankusa.Source{}` structs. Both describe the same pipeline, and the
loader validates every key, range and cross-key rule before the node binds a
port.

This page is the reference: the YAML sections first, then the Elixir structs.
`packages/ankusa_server/config-examples/reference.yml` carries every YAML key
with its default and a comment, and `check-config` proves a file loads without
starting the node.

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
signs. `${file:/abs/path}` reads a file and `${sm:<provider>:<ref>}` asks a
secret manager; see [Secret sources](#secret-sources). An invalid file exits
`78` (`EX_CONFIG`), a message, not a crash dump.
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
        allow_unsigned: true      # demo only: this worker does not verify; see Sinks below
```

### Sections

The top-level map. Each row gives the keys you are most likely to change and
their defaults; the exhaustive list, including every nested key and its range,
is the [key reference](#ankusa-config) below.

| Section | Keys (default) |
| --- | --- |
| `node` | `roles` (`[edge, dispatch, storage]`; under `wal.type: none` the queue's readers — `dispatch`, `storage` — are dropped, so an all-role node becomes `[edge]`), `data_dir` (`/var/lib/ankusa`), `min_free_bytes` (unset = no check; the free bytes `data_dir`'s file system must keep, or `/ready` answers `503` with `disk: failing`; see [`/ready`](deployment.md#running-the-container)) |
| `log` | `level` (`info`), `banner` (`true`; the ASCII-art banner on start) |
| `http` | `port` (4000), `max_body_bytes` (8000000), `routing` (`path` \| `tenant_path`), `prefix` (`/webhooks`), `store_headers` (`drop`: `authorization`, `proxy-authorization`, `cookie`, `set-cookie`, `x-api-key`, `x-auth-token`; `hash`: none). See [Stored headers](#stored-headers) |
| `listener` | Limits for every HTTP listener, and the ingest port's TLS: `max_connections` (20000), `num_acceptors` (null = two per scheduler), `read_timeout_ms` (10000), `body_read_timeout_ms` (15000), `request_deadline_ms` (30000), `max_requests_per_connection` (1000), `max_header_count` (64), `max_header_length` (16384), `http2` (`false`), `tls` (`certfile`, `keyfile`, `cacertfile`, `verify` = `verify_none` \| `verify_peer`, `versions`, `ciphers`). See [Listeners](#listeners-limits-tls-and-mtls) |
| `admin` | `enabled` (`true` in the image, `false` in core), `port` (4002), `ip` (`127.0.0.1`; `0.0.0.0` exposes it, and then needs `auth`), `gauge_interval_ms` (15000; how often the `/metrics` gauges are sampled), `tls` (a TLS block, like `listener.tls`), `auth` (who may call it: `mode` = `none` \| `token` \| `jwt` \| `mtls`, `allow_insecure`, `tokens`, `jwt`, `mtls`; see [Control-plane authentication](#control-plane-authentication)) |
| `metrics` | `enabled` (`false`), `port` (4004), `ip` (`127.0.0.1`), `tls`. A listener that serves only `/metrics`, `/health` and `/ready`. See [A separate metrics listener](#a-separate-metrics-listener) |
| `batcher` | `partitions` (4), `max_batch` (256), `max_commits` (64), `group_commit_max_bytes` (4194304), `group_commit_max_records` (1024), `group_commit_delay_ms` (1), `max_delay_ms` (0), `max_queue` (10000), `max_queue_bytes` (268435456) |
| `dispatch` | `batch` (128), `concurrency` (32), `max_inflight` (4096), `max_inflight_bytes` (134217728), `attempt_timeout_ms` (30000), `sink_concurrency` (8; null = only `concurrency`), `breaker_failures` (5; 0 disables breakers), `breaker_open_ms` (30000), `breaker_max_open_ms` (300000), `tenants` (`default` and one entry per tenant id, each any of `max_inflight` (1024), `max_inflight_bytes` (33554432), `concurrency` (16)), `max_retry_after_ms` (3600000; 1000 to 86400000), `retry.base_ms` (100), `retry.max_ms` (300000), `retry.max_attempts` (null: no attempt limit), `retry.jitter` (`true`), `retry.max_elapsed_ms` (86400000, 24 h; 1000 to 2592000000: the wall-clock horizon, see [Retry horizon](delivery.md#retry-horizon)). See [`delivery.md`](delivery.md#a-slow-or-dead-sink) and [Dispatch fairness between tenants](delivery.md#fairness-between-tenants) |
| `wal` | `type` (`disk` \| `dual` \| `none`; the queue's mode — the name is historical; `dual` also confirms every hook in the intake log, see `intake`), `publish_timeout_ms` (`8000`; `wal.type: none` only: the overall deadline every sink must confirm under — keep it below the provider's own timeout) |
| `intake` | The intake log behind `wal.type: dual` ([ADR 0002](https://github.com/jamescarr/ankusa/blob/main/docs/adr/0002-dual-commit-intake-log.md)); read whatever `wal.type` is, required under `dual`. `type` (`kafka`; required under `dual`), `node_id` (required under `dual`; `[a-z0-9._-]{1,64}`, the node's **stable** identity, the same on a replacement; `ANKUSA_INTAKE_NODE_ID`), `fleet` (`default`), `topic` (`ankusa.intake.<fleet>`), `kafka` (`brokers`: a list of `host:port`, required; `ssl`: bool; `sasl`: `mechanism` (`plain` \| `scram_sha_256` \| `scram_sha_512`), `username`, `password`; the same shape as a Kafka sink), `publish_timeout_ms` (`2000`; 100–60000, under the batcher's 15 s record deadline: the one publish attempt's deadline), `reconcile_deadline_ms` (`20000`; 1000–600000, and under `fleet.pending_ttl_ms` with `dedupe.scope: fleet`), `breaker_failures` (`3`; 1–100), `probe_ms` (`1000`; 100–60000), `retry_after_s` (`2`; 1–300), `offset_flush_ms` (`1000`; 100–60000), `pending.max_records` (`50000`; 1–10⁷), `pending.max_bytes` (`268435456`), `max_record_bytes` (`10000000`; at least `http.max_body_bytes` + 65536), `recover_on_boot` (`true`), `verify_topic` (`true`; `false` skips the boot check of the topic's settings and the startup line says `intake topic settings UNVERIFIED`). `kafka.sasl` is redacted in `GET /v1/config`. Boot refuses `dual` without `intake.type`, `intake.node_id`, `backup.enabled` and `backup.max_age_ms`. See [Host loss and the intake log](deployment.md#host-loss-and-the-intake-log) |
| `storage` | `type` (`local` \| `s3` \| `gcs`), `roll_bytes` (16777216), `roll_ms` (30000; a partial segment waits this long), `max_segments_per_tick` (16), `key_prefix` (`""`; `name/` parts prepended to every segment key, so nodes can share a bucket), `s3.*` (`bucket`, `region`, `endpoint`, `access_key_id`, `secret_access_key`, `session_token`), `gcs.*` (`bucket`, `endpoint`, `auth` = `metadata` \| `token` \| `none`; `token` is required when `auth: token`) |
| `storage.s3.sse`, `storage.gcs.sse` | `type` (`aes256` \| `kms`), `key_id` (the KMS key; required for `kms` on GCS, optional on S3). Server-side encryption sent on every object this store writes; the same `sse:` key is accepted by the `s3`/`gcs` block of `claim_check.store(s)` and `backup.store`. See [Encryption at rest](storage.md#encryption-at-rest) |
| `storage.s3.*`, `storage.gcs.*` transport options | `timeout_ms` (10000), `upload_timeout_ms` (60000), `op_deadline_ms` (120000), `multipart_threshold_bytes` (8388608), `multipart_part_bytes` (8388608), `retries` (2), `breaker_failures` (5; 0 = no breaker), `breaker_open_ms` (30000), `breaker_slow_ms` (5000), `pool_size` (32), `pool_count` (1), `pool_timeout` (5000). The same keys are accepted by the `s3`/`gcs` block of `claim_check.store(s)` and `backup.store`. Integers, range-checked when the instance starts. See [Conditional writes, retries, checksums, multipart, pools](storage.md#conditional-writes-retries-checksums-multipart-pools) |
| `archive` | `retention_days` (`nil` keeps segment objects; an integer deletes a segment object and its index through the blob store, with the catalogue row, once the newest hook in it is that old). See the key reference for its relation to `storage.catalogue_retention_days` |
| `claim_check` | `port` (4001), `ip` (`127.0.0.1`), `tls` (a TLS block), `auth` (like `admin.auth`; reading a claim needs `claims:read` and the claim's tenant), `pack_max_bytes` (16777216), `retention_days` (null disables the sweeper; else ≥ 1), `store` (unset = the `storage` bucket; same shape as `storage` plus `root` for `type: local`), `stores` (a list of `store`-shaped entries, exclusive with `store`: failover between them), `placement` (`ordered` \| `hash`), `store_cooldown_ms` (30000), `key_layout` (`flat` \| `sharded`), and the gateway's reads: `read_deadline_ms` (5000), `read_timeout_ms` (3000), `read_retries` (1), `read_max_inflight` (48), `read_queue_max` (256), `read_cache_bytes` (134217728; 0 = no cache), `read_cache_max_entry_bytes` (1048576), `max_concurrent_reads` (1024; 0 = no cap), and signed refs: `ref_signing_keys` (`[]`; a list or a comma-separated string of `[current, previous…]`, each ≥ 16 bytes; use `${VAR}`), `ref_ttl_ms` (null = the retention, else 30 days), `accept_unsigned_refs` (null = only while no key is set). `claim_check.authorize` is Elixir config only. See [`claim-check.md`](claim-check.md#a-dedicated-claim-store), [Under load](claim-check.md#under-load) and [The reference](claim-check.md#the-reference) |
| `backup` | `enabled` (`false`), `interval_ms` (60000), `keep` (3), `store` (unset = the `storage` bucket; same shape as `claim_check.store`), `secondary` (a second store of the same shape: every backup is written to both, restore falls back to it), `timeout_ms` (120000), `verify_interval_ms` (unset = never), `signing_key` (a string or a list, e.g. `["${BACKUP_SIGNING_KEY}"]`: the first signs, any verifies), `final_timeout_ms` (20000), `max_age_ms` (unset = no `/ready` label), `require_fresh` (`false`). Continuous backup of the node's store, restored at boot into an empty `data_dir`; keys go under `storage.key_prefix`. See [`storage.md`](storage.md#backup-and-restore) |
| `routes` | `enabled` (`false`), `max_routes` (1000000), `memory_budget_bytes` (8589934592), `store.type` (`ets` \| `redis`; `store.url`/`store.namespace`/`store.tick_ms`/`store.timeout_ms`/`store.load_timeout_ms` are Redis-only), `cache.*`, `trusted_proxies` (`[]`), `ip_rules.*`, `admin.port` (4003), `admin.ip` (`127.0.0.1`), `admin.tls` (a TLS block), `admin.auth` (like `admin.auth` of the operator API), `log_sample` (100), `ip_denied_status` (403), `seed` (`[]`; each entry takes `id`, `path`, `methods`, `enabled`, `ip_rules`, `metadata`, and any other key is a load error). See [Route management](#route-management) |
| `sources` | One entry per catch-URL source. See below |
| `source_store` | `type` (`static` \| `persistent` \| `redis`; `persistent` adds API-managed sources kept in this node's store, `redis` keeps them in Redis, shared by every node with the same namespace; a `url` with no `type` means `redis`), `url`, `namespace` (`ankusa:sources:<instance>`), `tick_ms` (30000), `timeout_ms` (5000), `load_timeout_ms` (300000) — the last five only with `type: redis`. See [`multi-tenancy.md#dynamic-sources`](multi-tenancy.md#dynamic-sources) |
| `secrets` | `kek` (a 32-byte key, base64, optionally `<id>:<base64>`; use `${VAR}`), `kek_previous` (a list, or one comma-separated string, of keys that may still decrypt). With a `kek`, the secrets of API-managed sources are encrypted before they are stored (`source_store.type: persistent` or `redis`); unset, they are stored as plain JSON. See [Encryption at rest](storage.md#encryption-at-rest). `providers.<name>` (`type: aws` or `vault`, and their settings) and `cache_ttl_ms` (`0`) name the secret managers `${sm:<name>:<ref>}` reads from; see [Secret sources](#secret-sources) |
| `rate_limits` | `default` (`null` = unlimited; **required** with `routing: tenant_path`), `tenants` (`{tenant_id: {rate, burst, ...}}`; a limit may add `bytes_per_second`, `burst_bytes`, `max_body_bytes`, `max_queue`, `max_queue_bytes`), `per_ip` (`{rate, burst}` or `null`), `node` (`{rate, burst}` or `null`), `sources` (`default: {bytes_per_second, burst_bytes}`; 100 MB/s, `null` = off), `scope` (`node` \| `fleet`; default `node`). Per node, except the tenant quota under `scope: fleet`. `per_ip`, `node` and `sources` are charged before the body is read; the tenant's after verification. See [Rate limits](#rate-limits) |
| `dedupe` | `scope` (`node` \| `fleet`; default `node`). Where a source's provider event key (its `dedupe:`) collapses: `fleet` claims it in `fleet.store` first. Needs `fleet.store`. See [Fleet scope](#fleet-scope). `max_entries` (100000000) and `max_entries_per_tenant` (30000000), integers ≥ 1: the most live dedupe keys the node, and one tenant, holds; past either a new key is `503 dedupe_quota`, `Retry-After: 60` |
| `fleet` | `store` (`{type: redis, url, namespace, pool_size}`; absent with both scopes `node`), `timeout_ms` (20; 1–1000), `pending_ttl_ms` (30000), `breaker_window_ms` (1000), `breaker_min_calls` (20), `breaker_failure_rate` (0.5), `breaker_open_ms` (5000). The shared store behind `rate_limits.scope: fleet` and `dedupe.scope: fleet`. See [Fleet scope](#fleet-scope) |
| `quarantine` | `burst` (100), `rate` (20 per second), `max_bytes` (1 GiB). The pen for hooks whose source has `on_verify_failure: quarantine`: one token bucket per source (over it, `429 quarantine_rate_limited`), and a byte cap (a full pen answers `503 quarantine_full`; it never evicts). At most 1 000 writes may wait for the pen's disk write at once (fixed, not a key; past it `503 overload` with `Retry-After: 1`; gauge `ankusa_quarantine_pending`). Per node. See [`delivery.md#quarantine`](delivery.md#quarantine) |
| `audit` | `retention_days` (365, an integer ≥ 1). How long the [audit trail](architecture.md#guarantees-by-component) keeps its rows: an hourly sweep deletes older ones, but never a row a lifecycle sink has not yet seen (`m:lifecycle_cursor`). Read with `GET /v1/audit` (scope `audit:read`). Per node |
| `tenants` | `max` (1000000, an integer from 1 to 4294967294). How many tenants the node's store hands a dense ordinal to ([ADR 0001](https://github.com/jamescarr/ankusa/blob/main/docs/adr/0001-tenant-key-layout.md)); the hook of one past it is refused with `503 {"error":"tenant_limit"}` and `Retry-After: 300`. Tenants it already knows are never refused, and a tenant keeps its ordinal forever. Per node |
| `queue` | `dlq` (`retention_days` 30, `max_rows` 10000000, `max_bytes` 53687091200; see [`delivery.md`](delivery.md)) and `rocksdb`: the RocksDB settings of the node's store, read at open: `block_cache_bytes` (67108864), `bloom_bits_per_key` (10; 0 = off), `max_open_files` (65536; -1 = no limit), `compaction_style` (`level` \| `universal`), `rate_limiter_bytes_per_sec` (0 = unlimited), `max_background_jobs` (4), `write_buffer_bytes` (a map of the column families `default`, `hooks`, `deliveries`, `index`, `archive`, `quarantine`, `audit`, `tenants`, `counters`, each at least 65536). See [`storage.md`](storage.md#rocksdb-tuning-and-the-index-filters) |
| `health` | `sink_error_window_ms` (5000, an integer ≥ 1). `wal.type: none` only: a sink whose last publish failed within this many ms makes `/ready` answer `503` with `sinks: failing`; a sink that publishes again clears itself at once. The other two readiness thresholds live beside the things they measure: `node.min_free_bytes` (the disk floor) and `queue.max_oldest_due_ms` (unset = no check; the oldest due delivery's wait, in ms, past which `/ready` carries `backlog: failing`, a label that never makes the node unready). See [`/ready`](deployment.md#running-the-container) |
| `lifecycle` | `sinks` (required and non-empty when the section is present; omit the section to turn events off). Sinks, shaped like a source's, that receive a CloudEvent when a source or route is created, updated, or deleted. See [AsyncAPI and lifecycle events](asyncapi.md) |
| `egress` | `allow_insecure_http` (`false`: `http` sink URLs fail the load), `deny_cidrs` (loopback, RFC 1918, carrier-grade NAT, link-local, unique-local IPv6 and the unspecified addresses; setting it replaces the list), `allow_cidrs` (`[]`), `allow_hosts` (`[]`), `deny_hosts` (`[metadata.google.internal]`), `pools` (`pool_size` (32), `pool_count` (1), `pool_timeout` (5000), `pool_max_idle_ms` (60000), `conn_max_idle_ms` (30000), `max_pools` (1024), `max_pools_per_tenant` (256), `protocols` (`[http1, http2]`)). Where `type: http` sinks may connect, checked on every attempt; and `egress.pools`, the outbound HTTP connection pools: one per receiver address, bounded in connections, count and idle time. See [Egress](#egress) and [Outbound connection pools](#outbound-connection-pools) |

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
| `verify.type` | `none` \| `stripe` \| `github` \| `standard_webhooks` \| `shopify` \| `slack` \| `hmac` \| `standard_webhooks_asym` \| `asymmetric` \| `token` \| `client_cert`. The HMAC types (`stripe` through `hmac`) and `token` require `secret` (or `secret_file`): a non-empty string or a list of non-empty strings (at most 8), newest first — every key is tried, so a rotation is `secret: ["${NEW}", "${OLD}"]`. An empty key would verify whatever anyone signs with it, so an empty value (an unset `${VAR:-}` included) is a load error. `secret_file` is `secret` read from a file, a path or a list of paths, exclusive with `secret` ([Secret sources](#secret-sources)). `stripe`, `standard_webhooks`, and `slack` also take `tolerance_seconds` (default 300, at most 3600: see [Custom HMAC schemes](#custom-hmac-schemes)). `hmac` takes the descriptor keys below. `standard_webhooks_asym` and `asymmetric` (provider public key), `token` (static credential) and `client_cert` (mTLS certificate pin) have their own keys: see [Public-key, token and client-certificate verifiers](#public-key-token-and-client-certificate-verifiers). |
| `on_verify_failure` | `reject` \| `quarantine` \| `accept_flag`: what happens when verification fails. |
| `trust_url_tenant` | `true` \| `false` (default `false`). With `routing: tenant_path`, a source with `tenant: default` and `verify: {type: none}` files hooks under whatever tenant the URL names, unverified: boot, `check-config` and an admin API write refuse it unless it sets `trust_url_tenant: true`. See [`multi-tenancy.md`](multi-tenancy.md#tenant-scoping-what-tenant_id-actually-does). |
| `sinks` | At least one; every sink is tried on every delivered hook. |
| `dedupe` | Collapse provider retries at ingest. A preset name (`github`, `standard_webhooks`, `svix`, `shopify`, `stripe`), or a mapping with exactly one of `preset` (same names), `header` (the event-id header, e.g. `X-GitHub-Delivery`) or `json` (a dot path into the body, e.g. `data.id`), plus an optional `ttl_seconds` (default 259 200 = 72 h). Hooks that share a key within the TTL get the same `201` and the original id, with `"duplicate": true` on the later ones. Keys are scoped to the tenant and the source, so two tenants behind one `tenant_path` source never collapse each other's events. The key travels into every delivery as `dedupe_key`. |
| `forward_headers` | Which provider request headers travel into sink messages (and `Sink.Http` requests): a list of names (lowercased; still minus the never-forwarded set below), or absent for the default set: every header except `authorization`, `proxy-authorization`, `cookie`, `x-api-key`, `host`, `content-length`, `content-type`, `connection`, `keep-alive`, `transfer-encoding`, `te`, `trailer`, `upgrade`, `expect`, and every `x-ankusa-*` header. (`:default` is the Elixir value only; YAML takes a list or nothing.) |
| `max_headers` | The most forwarded headers one sink message carries (default 64, a positive integer). Names are kept in order of arrival; one past the limit is dropped and counted (`ankusa_sink_headers_dropped_total{reason="count"}`). |
| `forward_headers_max_bytes` | The most bytes of forwarded header names plus values one sink message carries (default 16384, a positive integer). A header that would pass it is dropped whole, never truncated, and counted (`reason="bytes"`); later, smaller headers are still considered. The message then stays near `inline_max_bytes × 4/3` plus about 1 KiB plus this, however large the provider's headers are. |
| `store_headers` | `drop` / `hash` lists of header names that override `http.store_headers` for this source, key by key (an absent list inherits). See [Stored headers](#stored-headers). |
| `retry` | Overrides `dispatch.retry` for this source, key by key (an absent key inherits): `base_ms`, `max_ms`, `max_attempts`, `jitter`, and `max_elapsed_ms`, the horizon after which a delivery still failing since its first attempt is dead-lettered (default `86400000`, 24 h). Same ranges as `dispatch.retry`. A source that routes to a claim-checking sink in an object store cannot retry longer than `claim_check.bucket_expiry_days` allows: boot refuses it, and the admin API answers `422 retry_exceeds_claim_expiry`. See [Retry horizon](delivery.md#retry-horizon). |

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
| `hash` | `sha256` | `sha256` \| `sha512` \| `sha1`. `sha1` is no longer collision resistant: it works for a provider that still signs with it, but boot logs one warning naming the source and every verification counts toward `ankusa_verifier_weak_hash_total{instance,source_id,scheme}`. No named scheme uses it. |
| `encoding` | `hex` | `hex` (lowercase) \| `base64`. |
| `secret_decode` | `raw` | `raw` (use `secret` as the key) \| `whsec_base64` (strip `whsec_` then Base64-decode). |
| `timestamp_header` | none | Header carrying the Unix-seconds timestamp, both signed as `{ts}` and checked against `tolerance_seconds`. |
| `tolerance_seconds` | `300` | The replay window in seconds for `timestamp_header` (and for `stripe`, `standard_webhooks` and `slack`): a hook whose timestamp is further than this from the node's clock fails verification. `0` to `3600`; `3601` or a negative value is a load error naming `sources.<id>.verify.tolerance_seconds`, and the same bound is enforced when an embedded instance boots. A window longer than an hour leaves a captured signature replayable for just as long. |

Providers that are not body-HMAC, Twilio (SHA1 over the full request URL plus
sorted form params) and PayPal (RSA over a certificate fetched from a provider
URL), cannot be described by this engine. They need a bespoke
`Ankusa.Verifier` module.

### Public-key, token and client-certificate verifiers

A shared secret is the wrong tool when the provider can sign with a private key,
and the only tool when it can only send a static credential or present a
certificate. These four types cover those; each fails closed at load on a key,
token or pin it cannot use, naming the YAML key.

```yaml
listener:
  tls: {certfile: /etc/ankusa/tls/server.pem, keyfile: /etc/ankusa/tls/server-key.pem,
        cacertfile: /etc/ankusa/tls/providers-ca.pem, verify: verify_peer}  # for client_cert
sources:
  partner:                                  # Standard Webhooks v1a: Ed25519
    verify: {type: standard_webhooks_asym, public_key: "whpk_11qYAYKxCrfVS/7TyWQHOg7hcvPapiMlrwIaaPcHURo="}
    on_verify_failure: reject
    sinks: [{type: log}]
  signer:                                   # any Ed25519 or RSA provider, described inline
    verify:
      type: asymmetric
      algorithm: rsa_sha256                 # ed25519 (default) | rsa_sha256
      public_key: ["${SIGNER_KEY_NEW}", "${SIGNER_KEY_OLD}"]   # PEM, newest first
      signature_header: "X-Signature"
      encoding: hex                         # hex | base64 (default)
      timestamp_header: "X-Timestamp"       # optional: signed as {ts}, replay window
      signed: "{ts}.{body}"
    sinks: [{type: log}]
  internal:                                 # a static credential
    verify: {type: token, secret: "${INTERNAL_TOKEN}"}     # Authorization: Bearer <secret>
    on_verify_failure: reject
    sinks: [{type: log}]
  mesh:                                     # the client certificate of the mTLS handshake
    verify: {type: client_cert, fingerprints: ["${MESH_CERT_SHA256}"]}
    on_verify_failure: reject
    sinks: [{type: log}]
```

**`standard_webhooks_asym`** and **`asymmetric`** hold the provider's *public*
key, so a leaked config forges nothing. `public_key` is required: one key or a
list of at most 8, newest first, every one tried, so a rotation is
`public_key: ["${NEW}", "${OLD}"]`. A key is `whpk_<base64>` (the Standard
Webhooks form) or the bare base64 of the 32 raw bytes for Ed25519, a PEM
`PUBLIC KEY` block, or, for RSA, a PEM `RSA PUBLIC KEY` block or the bare base64
of either DER form; a key that is not a key of the scheme's algorithm is a load
error naming `sources.<id>.verify.public_key[<n>]`. `standard_webhooks_asym`
takes `public_key` and `tolerance_seconds` only (header `webhook-signature`,
entries `v1a,<base64>`, signed `<webhook-id>.<webhook-timestamp>.<body>`;
entries that are not `v1a,` are ignored, so a provider that signs both ways can
feed an HMAC and a public-key source). `asymmetric` takes the descriptor keys of
[Custom HMAC schemes](#custom-hmac-schemes) (`signature_header`, `parse`,
`sig_prefix`, `sig_key`, `version`, `signed`, `timestamp_header`,
`tolerance_seconds`, `encoding`) with `algorithm` in place of `hash` and
`secret_decode`.

| `algorithm` | Signature |
| --- | --- |
| `ed25519` | RFC 8032, 64 bytes. |
| `rsa_sha256` | RSASSA-PKCS1-v1_5 over SHA-256. A modulus under 2048 bits or an exponent under 65537 is refused. |

A forged flood is bounded: a signature whose decoded length no configured key
signs with is dropped before any public-key operation, and at most 8 are tried
per request.

**`token`** is for a provider that can only send a credential. It proves the
sender knows the secret, not what the body is, and a captured request replays:
prefer a signature whenever the provider has one. `secret` (or `secret_file`) is
required, as for HMAC (one, or a list of at most 8 for rotation; empty is a load error).
`location` is where it travels:

| `location` | The credential is | Extra key |
| --- | --- | --- |
| `bearer` (default) | `Authorization: Bearer <secret>` | none |
| `basic` | `Authorization: Basic base64(<secret>)`; the secret is `user:password` | none |
| `header` | the value of a header | `header: <name>` (required) |
| `query` | the value of a query parameter | `param: <name>` (required); URLs are logged by every proxy on the path, so this is the weakest form |

The comparison hashes both sides, so neither the secret nor its length shows in
timing. The credential is never stored: the header it travels in is added to the
source's `store_headers.drop` whatever the instance policy says, so it is in no
stored hook, archive segment, claim pack or sink message. A quarantined hook is
stored without it, so a quarantine replay cannot re-verify a token hook (it
stays in the pen): use `on_verify_failure: reject` here.

**`client_cert`** accepts a request whose mutual-TLS client certificate is
pinned: `fingerprints` (SHA-256 of the DER, 64 hex characters, any case,
optional `:` separators, as `openssl x509 -fingerprint -sha256` prints it)
and/or `subjects` (`dns:<name>`, `email:<address>`, `uri:<uri>`, `ip:<address>`
from the subject alternative names, or `cn:<common name>`); a list, or one
comma-separated string, and a certificate matching either is accepted. At least
one is required. A fingerprint pin holds whatever the CA bundle says; a subject
pin trusts `listener.tls.cacertfile` to have issued that identity only to the
party meant, so scope that bundle to a CA that does. The certificate is the one
the TLS handshake accepted, so the source only works on a listener with
`tls.verify: verify_peer`: boot and `check-config` refuse a `client_cert` source
otherwise, and a request with no certificate is `401`. Like a token source, use
`on_verify_failure: reject`: a quarantined hook keeps no certificate to re-verify.

The Elixir forms are `{Ankusa.Verifier.Asymmetric, scheme: :standard_webhooks_asym,
public_key: "whpk_…"}` (or `scheme: %Ankusa.Verifier.Asymmetric.Scheme{…}`),
`{Ankusa.Verifier.Token, secret: "…", location: :bearer | :basic | {:header, name} |
{:query, param}}` and `{Ankusa.Verifier.ClientCert, fingerprints: [...], subjects: [...]}`.
`/v1/config` shows `location`, `fingerprints` and `subjects`, and hides `secret`
and `public_key`.

### Sinks

| `type` | Keys |
| --- | --- |
| `log` | none |
| `http` | `url` (`https://` unless `egress.allow_insecure_http`), `method` (`post` \| `put` \| `patch`), `headers`, `timeout_ms` (5000), `secret` (**required** unless `allow_unsigned: true`: a `whsec_` secret, or a list while rotating, signs every delivery the Standard Webhooks way; `secret_file` reads it from a file instead, see [Secret sources](#secret-sources)), `allow_unsigned` (`false`; `true` sends deliveries with no signature, so any host that can reach the URL can inject hooks. A `secret` still signs when both are set. A sink with neither does not load, and the admin API answers `400 invalid_source` for a source that has one), `max_response_bytes` (65536; only the status is used), `cacertfile` (a PEM CA bundle to verify the receiver's certificate against, for a private CA; the file must exist at load), `allow_insecure_tls` (`false`; `true` skips certificate verification for this sink). `/v1/config` shows `allow_unsigned` (a boolean) and hides `secret`. Deliveries are not ordered; a consumer that needs order has to rebuild it from data it receives and tolerate redelivery. The receiver contract is in [`integrations.md#http-handoff-any-language`](integrations.md#http-handoff-any-language). |
| `rabbitmq` | `url`, `exchange`, `exchange_type` (`topic` \| `direct` \| `fanout` \| `headers`), `routing_key`, `inline_max_bytes` (65536), `max_message_bytes` (134217728, RabbitMQ's default `max_message_size`), `max_inflight` (256 unconfirmed publishes per connection; past it a publish fails fast and is retried), `idle_timeout_ms` (600000; the connection closes after this long with no delivery, `0` never; at least 16000). |
| `kafka` | `brokers` (a list, or one comma-separated string), `topic`, `key` (a static string), `inline_max_bytes` (65536), `max_record_bytes` (1000000; a record over it is never produced: see below), `ssl`, `sasl` (`mechanism` = `plain` \| `scram_sha_256` \| `scram_sha_512`, `username`, `password`), `idle_timeout_ms` (600000; the client closes after this long with no delivery, `0` never; at least the produce timeout plus 1000). |
| `nats` | `servers` (a list, or one comma-separated string; failover, one server at a time, picked at random per connect), `subject`, `inline_max_bytes` (65536), `max_message_bytes` (1048576, NATS's default `max_payload`; see below), `publish_timeout_ms` (5000), `tls`, `auth` (one scheme: `username` + `password`, `token`, or `nkey_seed` + `jwt`), `idle_timeout_ms` (600000; the connection closes after this long with no delivery, `0` never; at least `publish_timeout_ms` plus 1000). The stream must already exist. See [`delivery.md`](delivery.md#sinknats-subject-delivery). |
| `redis` | `url` (credentials and db go in it: `redis://:password@host:6379/0`, `rediss://` for TLS), `channel` (a static string), `inline_max_bytes` (65536), `max_message_bytes` (536870912, Redis's bulk limit), `publish_timeout_ms` (5000), `idle_timeout_ms` (600000; the connection closes after this long with no delivery, `0` never; at least `publish_timeout_ms` plus 1000). Pub/sub keeps no copy, so a publish nobody is subscribed to is an error and the sink is never durable. See [`delivery.md`](delivery.md#sinkredis-pubsub-delivery). |
| `sqs` | `queue_url`, `region`, `endpoint` (default: the queue URL's origin; set it for a VPC endpoint or an emulator), `message_group_id` (a static string), `inline_max_bytes` (65536), `max_message_bytes` (1048576, SQS's limit; body plus message attributes, a larger message is dead-lettered at once), `timeout_ms` (5000), `access_key_id` / `secret_access_key` / `session_token` (omit for the `AWS_*` env, IRSA, or the EC2 instance role). FIFO when `queue_url` ends `.fifo`: the group defaults to `tenant/source` and the deduplication id is the hook id (SQS dedupes for 5 minutes). The queue must already exist. See [`delivery.md`](delivery.md#sinksqs-queue-delivery). |
| `google_pubsub` | `project`, `topic` (the id, not `projects/…/topics/…`), `endpoint` (default `https://pubsub.googleapis.com`; set it for a regional endpoint or an emulator), `ordering_key` (a static string; default none), `inline_max_bytes` (65536), `max_message_bytes` (10000000, Pub/Sub's limit; data plus attributes plus ordering key, a larger message is dead-lettered at once), `timeout_ms` (5000), `auth` = `metadata` \| `token` \| `none` (default `metadata`: the GCE/GKE metadata server; `token` is required when `auth: token`; `none` sends no credential, for an emulator). The topic must already exist, and a topic with no subscription drops the message. See [`delivery.md`](delivery.md#sinkgooglepubsub-google-cloud-pubsub-delivery). |

Every sink except `log` also takes `ordered` (default `false`) and `ordering_key`
(`{header: x-entity-id}` or `{json: data.object.customer}`), read by core, not by
the sink: with `ordered: true` the hooks of one lane (`tenant`, source, sink,
key) are delivered one at a time, in order. `ordering_key` without
`ordered: true` is an error, except on `google_pubsub`, where a plain string
keeps its meaning. `ordered: true` needs `wal.type` `disk` or `dual` and, on `sqs`,
a `.fifo` queue; a `header` key must survive `store_headers` (the key is shown in
`/v1/config`, it is not a secret). No instance key is added. See
[Ordering lanes](delivery.md#ordering-lanes).

Bodies above a sink's `inline_max_bytes` are checked in to the object store and
the message carries a claim reference. See [`claim-check.md`](claim-check.md).

A message is also checked against the sink's `max_message_bytes` before it is
published (`max_record_bytes` for Kafka). An inline body whose base64 message
would not fit is checked in instead; a body that is over the limit by itself
(`inline_max_bytes` at least its size) is dead-lettered at once as
`{:message_too_large, size, max}`, with no broker call. Budget for an inline
message: `inline_max_bytes × 4/3` plus about 1 KiB of envelope plus the
forwarded headers (at most `forward_headers_max_bytes`, 16 KiB by default); the
64 KiB default is about 106 KiB, under every transport's limit.

The `http` sink's `url`, `headers` and `secret` are static strings here. An
embedding app can set each to a function of the envelope instead, to route a
shared source's tenants to their own receivers; those connect through a bounded
per-instance pool that takes no settings. See
[`delivery.md`](delivery.md#sinkhttp-per-tenant-destinations).

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
| `ANKUSA_WAL_TYPE` | `wal.type` (`disk`, `dual`, `none`; the queue's mode) |
| `ANKUSA_INTAKE_NODE_ID` | `intake.node_id` (`wal.type: dual`: the node's stable identity in the intake log) |
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
default, or a `${` that is not a variable, file or secret-manager reference) is
a load error naming the key, never a literal string.

Sources and sinks are not env-overridable: they carry behaviour, so they live in
the file, with secrets injected through `${VAR}`, `${file:…}` or `${sm:…}`.

### Secret sources

A secret reaches the file three ways, all resolved when the file is loaded and
again on a [reload](#rotating-without-a-redeploy):

| Form | Reads |
| --- | --- |
| `${VAR}` | the environment |
| `${file:/abs/path}` | the file, minus one trailing newline (what a Kubernetes secret volume or a Docker secret holds). Absolute path, at most 64 KiB. Anywhere a string goes |
| `secret_file: /abs/path` (or a list) | the same, as `secret:` of a `verify` block or an `http` sink (a source's or `lifecycle`'s). Exclusive with `secret`; a list is a rotation window. The path may use `${VAR}`; `${file:${VAR}/key}` is not a reference |
| `${sm:<provider>:<ref>}` | a secret manager, through the provider of that name under `secrets.providers` |

```yaml
secrets:
  cache_ttl_ms: 0                       # how long a ${sm:} value is reused by the next load (0..86400000)
  providers:
    prod-vault:
      type: vault
      address: https://vault.internal:8200   # https, or http on a loopback address
      token: "${file:/var/run/secrets/vault-token}"
      mount: secret                      # KV v2 mount (default secret)
      namespace: team-a                  # optional, Vault Enterprise
      timeout_ms: 5000
    aws:
      type: aws                          # AWS Secrets Manager
      region: us-east-1
      # endpoint, access_key_id, secret_access_key, session_token: optional; without keys
      # the usual chain is walked (environment, EKS web identity, EC2 instance profile)

sources:
  stripe:
    verify:
      type: stripe
      secret:
        - "${sm:prod-vault:ankusa/stripe#whsec}"   # field whsec of secret/ankusa/stripe
        - "${sm:aws:prod/stripe-old}"               # a whole secret
```

`#<field>` picks one field of a JSON secret (Vault KV v2 data, or an AWS
`SecretString` holding a JSON object). Vault with no `#` needs exactly one
field. A provider's own credentials come from `${VAR}` or `${file:…}`, never
from another `${sm:}`. Providers are `AnkusaServer.SecretProvider.Vault` and
`AnkusaServer.SecretProvider.AwsSecretsManager`, behind the
`Ankusa.SecretProvider` behaviour (`fetch(ref, opts)` returning
`{:ok, binary} | {:error, :not_found | :unavailable}`).

A reference that cannot be resolved at boot (a missing file, a manager that is
down, a secret that is not there) is a load error naming the key and the
reference, never a value. Secrets and tokens never appear in a message, and
`providers` (a token among its keys) is not part of the running config, so
`print-config` and `GET /v1/config` never show it. `secret_file` and `${file:}`
are read from the file of this host only: a source
an API client submits cannot use them.

#### Rotating without a redeploy

`AnkusaServer.Secrets.reload/2` (run from `remote`:
`AnkusaServer.Secrets.reload(:default)`) re-reads the file, resolves every
reference again and republishes the sources the file declares
(`source_store.type: static`): a rotated secret takes effect without a restart.
Nothing triggers it on a signal or over HTTP yet. Rotation is two reloads and a
list: put the new secret first and keep the old one second (`secret: [new,
old]`, or `secret_file: [/new, /old]`) until the provider has switched, reload,
then drop the old one and reload again. A hook that verifies with either secret
is accepted in between.

A reference that fails *on a reload* does not take verification down: it keeps
the value of the last good load, and the reload logs a warning naming the key
and the reference and returns it under `:kept`. A reference that has no
previous value (a path you just added) is an error and nothing is applied, as
is a file that no longer parses or a source the instance would refuse to boot
with. `cache_ttl_ms` above `0` makes a reload reuse a `${sm:}` value fetched
within that many milliseconds (a manager with an API quota); `refresh: true`
ignores it. With `source_store.type: persistent` or `redis` the file's sources
are seeds, which a reload does not republish. A reload republishes `sources`
only: a secret under `lifecycle`, `storage` or any other key is read at boot
and changes with a restart.

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
`ArgumentError` at boot (fail fast on a typo, not at 3am). Every map-valued
section is **deep-merged** over the defaults, so pass only the keys you want to
change: `:listener`, `:batcher`, `:dispatch`, `:storage`, `:claim_check`,
`:backup`, `:admin`, `:metrics`, `:routes`, `:rate_limits`, `:dedupe`,
`:fleet`, `:quarantine`, `:queue`, `:archive`, `:lifecycle`, `:audit`,
`:tenants`, `:egress` (`egress.pools` is merged one level down), `:secrets` and `:intake`.

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
    partitions: 4,
    max_batch: 256,
    max_commits: 64,
    group_commit_max_bytes: 4_194_304,
    group_commit_max_records: 1_024,
    group_commit_delay_ms: 1,
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
    sink_concurrency: 8,
    breaker_failures: 5,
    breaker_open_ms: 30_000,
    breaker_max_open_ms: 300_000,
    tenants: %{
      default: %{max_inflight: 1024, max_inflight_bytes: 33_554_432, concurrency: 16},
      "acme" => %{max_inflight: 2048, concurrency: 24}
    },
    retry: {Ankusa.RetryPolicy.Exponential, []}
  },
  storage: %{
    blob_store: {Ankusa.BlobStore.LocalFS, []},
    key_prefix: "",
    codec: {Ankusa.Codec.Raw, []},
    roll_bytes: 16 * 1024 * 1024,
    roll_ms: 30_000,
    interval_ms: 1_000,
    max_segments_per_tick: 16
  },
  claim_check: %{
    port: 4001,
    ip: "127.0.0.1",
    pack_max_bytes: 16_777_216,
    retention_days: nil,
    sweep_interval_ms: 3_600_000,
    redeem_grace_ms: 86_400_000,
    sweep_max_objects: 10_000,
    bucket_expiry_days: nil,
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
    read_cache_max_entry_bytes: 1024 * 1024,
    max_concurrent_reads: 1_024,
    ref_signing_keys: [],
    ref_ttl_ms: nil,
    accept_unsigned_refs: nil,
    authorize: nil
  },
  backup: %{
    enabled: false,
    interval_ms: 60_000,
    keep: 3,
    blob_store: nil,
    secondary: nil,
    timeout_ms: 120_000,
    verify_interval_ms: nil,
    signing_key: [],
    final_timeout_ms: 20_000,
    max_age_ms: nil,
    require_fresh: false
  },
  admin: %{enabled: false, port: 4002, ip: "127.0.0.1", gauge_interval_ms: 15_000},
  routes: %{
    enabled: false,
    max_routes: 1_000_000,
    memory_budget_bytes: 8_589_934_592,
    store: {Ankusa.Routes.Store.ETS, []},
    cache: %{max_size: 50_000, ttl_ms: 30_000, negative_ttl_ms: 5_000, gc_interval_ms: 60_000},
    trusted_proxies: [],
    ip_rules: %{default: :allow, rules: []},
    admin: %{port: 4003, ip: "127.0.0.1"},
    log_sample: 100,
    ip_denied_status: 403,
    seed: []
  },
  rate_limits: %{
    default: nil,
    tenants: %{},
    per_ip: nil,
    node: nil,
    sources: %{default: %{bytes_per_second: 100_000_000}}
  },
  quarantine: %{burst: 100, rate: 20, max_bytes: 1_073_741_824},
  audit: %{retention_days: 365},
  tenants: %{max: 1_000_000},
  lifecycle: %{sinks: []},
  egress: %{
    allow_insecure_http: false,
    deny_cidrs: ~w(0.0.0.0/8 127.0.0.0/8 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16
                   100.64.0.0/10 169.254.0.0/16 ::/128 ::1/128 fc00::/7 fe80::/10),
    allow_cidrs: [],
    allow_hosts: [],
    deny_hosts: ["metadata.google.internal"],
    pools: %{
      pool_size: 32,
      pool_count: 1,
      pool_timeout: 5_000,
      pool_max_idle_ms: 60_000,
      conn_max_idle_ms: 30_000,
      max_pools: 1_024,
      max_pools_per_tenant: 256,
      protocols: [:http1, :http2]
    }
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
| `wal` | `:disk` | The queue's durability mode; the name is historical. `:disk` fsyncs every hook in this node's RocksDB store before the ack. `:dual` fsyncs it here **and** confirms it in a replicated intake log before the ack; it needs `intake.type`, `intake.node_id`, `backup.enabled` with `backup.max_age_ms`, and the `:edge` role, and boot refuses otherwise (see [ADR 0002](https://github.com/jamescarr/ankusa/blob/main/docs/adr/0002-dual-commit-intake-log.md)). `:none` commits nothing: ingest acks on the sinks' confirm, every static source needs at least one sink whose `:ok` means durable (`c:Ankusa.Sink.durable?/1`), and the queue's reader roles (`:dispatch`, `:storage`) are dropped from `roles`. See [`delivery.md`](delivery.md#direct-mode). |
| `direct_publish_timeout_ms` | `8_000` | `wal: :none` only: the one deadline every sink must confirm under (YAML `wal.publish_timeout_ms`). Keep it below the provider's own timeout. |
| `intake.type` | `nil` | `:kafka` under `wal: :dual` (the `ankusa_kafka` package, the only implemented adapter). The loader also accepts `:jetstream`, for which no adapter has shipped, and `:memory`, a test double (`Ankusa.Intake.Memory`), not a production mode. |
| `intake.node_id` | `nil` | This node's identity in the log, `[a-z0-9._-]{1,64}`, **stable across a replacement**: the replacement node starts with the lost node's id and replays its records. A store stamped with another id refuses to boot. |
| `intake.fleet`, `intake.topic` | `"default"`, `nil` | The log's name is `intake.topic`, else `ankusa.intake.<fleet>` (a Kafka topic). |
| `intake.kafka`, `intake.jetstream`, `intake.memory` | `[]` | The adapter's own options (a keyword list, passed through whole). Redacted in `GET /v1/config`: only their keys and non-secret identifiers show. |
| `intake.publish_timeout_ms` | `2_000` | The one publish attempt's deadline (100-60 000, and under the batcher's 15 s record deadline under `dual`). A timeout is a failure; the reconciler republishes. |
| `intake.reconcile_deadline_ms` | `20_000` | A half-persisted hook the reconciler has not completed within this is *stuck*: its fleet dedupe claim is released and `ankusa_intake_reconcile_stuck` rises (1 000-600 000; under `dedupe.scope: :fleet` it must be below `fleet.pending_ttl_ms`). |
| `intake.breaker_failures`, `intake.probe_ms`, `intake.retry_after_s` | `3`, `1_000`, `2` | The breaker opens after this many failed attempts in a row, lets one batch probe after `probe_ms`; while open every new hook is `503 intake_unavailable` with this `Retry-After` (1-100, 100-60 000, 1-300). |
| `intake.offset_flush_ms` | `1_000` | How often the watermark (`m:intake_seq`) and offset (`m:intake_offset`) are written without traffic (100-60 000). |
| `intake.pending.max_records`, `intake.pending.max_bytes` | `50_000`, `268_435_456` | The pending set (records not yet confirmed in both places) is bounded; a full set is `503 intake_unavailable` (`ankusa_intake_refused_total{reason="backlog"}`) before a seq is assigned. |
| `intake.max_record_bytes` | `10_000_000` | One intake record's ceiling; at least `max_body_bytes + 65 536` so a body the node accepts always fits. |
| `intake.recover_on_boot`, `intake.verify_topic` | `true`, `true` | Replay the log into the store at boot (`false`: run `mix ankusa.recover` by hand); verify the log's settings at boot (`false` says `intake topic settings UNVERIFIED` in the startup line). |
| `batcher.partitions` | `4` | One group-commit `GenServer` per partition: where request bodies are held, delivery rows encoded and dedupe keys read (a prefetch the writer re-validates). Partitions do not add commits: the queue writer merges every partition's queued batches into one group (`batcher.group_commit_*`), so the number buys parallel preparation and fault isolation, not fsyncs. |
| `batcher.max_batch` | `256` | Flush once this many envelopes have queued in one partition. |
| `batcher.max_commits` | `64` | Commits one partition keeps in flight. A batch is handed to the writer without waiting for the one ahead, so the writer finds every waiting caller in its mailbox and merges them into one group (and, under `wal.type: dual`, a publish overlaps the next local write); a caller waits for its own group, not for the commit ahead of it plus its own. The writer applies a group without an fsync and the queue's syncer (`Ankusa.Queue.Syncer`) fsyncs the WAL once for every group applied so far, so the next group is assembled while one syncs ([`storage.md`](storage.md)). Measured with 256 closed-loop clients, 10 KB bodies, 4 partitions and a 5 ms fsync (`bench/store_commit.exs`, `SWEEP=1`): 1 slot, 64 hooks per commit and 5.6 k hooks/s; 64 slots, 130 per commit and 10.7 k/s (before the pipelined fsync). `1` is the old one-at-a-time behaviour. Bounded by `batcher.max_queue` either way. |
| `batcher.group_commit_max_bytes` | `4_194_304` (4 MiB) | The most body bytes the writer merges into one group, across partitions: one store write, one fsync and, under `wal.type: dual`, one produce request. Every caller in a group waits for the whole group, so this bounds the ack latency a backlog adds (64 MiB at ~1 GB/s of WAL write is 65 ms before the fsync, which alone breaks the 50 ms p99); a single call larger than the cap is still committed alone. |
| `batcher.group_commit_max_records` | `1_024` | The most hooks in one group. A call that would pass either cap starts the next group. |
| `batcher.group_commit_delay_ms` | `1` | How long a saturated writer waits for the callers it just answered to call again, so they join the next group instead of forming the one after it (a closed loop of clients otherwise splits into two cohorts that commit alternately, each half the size it could be). Applies only when the writer's last group held 64 or more hooks, so it never delays a request at low concurrency (a wait there costs most of the throughput of a 32-client closed loop once the fsync is cheap); `0` disables it. |
| `batcher.max_delay_ms` | `0` | Commit immediately. The store commit runs in a task, so waiting is a scheduling hop rather than head-of-line blocking. Raise it to trade a little ack latency for larger batches. |
| `batcher.max_queue` | `10_000` | Bound per partition, counting buffered **and** in-flight records; full means `{:error, :overload}` → `503`. |
| `batcher.max_queue_bytes` | `268_435_456` (256 MiB) | The same bound in body bytes, buffered and in flight, so a burst of large hooks sheds load (`503`) before it exhausts memory. |
| `dispatch.batch` | `128` | Delivery rows claimed per store scan. |
| `dispatch.concurrency` | `32` | Max sink deliveries in flight at once. Keep Req's Finch pool (default 50) at least this large for `Sink.Http`. |
| `dispatch.max_inflight` | `4096` | Max claimed, unfinished deliveries. Bounds how much a stalled destination can hold. |
| `dispatch.max_inflight_bytes` | `134_217_728` (128 MiB) | ...and the max sum of their stored hook sizes. |
| `dispatch.attempt_timeout_ms` | `30_000` | A delivery attempt that has not returned after this is killed and counts as a failed attempt (`{:attempt_timeout, ms}`). Keep it above every sink's own timeout. |
| `dispatch.sink_concurrency` | `8` | Each sink of each source of each tenant (`{tenant_id, source_id, sink index, module}`) has its own queue and its own breaker; at most this many of its attempts run at once. `nil` leaves only the global `concurrency` cap. A value above `concurrency` is accepted: the global cap binds first. It is also a rate ceiling: one key delivers at most `sink_concurrency` ÷ the sink's latency per second (8 attempts of 5 ms is 1 600 a second, of 100 ms is 80), so raise it, or set `nil`, for a source whose volume needs more. |
| `dispatch.tenants.default` | `%{max_inflight: 1024, max_inflight_bytes: 33_554_432, concurrency: 16}` | A tenant's share of the three global limits above: the most claimed, unfinished deliveries it may hold, the most stored hook bytes in them, and the most of its attempts running at once. A tenant whose sink is slow fills its own window and nobody else's; the global limits still apply on top. A tenant is the hook's tenant id (`default` for a hook without one). Defaults are a quarter of the global window and half of `concurrency`: **a deployment whose hooks all share one tenant should raise them to the global values.** Each key is a positive integer. |
| `dispatch.tenants["acme"]` | _(none)_ | Overrides any of the three keys for the tenant with that id; a key it leaves out comes from `default`. In YAML the tenant id is the key under `dispatch.tenants` (`default` there is always the default entry, so a tenant named `default` takes it). `ankusa_dispatch_tenant_inflight{tenant}` reports the claimed rows of the 100 tenants holding the most. |
| `dispatch.breaker_failures` | `5` | Consecutive failures that open a sink's breaker; `0` never opens one. An open sink's rows wait without spending attempts. |
| `dispatch.breaker_open_ms`, `dispatch.breaker_max_open_ms` | `30_000`, `300_000` | How long a breaker stays open, doubling per consecutive open up to the max; one probe attempt then closes it or reopens it. In memory: a restart closes every breaker. |
| `dispatch.retry` | `{Ankusa.RetryPolicy.Exponential, []}` | `{module, opts}` implementing `Ankusa.RetryPolicy`: the instance's policy; a source's `retry:` lays options over `opts`. Whatever the module, `opts[:max_elapsed_ms]` (default `86_400_000`, 24 h; 1000 to 2 592 000 000) is the horizon: a delivery still failing that long after its first attempt is dead-lettered. The shipped policy's other options: `base_ms` (100), `max_ms` (300 000), `max_attempts` (`nil`: unlimited), `jitter` (`true`). |
| `dispatch.max_retry_after_ms` | `3_600_000` | The longest pause a sink's `Retry-After` or throttle answer (`{:retry_after, ms, _}`) can impose on one retry. 1000 to 86 400 000. The horizon still ends the delivery. |
| `egress.pools.pool_size`, `egress.pools.pool_count` | `32`, `1` | HTTP/1 connections per pool, and shards per pool, of the pool each receiver address gets (`Ankusa.HttpClient.Pools`). Integers 1 to 1024 and 1 to 64. Several sinks posting to one receiver share one pool, so keep `pool_size * pool_count` at or above `dispatch.concurrency`: boot warns when it is not, and a pool smaller than the concurrency that can reach it caps that receiver's throughput at `pool_size ÷ latency` with nothing to say so. A sink overrides them with `pool: [pool_size:, pool_count:, pool_timeout:, protocols:]`. |
| `egress.pools.pool_timeout` | `5_000` | How long a request waits for a free connection of its pool; then that attempt fails as a transport error and is retried. 1 to 600000. |
| `egress.pools.pool_max_idle_ms`, `egress.pools.conn_max_idle_ms` | `60_000`, `30_000` | A pool with no request in flight for `pool_max_idle_ms` is closed by the sweep (every 30 s), and so is a deleted source's idle pool at once; a connection idle for `conn_max_idle_ms` is not reused. Positive integers. |
| `egress.pools.max_pools`, `egress.pools.max_pools_per_tenant` | `1_024`, `256` | The most live pools of the instance, and the most one tenant's deliveries opened. At a ceiling the least recently used idle pool (the tenant's own, for its ceiling) is closed to make room; when every pool in scope has a request in flight the request is refused as `{:error, {:unavailable, :pool_limit}}` and retried. `max_pools * pool_size * pool_count` is the most connections this can open: boot warns when it passes `ulimit -n`. Integers 1 to 65536. Gauge `ankusa_http_pools`, counters `ankusa_http_pool_closed_total{reason}` and `ankusa_http_pool_refused_total{scope}`. |
| `egress.pools.protocols` | `[:http1, :http2]` | The HTTP versions a pooled connection may negotiate (ALPN). `[:http2]` alone multiplexes one connection per shard (`pool_size` does not apply). |
| `storage.blob_store` | `{Ankusa.BlobStore.LocalFS, []}` | `{module, opts}` implementing `Ankusa.BlobStore`. See [`storage.md`](storage.md). |
| `storage.key_prefix` | `""` | Prepended to the key of every new segment: `""`, or one or more `name/` parts (`[A-Za-z0-9._-]`). Nodes sharing one bucket each take their own, or share the prefix and differ by `intake.node_id` (the dated layout puts the node in the key). The archive catalogue records each segment's full key, so changing the prefix moves only new segments; archived ones are still read where they were written. Claims are never prefixed. |
| `storage.codec` | `{Ankusa.Codec.Raw, []}` | `{module, opts}` implementing `Ankusa.Codec`: segment record framing. A module that exports `id/0` has that one-byte id written in the segment header (`Raw` is `1`). |
| `storage.roll_bytes` | `16 MiB` | Roll a new segment past this size: a backlog of this many bytes is written at once. |
| `storage.roll_ms` | `30_000` | ...and a smaller batch (a partial segment) is written once its oldest hook has waited this long. `0` writes every tick. An archive replay's `to` must be at least `roll_ms + interval_ms` old, so the compactor has written the window. |
| `storage.interval_ms` | `1_000` | Compactor tick interval. |
| `storage.max_segments_per_tick` | `16` | The most segments one compactor tick writes; the rest of a backlog waits for the next tick. A positive integer. |
| `storage.blob_store` `:sse` | not set | `sse: %{type: :aes256}` or `%{type: :kms, key_id: key}` in the options of `Ankusa.BlobStore.S3`, `GCS`, `Azure` (`key_id` = the encryption scope) and `OCI`: server-side encryption, sent on every `put/4` of that store (segments, backups, claim packs: whichever are configured on it). Checked at boot; `LocalFS` refuses it. See [`Ankusa.BlobStore.SSE`](storage.md#encryption-at-rest). |
| `secrets.kek` | `nil` | The key-encryption key for stored source secrets: 32 bytes, base64 (`openssl rand -base64 32`), optionally `"<id>:<base64>"` (the id is written into every value it encrypts; without one it is a digest of the key). Encrypts the secret-bearing fields (those `Ankusa.Admin.Redact` hides) of every spec `Ankusa.SourceStore.Persistent` or `Ankusa.SourceStore.Redis` stores, with AES-256-GCM. A stored source no configured key opens refuses the boot. `/v1/config` shows it as `[REDACTED]`. |
| `secrets.kek_previous` | `[]` | Keys that may still decrypt, never encrypt: the old key while a rotation is rolled out. Needs `kek` or `kek_provider`. |
| `secrets.kek_provider` | `nil` | `{module, fun, args}` instead of `kek`: called once when the source store starts, returning `{:ok, [kek \| previous]}` (each a `kek` string or `{id, key}`; the first is current). Exclusive with `kek`. A provider that fails refuses the boot. |
| `claim_check.port` | `4001` | The `:claim_check` role's Bandit port. |
| `claim_check.ip` | `"127.0.0.1"` | The address the gateway binds: a strict IPv4/IPv6 literal (or an `:inet` tuple). Loopback by default; `"0.0.0.0"` exposes it on every interface, which a container needs before a published port reaches it. |
| `claim_check.pack_max_bytes` | `16_777_216` | Target size of one claim pack; a body larger than this still gets a pack of its own. Must be a positive integer. |
| `claim_check.retention_days` | `nil` | LocalFS-only sweeper retention; `nil` disables the retention pass. A pack a claim lease holds is never deleted, whatever its date. |
| `claim_check.sweep_interval_ms` | `3_600_000` | Sweeper tick interval: the lease pass and one tenant's claim partitions. A tick that left work behind in its tenant is followed by one after 5 s, and one that finished a tenant with more to visit by a step after 100 ms, so a pass over many tenants runs back to back and then waits this interval. |
| `claim_check.redeem_grace_ms` | `86_400_000` | How long after its hook is delivered, or evicted from the DLQ, a claim pack stays leased, so the consumer of a queue message that carries the ref can still redeem it. At least the longest a consumer can lag its broker; ≥ 1. See [`claim-check.md`](claim-check.md#claim-leases). |
| `claim_check.sweep_max_objects` | `10_000` | The most packs, and the most lease rows, one sweeper tick deletes; the rest is `ankusa_claim_check_sweep_backlog` and waits for the next tick. ≥ 1. |
| `claim_check.bucket_expiry_days` | `nil` | Object-store claims only: the age, in days, at which the bucket's lifecycle rule expires `claims/` objects. A rule cannot see claim leases, so boot refuses a value below the retry horizon + `queue.dlq.retention_days` + the redeem grace, each in whole days. The horizon is the longest of `dispatch.retry`'s `max_elapsed_ms` (24 h, so 1 day, by default) and the `retry.max_elapsed_ms` of every source that routes to a claim-checking sink; a source written later (the admin API, `Ankusa.SourceStore.put/6`) that would outlast the rule is refused with `422 retry_exceeds_claim_expiry`, and a stored source that does refuses boot. `nil` logs the minimum instead. `nil` or ≥ 1. |
| `claim_check.blob_stores` | `nil` | `[{module, opts}, ...]`, the stores claim packs are written to and read from; `nil` uses `[storage.blob_store]`. A `LocalFS` claim store takes `root:` (an absolute path). A failed write moves to the next store; a read searches every one. Removing a store makes its claims unreadable. See [`claim-check.md`](claim-check.md#several-stores-and-failover). |
| `storage.blob_store`, `backup.blob_store`, `backup.secondary`, `claim_check.blob_stores` transport options | see the row below | The HTTP stores (`S3`, `GCS`, `Azure`, `OCI`) take these adapter options besides their credentials: `:timeout_ms` (`10_000`, a request carrying at most 1 MiB), `:upload_timeout_ms` (`60_000`, one request carrying more), `:op_deadline_ms` (`120_000`, 1 000 to 3 600 000: **one absolute deadline per blob operation**, covering every attempt, multipart part and, for a claim check-in, pack-id retry; a `Retry-After` that does not fit it is `{:error, {:throttled, seconds}}` at once), `:multipart_threshold_bytes` (`8_388_608`) and `:multipart_part_bytes` (`8_388_608`; at least 5 MiB, a multiple of 256 KiB), `:retries` (`2`, 0 to 10), `:breaker_failures` (`5`; `0` turns the per-`{endpoint, bucket}` breaker off), `:breaker_open_ms` (`30_000`), `:breaker_slow_ms` (`5_000`: the breaker also opens when the p99 of recent requests passes it), `:pool_size` (`32`), `:pool_count` (`1`) and `:pool_timeout` (`5_000`) of the Finch pool the instance starts for the endpoint. A value out of range is a boot error naming the store. See [`storage.md`](storage.md#conditional-writes-retries-checksums-multipart-pools). |
| `claim_check.placement` | `:ordered` | Which store a new pack goes to: `:ordered` the first, the others only while it fails; `:hash` the pack id's random bits pick one, spreading packs over every store. |
| `claim_check.store_cooldown_ms` | `30_000` | A store that failed a write, or could not answer a read, is skipped by later writes and searched last by later reads for this long, then tried again. Positive integer. |
| `claim_check.key_layout` | `:flat` | `:flat` = `claims/tenant=<t>/dt=<d>/<pack>`; `:sharded` adds `shard=<c>/` under the tenant (32 prefixes per tenant, against per-prefix request limits like S3's `503 SlowDown`). Reads try both layouts. See [`claim-check.md`](claim-check.md#sharded-keys). |
| `claim_check.read_deadline_ms` | `5_000` | The gateway answers every claim request within this: past it, `503 store_unavailable`. A store read that outlives it still finishes and fills the cache. Also bounds `redeem/3` on a `:claim_check` node. See [`claim-check.md`](claim-check.md#under-load). |
| `claim_check.read_timeout_ms`, `claim_check.read_retries` | `3_000`, `1` | Per store request while reading a claim, and the retries after a failed one. A claim store's own `timeout_ms`/`retries` options are for writes. |
| `claim_check.read_max_inflight` | `48` | Per store, the ceiling of the gateway's adaptive limit on concurrent store reads: it halves when the store throttles (`429`, `503`) or does not answer, and grows back by one per round of successes. The HTTP stores read through a client connection pool of 50 per host (claim reads get a pool of their own, keyed by `read_timeout_ms`), which is why the default sits under 50: a ceiling above it only queues reads inside the client, and one that waits past the client's 5 s checkout timeout fails as a transport error. Two claim stores on one host share that pool. |
| `claim_check.read_queue_max` | `256` | Per store, reads waiting for a free slot; past it a read is answered `503` at once. `0` never waits. |
| `claim_check.read_cache_bytes` | `134_217_728` | Memory for the gateway's cached pack indexes and claims; `0` turns the cache off. |
| `claim_check.read_cache_max_entry_bytes` | `1_048_576` | A claim larger than this is served but never cached. |
| `claim_check.max_concurrent_reads` | `1_024` | Gateway requests this node serves at once, cache hits included. A request past it is `503 overloaded` with `Retry-After` 1–3 before anything is read; `0` is no limit. The count is a set of request processes the reader sweeps every second, so a request process that dies never keeps a slot. |
| `claim_check.integrity_check` | `manifest` | `manifest`: the gateway checks every whole-claim `GET` against the sha256 its pack's manifest records for that claim, and answers `502 integrity_mismatch` rather than send bytes that differ (a damaged claim, or a manifest that does not parse). The manifest is read once per pack, the first time one of its claims is read, and kept in the pack's cache entry (`claim_check.read_cache_bytes`, 32 bytes a claim); `200` and `206` carry the digest as `x-ankusa-sha256`. `off` serves the store's bytes unchecked and sends no digest. With `read_cache_bytes: 0` the manifest is read again for every `GET`: set `off` there. See [`claim-check.md`](claim-check.md#integrity). |
| `claim_check.ref_signing_keys` | `[]` | `[current \| previous]`: strings of at least 16 bytes. With a key, every claim a node checks in gets a **v2 ref**, `urn:ankusa:claim:v2:<tenant>:<claim_id>:<exp_unix>:<signature>`, an HMAC-SHA256 under the first key over the tenant, claim and expiry, and the gateway holds each request to it before it reads anything (`400 bad_signature`; `410 ref_expired`). A ref signed with any listed key is served, so a rotation is: put the new key first, drop the old one once every ref it signed has expired. Set the same keys on every claim-writing node, the gateway, and any node calling `redeem/3`. `/v1/config` shows each as `[REDACTED]`. `[]` mints v1 refs. See [`claim-check.md`](claim-check.md#the-reference). |
| `claim_check.ref_ttl_ms` | `nil` | How long a v2 ref is good for, from the moment it is minted; positive. `nil` is the retention (`claim_check.retention_days`), else 30 days. |
| `claim_check.accept_unsigned_refs` | `nil` | Whether the gateway (and `redeem/3`) still serves a v1 ref, which carries no expiry. `nil` means only while `ref_signing_keys` is empty, so setting keys refuses every v1 ref (`400 unsigned_ref`); `true` keeps the refs already in queues working while keys roll out. `false` with no keys is a boot error. |
| `claim_check.authorize` | `nil` | `{module, fun, args}`, Elixir config only: called after authentication and before any read, as `apply(module, fun, [%{principal, tenant_id, claim_id, conn} \| args])`. `:ok` lets the request through; anything else is `403 forbidden`. It replaces the built-in check (`claims:read` plus `tenant:<tenant_id>` or `tenant:*`). The module must export `fun/(length(args) + 1)` at boot. |
| `backup.enabled` | `false` | Back the store up to an object store while it runs, and restore the latest backup at boot when the store directory holds no database. A backup location that can't be read then refuses the boot (`{:store_restore_failed, path, reason}`) instead of starting empty. See [`storage.md`](storage.md#backup-and-restore). |
| `backup.interval_ms` | `60_000` | How often a checkpoint is uploaded: the most a lost host loses. Only files the last backup doesn't already have are uploaded. |
| `backup.keep` | `3` | Backups kept; older ones, and the files only they used, are deleted after each successful backup. |
| `backup.blob_store` | `nil` | `{module, opts}` for backup objects; `nil` uses `storage.blob_store`. Keys are under `storage.key_prefix` either way: a backup is one node's, and a node whose prefix holds another store's backups refuses to back up. With `LocalFS` the backup shares the host's fate, and the node logs a warning saying so. |
| `backup.secondary` | `nil` | `{module, opts}` of a second object store (another region or provider), under the same `storage.key_prefix`. Every backup is written to the primary and then to it, with the same checks; a run succeeds only when both hold it (a failing secondary is retried with backoff, and the primary is not uploaded again meanwhile). At restore the primary is tried first; if it is unreachable, empty, or fails a check, the secondary is. Its `:sse` option is checked like every other store's. |
| `backup.timeout_ms` | `120_000` | The most one object-store call of a backup, a verify or a restore may take before it counts as failed. A positive integer. |
| `backup.verify_interval_ms` | `nil` | Every this often the latest backup of every store is read back and checked (manifest signature, every file's size and sha256) without writing anything; a failure is logged and counted by `ankusa_backup_verify_failed_total{target}`. `nil` = never. A positive integer. |
| `backup.signing_key` | `[]` | HMAC-SHA256 key(s), each a string of at least 32 bytes (`openssl rand -base64 32`), as a string or a list: the first signs every manifest, any of them verifies, so a rotation lists the new key first and keeps the old one until its backups have aged out. With keys configured a restore refuses an unsigned or mis-signed manifest (`{:manifest_invalid, id, :unsigned \| :bad_signature}`); with none it checks nothing. `/v1/config` shows each as `[REDACTED]`. |
| `backup.final_timeout_ms` | `20_000` | A clean stop (SIGTERM, `docker stop`) takes one last backup before the store closes, at most this long. A positive integer; the supervisor waits this plus 10 s. |
| `backup.max_age_ms` | `nil` | `/ready` carries `"backup": "ok"` or `"failing"` (reason `"stale"`) once the last successful backup is older than this (or the uploader is not running). `nil` = no `backup` key. At least `backup.interval_ms`. |
| `backup.require_fresh` | `false` | A stale backup makes `/ready` answer `503`. Needs `backup.max_age_ms`. |
| `listener.ready_sink_error_window_ms` | `5_000` | `wal: :none` only: a sink whose last publish failed within this many ms makes `/ready` `503` with `"sinks": "failing"` (reason `"publish_failed"`). A sink that publishes again clears its own entry at once, so the window only bounds how long a failure counts when nothing publishes. YAML `health.sink_error_window_ms`. An integer >= 1. Not secret: `/v1/config` shows it. |
| `listener.ready_min_free_bytes` | `nil` | The free bytes the file system holding `data_dir` must keep. Below it `/ready` is `503` with `"disk": "failing"` (reason `"below_floor"`), before the store hits `ENOSPC`. Measured by `:disksup` (`:os_mon`), which core does not start and the server image does (an embedding app adds `:os_mon` to its `extra_applications`): with a floor and no `:disksup`, or a `df` that takes over 2 s, the reason is `"unmeasured"` and the node is unready. YAML `node.min_free_bytes`. `nil` = no `disk` key. An integer >= 1. Not secret. |
| `queue.max_oldest_due_ms` | `nil` | `/ready` carries `"backlog": "failing"` (reason `"over_threshold"`) while the oldest due delivery has waited longer than this, read from the same counters as `ankusa_queue_oldest_due_age_seconds`. A label: the answer stays `200`, since taking a node that is behind out of the load balancer only moves the load. `nil` = no `backlog` key. An integer >= 1. Not secret. |
| `admin.enabled` | `false` | Start the admin API and `Ankusa.Metrics` on this instance. Off for embedded use; the `jamescarr/ankusa` image turns it on. |
| `admin.port` | `4002` | The admin API's Bandit port. |
| `admin.ip` | `"127.0.0.1"` | The address the admin API binds, same rules as `claim_check.ip`. |
| `admin.gauge_interval_ms` | `15_000` | How often `Ankusa.Metrics.Gauges` samples store/queue/quarantine depth, disk free, dispatch in-flight and open breakers for `/metrics`. |
| `admin.tls` | `nil` | A TLS block for the admin listener; see [Listeners](#listeners-limits-tls-and-mtls). |
| `admin.auth` | `%{mode: :none}` | Who may call the admin API: `mode` (`:none`, `:token`, `:jwt`, `:mtls`), `allow_insecure`, `tokens`, `jwt`, `mtls`. `mode: :none` is accepted only when `admin.ip` is loopback (or `allow_insecure: true`). See [Control-plane authentication](#control-plane-authentication). |
| `metrics.enabled`, `metrics.port`, `metrics.ip` | `false`, `4004`, `"127.0.0.1"` | A listener serving only `/metrics`, `/health` and `/ready` (`Ankusa.Metrics.Router`); see [A separate metrics listener](#a-separate-metrics-listener). |
| `metrics.tls` | `nil` | A TLS block for the metrics listener. |
| `metrics.detail` | `:tenant` | Which identity labels the per-hook series carry: `:tenant` (`tenant_id`), `:source` (`tenant_id` and `source_id`) or `:none`. See [Metric cardinality](#metric-cardinality). |
| `metrics.max_series_per_metric` | `5000` | The most label sets one metric keeps; later tenants/sources are folded into an `"other"` series (one slot is reserved for it). A positive integer. |
| `claim_check.tls`, `routes.admin.tls` | `nil` | A TLS block for the claim-check gateway and the route-management API. |
| `routes.admin.auth`, `claim_check.auth` | `%{mode: :none}` | The same block for the route-management API and the claim-check gateway. |
| `routes.enabled` | `false` | Enforce route management. Off captures every `POST`, as before; on is **deny by default** — see [Route management](#route-management). |
| `routes.max_routes` | `1_000_000` | Hard cap on definitions. Creating one past it is a `409`; nothing is ever evicted. The table is indexed (see [Route management](#route-management)), so a miss, a create and a list page cost the same at a million routes as at a thousand. |
| `routes.memory_budget_bytes` | `8_589_934_592` | What this node may spend on route definitions. At boot `max_routes` is multiplied by the per-route estimate (5 KiB: the snapshot rows plus the store's copy of the definition, measured by `bench/routes_bench.exs`; a million routes are about 5 GB of RAM) and a cap that does not fit is a config error naming both keys; a table that a Redis store loads that does not fit refuses the boot (or, on a reload, keeps the table the node is serving). Lower `max_routes` or raise the budget, never one without the other. |
| `routes.store` | `{Ankusa.Routes.Store.ETS, []}` | `{module, opts}` implementing `Ankusa.Routes.Store`. `Ankusa.Routes.Store.Redis` (package `ankusa_redis`) shares definitions across nodes. |
| `routes.cache.*` | `max_size: 50_000`, `ttl_ms: 30_000`, `negative_ttl_ms: 5_000`, `gc_interval_ms: 60_000` | The per-request decision cache. Entries are keyed by the published snapshot's `epoch`, so publishing a snapshot retires every earlier decision at once. Only short requests are cached — at most 16 path segments and 256 bytes of path — because the key is the sender's own path; a longer one is still decided correctly, but by a match against the table's index on every request, and its `[:ankusa, :routes, :match]` event carries `cacheable: false` so that cost is visible. `max_size` is approximate between the adapter's 1s memory checks, and an eviction only costs a re-match: it can never change a decision. `ttl_ms` must stay under `gc_interval_ms`. |
| `routes.trusted_proxies` | `[]` | CIDRs whose peers may set `X-Forwarded-For`. Empty means the header is never read. |
| `routes.ip_rules` | `%{default: :allow, rules: []}` | Ordered global rules, first match wins, plus the `default` when none match. |
| `routes.admin.port` | `4003` | The management API's own Bandit port. Authenticated by `routes.admin.auth`, like `admin.port`. |
| `routes.admin.ip` | `"127.0.0.1"` | The address the management API binds, same rules as `claim_check.ip`. |
| `routes.log_sample` | `100` | 1 in N rejections is logged at `:debug`; `0` disables it. |
| `routes.ip_denied_status` | `403` | Status for an IP denial, or `404` for uniformity with a missing route. With `403` a sender can tell a route that has its own `ip_rules` (which denied it) from a path that does not exist (`404`); `404` removes that distinction. |
| `routes.seed` | `[]` | Route definitions loaded at boot (see below). With the ETS store they are loaded on **every** boot. |
| `rate_limits.default`, `rate_limits.tenants` | `nil`, `%{}` | Per-tenant ingest limits, `%{rate: hooks_per_second, burst: hooks}`; fractions are allowed (`0.5` is one every two seconds) and both keys are required. Optional: `bytes_per_second` (a number > 0) and `burst_bytes` (an integer ≥ 1, needs `bytes_per_second`; default one second's worth), `max_body_bytes` (an integer from 1 to the instance's `max_body_bytes`; `413` past it), `max_queue` and `max_queue_bytes` (integers ≥ 1: what the tenant may hold in this node's batchers at once; past either is `429 tenant_overload`). Precedence is a runtime override, then the tenant's own entry, then `default`, and the first one found applies whole; no limit means unlimited. A `default` is required when the route resolver is `RouteResolver.TenantPath`. See [Rate limits](#rate-limits). |
| `rate_limits.per_ip`, `rate_limits.node` | `nil`, `nil` | `%{rate: hooks_per_second, burst: hooks}` charged before the body is read: per client address (`routes.trusted_proxies` decides whose `X-Forwarded-For` counts; an IPv6 address is its `/64`) and for the whole node. Over it: `429 ip_rate_limited` / `429 node_rate_limited`. `nil` is off. |
| `rate_limits.sources` | `%{default: %{bytes_per_second: 100_000_000}}` | Body bytes per second for each source, charged before the body is read with its declared `Content-Length` (a body with none is charged when read): `429 source_rate_limited`. `%{default: %{bytes_per_second: n, burst_bytes: n}}` (`burst_bytes` defaults to one second's worth); `%{default: nil}` switches it off. CPU protection, not a quota. |
| `rate_limits.scope` | `:node` | `:node` or `:fleet`. Where the tenant quota (`default`, `tenants`: hooks and bytes per second) is counted. `:fleet` shares one bucket per tenant across every edge node through `fleet.store`; `per_ip`, `node` and `sources` stay per node. Needs `fleet.store`. See [Fleet scope](#fleet-scope). |
| `dedupe.scope` | `:node` | `:node` or `:fleet`. `:fleet` claims a source's provider event key in `fleet.store` before the commit, so a retry on another node is `201 duplicate` (the first hook's id) or `409 dedupe_pending`, never a second hook. Needs `fleet.store`. See [Fleet scope](#fleet-scope). |
| `dedupe.max_entries` | `100_000_000` | The most live dedupe keys the node's index holds (about 200 B of disk each). A hook whose key would be a new one past it is refused with `503 dedupe_quota` and `Retry-After: 60` (`outcome="dedupe_quota"` on `ankusa_ingest_requests_total`); it is never stored without its key. Keys expire on their own after the source's ttl (72 h by default), swept by `Ankusa.Queue.DedupeSweeper`; `ankusa_dedupe_keys` is the live count. An integer ≥ 1. Per node. |
| `dedupe.max_entries_per_tenant` | `30_000_000` | The same bound for one tenant, so one tenant cannot fill the node's share: a tenant at 100 hooks/s with the 72 h ttl holds about 26 M keys. An integer ≥ 1. |
| `fleet.store` | `nil` | `{module, opts}` implementing `Ankusa.RateLimiter.Store` and `Ankusa.Dedupe.Store`: `{Ankusa.RateLimiter.Store.Redis, url: "redis://…", namespace: "ankusa:fleet", pool_size: 4}` from `ankusa_redis`. Required by either `:fleet` scope; `nil` or `{module, keyword}`. |
| `fleet.timeout_ms` | `20` | The hard budget of one fleet-store call, an integer from 1 to 1000. A call that fails or runs out of it is answered from this node alone. |
| `fleet.pending_ttl_ms` | `30000` | How long a claimed dedupe key is "in flight" before another node may claim it; a positive integer, longer than the slowest commit. |
| `fleet.breaker_window_ms`, `fleet.breaker_min_calls`, `fleet.breaker_failure_rate`, `fleet.breaker_open_ms` | `1000`, `20`, `0.5`, `5000` | The fleet breaker: once `breaker_min_calls` calls in a `breaker_window_ms` window were made and `breaker_failure_rate` (a number above 0, at most 1) of them failed or timed out, every hook is answered from this node for `breaker_open_ms`, then one probe decides. Positive integers except the rate. |
| `quarantine.burst`, `quarantine.rate`, `quarantine.max_bytes` | `100`, `20`, `1_073_741_824` | The quarantine pen: one token bucket per source (`burst` hooks back to back, refilled `rate` per second; over it, `429 quarantine_rate_limited`) and a cap on the pen's total bytes (a hook that would cross it is `503 quarantine_full`; the pen never evicts). The bucket and the byte count are ETS compare-and-swap/counter updates in the request process; the writes are group-committed by the pen. See [`delivery.md#quarantine`](delivery.md#quarantine). |
| `audit.retention_days` | `365` | Days the audit trail keeps a row. An hourly sweep deletes older rows, oldest first, in bounded batches (`[:ankusa, :audit, :pruned]`). A positive integer. Every committed row emits `[:ankusa, :audit]`. While `lifecycle.sinks` is set, a row (and its lifecycle entity) at or above `m:lifecycle_cursor` is never deleted, however old: a lifecycle sink that is down longer than the retention keeps the events it has not seen, and the held rows are reported as `ankusa_audit_retention_blocked_by_cursor`. See [Architecture](architecture.md#guarantees-by-component). |
| `audit.max_rows`, `audit.max_bytes` | `10_000_000`, `5_368_709_120` | Ceilings on the audit trail, from counters kept in every batch. Before a change is accepted the trail is pruned to them, oldest first and never past `m:lifecycle_cursor`; a trail still over a ceiling refuses *new* control-plane changes with `503 audit_backlog` and `Retry-After: 60`. An unpublished row is never dropped. Positive integers. |
| `audit.outbox_max_backlog` | `100_000` | With `lifecycle.sinks` set, the most audit `seq`s (`m:audit_seq` minus the lifecycle cursor) no sink has confirmed before new changes are refused with `503 audit_backlog`. `ankusa_audit_outbox_backlog` is the gauge. A positive integer. |
| `queue.dlq.retention_days`, `queue.dlq.max_rows`, `queue.dlq.max_bytes` | `30`, `10_000_000`, `53_687_091_200` | The dead-letter queue's retention and ceilings (`Ankusa.Queue.DlqReaper`, every 10 s). A dead row is evicted when older than `retention_days`, and oldest first while the DLQ holds more than `max_rows` rows or `max_bytes` stored bytes, **only while its hook is archived and findable** (`Storage.fetch/2` answers). A DLQ still over a ceiling with nothing archived left to evict refuses new hooks with `503 dlq_full` and `Retry-After: 60` (`ankusa_dlq_full`); an acked hook is never deleted to make room. Every eviction is one audit row per tenant and batch (`dlq.evict`) and `ankusa_dlq_evicted_total{tenant, reason}`. Needs the `:storage` role to ever evict. Positive integers. |
| `quarantine.retention_days` | `14` | Days a quarantined hook is held; an hourly sweep deletes older entries (audit row `quarantine.retention`, `ankusa_quarantine_pruned_total`) unless a running or paused `kind: quarantine` replay job can still reach them. A positive integer. |
| `storage.catalogue_retention_days` | `365` | Days the archive catalogue keeps a segment's row (`?S`) after the newest hook in it, and a 0.3 import's `?L` row after its hook. Past it `Storage.fetch/2` stops finding the hook. A segment holding a hook that still has a delivery row, or one a live archive replay job can read, stays. A positive integer. |
| `archive.retention_days` | `nil` | `nil` keeps segment objects. An integer deletes the segment object and its index through the blob store's `delete/3` together with the catalogue row once the newest hook is that old. At most `storage.catalogue_retention_days`. |
| `tenants.max` | `1_000_000` | The most tenants the store knows (each gets an ordinal at its first committed hook, none is ever reused). A hook of a tenant one past it is refused with `503 tenant_limit` and `Retry-After: 300`, tagged `outcome: :tenant_limit` on the ingest metrics; a known tenant is never refused. Not a secret, so `/v1/config` shows it. An integer from 1 to 4294967294; the store holds about 80 B per tenant plus seven counters of 40 B. |
| `queue.rocksdb.block_cache_bytes` | `67_108_864` (64 MiB) | The LRU block cache shared by every column family. The index and filter blocks of `index` and `deliveries` live in it (`cache_index_and_filter_blocks`), so a store with tens of millions of dedupe keys wants a cache that holds them: at the default 10 bits a dedupe key (a `?u` and a `?e` key) costs about 2.5 bytes of filter, plus its share of the index blocks. An integer ≥ 1. |
| `queue.rocksdb.bloom_bits_per_key` | `10` | Bloom filter on whole keys in the `index` family (dedupe keys, due/inflight/dead rows) and the `deliveries` family: a first-seen dedupe key costs one filter probe per file instead of an index and data block read. 10 bits is about a 1 % false-positive rate. `0` turns the filters off and restores the plain block-cache table options. A filter exists only in files written after it was turned on, so an upgraded store gains it as compaction rewrites its files; the store format does not change, so no migration runs. `0` or an integer from 1 to 32. |
| `queue.rocksdb.max_open_files` | `65_536` | Table-file handles RocksDB keeps open; `-1` keeps every file open. The process's file-descriptor limit still applies. `-1` or an integer ≥ 1. |
| `queue.rocksdb.compaction_style` | `:level` | `:level` or `:universal`, for every column family. |
| `queue.rocksdb.rate_limiter_bytes_per_sec` | `0` | Caps the bytes per second flushes and compactions write, so background I/O leaves room for the synced commits; `0` is unlimited. An integer ≥ 0. |
| `queue.rocksdb.max_background_jobs` | `4` | The flush and compaction thread budget. An integer ≥ 1. |
| `queue.rocksdb.write_buffer_bytes` | `%{hooks: 64 MiB, deliveries: 16 MiB, index: 16 MiB, counters: 1 MiB, default/archive/quarantine/audit/tenants: 4 MiB}` | The memtable of each column family, a map merged over the defaults (an unknown family fails the boot). Each at least 65536. `counters` stays small on purpose: a read applies every merge operand its memtable still holds. The whole database's memtable total (`db_write_buffer_size`, 256 MiB) and WAL ceiling (`max_total_wal_size`, 512 MiB) are fixed. Not secrets: `/v1/config` shows the whole `queue.rocksdb` section. |

Boot checks ranges, not only types, and fails with `"<dotted.key> must be
<constraint>, got <value>"`: every port is `0..65535`; `max_body_bytes`,
`direct_publish_timeout_ms`, the `batcher` and `dispatch` sizes and counts,
`storage.roll_bytes`, `claim_check.sweep_interval_ms`,
`claim_check.redeem_grace_ms`, `claim_check.sweep_max_objects`,
`claim_check.store_cooldown_ms`, `claim_check.read_deadline_ms`,
`claim_check.read_timeout_ms`, `claim_check.read_max_inflight`,
`claim_check.read_cache_max_entry_bytes`, `backup.interval_ms`,
`backup.keep`, `backup.timeout_ms`, `backup.final_timeout_ms`,
`admin.gauge_interval_ms`, `metrics.max_series_per_metric`,
`routes.max_routes`, `routes.memory_budget_bytes` and every
numeric `listener.*` key (`max_connections`, `num_acceptors`, the three
timeouts, `max_requests_per_connection`, `max_header_count`,
`max_header_length`) must be positive; `listener.http2` and `metrics.enabled`
are booleans, `metrics.ip` an IP address, and a `tls` block names existing
`certfile`/`keyfile`, a `verify` of `verify_none`/`verify_peer` (the latter
with a `cacertfile`), non-empty `versions` from `tlsv1.2`/`tlsv1.3` and known
`ciphers`;
`batcher.max_delay_ms`, `batcher.group_commit_delay_ms`, `storage.roll_ms`, `storage.interval_ms`,
`claim_check.read_retries`, `claim_check.read_queue_max`,
`claim_check.read_cache_bytes` and `claim_check.max_concurrent_reads` may be
`0` (no linger, roll at once, a compactor that runs only when asked, no read
retry, no waiting for a slot, no claim cache, no request cap);
`claim_check.ref_ttl_ms` is `nil` or ≥ 1, `claim_check.accept_unsigned_refs` is
`nil`, `true` or `false`, every `claim_check.ref_signing_keys` entry is at
least 16 bytes, and `claim_check.integrity_check` is `off` or `manifest`;
`claim_check.retention_days` and `claim_check.bucket_expiry_days` are `nil` or ≥ 1; `audit.retention_days` is ≥ 1; `tenants.max` is an integer from 1 to 4294967294; every `queue.rocksdb` key is in the range its row names (`write_buffer_bytes` per family, ≥ 65536, an unknown family refused); `storage.key_prefix` is `""` or
`name/` parts; `dispatch.sink_concurrency` is `nil` or ≥ 1,
every `dispatch.tenants.default` key and every key of a `dispatch.tenants` entry is
an integer ≥ 1 (a tenant entry holds only `max_inflight`, `max_inflight_bytes`,
`concurrency`, under a non-empty tenant id),
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

#### Metric cardinality

A scrape must not grow with the number of tenants or sources, so the per-hook
series carry an identity label only as far as `metrics.detail` says, and each
metric keeps a bounded number of them.

| `metrics.detail` | Labels on the ingest, verify-failure, quarantine, rate-limit and delivery counters | Also |
| --- | --- | --- |
| `:tenant` (default) | `tenant_id` | the top-100 `ankusa_tenant_pending` and `ankusa_dispatch_tenant_inflight` gauges (label `tenant`) |
| `:source` | `tenant_id` and `source_id` | the `ankusa_ingest_duration_seconds` histogram carries both, too |
| `:none` | neither | no per-tenant gauges: the series set is the same for one tenant and for a million |

`metrics.max_series_per_metric` (default `5000`) caps the label sets one metric
keeps. A metric admits its first `max_series_per_metric - 1` label sets that
carry an identity; every later one is counted in a series whose identity
labels (`tenant_id`, `source_id`, `tenant`) are `"other"`, with its other
labels (`outcome`, `sink`, …) intact, so nothing is lost from a total and
`sum without (tenant_id, source_id) (…)` is unchanged. The `"other"` series
uses the last of the slots (one per combination of the metric's other labels);
a tenant that is really named `other` shares it. The two per-tenant gauges
drop a tenant past the ceiling instead, since `"other"` would show whichever
tenant reported last. A series that was admitted is kept until the node
restarts. A request for a source that does not exist never carries a label
(`ankusa_ingest_refused_total{reason="unknown_source"}`), whatever the setting.

Worst case, a scrape has up to `max_series_per_metric` series for each of the
eight identity-carrying counters (`ankusa_ingest_requests_total`,
`ankusa_verify_failures_total`, `ankusa_verifier_weak_hash_total`,
`ankusa_quarantine_rate_limited_total`, `ankusa_quarantine_full_total`,
`ankusa_rate_limit_rejected_total`, `ankusa_dispatch_deliveries_total`,
`ankusa_dispatch_dead_lettered_total`) and each of the two gauges, whatever the
number of tenants. The platform's budget is 10 000 series per node: with many
active tenants, lower `max_series_per_metric` to fit it, or set `detail:
none`, which makes the series set independent of the tenant count (the number
of tenants is `ankusa_tenants`). The ingest and delivery counters are the ones
that fill first; the failure counters (`verify_failures`, `weak_hash`,
`quarantine_*`) only grow with tenants that have failures.

#### The admin API

With `admin.enabled: true`, every node serves `GET /health` (the process
answers), `GET /ready` (every readiness check that applies to this node, each
listed as `ok`, `failing` or `draining`: the store takes a synced write,
checked at most once a second, and no write failed in the last 5 s; plus the
intake log, the sinks, the disk floor, the backlog and the backup where
configured — see [Running the container](deployment.md#running-the-container); `503` with
`Retry-After: 1` when one that gates is not `ok` — the ingest
port serves the same `/ready`), `GET /metrics`
(Prometheus text), `GET /v1/config` (the effective config; adapter option
values are hidden unless on an allowlist of non-secret keys such as `region`,
`bucket` or `topic`, and URL passwords and query values are hidden),
`GET /v1/wal` (this node's store stats: `next_seq`, and the `hooks`,
`deliveries` and `disk_bytes` estimates; `{}` when the store cannot be read,
`409 wal_disabled` under `wal.type: none`), `GET /v1/dlq`, the replay jobs API
— `POST /v1/replays`, `GET /v1/replays`, `GET|PATCH /v1/replays/{id}`; `kind`
is `dlq`, `archive`, `quarantine` or `lifecycle` — and `GET|DELETE /v1/quarantine` (`:edge`
role: list the pen, or purge entries by `source_id`, `id`, `since`/`until`,
`limit`) on `admin.port`, independent of the node's roles. Replay jobs are per
node, so a fleet replay is one `POST /v1/replays` per dispatch node; a
`quarantine` job also needs the `:edge` role, since it releases this node's
pen into this node's queue, and a `lifecycle` job (`since`, `until`, `tenant`)
needs a [lifecycle sink](asyncapi.md#lifecycle-events) on this node: it
re-emits the node's own lifecycle events. Source management is
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
`rate_limits:write`, `routes:read` and `routes:write`, `claims:read`,
`tenants:write` (`DELETE /v1/tenants/{tenant}` and `GET /v1/tenants/{tenant}/erase`,
a [tenant erase](multi-tenancy.md#erasing-a-tenant); it also needs `tenant:*`), and
`dlq:write`, which starting a `dlq` replay also needs (`quarantine:write` for a
`quarantine` replay). `config:write` is reserved: no route uses it yet.
`audit:read` reads `GET /v1/audit`: every change to a source, route, IP rule,
rate-limit override, quarantine purge or replay job, newest first, with
`since`, `until` (ms), `actor`, `action`, `tenant`, `limit` and `cursor`
filters. The trail spans tenants, so a reader without `tenant:*` sees only its
own tenants' rows (none of the rows that belong to no tenant), and naming
another tenant is a `403`. Tenants are scopes too: `tenant:<id>` for one, `tenant:*` for all.
A tenant's resources (its sources and rate limit, its claims) need that tenant.
The DLQ, replay jobs, the quarantine pen and the rate-limit listing hold every
tenant's rows, so they are filtered by tenant instead: a token limited to some
`tenant:<id>` scopes gets only those tenants' rows (the filter is forced on the
server), `?tenant=<id>` narrows a read to one tenant (the `tenant` key of a
`POST /v1/replays` body), and naming a tenant the token does not hold is `403`.
`tenant:*` is what reads, purges or replays across tenants. A quarantine purge
or a replay started by a limited token touches only its tenants' rows, and a
replay job is visible to (and steerable by) a limited token only when it is
scoped to tenants that token holds. Route definitions and IP rules belong to no
tenant and still need `tenant:*`.

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

`store.url`, `store.namespace`, `store.tick_ms`, `store.timeout_ms`, and
`store.load_timeout_ms` are Redis-store keys: with `type: ets` any of them is a
config error, not a silently ignored key (a `url` with no `type` at all still
means Redis). `timeout_ms` (default 5000) bounds every Redis command; a load
reads the definitions a page at a time (`HSCAN`, 1000 per page) so it applies
to each page, and `load_timeout_ms` (default 300000) is the deadline of the
whole load. A million definitions boot and reload without a command near
either.

Every node with the same `namespace` enforces the same routes: writes bump a
version counter and publish it, each node reloads on the broadcast, and a
periodic tick (`tick_ms`, default 30s) is the safety net for a missed one. A
write that arrives in step with the node's version is applied to its snapshot as
one row change; only a write that raced a peer's reloads the whole table. A
node keeps serving its in-memory snapshot through a Redis outage; only writes
report `503 store_unavailable`. The Redis store seeds a namespace once, on its
first boot, so a route deleted there is not resurrected by a restart.

**Matching at scale.** Routes are indexed by the literal segments their pattern
starts with (`/hooks/stripe/:id` under `hooks/stripe`, up to eight segments), so
a request is matched against the few routes that share its leading literals,
not the whole table: a miss with the decision cache out of the picture is
microseconds at a million routes (`bench/routes_bench.exs`). A pattern that
*starts* with a parameter or a wildcard (`/:tenant/hook`) has no literal to
index under and joins one shared list that every request is matched against,
so keep those few; put the literal first (`/hooks/:tenant/...`). Creating a
route checks for a conflicting enabled route by pattern and method in one
lookup, and the route list pages straight from the cursor, so neither slows as
the table grows. The same index answers "which existing patterns overlap this
one, and which tenant owns each" (`Ankusa.Routes.Snapshot.overlaps/4`, for a
future cross-tenant overlap check) by walking one prefix chain, not the table.

**Management API**, on `routes.admin.port` (its own listener, never the ingest
port), authenticated by `routes.admin.auth` like the operator admin API (see
[Control-plane authentication](#control-plane-authentication)): `routes:read`
for `GET` and the dry run, `routes:write` for everything else. It has two
surfaces over the same definitions. `/admin/routes` and `/admin/ip-rules` span
tenants (and the IP rules belong to none), so they need `tenant:*` as well.
`/v1/tenants/:tenant/routes` needs `tenant:<tenant>` instead and shows and
changes only that tenant's routes. The HTTP contract is the `routes` tag of
[`priv/openapi/admin.v1.yaml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa/priv/openapi/admin.v1.yaml),
examples included — and
[`test/ankusa/routes/router_openapi_test.exs`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa/test/ankusa/routes/router_openapi_test.exs)
executes those examples against the implementation, so the document and the code
cannot drift apart:

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/admin/routes?enabled=&limit=&cursor=` | list, id-ordered, cursor-paginated |
| `POST` / `PUT` / `PATCH` / `DELETE` | `/admin/routes[/:id]` | create, replace, update, delete |
| `GET` | `/v1/tenants/:tenant/routes?enabled=&limit=&cursor=` | the same list, for one tenant's routes |
| `POST` / `PUT` / `PATCH` / `DELETE` | `/v1/tenants/:tenant/routes[/:id]` | the same writes, for one tenant's routes |
| `GET` / `PUT` | `/admin/ip-rules` | the global rules and default |
| `POST` | `/admin/routes/test` | dry run: `{method, path, ip}` → decision, reason, route id, and the rule that decided it |
| `GET` | `/health` | `{status, routes}` |

A route is `{id, tenant_id, source_id, path, methods: [POST], enabled,
ip_rules, metadata, revision, inserted_at, updated_at}`. Ids are lowercase slugs
(one namespace across tenants), `id`/`path`/`tenant_id`/`source_id` are immutable
under `PATCH` (moving a route changes what it captures and where it files it —
that is a `PUT`), and two enabled routes may not share a path and method.
`POST /admin/routes/test` answers "why was this rejected" without capturing
anything and **without reading or writing the decision cache**, so a dry run can
never change what live requests decide, and a route change can be checked before
it goes live.

**A route is owned by a tenant and may be bound to a source.** `tenant_id`
(default `default`, the shared tenant) and `source_id` (default none) are held
against what the catch URL's resolver says (see
[Multi-tenancy](multi-tenancy.md#routes-are-owned-by-a-tenant-and-bound-to-a-source)):
a route owned by one tenant admits nothing into another tenant's source, and a
route bound to a source admits nothing for another one. A disagreement is the
`404` an unknown source gets. In a `routes.seed` entry both are plain keys.

**The tenant surface.** `POST /v1/tenants/acme/routes` creates a route owned by
`acme` (a body that names another tenant is `400 invalid_route`, field
`tenant_id`). Another tenant's route is `404` there, never listed, never
changed; an id or an enabled path and method that another tenant holds is
`409 duplicate_route` without `conflicting_id`, so the answer does not say whose
it is. Its delete is checked against the owner in the store itself (a Lua
script, for the Redis store), so a route that was re-created under another
tenant a moment ago is never removed by it. Route paths are one fleet-wide
namespace — a tenant that may write routes can claim any path nobody holds and
shadow a less specific pattern — so give `routes:write` with `tenant:<t>` only to
a principal you would trust with that. The Redis store keeps `tenant_id` and
`source_id` out of a definition that has none (so a node from before them still
reads a table with no tenant routes); once a route owned by a tenant is stored,
an older node refuses the reload and keeps serving what it had until it is
upgraded.

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

Ingest admission. By default each edge node enforces it **in its own memory**: a
fleet of N edges admits up to N × the limit, so size the numbers per node, not
for the fleet — or set `rate_limits.scope: fleet` for the tenant quota (see
[Fleet scope](#fleet-scope)). It has two layers, in the order a request meets
them.

**Before the body is read** — CPU protection for the node, not a quota. These
are charged after the source is looked up and before a byte of the body is
read, so a forged flood costs neither a read nor an HMAC:

```yaml
rate_limits:
  per_ip: {rate: 20, burst: 40}     # hooks/s per client address; off by default
  node: {rate: 5000, burst: 10000}  # hooks/s for the whole node; off by default
  sources:
    default: {bytes_per_second: 100000000}   # body bytes/s per source; the default
```

Each answers `429` with its own error and a `Retry-After`: `ip_rate_limited`,
`node_rate_limited`, `source_rate_limited`. Each is counted on
`ankusa_ingest_refused_total{reason}` and nothing else, so none of them mints
a metric series per sender. The client address is the peer's, or, when the peer
is in `routes.trusted_proxies`, the client in `X-Forwarded-For` (the same rule
as the route guard's `ip_rules`); an IPv6 address counts as its `/64`. Addresses
hash into a fixed 2^20 buckets, so a flood from invented addresses cannot grow
the table. The source ceiling is charged with the request's declared
`Content-Length` (a chunked body is charged once it has been read); a source's
ceiling is generous by default (100 MB/s) and `default: null` switches it off.
An address refused by `per_ip` has not touched the node's bucket.

**After verification** — the tenant's quota. A catch-URL scheme that lets the
sender choose its tenant (`routing: tenant_path`) has as many budgets as there
are tenant names, so it **requires** a `default`: without one the boot is
refused, because every tenant the config does not list would be unlimited.

```yaml
rate_limits:
  default: {rate: 50, burst: 100}
  tenants:
    acme:
      rate: 500                   # hooks per second
      burst: 1000                 # the most admitted back to back
      bytes_per_second: 5000000   # body bytes per second (optional)
      burst_bytes: 10000000       # default: one second's worth
      max_body_bytes: 1000000     # at most http.max_body_bytes; 413 past it
      max_queue: 2000             # hooks held in this node's batchers at once
      max_queue_bytes: 67108864   # body bytes held there at once
```

`rate` and `burst` are required; the rest are optional, and a tenant's entry
replaces the `default` whole (its keys do not merge). A hook is charged **after
verification and before the durable write**, so only hooks verification
accepted spend a tenant's budget: a flood of forged requests is free and can
never lock a tenant out. Over `rate` or `bytes_per_second` the sender gets
`429 rate_limited` with a `Retry-After` header and nothing is stored — no hook
record, no dispatch, no claim. A body bigger than `burst_bytes` is admitted
into a full bucket and empties it, rather than never. `max_body_bytes` is
checked against the declared `Content-Length` before the body is read and
enforced on the bytes received otherwise: `413 payload_too_large`, with that
tenant's limit.

`max_queue` and `max_queue_bytes` are the tenant's share of the group-commit
batchers (`wal: disk`; `wal: none` publishes inside the request and has no
queue): the hooks and body bytes it may have buffered or in flight on this node
across partitions. Past either, the sender gets `429 tenant_overload` with
`Retry-After: 1`, **never** the global `503 overload`, so one tenant filling its
share does not shed the tenants still within theirs. The share is counted when
the hook is handed to the batcher and given back when the call returns, so a
batcher that crashes leaks nothing. Leave them unset and a tenant is bounded by
the partition's `batcher.max_queue` alone, which it can fill for everybody.

The checks cost one ETS compare-and-swap per bucket per request, and a denial
writes nothing; the `[:ankusa, :rate_limit, :rejected]` event and its
`ankusa_rate_limit_rejected_total` counter (tagged by tenant) are the signal for
the tenant's own denials, so rejections are not logged. A hook that clears the
hook bucket and is then denied on bytes has spent its hook token.

Tenant limits can also be adjusted at runtime, with no restart:
`GET /v1/rate-limits` and `GET|PUT|DELETE /v1/tenants/{tenant}/rate-limit` on
`admin.port` (`:edge` role only, `409 role_not_enabled` elsewhere). The body of
a `PUT` takes every key above, and the override replaces the tenant's limit
whole. An override lives on **the node that accepted it** and is persisted in
this node's store (one key per tenant), the same node-local model as
`source_store.type: persistent`'s API-managed sources — put durable limits in
the config. `PUT` and `DELETE` reset that tenant's buckets, so a raised limit
is not held back by the old limit's accumulated debt. There is no "unlimited"
value: to exempt a tenant from a `default`, give it a high limit of its own.

#### Fleet scope

`rate_limits.scope: fleet` and `dedupe.scope: fleet` move two node-local things
into a store every edge node shares (`fleet.store`, Redis from `ankusa_redis`):
the **tenant quota** (hooks and bytes per second, `rate_limits.default` and
`tenants`) and the **provider event key** (a source's `dedupe:`).

```yaml
rate_limits:
  scope: fleet
  default: {rate: 10, burst: 20}
dedupe:
  scope: fleet
fleet:
  store: {type: redis, url: "${REDIS_URL}", namespace: ankusa:fleet}
  timeout_ms: 20
```

With it, two nodes enforce one tenant's `10` hooks per second *combined*, and the
same event posted to each node is one hook. Nodes share a bucket and a dedupe
window only on the same `namespace`, and the limit each applies is the one in its
own config (and its own runtime override: set an override on every node; a `PUT`
or `DELETE` resets that tenant's fleet bucket). `per_ip`, `node` and `sources` are
CPU protection for one node and stay per node: a flood is refused before the body
is read and must not cost Redis a round trip.

**One round trip.** A hook costs one store call: the tenant's charges and the
dedupe claim are one script (`EVALSHA`, with `EVAL` when Redis lost the script).
A tenant with no limit and a source with no dedupe key cost none. A hook that is a
duplicate, or whose key is in flight elsewhere, spends none of the tenant's budget.

**The claim is two-phase.** The key is claimed *pending* ahead of the commit and
promoted to *committed* (with the hook's id, for the source's `dedupe.ttl_ms`)
only after the synced commit, off the request path, in batches. Another node
that meets a *committed* key answers `201` with the first hook's `id` and
`"duplicate": true`; one that meets a *pending* key answers
`409 dedupe_pending` with `Retry-After`, because "duplicate" is a `2xx`, and a
`2xx` for a hook whose commit then failed is a lost webhook. A commit that
fails releases the claim; a node that dies holding one leaves it to expire
after `fleet.pending_ttl_ms`. Pending answers are counted as
`outcome="dedupe_pending"` on `ankusa_ingest_requests_total`.

**A store outage never refuses a hook.** Every fleet call has a hard budget,
`fleet.timeout_ms`, so a Redis that answers in 300 ms costs each hook at most that
budget, not 300 ms (which at 10 000 requests per second would pile up 3 000
requests per node). A call that fails or times out is answered from this node's
own limiter and its own dedupe index (`ankusa_fleet_fallback_total`). When
`breaker_failure_rate` of at least `breaker_min_calls` calls in a
`breaker_window_ms` window failed, a breaker opens and hooks go to the node with no
call at all for `breaker_open_ms`; then one probe call decides whether it closes
(`ankusa_fleet_breaker_transitions_total{state}`). While the node is answering
alone, `ankusa_fleet_limiter_degraded{kind="rate_limit"|"dedupe"}` is `1` and a
warning is logged at most once a minute. In that mode N nodes admit N × the
limit again, and a retry on another node can be a second hook, which the consumer
collapses on the `idempotency_key`.

Settling the claims is bounded too: at most 50 000 outcomes wait for the next
batch; one more is dropped (`ankusa_fleet_settle_dropped_total{reason}`), which
costs a delay or a duplicate, never a hook. Tests:
[`edge_fleet_test.exs`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa/test/ankusa/edge_fleet_test.exs)
(the claim protocol, the breaker, fallback), and, against a real Redis,
[`rate_limiter/store/redis_test.exs`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_redis/test/ankusa/rate_limiter/store/redis_test.exs)
(`fleet limiter`, `fleet dedupe`, `degrades without redis`).

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
- **Pinning.** Every delivery connects to the address that was checked: the URL
  is rewritten to it, with the name in the `host` header, and for `https` the
  name is also the TLS `hostname`, so the certificate is still checked against
  the name (SNI included). The connection pool is keyed by that address and name,
  so every connection a pool opens goes to the validated address and nothing
  resolves the name again; a name that moves is checked on its next attempt,
  before a pool is touched, and one that moves to another public address gets a
  pool of its own. Tests: `sink/http_test.exs` (`every connection of a pool goes
  to the pinned address`, `a name that turns private between two deliveries
  through one pool is denied on the second`), `http_client/egress_test.exs`.

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

#### Outbound connection pools

Every outbound HTTP request that does not name a transport of its own
(`Sink.Http` deliveries, SQS, Pub/Sub, JWKS and secret-provider fetches) goes
through `Ankusa.HttpClient.Pools`, not Req's default of one 50-connection pool per
origin that is never closed. A pool is started on demand for each receiver
address and TLS profile, per instance, and is bounded by `egress.pools`:

| | |
| --- | --- |
| Cap | `egress.pools.pool_size` × `pool_count` connections per pool (32 × 1); `max_pools` live pools per instance (1,024) and `max_pools_per_tenant` per tenant (256), so a tenant with a thousand receivers only ever churns its own pools |
| Sweep | every 30 s a pool with no request in flight for `pool_max_idle_ms` (60 s) is closed; a connection idle for `conn_max_idle_ms` (30 s) is not reused; a deleted source's idle pools are closed at once |
| Metric | gauge `ankusa_http_pools`; counters `ankusa_http_pool_closed_total{reason}` (`idle`, `evicted`, `source`) and `ankusa_http_pool_refused_total{scope}` (`tenant`, `instance`) |
| Refusal | at a ceiling the least recently used idle pool is closed to make room; with every pool in scope serving a request, `{:error, {:unavailable, :pool_limit}}`, retried after the retry policy's backoff. A request that cannot check a connection out within `pool_timeout` fails that attempt as a transport error, also retried |

Connections negotiate HTTP/2 where the receiver offers it (ALPN, `protocols`). Size
`pool_size` for the concurrency that can reach one receiver: several sinks posting
to one endpoint share a pool, up to `dispatch.concurrency` attempts at once, and a
pool of 8 against a 100 ms receiver tops out at 80 requests a second. Boot warns
when `pool_size * pool_count` is below `dispatch.concurrency`, or when `max_pools *
pool_size * pool_count` is more than `ulimit -n`. A sink can set its own
`pool: [pool_size:, pool_count:, pool_timeout:, protocols:]`; a request with its own
`req_options: [finch: [name: …]]` (the object stores' `Ankusa.BlobStore.Pool`) or
`plug:` keeps that transport. Tests: `http_client_test.exs` (`pool reaped`,
`http2`, the two ceilings, the refusal), `bench/pools_bench.exs` (10⁵ origins).

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
      sinks: [{Ankusa.Sink.Http, url: "https://example.internal/stripe", secret: System.get_env("SINK_WHSEC")}]
    ]
  }
```

| Field | Default | Meaning |
| --- | --- | --- |
| `tenant_id` | `"default"` | The storage/retention scope. See [`multi-tenancy.md`](multi-tenancy.md). |
| `verifier` | `{Ankusa.Verifier.None, []}` | `{module, opts}` implementing `Ankusa.Verifier`. `Ankusa.Verifier.Hmac`'s `:secret` is a binary or a list of binaries, newest first — every key is tried, so a rotation is `secret: [new, old]`. `Ankusa.Verifier.Asymmetric` takes `:public_key` (one or a list) and `:scheme` (`:standard_webhooks_asym` or a `%Ankusa.Verifier.Asymmetric.Scheme{}`), `Ankusa.Verifier.Token` a `:secret` and a `:location`, `Ankusa.Verifier.ClientCert` `:fingerprints` and/or `:subjects`: see [Public-key, token and client-certificate verifiers](#public-key-token-and-client-certificate-verifiers). Every one of them that cannot work (no usable key, secret or pin) refuses the instance's boot, naming the source. |
| `on_verify_failure` | `:reject` | `:reject` (`401`, nothing stored) / `:quarantine` (`202`, durable pen; see [`delivery.md#quarantine`](delivery.md#quarantine)) / `:accept_flag` (commits, envelope flagged). |
| `trust_url_tenant` | `false` | Under a `route_resolver` other than `Ankusa.RouteResolver.Path`, a source with `tenant_id: "default"` and `Ankusa.Verifier.None` must set it to `true` (`Ankusa.Verifier.validate_shared!/1`); see [`multi-tenancy.md`](multi-tenancy.md#tenant-scoping-what-tenant_id-actually-does). |
| `sinks` | `[{Ankusa.Sink.Log, []}]` | `[{module, opts}]` implementing `Ankusa.Sink`: one delivery per sink, each retried independently. |
| `dedupe` | `nil` | Collapse provider retries at ingest: `:github \| :standard_webhooks \| :svix \| :shopify \| :stripe`, or `%{header: "X-Event-Id"}` / `%{json: "data.id"}`, each with an optional `ttl_ms` (default `259_200_000`). |
| `forward_headers` | `:default` | `:default` (every header except the never-forwarded set; see [Sources](#sources)) or a list of header names. |
| `max_headers` | `64` | The most forwarded headers one sink message carries; a name past it is dropped (`ankusa_sink_headers_dropped_total{reason="count"}`). A positive integer. |
| `forward_headers_max_bytes` | `16_384` | The most bytes of forwarded header names and values one sink message carries; a header that would pass it is dropped whole (`reason="bytes"`). A positive integer. |
| `store_headers` | `nil` | `nil` (the instance's `store_headers`) or `%{drop: [..], hash: [..]}` overriding it per key; an absent `drop`/`hash` inherits the instance's list. |
| `retry` | `nil` | `nil` (the instance's `dispatch.retry` as it is) or a keyword list of `base_ms`, `max_ms`, `max_attempts`, `jitter`, `max_elapsed_ms` laid over it: how long this source's deliveries retry (`max_elapsed_ms`, default 24 h) and how they back off. |

A source can lay options over the instance's retry policy with `retry:`; it
cannot swap the policy module. `Sink.Http`/`Sink.RabbitMQ`/`Sink.Kafka`/
`Sink.NATS`/`Sink.Redis` retries are all driven by `config.dispatch.retry`, with
the source's `retry:` laid over it, by `Ankusa.Dispatch.Pipeline`. The options
are `base_ms`, `max_ms`, `max_attempts`, `jitter` and `max_elapsed_ms`, with the
ranges of `dispatch.retry`; an unknown key or a value out of range fails boot
(or is a `400 invalid_source` through the admin API). A claim-checking source
whose `max_elapsed_ms` would outlast `claim_check.bucket_expiry_days` is refused
(see that key).

### Every behaviour, at a glance

Full detail (options, guarantees, how to write your own) lives in
[`storage.md`](storage.md) and [`delivery.md`](delivery.md); this table is
the map.

| Behaviour | Job | Default | Also shipped |
| --- | --- | --- | --- |
| `Ankusa.RouteResolver` | Catch-URL scheme → `%Route{tenant_id, source_id}` | `RouteResolver.Path` (`/webhooks/:source_id`) | `RouteResolver.TenantPath` (`/webhooks/:tenant/:source`) |
| `Ankusa.Queue` | Durable ack and seq assignment over this node's store | `wal: :disk` (one synced commit per batch) | `wal: :dual` (the same commit plus a confirmed intake-log publish), `wal: :none` (no queue: ingest acks on the sinks' confirm, see [`delivery.md`](delivery.md#direct-mode)) |
| `Ankusa.Verifier` | Signature/timestamp checks | `Verifier.None` | `Verifier.Hmac` (configurable HMAC engine; named schemes Stripe, GitHub, Standard Webhooks, Shopify, Slack), `Verifier.Asymmetric` (Ed25519 or RSA over the same wire templates; named scheme `standard_webhooks_asym`), `Verifier.Token` (bearer, basic, header or query credential), `Verifier.ClientCert` (mTLS client certificate pin) |
| `Ankusa.SourceStore` | Source config, secrets, policy | `SourceStore.Static` | `SourceStore.Persistent` (API-managed sources in this node's store) |
| `Ankusa.Sink` | What happens to a delivered hook | `Sink.Log` | `Sink.Http` (Req forward), `Sink.SQS` (`SendMessage`, in core), `Sink.RabbitMQ` (exchange publish, `ankusa_rabbitmq` package), `Sink.Kafka` (topic produce, `ankusa_kafka` package), `Sink.NATS` (JetStream subject publish, `ankusa_nats` package), `Sink.Redis` (pub/sub channel publish, `ankusa_redis` package) |
| `Ankusa.RetryPolicy` | Backoff / give-up | `RetryPolicy.Exponential` (jitter) | none |
| `Ankusa.BlobStore` | Segment PUT / range GET / delete | `BlobStore.LocalFS` | `BlobStore.S3` (+R2/MinIO), `BlobStore.GCS` |
| `Ankusa.ClaimCheck` | Pack claims into the object store, redeem by reference | none (the instance's `BlobStore`) | none |
| `Ankusa.Codec` | Segment record framing | `Codec.Raw` (len-prefixed, CRC32) | none |

Swapping a behaviour is a one-line config change, `route_resolver:
{Ankusa.RouteResolver.TenantPath, []}`, because every pluggable layer is a
behaviour with `{module, opts}` config, resolved at the call site, never
hardcoded. The queue is the exception: its mode is `wal: :disk | :dual | :none`,
not a module.

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
