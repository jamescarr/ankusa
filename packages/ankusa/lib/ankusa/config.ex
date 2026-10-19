defmodule Ankusa.Config do
  @moduledoc """
  The configuration struct passed down the supervision tree at start. No
  `Application.get_env/2` buried in call sites — instance-scoped config falls
  out of this for free.
  """

  defstruct instance: :default,
            data_dir: "./data",
            # which OTP roles boot in this node (ANKUSA_ROLES in production)
            roles: [:edge, :dispatch, :storage],
            # Bandit edge
            port: 4000,
            # limits for every HTTP listener this instance runs (the edge, the admin
            # and route-management APIs, the claim-check gateway, the metrics
            # listener), and the TLS block of the ingest listener (see
            # `Ankusa.Listener`).
            listener: %{
              # nil, or %{certfile, keyfile, cacertfile, verify, versions, ciphers}
              tls: nil,
              # open connections per listener, spread over the acceptors
              max_connections: 20_000,
              # nil = two per scheduler, filled in by `new/1`
              num_acceptors: nil,
              # a connection that sends nothing for this long is closed: it bounds a
              # request head dribbled in byte by byte, and an idle keep-alive
              read_timeout_ms: 10_000,
              # one read of the request body must complete within this
              body_read_timeout_ms: 15_000,
              # the whole body must arrive within this, however it is chunked
              request_deadline_ms: 30_000,
              # a keep-alive connection is closed after this many requests
              max_requests_per_connection: 1_000,
              max_header_count: 64,
              max_header_length: 16_384,
              http2: false
            },
            # {module, opts} implementing Ankusa.RouteResolver (URL scheme → identity)
            route_resolver: {Ankusa.RouteResolver.Path, []},
            max_body_bytes: 8_000_000,
            # which provider request headers a hook is stored with, applied after
            # verification (`Ankusa.Edge.StoredHeaders`): `drop` removes a header
            # from the stored hook (and so from segments, claim packs and sink
            # messages), `hash` keeps it with its value replaced by
            # "sha256:<hex>". A source's own `store_headers` overrides it per key.
            store_headers: Ankusa.Edge.StoredHeaders.default(),
            # {module, opts} implementing Ankusa.SourceStore
            source_store: {Ankusa.SourceStore.Static, sources: %{}},
            # :disk (the RocksDB store: every hook is on disk before the ack) or
            # :none: no log, ack on the sink's confirm. With :none, `new/1` drops
            # :dispatch and :storage from :roles (there is nothing for them to
            # read) and requires :edge to remain.
            wal: :disk,
            # direct mode only: the overall deadline every sink must confirm
            # under. Must stay under the provider's own timeout (GitHub's is 10 s).
            direct_publish_timeout_ms: 8_000,
            # group-commit batcher. The queue writer serializes commits itself,
            # so more partitions only add contention now that a partition commits
            # asynchronously instead of holding the caller's message queue.
            batcher: %{
              partitions: 2,
              max_batch: 256,
              # 0 = commit as soon as the batch fills, no linger: the commit is a
              # Task, so waiting costs a scheduling hop, not head-of-line
              # blocking.
              max_delay_ms: 0,
              # load shedding: a partition refuses (503 overload) once it holds
              # max_queue records, or max_queue_bytes of bodies, counting both
              # the buffer and the batch being committed
              max_queue: 10_000,
              max_queue_bytes: 268_435_456
            },
            # dispatch pipeline
            dispatch: %{
              # delivery rows claimed per store scan
              batch: 128,
              # max sink deliveries in flight at once
              concurrency: 32,
              # max claimed, unfinished deliveries...
              max_inflight: 4096,
              # ...and the max sum of their stored hook sizes in bytes
              max_inflight_bytes: 134_217_728,
              # a delivery attempt (the sink call, plus the fallback claim check-in
              # when a pack failed) that has not returned after this is killed and
              # counts as a failed attempt. Keep it above every sink's own timeout
              # (http `timeout_ms` 5 s, nats/redis `publish_timeout_ms` 5 s, kafka
              # produce 5 s, the rabbitmq connection call 15 s).
              attempt_timeout_ms: 30_000,
              # per-sink isolation, keyed by {tenant_id, source_id, sink index, sink
              # module}: at most sink_concurrency attempts of one sink of one
              # tenant run at once (nil = only the global `concurrency` cap)...
              sink_concurrency: 8,
              # ...and a sink that fails breaker_failures times in a row (0 =
              # never) is skipped for breaker_open_ms, doubling per consecutive
              # open up to breaker_max_open_ms. Rows parked by an open breaker
              # keep their attempt count.
              breaker_failures: 5,
              breaker_open_ms: 30_000,
              breaker_max_open_ms: 300_000,
              retry: {Ankusa.RetryPolicy.Exponential, []},
              # Per-tenant shares of the three global limits above, so one tenant
              # cannot exhaust what everyone else needs: `default` applies to every
              # tenant, `tenants["acme"]` (a tenant id string) overrides any of its
              # keys for that tenant. The global limits still apply on top.
              #   max_inflight       claimed, unfinished deliveries of one tenant
              #   max_inflight_bytes ...and the sum of their stored hook sizes
              #   concurrency        attempts of one tenant running at once
              tenants: %{
                default: %{
                  max_inflight: 1024,
                  max_inflight_bytes: 33_554_432,
                  concurrency: 16
                }
              }
            },
            # The queue's store. `dlq` is the dead-letter queue's ceilings
            # (`Ankusa.Queue.DlqReaper`): a dead row is evicted once it is older
            # than `retention_days`, and while the DLQ holds more than `max_rows`
            # rows or `max_bytes` stored bytes, but only once its hook is archived
            # and the catalogue can find it. A DLQ over a ceiling with nothing
            # archived refuses new hooks with `503 dlq_full` instead of deleting
            # an acked one. Integers >= 1.
            #
            # `rocksdb` is the RocksDB settings of the node's store
            # (`Ankusa.Store`), read when the database opens (a change needs a
            # restart) and again by every reopen. It lives here, not in a section
            # of its own, because `Ankusa.Config` is at the 31 fields a struct
            # may have before it stops being a flat map.
            # `block_cache_bytes` is the LRU cache shared by every column family;
            # with the filters below its index and filter blocks live in it
            # (`cache_index_and_filter_blocks`), so size it for them on a large
            # store. `bloom_bits_per_key` is the bloom filter on the `index`
            # family (dedupe keys, due/inflight/dead rows) and the `deliveries`
            # family, which turns a point lookup of an absent key (a first-seen
            # dedupe key) into one filter probe per file instead of an index and
            # data block read; 0 turns it off. A filter exists only in files
            # written after it was turned on, so an upgraded store gains it as
            # compaction rewrites its files. `max_open_files` is -1 (no limit) or
            # the table-file handles kept open (the process's file-descriptor
            # limit still applies). `compaction_style` is :level or :universal
            # for every family. `rate_limiter_bytes_per_sec` caps flush and
            # compaction writes (0 = unlimited). `max_background_jobs` is the
            # flush and compaction thread budget. `write_buffer_bytes` is the
            # memtable of each column family (at least 65 536). Not secrets:
            # `/v1/config` shows the section as it is.
            queue: %{
              dlq: %{retention_days: 30, max_rows: 10_000_000, max_bytes: 53_687_091_200},
              rocksdb: %{
                block_cache_bytes: 64 * 1024 * 1024,
                bloom_bits_per_key: 10,
                max_open_files: 65_536,
                compaction_style: :level,
                rate_limiter_bytes_per_sec: 0,
                max_background_jobs: 4,
                write_buffer_bytes: %{
                  default: 4 * 1024 * 1024,
                  hooks: 64 * 1024 * 1024,
                  deliveries: 16 * 1024 * 1024,
                  index: 16 * 1024 * 1024,
                  archive: 4 * 1024 * 1024,
                  quarantine: 4 * 1024 * 1024,
                  audit: 4 * 1024 * 1024,
                  tenants: 4 * 1024 * 1024,
                  counters: 1024 * 1024
                }
              }
            },
            # How long the archive keeps a segment object (and its index) through
            # the blob store's `delete/3`: `nil` keeps them (an S3/GCS bucket can
            # take a lifecycle rule instead), an integer >= 1 deletes the object
            # together with its catalogue row once the newest hook in it is that
            # many days old. At most `storage.catalogue_retention_days`.
            archive: %{retention_days: nil},
            # segment compaction
            storage: %{
              blob_store: {Ankusa.BlobStore.LocalFS, []},
              # prepended to every segment key ("" or "a/b/" — segments of
              # fleet nodes sharing one bucket must not collide)
              key_prefix: "",
              codec: {Ankusa.Codec.Raw, []},
              roll_bytes: 16 * 1024 * 1024,
              roll_ms: 30_000,
              interval_ms: 1_000,
              # How long the archive catalogue keeps a segment's row (`?S`) after
              # the newest hook in it, and a 0.3 import's `?L` row after its hook:
              # `Storage.fetch/2` and an archive replay stop finding a hook past it.
              # An integer >= 1.
              catalogue_retention_days: 365
            },
            # claim check: payloads too large to ride inline in a queue message
            claim_check: %{
              # :claim_check role only
              port: 4001,
              # address the claim gateway binds; "0.0.0.0" exposes it on every interface
              ip: "127.0.0.1",
              # nil (plain HTTP) or a TLS block, shaped like `listener.tls`
              tls: nil,
              # who may read claims: see `Ankusa.Admin.Auth`. `mode: :none` is
              # accepted on loopback only
              auth: %{mode: :none},
              # `{module, fun, args}`: called before every claim read, after
              # authentication, as `apply(module, fun, [ctx | args])` with
              # `ctx = %{principal, tenant_id, claim_id, conn}`; it returns
              # `:ok` or `{:error, :forbidden}`. nil = the built-in check: the
              # principal holds `claims:read` and `tenant:<t>` or `tenant:*`
              authorize: nil,
              # HMAC keys for v2 refs (`Ankusa.ClaimCheck.Ref`), each at least
              # 16 bytes, `[current | previous]`: a ref is signed with the
              # first and accepted if any signed it. [] mints v1 refs
              ref_signing_keys: [],
              # how long a v2 ref is good for, from the moment it is minted;
              # nil = the retention (`retention_days`), else 30 days
              ref_ttl_ms: nil,
              # whether a v1 ref (no signature, no expiry) is still served;
              # nil = only while `ref_signing_keys` is empty
              accept_unsigned_refs: nil,
              # claim requests the gateway serves at once, node-wide; past it
              # a request is `503` + `Retry-After`. 0 = no limit
              max_concurrent_reads: 1024,
              # target size of one pack object; a body larger than this still
              # gets a pack of its own
              pack_max_bytes: 16 * 1024 * 1024,
              # LocalFS retention only; nil disables the sweeper
              retention_days: nil,
              sweep_interval_ms: 3_600_000,
              # Claim leases (the LocalFS sweeper never deletes a leased pack): how
              # long (ms) after its hook is delivered or evicted from the DLQ a
              # pack stays redeemable, for the consumer of a queue message that
              # carries the ref. At least the longest a consumer can lag its
              # broker; default 24 h
              redeem_grace_ms: 86_400_000,
              # the most pack objects (and lease rows) one sweep tick deletes;
              # what is left is `ankusa_claim_check_sweep_backlog` and the next
              # tick's
              sweep_max_objects: 10_000,
              # object-store claims only: the age (days) at which the bucket's
              # own lifecycle rule expires `claims/` objects, declared so boot can
              # refuse a rule that is shorter than a claim can be needed (retry
              # horizon + `queue.dlq.retention_days` + `redeem_grace_ms`); nil =
              # undeclared (boot warns). A bucket rule cannot see claim leases
              bucket_expiry_days: nil,
              # [{module, opts}, ...] claim packs are written to; nil =
              # [storage.blob_store]. Claim keys are never prefixed, so one
              # gateway reads every node's claims.
              blob_stores: nil,
              # which store a new pack goes to: :ordered = the first, the others
              # only while it is down; :hash = the pack id's random bits pick
              # one, the rest take over when it fails
              placement: :ordered,
              # a store that failed a write or could not answer a read is
              # skipped by writes, and searched last by reads, for this long
              # (ms) before it is tried again
              store_cooldown_ms: 30_000,
              # :flat = claims/tenant=<t>/dt=<d>/<pack>; :sharded adds
              # shard=<c>/ under the tenant (32 prefixes per tenant) — see
              # Ankusa.ClaimCheck.Ref
              key_layout: :flat,
              # ── reads: the gateway, and redeem/3 in-process on a :claim_check node ──
              # one read (the pack index, then the bytes) must finish within this;
              # past it the gateway answers 503 + Retry-After, and the store read
              # that outlived it still completes and fills the cache
              read_deadline_ms: 5_000,
              # per store request while reading a claim (a store's own timeout_ms
              # and retries are for writes)
              read_timeout_ms: 3_000,
              read_retries: 1,
              # per store: the ceiling of the adaptive limit on concurrent store
              # reads from this node; it halves on a throttle or an unreachable
              # answer and grows back by one per round of successes. Under the
              # HTTP client's 50 connections per host, so no read waits for one.
              read_max_inflight: 48,
              # per store: reads waiting for a slot; past this a read is shed at
              # once (0 = never wait)
              read_queue_max: 256,
              # memory for cached pack indexes and claim bodies; 0 = no cache
              read_cache_bytes: 128 * 1024 * 1024,
              # a claim larger than this is served but never cached
              read_cache_max_entry_bytes: 1024 * 1024
            },
            # continuous backup of the store to an object store, and restore into
            # an empty data_dir at boot (see Ankusa.Store.Backup). Off by default.
            backup: %{
              enabled: false,
              # a checkpoint is uploaded this often; a lost host loses at most
              # the hooks acked since the last one
              interval_ms: 60_000,
              # backups kept in the object store (older ones and the files only
              # they referenced are deleted after each successful upload)
              keep: 3,
              # {module, opts} for backup objects; nil = storage.blob_store.
              # Keys go under storage.key_prefix either way: backups are per node.
              blob_store: nil,
              # {module, opts} of a second object store every backup is written to
              # as well (another region or provider); restore falls back to it
              # when the primary cannot be read or fails its checks. nil = none.
              secondary: nil,
              # most one object-store call of a backup, a verify or a restore may
              # take before it counts as failed
              timeout_ms: 120_000,
              # how often the latest backup is read back and checked (size and
              # sha256 of every file, the manifest's signature); nil = never
              verify_interval_ms: nil,
              # HMAC-SHA256 key(s) the manifest is signed with, as a string or
              # a list (the first signs, any verifies: rotation). Normalised to
              # a list; [] = unsigned manifests, and a restore accepts any
              signing_key: [],
              # how long a clean stop may spend on one last backup
              final_timeout_ms: 20_000,
              # `/ready` says `backup: stale` once the last successful backup is
              # older than this; nil = no label
              max_age_ms: nil,
              # a stale backup makes `/ready` answer 503 (needs max_age_ms)
              require_fresh: false
            },
            # operator HTTP API + Prometheus /metrics; off by default for embedded
            # use. Authenticated per `auth` (see `Ankusa.Admin.Auth`); `mode: :none`
            # is accepted on loopback only
            admin: %{
              enabled: false,
              port: 4002,
              ip: "127.0.0.1",
              gauge_interval_ms: 15_000,
              # nil (plain HTTP) or a TLS block, shaped like `listener.tls`
              tls: nil,
              auth: %{mode: :none}
            },
            # a listener that serves only `/metrics`, `/health` and `/ready`
            # (`Ankusa.Metrics.Router`), so a scraper needs no route to the
            # mutating admin API. Off by default; the admin API keeps serving
            # `/metrics` either way. Also starts the metrics reporter and the
            # gauge poller when the admin API is off.
            #
            # `detail` is the cardinality policy of the Prometheus scrape:
            # `:tenant` (the default) labels per-hook series with `tenant_id`,
            # `:source` adds `source_id` as well, `:none` carries neither (and
            # omits the per-tenant gauges). `max_series_per_metric` caps how
            # many distinct identity label sets one metric keeps; the rest are
            # folded into `"other"` (`Ankusa.Metrics.Cardinality`).
            metrics: %{
              enabled: false,
              port: 4004,
              ip: "127.0.0.1",
              tls: nil,
              detail: :tenant,
              max_series_per_metric: 5_000
            },
            # route management: the allowlist guard plus its admin API. Off by
            # default, and off means "capture every POST", as it always has. On
            # means deny-by-default: a request is captured only if it matches an
            # enabled route and passes the IP rules.
            routes: %{
              enabled: false,
              # a hard cap on definitions; nothing is ever evicted
              max_routes: 1_000_000,
              # what this node may spend on routes: checked at boot against an
              # estimate of `max_routes` definitions (`Ankusa.Routes.Snapshot`)
              memory_budget_bytes: 8_589_934_592,
              # {module, opts} implementing Ankusa.Routes.Store
              store: {Ankusa.Routes.Store.ETS, []},
              # the decision cache (see Ankusa.Routes.Cache); gc_interval_ms
              # must stay above ttl_ms, or the adapter's generational sweep can
              # evict an entry before its own TTL expires
              cache: %{
                max_size: 50_000,
                ttl_ms: 30_000,
                negative_ttl_ms: 5_000,
                gc_interval_ms: 60_000
              },
              # CIDRs whose peers may set X-Forwarded-For (see Ankusa.Net.ClientIP)
              trusted_proxies: [],
              # global rules: a floor. A route's own ip_rules replace this list
              ip_rules: %{default: :allow, rules: []},
              # its own listener, authenticated per `auth` like the operator
              # admin API (see `Ankusa.Admin.Auth`)
              admin: %{port: 4003, ip: "127.0.0.1", tls: nil, auth: %{mode: :none}},
              # 1 in log_sample rejections is logged at :debug (0 = silent)
              log_sample: 100,
              # 403, or 404 for uniformity with :no_route
              ip_denied_status: 403,
              # route attrs (maps or keyword lists) loaded at boot
              seed: []
            },
            # ingest admission, enforced in this node's memory (see
            # `Ankusa.Edge.RateLimiter`). Per-tenant precedence: a runtime
            # override (admin API) beats `tenants[tenant]`, which beats `default`.
            # `default: nil` and no tenant entry means unlimited; with a
            # tenant-carrying URL scheme (`RouteResolver.TenantPath`) a `default`
            # is required.
            #
            #   %{default: nil | limit, tenants: %{tenant_id => limit},
            #     per_ip: nil | %{rate: number, burst: pos_integer},
            #     node: nil | %{rate: number, burst: pos_integer},
            #     sources: %{default: nil | %{bytes_per_second: number,
            #                                 burst_bytes: pos_integer}}}
            #
            # A tenant `limit` is `%{rate: number, burst: pos_integer}` (`rate` is
            # hooks per second, `burst` the most admitted back to back) plus,
            # optionally: `bytes_per_second` and `burst_bytes` (body bytes),
            # `max_body_bytes` (at most the instance `max_body_bytes`), and
            # `max_queue` / `max_queue_bytes` (what this tenant may hold in this
            # node's batchers at once).
            #
            # `per_ip`, `node` and `sources` are charged before the body is read,
            # so they are CPU protection, not quota: `per_ip` by client address
            # (`routes.trusted_proxies` decides whose `X-Forwarded-For` counts),
            # `node` for every request, `sources.default.bytes_per_second` for
            # each source's declared body bytes (100 MB/s unless changed).
            rate_limits: %{
              default: nil,
              tenants: %{},
              per_ip: nil,
              node: nil,
              sources: %{default: %{bytes_per_second: 100_000_000}},
              # `:node` | `:fleet`: where the *tenant quota* (`default`/`tenants`,
              # hooks and bytes per second) is counted. `:fleet` shares one bucket
              # per tenant across every edge node through `fleet.store`; `per_ip`,
              # `node` and `sources` are CPU protection for this node and stay
              # per node under either.
              scope: :node
            },
            # `scope: :node | :fleet`: where the provider event key collapses
            # (`Ankusa.Dedupe`). `:node` is the writer's local index alone;
            # `:fleet` claims the key in `fleet.store` first, so a provider retry
            # that lands on another node is a duplicate, not a second hook.
            #
            # `max_entries` / `max_entries_per_tenant`: the most live dedupe keys
            # the node's index holds, in all and for one tenant (an integer >= 1).
            # A hook that would insert a key past either is refused with
            # `503 dedupe_quota` (`Retry-After: 60`), never stored without its
            # key: the keys expire on their own (`Ankusa.Queue.DedupeSweeper`).
            # A key costs about 200 B of disk, so the defaults bound the index at
            # about 20 GB; a tenant at 100 hooks/s with the 72 h default ttl holds
            # about 26 M keys, so raise both for a bigger tenant or a longer ttl.
            dedupe: %{scope: :node, max_entries: 100_000_000, max_entries_per_tenant: 30_000_000},
            # The shared store behind `rate_limits.scope: :fleet` and
            # `dedupe.scope: :fleet`, `{module, opts}` (a module implementing
            # `Ankusa.RateLimiter.Store` and `Ankusa.Dedupe.Store`, e.g.
            # `Ankusa.RateLimiter.Store.Redis` from `ankusa_redis`); `nil` with
            # both scopes `:node`.
            #
            # A fleet call never holds a request longer than `timeout_ms`: a slow
            # or dead store is answered from this node's own limiter and dedupe
            # index instead (the ingest never refuses for a limiter outage). A
            # breaker opens when, within `breaker_window_ms`, at least
            # `breaker_min_calls` calls were made and `breaker_failure_rate` of
            # them failed or timed out, stays open `breaker_open_ms`, then lets one
            # probe call through. `pending_ttl_ms` is how long a claimed dedupe key
            # stays "in flight" before another node may claim it: it must outlast
            # the longest commit.
            fleet: %{
              store: nil,
              timeout_ms: 20,
              pending_ttl_ms: 30_000,
              breaker_window_ms: 1_000,
              breaker_min_calls: 20,
              breaker_failure_rate: 0.5,
              breaker_open_ms: 5_000
            },
            # The intake log behind `wal: :dual` (`Ankusa.Intake`, ADR 0002): an
            # acked hook is also confirmed in a replicated broker log. Read only
            # under `wal: :dual`, where `type` and `node_id` are required;
            # `node_id` is the node's stable identity, the same on a replacement.
            # `type` is `:kafka`, `:jetstream` or `:memory` (the test double). `topic`
            # defaults to `ankusa.intake.<fleet>`. `kafka`/`jetstream`/`memory`
            # are the adapter's own options (a keyword list), passed through.
            # `publish_timeout_ms` is the one attempt's deadline; a record the
            # reconciler has not completed within `reconcile_deadline_ms` is stuck
            # and releases its fleet dedupe claim. The pending set is bounded by
            # `pending.max_records` and `pending.max_bytes`; `max_record_bytes`
            # bounds one record and must fit `max_body_bytes` plus its framing.
            intake: %{
              type: nil,
              node_id: nil,
              fleet: "default",
              topic: nil,
              kafka: [],
              jetstream: [],
              memory: [],
              publish_timeout_ms: 2_000,
              reconcile_deadline_ms: 20_000,
              breaker_failures: 3,
              probe_ms: 1_000,
              retry_after_s: 2,
              offset_flush_ms: 1_000,
              pending: %{max_records: 50_000, max_bytes: 268_435_456},
              max_record_bytes: 10_000_000,
              recover_on_boot: true,
              verify_topic: true
            },
            # tenants this node's store will know (`Ankusa.Queue.Tenants` hands
            # each one a dense ordinal at first sight and never takes it back): a
            # hook of the one past it is refused with 503 `tenant_limit`
            tenants: %{max: 1_000_000},
            # the quarantine pen (`Ankusa.Edge.Quarantine`): one token bucket per
            # source (`burst` tokens, `rate` refilled per second), a cap on the
            # pen's total bytes (a full pen refuses with 503, it never evicts a
            # held hook for room) and `retention_days`: an entry held longer is
            # deleted by an hourly sweep, unless a live replay job still needs it.
            quarantine: %{burst: 100, rate: 20, max_bytes: 1_073_741_824, retention_days: 14},
            # where lifecycle events (a source or route created/updated/deleted,
            # as CloudEvents) are delivered: `[{module, opts}]` implementing
            # Ankusa.Sink, run through the WAL and dispatch like a hook. `[]`
            # means lifecycle events are off. See `Ankusa.Lifecycle`.
            lifecycle: %{sinks: []},
            # the audit trail (`Ankusa.Audit`): rows older than `retention_days` are
            # deleted by an hourly sweep, and the trail is bounded in `max_rows` and
            # `max_bytes` (oldest first, never past the lifecycle outbox's cursor).
            # A trail that is still over a ceiling, or a lifecycle outbox holding
            # more than `outbox_max_backlog` events no sink has confirmed, refuses
            # new control-plane mutations with `503 audit_backlog` instead of
            # dropping an unpublished row. Integers >= 1.
            audit: %{
              retention_days: 365,
              max_rows: 10_000_000,
              max_bytes: 5_368_709_120,
              outbox_max_backlog: 100_000
            },
            # where outbound HTTP may go (`Ankusa.HttpClient.Egress`): every
            # `Ankusa.Sink.Http` delivery — a source's or a lifecycle sink's — is
            # checked against it on every attempt, and a denied target is a
            # permanent failure. `https` only unless `allow_insecure_http`; an
            # address in `deny_cidrs` is refused unless `allow_cidrs` or
            # `allow_hosts` exempts it; `deny_hosts` names are refused before any
            # lookup. `allow_cidrs` is how a deployment opts in to the private
            # network its sinks live on; `deny_cidrs` replaces the default list.
            egress: %{
              allow_insecure_http: false,
              deny_cidrs: [
                "0.0.0.0/8",
                "127.0.0.0/8",
                "10.0.0.0/8",
                "172.16.0.0/12",
                "192.168.0.0/16",
                "100.64.0.0/10",
                "169.254.0.0/16",
                "::/128",
                "::1/128",
                "fc00::/7",
                "fe80::/10"
              ],
              allow_cidrs: [],
              allow_hosts: [],
              deny_hosts: ["metadata.google.internal"]
            },
            # secrets at rest (`Ankusa.Crypto.Envelope`): the key-encryption key
            # that source specs' secrets (verify secrets, sink credentials) are
            # encrypted under before they are stored. `kek` is 32 bytes, base64
            # (optionally `"<id>:<base64>"`); `kek_provider` is a
            # `{module, fun, args}` returning `{:ok, [kek | previous]}` instead;
            # `kek_previous` keys decrypt but never encrypt, for rotation. With
            # none, specs are stored as plain JSON, as they always were.
            secrets: %{kek: nil, kek_previous: [], kek_provider: nil}

  # `lifecycle.sinks` is spelled out because the struct's default (`[]`) is all
  # a bare `%__MODULE__{}` tells dialyzer about the field.
  @type t :: %__MODULE__{lifecycle: %{sinks: [{module(), keyword()}]}}

  @roles [:edge, :dispatch, :storage, :claim_check]

  @role_names %{
    "edge" => :edge,
    "dispatch" => :dispatch,
    "storage" => :storage,
    "claim_check" => :claim_check
  }

  @doc """
  Parse a comma-separated `ANKUSA_ROLES`-shaped string into role atoms.
  Never calls `String.to_atom/1` — each part must name one of the fixed
  roles (`edge`, `dispatch`, `storage`, `claim_check`).
  """
  @spec parse_roles!(String.t()) :: [atom()]
  def parse_roles!(value) do
    roles =
      value
      |> String.split(",")
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))
      |> Enum.map(fn name ->
        Map.get(@role_names, name) ||
          raise ArgumentError,
                "unknown Ankusa role #{inspect(name)}; expected one of: edge, dispatch, storage, claim_check"
      end)

    if roles == [] do
      raise ArgumentError, "ANKUSA_ROLES must name at least one role"
    end

    roles
  end

  @doc """
  Build a `%Ankusa.Config{}` from a keyword list, deep-merging the map-valued
  sections (`:listener`, `:batcher`, `:dispatch`, `:storage`, `:claim_check`,
  `:backup`, `:admin`, `:metrics`, `:routes`, `:rate_limits`, `:dedupe`, `:fleet`,
  `:quarantine`, `:tenants`, `:lifecycle`, `:egress`, `:secrets`) over the defaults. Every `tls` block (`listener.tls`,
  `admin.tls`, `routes.admin.tls`, `claim_check.tls`, `metrics.tls`) is
  validated and normalised by `Ankusa.Listener.normalize_tls!/2`, and
  `listener.num_acceptors: nil` becomes two per scheduler.

  `:routes` is nested one level deeper than the rest (`:routes` has its own
  `:cache`, `:ip_rules`, and `:admin` sections), so `put_routes/2` merges those
  too — `routes.cache.max_size` keeps the other cache keys.

  `wal: :none` is normalized last: the roles are settled before `normalize_wal/1`
  drops the queue's readers (`:dispatch`, `:storage`) from them.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    base = %__MODULE__{}

    opts
    |> Enum.reduce(base, fn {k, v}, acc ->
      cond do
        k == :roles ->
          bad = Enum.reject(v, &(&1 in @roles))

          if bad != [] do
            raise ArgumentError, "unknown Ankusa role(s) #{inspect(bad)} in :roles"
          end

          Map.put(acc, k, v)

        k == :routes ->
          put_routes(acc, v)

        k in [
          :listener,
          :batcher,
          :dispatch,
          :storage,
          :claim_check,
          :backup,
          :admin,
          :metrics,
          :rate_limits,
          :dedupe,
          :fleet,
          :quarantine,
          :queue,
          :archive,
          :lifecycle,
          :audit,
          :tenants,
          :egress,
          :secrets,
          :intake
        ] ->
          put_section(acc, k, v)

        Map.has_key?(base, k) ->
          Map.put(acc, k, v)

        true ->
          raise ArgumentError, "unknown Ankusa.Config key: #{inspect(k)}"
      end
    end)
    |> normalize_wal()
    |> validate_listen_ips!()
    |> validate_egress!()
    |> normalize_listeners!()
    |> validate_ranges!()
    |> validate_intake!()
    |> normalize_store_headers!()
    |> normalize_backup!()
    |> validate_secrets!()
  end

  # `backup.signing_key` is a string or a list of them; every key is a non-empty
  # binary of at least 32 bytes (HMAC-SHA256 gains nothing from a shorter one
  # and a short key is brute-forceable). The key is never echoed in the error.
  defp normalize_backup!(config) do
    keys =
      case config.backup.signing_key do
        nil -> []
        key when is_binary(key) -> [key]
        keys when is_list(keys) -> keys
        _other -> :invalid
      end

    unless is_list(keys) and Enum.all?(keys, &(is_binary(&1) and byte_size(&1) >= 32)) do
      raise ArgumentError,
            "backup.signing_key must be a string or a list of strings of at least 32 bytes each"
    end

    %{config | backup: %{config.backup | signing_key: keys}}
  end

  defp validate_secrets!(config) do
    :ok = Ankusa.Crypto.Envelope.validate_config!(config)
    config
  end

  @doc "The address a listener binds: a strict IPv4/IPv6 literal string, or an `:inet` address tuple."
  @spec listen_ip!(term(), String.t()) :: :inet.ip_address()
  def listen_ip!(ip, key) when is_binary(ip) do
    case :inet.parse_strict_address(String.to_charlist(ip)) do
      {:ok, addr} -> addr
      {:error, _} -> raise ArgumentError, bad_ip(ip, key)
    end
  end

  def listen_ip!(ip, key) when is_tuple(ip) do
    if :inet.is_ip_address(ip), do: ip, else: raise(ArgumentError, bad_ip(ip, key))
  end

  def listen_ip!(ip, key), do: raise(ArgumentError, bad_ip(ip, key))

  defp bad_ip(ip, key) do
    "#{key} must be an IP address such as \"127.0.0.1\" or \"0.0.0.0\", got: #{inspect(ip)}"
  end

  defp validate_listen_ips!(config) do
    listen_ip!(config.admin.ip, "admin.ip")
    listen_ip!(config.claim_check.ip, "claim_check.ip")
    listen_ip!(config.routes.admin.ip, "routes.admin.ip")
    listen_ip!(config.metrics.ip, "metrics.ip")
    config
  end

  # The policy's own shape. The sinks it governs are checked where sources are
  # built (`Ankusa.HttpClient.Egress.validate_config!/1`, at boot).
  defp validate_egress!(config) do
    Ankusa.HttpClient.Egress.validate_policy!(config.egress)
    config
  end

  defp normalize_store_headers!(config) do
    %{
      config
      | store_headers: Ankusa.Edge.StoredHeaders.normalize!(config.store_headers, "store_headers")
    }
  end

  # Every `tls` block is checked and normalised here (files exist, `verify_peer`
  # has its CA), the acceptor count the section leaves open is settled, and so is
  # every control plane's `auth` block (see `Ankusa.Admin.Auth`): it is checked
  # against the listener's own address and TLS block, so it runs after them.
  defp normalize_listeners!(config) do
    listener = config.listener

    listener = %{
      listener
      | tls: Ankusa.Listener.normalize_tls!(listener.tls, "listener.tls"),
        num_acceptors: listener.num_acceptors || Ankusa.Listener.default_acceptors()
    }

    routes_admin = put_tls!(config.routes.admin, "routes.admin.tls")
    routes_enabled? = config.routes.enabled == true and :edge in config.roles

    %{
      config
      | listener: listener,
        admin:
          config.admin |> put_tls!("admin.tls") |> put_auth!("admin.auth", config.admin.enabled),
        claim_check:
          config.claim_check
          |> put_tls!("claim_check.tls")
          |> put_auth!("claim_check.auth", :claim_check in config.roles),
        metrics: put_tls!(config.metrics, "metrics.tls"),
        routes: %{
          config.routes
          | admin: put_auth!(routes_admin, "routes.admin.auth", routes_enabled?)
        }
    }
  end

  defp put_tls!(section, key),
    do: %{section | tls: Ankusa.Listener.normalize_tls!(section.tls, key)}

  defp put_auth!(section, key, enabled?),
    do: %{section | auth: Ankusa.Admin.Auth.normalize!(section.auth, key, section, enabled?)}

  # Types and ranges of the numeric keys, so `0` partitions or a negative
  # concurrency fail at boot with the key's name instead of misbehaving later.
  # Module-specific rules (dispatch breakers, claim retention, quarantine
  # buckets, routes) stay with their modules' `validate_config!/1`.
  @ports [
    {[:port], "port"},
    {[:admin, :port], "admin.port"},
    {[:claim_check, :port], "claim_check.port"},
    {[:routes, :admin, :port], "routes.admin.port"},
    {[:metrics, :port], "metrics.port"}
  ]

  @positive [
    {[:max_body_bytes], "max_body_bytes"},
    {[:direct_publish_timeout_ms], "direct_publish_timeout_ms"},
    {[:batcher, :partitions], "batcher.partitions"},
    {[:batcher, :max_batch], "batcher.max_batch"},
    {[:batcher, :max_queue], "batcher.max_queue"},
    {[:batcher, :max_queue_bytes], "batcher.max_queue_bytes"},
    {[:dispatch, :batch], "dispatch.batch"},
    {[:dispatch, :concurrency], "dispatch.concurrency"},
    {[:dispatch, :max_inflight], "dispatch.max_inflight"},
    {[:dispatch, :max_inflight_bytes], "dispatch.max_inflight_bytes"},
    {[:storage, :roll_bytes], "storage.roll_bytes"},
    {[:claim_check, :sweep_interval_ms], "claim_check.sweep_interval_ms"},
    {[:claim_check, :redeem_grace_ms], "claim_check.redeem_grace_ms"},
    {[:claim_check, :sweep_max_objects], "claim_check.sweep_max_objects"},
    {[:claim_check, :store_cooldown_ms], "claim_check.store_cooldown_ms"},
    {[:claim_check, :read_deadline_ms], "claim_check.read_deadline_ms"},
    {[:claim_check, :read_timeout_ms], "claim_check.read_timeout_ms"},
    {[:claim_check, :read_max_inflight], "claim_check.read_max_inflight"},
    {[:claim_check, :read_cache_max_entry_bytes], "claim_check.read_cache_max_entry_bytes"},
    {[:backup, :interval_ms], "backup.interval_ms"},
    {[:backup, :keep], "backup.keep"},
    {[:backup, :timeout_ms], "backup.timeout_ms"},
    {[:backup, :final_timeout_ms], "backup.final_timeout_ms"},
    {[:admin, :gauge_interval_ms], "admin.gauge_interval_ms"},
    {[:metrics, :max_series_per_metric], "metrics.max_series_per_metric"},
    {[:routes, :max_routes], "routes.max_routes"},
    {[:fleet, :pending_ttl_ms], "fleet.pending_ttl_ms"},
    {[:fleet, :breaker_window_ms], "fleet.breaker_window_ms"},
    {[:fleet, :breaker_min_calls], "fleet.breaker_min_calls"},
    {[:fleet, :breaker_open_ms], "fleet.breaker_open_ms"},
    {[:listener, :max_connections], "listener.max_connections"},
    {[:listener, :num_acceptors], "listener.num_acceptors"},
    {[:listener, :read_timeout_ms], "listener.read_timeout_ms"},
    {[:listener, :body_read_timeout_ms], "listener.body_read_timeout_ms"},
    {[:listener, :request_deadline_ms], "listener.request_deadline_ms"},
    {[:listener, :max_requests_per_connection], "listener.max_requests_per_connection"},
    {[:listener, :max_header_count], "listener.max_header_count"},
    {[:listener, :max_header_length], "listener.max_header_length"}
  ]

  # `0` means something for these: no linger before a commit, roll at once,
  # a compactor that only runs when asked (`Compactor.tick/1`), a claim read
  # with no retry, one that never waits for a slot, and no claim cache.
  @non_negative [
    {[:batcher, :max_delay_ms], "batcher.max_delay_ms"},
    {[:storage, :roll_ms], "storage.roll_ms"},
    {[:storage, :interval_ms], "storage.interval_ms"},
    {[:claim_check, :read_retries], "claim_check.read_retries"},
    {[:claim_check, :read_queue_max], "claim_check.read_queue_max"},
    {[:claim_check, :read_cache_bytes], "claim_check.read_cache_bytes"},
    {[:claim_check, :max_concurrent_reads], "claim_check.max_concurrent_reads"}
  ]

  @key_prefix ~r"\A([A-Za-z0-9._-]+/)+\z"

  # `queue.rocksdb` (DUR-3): the ranges, so a bad value fails at `new/1` naming
  # its key, not at the open (RocksDB would keep its own default for a value it
  # cannot use, and report nothing).
  defp validate_rocksdb!(config) do
    for {key, constraint, valid?} <- [
          {:block_cache_bytes, "an integer >= 1", &(is_integer(&1) and &1 >= 1)},
          {:bloom_bits_per_key, "0 (off) or an integer from 1 to 32",
           &(is_integer(&1) and &1 in 0..32)},
          {:max_open_files, "-1 (no limit) or an integer >= 1",
           &(is_integer(&1) and (&1 == -1 or &1 >= 1))},
          {:compaction_style, ":level or :universal", &(&1 in [:level, :universal])},
          {:rate_limiter_bytes_per_sec, "an integer >= 0 (0 = unlimited)",
           &(is_integer(&1) and &1 >= 0)},
          {:max_background_jobs, "an integer >= 1", &(is_integer(&1) and &1 >= 1)}
        ] do
      check!(config, [:queue, :rocksdb, key], "queue.rocksdb.#{key}", constraint, valid?)
    end

    for {cf, _} <- config.queue.rocksdb.write_buffer_bytes do
      check!(
        config,
        [:queue, :rocksdb, :write_buffer_bytes, cf],
        "queue.rocksdb.write_buffer_bytes.#{cf}",
        "an integer >= 65536",
        &(is_integer(&1) and &1 >= 65_536)
      )
    end

    config
  end

  # The retention windows and ceilings of the DLQ, the pen, the catalogue and the
  # audit trail (DUR-2.b).
  defp validate_retention!(config) do
    for {path, key} <- [
          {[:queue, :dlq, :retention_days], "queue.dlq.retention_days"},
          {[:queue, :dlq, :max_rows], "queue.dlq.max_rows"},
          {[:queue, :dlq, :max_bytes], "queue.dlq.max_bytes"},
          {[:storage, :catalogue_retention_days], "storage.catalogue_retention_days"},
          {[:quarantine, :retention_days], "quarantine.retention_days"},
          {[:audit, :max_rows], "audit.max_rows"},
          {[:audit, :max_bytes], "audit.max_bytes"},
          {[:audit, :outbox_max_backlog], "audit.outbox_max_backlog"}
        ] do
      check!(config, path, key, "an integer >= 1", &(is_integer(&1) and &1 >= 1))
    end

    check!(
      config,
      [:archive, :retention_days],
      "archive.retention_days",
      "nil or an integer >= 1",
      &(&1 == nil or (is_integer(&1) and &1 >= 1))
    )

    if config.archive.retention_days != nil and
         config.archive.retention_days > config.storage.catalogue_retention_days do
      raise ArgumentError,
            "archive.retention_days (#{config.archive.retention_days}) must not exceed " <>
              "storage.catalogue_retention_days (#{config.storage.catalogue_retention_days}): " <>
              "a segment object is deleted together with its catalogue row, so the " <>
              "catalogue would forget segments it never deleted"
    end
  end

  defp validate_ranges!(config) do
    for {path, key} <- @ports do
      check!(
        config,
        path,
        key,
        "an integer from 0 to 65535",
        &(is_integer(&1) and &1 in 0..65_535)
      )
    end

    for {path, key} <- @positive do
      check!(config, path, key, "a positive integer", &(is_integer(&1) and &1 >= 1))
    end

    for {path, key} <- @non_negative do
      check!(config, path, key, "a non-negative integer", &(is_integer(&1) and &1 >= 0))
    end

    for {path, key} <- [
          {[:claim_check, :retention_days], "claim_check.retention_days"},
          {[:claim_check, :bucket_expiry_days], "claim_check.bucket_expiry_days"}
        ] do
      check!(config, path, key, "nil or an integer >= 1", &nil_or_positive?/1)
    end

    check!(
      config,
      [:tenants, :max],
      "tenants.max",
      "an integer from 1 to 4294967294",
      &(is_integer(&1) and &1 in 1..4_294_967_294)
    )

    check!(
      config,
      [:claim_check, :ref_ttl_ms],
      "claim_check.ref_ttl_ms",
      "nil or a positive integer",
      &(&1 == nil or (is_integer(&1) and &1 >= 1))
    )

    check!(
      config,
      [:claim_check, :accept_unsigned_refs],
      "claim_check.accept_unsigned_refs",
      "nil, true or false",
      &(&1 == nil or is_boolean(&1))
    )

    check!(
      config,
      [:audit, :retention_days],
      "audit.retention_days",
      "an integer >= 1",
      &(is_integer(&1) and &1 >= 1)
    )

    validate_retention!(config)
    validate_rocksdb!(config)

    for key <- [:max_entries, :max_entries_per_tenant] do
      check!(
        config,
        [:dedupe, key],
        "dedupe.#{key}",
        "an integer >= 1",
        &(is_integer(&1) and &1 >= 1)
      )
    end

    check!(
      config,
      [:storage, :key_prefix],
      "storage.key_prefix",
      ~s(\"\" or path segments each ending in "/" such as "node-a/"),
      &(&1 == "" or (is_binary(&1) and Regex.match?(@key_prefix, &1)))
    )

    validate_fleet!(config)
    validate_backup!(config)

    for {path, key} <- [
          {[:listener, :http2], "listener.http2"},
          {[:metrics, :enabled], "metrics.enabled"}
        ] do
      check!(config, path, key, "true or false", &is_boolean/1)
    end

    check!(
      config,
      [:metrics, :detail],
      "metrics.detail",
      ":tenant, :source or :none",
      &(&1 in [:tenant, :source, :none])
    )

    config
  end

  # The backup block's cross-key rules; the plain ranges are in `@positive`.
  defp validate_backup!(config) do
    check!(
      config,
      [:backup, :verify_interval_ms],
      "backup.verify_interval_ms",
      "nil or a positive integer",
      &(&1 == nil or (is_integer(&1) and &1 >= 1))
    )

    interval = config.backup.interval_ms

    check!(
      config,
      [:backup, :max_age_ms],
      "backup.max_age_ms",
      "nil or an integer >= backup.interval_ms (#{interval})",
      &(&1 == nil or (is_integer(&1) and &1 >= interval))
    )

    check!(
      config,
      [:backup, :require_fresh],
      "backup.require_fresh",
      "true or false",
      &is_boolean/1
    )

    if config.backup.require_fresh and config.backup.max_age_ms == nil do
      raise ArgumentError, "backup.require_fresh needs backup.max_age_ms"
    end

    :ok
  end

  defp nil_or_positive?(value), do: value == nil or (is_integer(value) and value >= 1)

  @intake_id ~r/\A[a-z0-9._-]{1,64}\z/

  # The batcher answers a record 503 after this long (`Ankusa.Edge.Batcher.commit/4`'s
  # default): one intake attempt must end before the provider's answer is due.
  @batcher_record_deadline_ms 15_000

  # The intake block: every range is checked whatever `wal` is; the cross-key
  # rules (ADR 0002, section 11) only under `wal: :dual`, which is where the
  # block is read.
  defp validate_intake!(config) do
    for {path, key, lo, hi} <- [
          {[:intake, :publish_timeout_ms], "intake.publish_timeout_ms", 100, 60_000},
          {[:intake, :reconcile_deadline_ms], "intake.reconcile_deadline_ms", 1_000, 600_000},
          {[:intake, :breaker_failures], "intake.breaker_failures", 1, 100},
          {[:intake, :probe_ms], "intake.probe_ms", 100, 60_000},
          {[:intake, :retry_after_s], "intake.retry_after_s", 1, 300},
          {[:intake, :offset_flush_ms], "intake.offset_flush_ms", 100, 60_000},
          {[:intake, :pending, :max_records], "intake.pending.max_records", 1, 10_000_000}
        ] do
      check!(
        config,
        path,
        key,
        "an integer from #{lo} to #{hi}",
        &(is_integer(&1) and &1 in lo..hi)
      )
    end

    for {path, key} <- [
          {[:intake, :pending, :max_bytes], "intake.pending.max_bytes"},
          {[:intake, :max_record_bytes], "intake.max_record_bytes"}
        ] do
      check!(config, path, key, "a positive integer", &(is_integer(&1) and &1 >= 1))
    end

    for {path, key} <- [
          {[:intake, :recover_on_boot], "intake.recover_on_boot"},
          {[:intake, :verify_topic], "intake.verify_topic"}
        ] do
      check!(config, path, key, "true or false", &is_boolean/1)
    end

    check!(
      config,
      [:intake, :type],
      "intake.type",
      "nil, :kafka, :jetstream or :memory",
      &(&1 in [nil, :kafka, :jetstream, :memory])
    )

    check!(
      config,
      [:intake, :node_id],
      "intake.node_id",
      "nil or a string of [a-z0-9._-], 1 to 64 characters",
      &(&1 == nil or (is_binary(&1) and Regex.match?(@intake_id, &1)))
    )

    check!(
      config,
      [:intake, :fleet],
      "intake.fleet",
      "a string of [a-z0-9._-], 1 to 64 characters",
      &(is_binary(&1) and Regex.match?(@intake_id, &1))
    )

    check!(
      config,
      [:intake, :topic],
      "intake.topic",
      "nil or a non-empty string",
      &(&1 == nil or (is_binary(&1) and &1 != ""))
    )

    if config.wal == :dual, do: validate_dual!(config)
    config
  end

  defp validate_dual!(config) do
    intake = config.intake

    if intake.type == nil,
      do: raise(ArgumentError, "wal: :dual needs intake.type (:kafka or :jetstream)")

    if intake.node_id == nil,
      do: raise(ArgumentError, "wal: :dual needs intake.node_id, stable across a replacement")

    unless config.backup.enabled and is_integer(config.backup.max_age_ms) do
      raise ArgumentError,
            "wal: :dual needs backup.enabled and backup.max_age_ms: " <>
              "the backup is what covers a backlog older than the log's retention"
    end

    unless :edge in config.roles do
      raise ArgumentError, "wal: :dual requires the :edge role"
    end

    check!(
      config,
      [:intake, :publish_timeout_ms],
      "intake.publish_timeout_ms",
      "less than the batcher's record deadline (#{@batcher_record_deadline_ms} ms)",
      &(&1 < @batcher_record_deadline_ms)
    )

    if config.dedupe.scope == :fleet and
         intake.reconcile_deadline_ms >= config.fleet.pending_ttl_ms do
      raise ArgumentError,
            "intake.reconcile_deadline_ms (#{intake.reconcile_deadline_ms}) must be less than " <>
              "fleet.pending_ttl_ms (#{config.fleet.pending_ttl_ms}) under dedupe.scope: :fleet"
    end

    floor = config.max_body_bytes + 65_536

    if intake.max_record_bytes < floor do
      raise ArgumentError,
            "intake.max_record_bytes (#{intake.max_record_bytes}) must be at least " <>
              "max_body_bytes + 65536 (#{floor}), so a body the node accepts always fits"
    end

    :ok
  end

  defp check!(config, path, key, constraint, valid?) do
    value = get_in(config, Enum.map(path, &Access.key/1))

    unless valid?.(value) do
      raise ArgumentError, "#{key} must be #{constraint}, got #{inspect(value)}"
    end
  end

  # The fleet block's own ranges, and the rule that a fleet scope has a store to
  # scope into.
  defp validate_fleet!(config) do
    check!(
      config,
      [:fleet, :timeout_ms],
      "fleet.timeout_ms",
      "an integer from 1 to 1000",
      &(is_integer(&1) and &1 in 1..1_000)
    )

    check!(
      config,
      [:fleet, :breaker_failure_rate],
      "fleet.breaker_failure_rate",
      "a number above 0 and at most 1",
      &(is_number(&1) and &1 > 0 and &1 <= 1)
    )

    check!(
      config,
      [:fleet, :store],
      "fleet.store",
      "nil or {module, keyword}",
      &(&1 == nil or match?({mod, opts} when is_atom(mod) and is_list(opts), &1))
    )

    for {path, key} <- [
          {[:rate_limits, :scope], "rate_limits.scope"},
          {[:dedupe, :scope], "dedupe.scope"}
        ] do
      check!(config, path, key, ":node or :fleet", &(&1 in [:node, :fleet]))

      if get_in(config, Enum.map(path, &Access.key/1)) == :fleet and config.fleet.store == nil do
        raise ArgumentError, "#{key}: :fleet needs fleet.store (a shared store module)"
      end
    end

    :ok
  end

  # `:dispatch` and `:storage` read the queue and nothing else, so with no queue
  # they have no work: they are dropped rather than rejected, which lets an
  # existing `roles: [:edge, :dispatch, :storage]` deployment flip
  # `wal.type: none` with no other change. The check is "a role is left", not
  # "`:edge` is left": a `:claim_check`-only node survives, which is a
  # legitimate standalone deployment. The effective roles are visible on the
  # admin API's `GET /health`.
  defp normalize_wal(%__MODULE__{wal: :none} = config) do
    case Enum.reject(config.roles, &(&1 in [:dispatch, :storage])) do
      [] -> raise ArgumentError, "wal: :none requires the :edge role"
      roles -> %{config | roles: roles}
    end
  end

  defp normalize_wal(%__MODULE__{wal: :disk} = config), do: config
  defp normalize_wal(%__MODULE__{wal: :dual} = config), do: config

  defp normalize_wal(%__MODULE__{wal: {Ankusa.WAL.DiskLog, _}}) do
    raise ArgumentError,
          "wal: {Ankusa.WAL.DiskLog, _} was removed: use wal: :disk (the RocksDB store) or wal: :none"
  end

  defp normalize_wal(%__MODULE__{wal: other}) do
    raise ArgumentError, "wal must be :disk, :dual or :none, got #{inspect(other)}"
  end

  # `dispatch.tenants` is a map of maps: a single-level merge would let
  # `tenants: %{"acme" => ...}` drop the built-in `default` limits.
  defp put_section(acc, :dispatch, v) do
    merged = merge_known!(acc.dispatch, v, "dispatch")
    %{acc | dispatch: %{merged | tenants: merge_tenants(acc.dispatch.tenants, merged.tenants)}}
  end

  # `intake.pending` is one level down, and the adapter blocks are the
  # adapter's own vocabulary: a keyword list, taken whole.
  defp put_section(acc, :intake, v) do
    incoming = section_map(v, "intake")
    {pending, incoming} = Map.pop(incoming, :pending)

    {adapters, incoming} = Map.split(incoming, [:kafka, :jetstream, :memory])

    adapters =
      Map.new(adapters, fn {k, opts} -> {k, adapter_opts(opts, "intake.#{k}")} end)

    merged = acc.intake |> then(&merge_known!(&1, incoming, "intake")) |> Map.merge(adapters)

    pending =
      if pending == nil,
        do: merged.pending,
        else: merge_known!(merged.pending, pending, "intake.pending")

    %{acc | intake: %{merged | pending: pending}}
  end

  defp put_section(acc, :queue, v) do
    merged = merge_known!(acc.queue, v, "queue")
    rocksdb = merge_known!(acc.queue.rocksdb, merged.rocksdb, "queue.rocksdb")

    buffers =
      merge_known!(
        acc.queue.rocksdb.write_buffer_bytes,
        rocksdb.write_buffer_bytes,
        "queue.rocksdb.write_buffer_bytes"
      )

    %{
      acc
      | queue: %{
          merged
          | dlq: merge_known!(acc.queue.dlq, merged.dlq, "queue.dlq"),
            rocksdb: %{rocksdb | write_buffer_bytes: buffers}
        }
    }
  end

  defp put_section(acc, k, v) do
    Map.put(acc, k, merge_known!(Map.get(acc, k), v, to_string(k)))
  end

  defp adapter_opts(opts, key) do
    cond do
      is_map(opts) -> Map.to_list(opts)
      is_list(opts) and Keyword.keyword?(opts) -> opts
      true -> raise ArgumentError, "Ankusa.Config #{key} must be a map or keyword list"
    end
  end

  defp merge_tenants(defaults, %{} = incoming) do
    case Map.get(incoming, :default, %{}) do
      %{} = default -> Map.put(incoming, :default, Map.merge(defaults.default, default))
      _other -> incoming
    end
  end

  defp merge_tenants(_defaults, incoming), do: incoming

  defp merge_known!(defaults, value, dotted_name) do
    incoming =
      case value do
        v when is_map(v) ->
          Map.new(v)

        v when is_list(v) ->
          if Keyword.keyword?(v),
            do: Map.new(v),
            else:
              raise(
                ArgumentError,
                "Ankusa.Config #{dotted_name} must be a map or keyword list"
              )

        _ ->
          raise ArgumentError, "Ankusa.Config #{dotted_name} must be a map or keyword list"
      end

    Enum.each(Map.keys(incoming), fn k ->
      unless Map.has_key?(defaults, k),
        do: raise(ArgumentError, "unknown Ankusa.Config key: #{dotted_name}.#{k}")
    end)

    Map.merge(defaults, incoming)
  end

  # Sections of :routes that hold their own keys; everything else is a scalar
  # or a list. A single-level merge would let `routes.cache.max_size` replace
  # the whole cache map, silently dropping the TTLs.
  @routes_sections [:cache, :ip_rules, :admin]

  defp put_routes(acc, v) do
    routes = section_map(v, "routes")

    merged =
      Enum.reduce(routes, acc.routes, fn {k, value}, routes ->
        cond do
          k in @routes_sections ->
            Map.put(routes, k, merge_known!(routes[k], value, "routes.#{k}"))

          Map.has_key?(routes, k) ->
            Map.put(routes, k, value)

          true ->
            raise ArgumentError, "unknown Ankusa.Config key: routes.#{k}"
        end
      end)

    %{acc | routes: merged}
  end

  defp section_map(value, key) do
    cond do
      is_map(value) -> Map.new(value)
      Keyword.keyword?(value) -> Map.new(value)
      true -> raise ArgumentError, "Ankusa.Config #{key} must be a map or keyword list"
    end
  end

  @doc "Absolute path for an instance-scoped data sub-directory."
  @spec path(t(), Path.t()) :: Path.t()
  def path(%__MODULE__{data_dir: dir, instance: instance}, sub) do
    Path.join([dir, to_string(instance), sub])
  end

  @doc "Is a role enabled for this node?"
  @spec role?(t(), atom()) :: boolean()
  def role?(%__MODULE__{roles: roles}, role), do: role in roles
end
