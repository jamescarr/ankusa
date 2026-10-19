# Storage: the local store and the object store

Two tiers, on purpose. The **local store** is the fast, small, durable tier the
ack depends on under the default `wal.type: disk`: one RocksDB database per
instance holding every committed hook, one delivery row per hook and sink, the
archive obligations and the segment catalogue. The **object store** is the
cheap, large, long-term tier: segments roll into it asynchronously, never
blocking an ack. See [`architecture.md`](architecture.md) for how this fits the
request path, and the [`wal: :none`](#none-no-queue-no-store-hooks) section
below for the mode that has none of it.

## `Ankusa.Store` and `Ankusa.Queue`

The local store is one RocksDB database per instance at
`<data_dir>/<instance>/store`, opened by `Ankusa.Store`. Everything below lives
in it: the committed hooks, their delivery rows and the archive's catalogue.
The queue's mode is named by the historical `wal` key (`wal: :disk | :none`,
YAML `wal.type`): `:disk` is the store-backed queue described here, `:none` is
the direct path [below](#none-no-queue-no-store-hooks).

Column families, all in that one database:

| Family | Holds |
| --- | --- |
| `default` | the next-seq marker, the next tenant ordinal (`m:next_ord`), the store format version (`m:format_version`, see [Upgrading between releases](deployment.md#upgrading-between-releases)), migration markers, API-managed sources (`s:`) and their revisions (`v:`, kept after a source is deleted so a re-created one continues the count), rate-limit overrides, replay jobs (`j:<id>`), the audit sequence (`m:audit_seq`) and the key audit hashes are made with (`m:audit_key`) |
| `hooks` | each committed hook, keyed by its `seq` |
| `deliveries` | one delivery row per hook and sink |
| `index` | due `?D`, claimed `?F` and dead `?x`/`?X` rows, active-tenant markers `?A`, archive obligations, cleared markers, claim-check refs, ingest dedupe keys (`?u`) and their expiry index (`?e`); see [Tenants, counters and the active index](#tenants-counters-and-the-active-index) |
| `archive` | the segment catalogue, and locations imported from a 0.3 node's index |
| `quarantine` | the quarantine pen: one summary and one body key per held envelope |
| `audit` | the audit trail, `<<?a, seq::64, at::64>>` → a versioned row (`<<"AA", 1, term>>`), in commit order; and the rows of changes still in flight, `<<?p, seq::64>>` (see [Architecture](architecture.md#guarantees-by-component)). Pruned by `audit.retention_days` |
| `tenants` | tenant ordinal ↔ tenant id: `<<?t, ord::32>>` → id, `<<?n, id>>` → `<<ord::32>>` |
| `counters` | per-tenant queue counters, `<<ord::32, field::8>>` → an integer, written only as merge operands; ordinal 0 is the node's total |

`Ankusa.Store` owns the DB handle; every other process calls
`Ankusa.Store.write/3`, `get/3`, `multi_get/3`, `fold/6` and `property/3`
directly (the NIF runs on dirty schedulers). There is no per-reader cursor:
`Ankusa.Queue.hooks/3` walks the `hooks` family by `seq`.

`Ankusa.Queue` is the public face of the queue:

```elixir
entry = %{envelope: envelope, sinks: [{Ankusa.Sink.Http, url: "https://example.internal"}]}
{:ok, [{:committed, envelope}]} = Ankusa.Queue.enqueue(:default, [entry])

{:ok, hooks} = Ankusa.Queue.hooks(:default, 0, 100)            # seq > 0, ascending
{:ok, %{next_seq: _, hooks: _, deliveries: _, disk_bytes: _}} = Ankusa.Queue.stats(:default)
{:ok, %{total: _, entries: _}} = Ankusa.Queue.dead(:default, source_id: "stripe", limit: 100)
```

### The commit: one synced batch

`Ankusa.Queue.Writer`, one per instance, is the only process that assigns a
`seq`. It writes **one** synced RocksDB batch containing:

- the hook, keyed by seq;
- one pending delivery row and one due key per sink of its source, bound by
  sink index and module at ack time, each due key with its active-tenant marker;
- an archive obligation, but only while the `:storage` role runs;
- the dedupe key and its expiry entry, for a hook with a dedupe key;
- the counter deltas of all of them (`pending`, `bytes`, `archive_pending`, for
  the tenant and for the node);
- the tenant's ordinal and the next-ordinal marker, for a tenant seen for the
  first time (the Writer is the only process that assigns them);
- the next-seq marker.

The batch is atomic: a commit either lands whole or nothing is acked, and a
failed commit makes the batcher answer `503 store_unavailable`. Seqs are
strictly increasing and never reused — gaps are allowed, because a failed
commit still consumes seqs. `[:ankusa, :commit, :stop]` reports `batch_size`,
`bytes` and `duration`. A hook whose source has no sinks while `:storage` is
off has no obligations at all: it is acked and stamped with a seq but not
stored.

### Encoding and versions

A hook (the `hooks` family, and each record of an archive segment) is an
`Ankusa.Envelope`; a delivery row (the `deliveries` family) is a map. Both are
Erlang terms and both carry a version:

| Value | v0 | v1 |
| --- | --- | --- |
| hook | the bare `term_to_binary` of the envelope's fields (every release up to 0.6) | `"AE"`, `1`, the same term |
| delivery row | the bare `term_to_binary` of the row map (every release up to 0.6) | `"AD"`, `1`, the same term |

Readers take both. A key a reader does not know is not an error: an envelope
drops it, a row keeps it (so a field a newer node added survives this node
rewriting the row on a retry). Decoding does not depend on which atoms the
reader's VM has seen, so a newer node's new field never makes a row
unreadable. A version the reader does not know, or bytes that are no value at
all, is an error value (`:unsupported_envelope_version`,
`:unsupported_delivery_row_version`, `:undecodable`), never a raise: dispatch
dead-letters the delivery with that reason (the hook stays stored, and the
dead letter lists in `GET /v1/dlq`), a replay counts the hook as skipped, the
compactor logs it and drops its archive obligation, `Ankusa.Queue.hooks/3`
returns `{:error, {:corrupt_value, :hooks, _}}`, and the 0.3 import refuses
to boot with `{:corrupt_legacy_wal_record, path, seq, reason}`.

This release writes v0. The release after it writes v1, so a rollback of
*this* release to 0.6 still reads every row; see
[Rolling back a minor](deployment.md#rolling-back-a-minor). Proof:
`envelope_test.exs`, `queue_test.exs` and `dispatch_test.exs` (see
[`testing.md`](testing.md)).

### Durability, corruption, full disk

- One `sync: true` batch per commit: hundreds of hooks, one fsync. The boot
  line says it plainly: `[ankusa] store at <path>. Durable to power loss on
  THIS host only.` `kill -9` loses no acked hook; RocksDB recovers its own WAL
  (`loss_test.exs`, and the [crash drill](testing.md#crash-consistency-drill-mise-run-testcrash),
  which also cuts power under load). Local `BlobStore.LocalFS` writes are
  durable too (see below).
- RocksDB opens with `paranoid_checks` and
  `wal_recovery_mode: tolerate_corrupted_tail_records`: a torn trailing write
  (the last, never-acked one) is dropped. Damage anywhere before it refuses to
  open (`{:store_open_failed, path, reason}`, with a log line), because a store
  this node cannot read is never treated as empty. Point reads report checksum
  failures; a scan only trusts a result that reaches the end of its range, so
  corruption surfaces as an error, never as a silently shorter list. The
  Writer, the source store and the rate limiter refuse to start when they
  cannot read their state.
- Full disk: commits fail (`503 store_unavailable`, nothing acked) and writers
  resume by themselves once space frees. Any failed store write (ingest,
  dispatch, the archive, the quarantine pen, source and rate-limit edits) also
  tells the Store, whichever process saw it, and the Store closes and reopens
  itself at most once every 5 s. That clears a latched RocksDB write error if
  one outlives the freed space. The crash drill fills a 96 MiB tmpfs under load
  and checks the `503`s, the `/ready` flip and the resume
  ([`testing.md`](testing.md#crash-consistency-drill-mise-run-testcrash)).
- Stop an instance cleanly (supervisor shutdown, `docker stop`'s SIGTERM) and
  the store closes with it. SIGKILL keeps the data (RocksDB WAL recovery), but a
  node halted mid-write-load with the DB open can segfault at exit.

### Reclamation: a hook is deleted by its obligations

A hook's obligations are its delivery rows (pending or dead) and, only if
`:storage` ran at ack time, its archive obligation. When the last one clears,
the hook is deleted — whatever roles the node runs. So a node without the
archive (`ANKUSA_ROLES=edge,dispatch`) reclaims on delivery, and an archive
that is behind, off or on another node never blocks delivery.

### Tenants, counters and the active index

Design and reasons: [ADR 0001](https://github.com/jamescarr/ankusa/blob/main/docs/adr/0001-tenant-key-layout.md). Every tenant gets
a dense 32-bit **ordinal** at the first hook the store commits for it
(`default` is the tenant of a hook with none); ordinals are never reused, 0 is
the node's own aggregate, and `tenants.max` bounds how many exist (a hook of
the tenant past it is `503 tenant_limit`). Index keys lead with the ordinal, so
one tenant's rows are one contiguous range:

| Key (`index` family) | Value | What |
| --- | --- | --- |
| `<<?D, ord::32, at::64, seq::64, sink::16>>` | `<<size::32, kind::8>>` | due: `kind` 0 a first attempt, 1 a retry |
| `<<?F, ord::32, seq::64, sink::16>>` | `<<size::32>>` | claimed by dispatch |
| `<<?x, at::64, seq::64, sink::16>>` | `{source_id, id, ord}` | dead, in the node-wide time order `GET /v1/dlq` lists |
| `<<?X, ord::32, at::64, seq::64, sink::16>>` | empty | dead, by tenant |
| `<<?A, second::64, ord::32>>` | empty | active: the tenant has a due row in that second |
| `<<?a, seq::64>>` | `<<size::32, ord::32>>` | archive obligation |

`pending`, `scheduled`, `inflight`, `dead`, `bytes` (the stored size of every
delivery row the tenant holds), `archive_pending` and `dedupe` (live ingest-dedupe
keys, see below) are kept per tenant and per
node in the `counters` family, as blind RocksDB merge operands in the same
batch as the row change they count, so any process may change them and a crash
leaves rows and counts together or neither. `Ankusa.Queue.pending_for_tenant/2`
and `dead_for_tenant/2` and every queue gauge read them: O(1), not a scan.
`pending` is first attempts waiting and `scheduled` retries waiting (a retry
whose backoff has ended counts until it is claimed).

The active index is what makes a dispatch scan cost the tenants with work due,
not all tenants: a scan reads the markers up to the current second, claims from
each tenant named, oldest second first, and removes a marker once its second is
over and the tenant has nothing due in it (delete, then verify no commit slipped
a row in). Until TEN-3 that order is the only fairness: a tenant with a long
backlog is served before one that arrived later in the same scan. A due row
is never written without its marker (`Deliveries.due_ops/6`), and a counter is
never changed by hand: both only through the `Deliveries` op builders.
`Ankusa.Queue.purge_tenant/3` removes a tenant's pending or dead rows in chunks
of 500, resumable by calling it again.

#### Ingest dedupe keys: bounded, expired outside the writer

A source with `dedupe:` makes the writer put two index keys per new provider
event key, in the commit batch of the hook: `?u` (tenant, source, event key →
`<<expires_at::64, original_id>>`) and its expiry index `?e`. Both are bounded
(ground rule 3 of the hardening plan, `ANKUSA_HARDENING_PLAN.md`):

| | |
| --- | --- |
| **Cap** | `dedupe.max_entries` (100 000 000) live keys on the node, `dedupe.max_entries_per_tenant` (30 000 000) for one tenant: about 200 B of disk each |
| **Count** | the `dedupe` counter (field 7 of the `counters` family, per tenant and node), a merge operand in the batch that inserts or deletes the key; the check is one `multi_get` per commit batch that has a keyed hook |
| **Refusal** | a hook whose key would be a *new* live key past either cap is `503 dedupe_quota` with `Retry-After: 60`, counted as `outcome="dedupe_quota"` on `ankusa_ingest_requests_total`. It is never stored without its key (dedupe is never silently skipped); a retry of a live key is still a `duplicate`, and an expired key is replaced in place, so neither is refused at the cap |
| **Sweep** | `Ankusa.Queue.DedupeSweeper`, its own supervised process: it scans `?e` for keys past their expiry every 10 s and then back to back while full chunks of 1 000 keep coming, and hands the writer each chunk to apply. The writer deletes a key only if it still holds the expiry the sweeper read, so a refresh is never undone, and takes the `dedupe` counter down in the same batch. The scan, decode and batching never run in the writer |
| **Metric** | `ankusa_dedupe_keys` (the node counter), `ankusa_dedupe_pruned_total` |

Store format 4 added the counter: its migration counts the `?e` keys a format 3
store holds, in resumable batches of 500.

### `:none`: no queue, no store hooks

`wal: :none` is the other ack path: no hook queue, no batcher, no compactor, no
dispatch pipeline, no DLQ. Ingest verifies, publishes to the source's sinks
inside the request (`Ankusa.Edge.Publish`), and answers `201` only once every
sink has confirmed. `c:Ankusa.Sink.durable?/1` is the promise that makes a
sink's `:ok` mean "something that outlives this node accepted it" — true for
every shipped sink except `Sink.Log` and `Sink.Redis` — and a `wal: :none`
node refuses a source with no durable sink: at boot for a static one, on write
for one managed through the admin API. One durable
sink is enough to pass that check, but *every* sink still has to confirm, so a
non-durable sink that cannot (Redis with no subscriber) turns every ingest
into a `503`. Nothing is committed and no segment is written, so the two-tier
story in this document does not apply. The node still runs `Ankusa.Store` for
the quarantine pen, API-managed sources and rate-limit overrides. See
[`delivery.md#direct-mode`](delivery.md#direct-mode) and
[`config-examples/direct.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/config-examples/direct.yml).

### The default, single-node

The default `wal: :disk` runs the store on local disk, with no external
dependency beyond RocksDB itself. Every store-backed role (`edge`, `dispatch`,
`storage`) reads that same database, and RocksDB is single-process, so they
must all run in **one** BEAM node; splitting them across processes or hosts is
not supported. It is durable to process crash and power loss **on that box**,
not to losing the box — unless [`backup`](#backup-and-restore) is on. See
"Scaling out" below.

```elixir
config :ankusa, wal: :disk   # the default
```

### Backup and restore

`backup.enabled` (off by default) turns on `Ankusa.Store.Backup`, which closes
the gap above to `backup.interval_ms` (60 s): what a lost host takes with it
is the hooks acked since the last backup.

- **Backup.** Every interval the store writes a RocksDB checkpoint (memtables
  flushed, files hard-linked under `<data_dir>/<instance>/store.checkpoint`,
  in a process of its own so `/ready` keeps answering), and the uploader puts
  it in the object store — `backup.blob_store`, else `storage.blob_store` —
  under `storage.key_prefix`: `backup/sst/<name>` for every `.sst` and
  `.blob` file (immutable, so each is uploaded once and shared by every
  backup that lists it), `backup/<id>/<name>` for the manifest, options, WAL
  and `CURRENT`, then `backup/<id>/manifest.json` (every file with its size
  and sha256), then `backup/LATEST`. A backup exists only once `LATEST` names
  it. After each one, backups beyond `backup.keep` (3) and every shared file
  no kept backup lists are deleted; a purge that can't list the store or read
  a kept manifest deletes nothing. A failed attempt is retried with the
  compactor's backoff and never touches the store or the edge.
- **Restore.** A store directory with no `CURRENT` file holds no database. With
  `backup.enabled`, `Ankusa.Store` then restores `LATEST` before opening:
  every file is checked against the manifest's size and sha256 and written
  durably, `CURRENT` last, so a restore that fails part-way leaves no database
  and the next boot starts it over. No `LATEST` at all starts an empty store.
  A backup location that can't be read, a manifest that doesn't parse or a
  file that fails its checksum refuses the boot
  (`{:store_restore_failed, path, reason}`): an empty directory with a backup
  configured is never taken as empty. Database files (`MANIFEST-*`, `.sst`,
  `.blob`, `.log`) without a `CURRENT` that no interrupted restore left behind
  are refused too (`{:unrecognized_store_files, names}`), never deleted: that
  may be a damaged store worth recovering by hand.
- **The archive.** A restored (or newly created) store may be behind the
  segments in the bucket: hooks acked after the backup may already be
  archived. Before the queue writer starts, `m:next_seq` moves past the
  highest archived seq, so no new segment key overwrites one, and every
  segment without a catalogue row is catalogued from its `.idx` object (with
  the restored hooks' archive obligations it settles), so `Ankusa.Storage.fetch/2`
  finds those hooks again. Their delivery state is gone with the host. The
  restore leaves a `RESTORE-IN-PROGRESS` marker beside `CURRENT` until this
  reconcile succeeds: a boot that finds both (the node died, or the reconcile
  failed, in between) logs "reconciling now" and reconciles before serving,
  instead of opening the database as an existing, reconciled store.
- **Where to point it.** With `LocalFS` the backup lives on the same disk as the
  store, and the node logs a warning saying so. Use a bucket: `storage.type:
  s3|gcs`, or a `backup.store` of its own. Backups are per node, so nodes
  sharing a bucket need distinct `storage.key_prefix`es, exactly as for
  segments. Each store gets an id (`m:store_id`) on its first backup, carried
  by every checkpoint and recorded in each manifest; a node whose prefix
  already holds the latest backup of another store refuses to back up (no
  upload, no purge), logs `backup refused … already holds backups of store
  …` at error level, and counts it as a failed run. A replacement node takes
  over by starting with the same config on an empty volume: it restores the
  latest backup and inherits the id. If the old host comes back beside its
  replacement, both hold that id; the first of them to find `LATEST` moved
  by the other (after it has backed up once itself) refuses with
  `backup refused: … a backup of this store that this node did not write`.
  Retire one of them and restart the one you keep (it stays refused until
  then).
- **Bucket lifecycle rules.** Keep `<key_prefix>backup/` out of every
  expiration rule. Retention is `backup.keep`; a shared `backup/sst/*` file is
  listed by every later backup that still uses it, whatever its age, and an
  expired one makes the restore fail (`{:download_failed, key, _}`) and the
  node refuse to boot.
- **Costs.** An upload reads one file into memory at a time; RocksDB files here
  are bounded by the 64 MiB write buffers and the 64 MiB blob file size the
  store sets. Each backup uploads the files written since the previous one,
  plus a few small per-backup files.
- **Watching it.** `ankusa_backup_age_seconds` is the time since the last
  successful backup — the data a lost host would take with it — and
  `ankusa_backup_runs_total{result}` counts attempts.

## Encryption at rest

What rests where, and who encrypts it:

| Data | Where | Encrypted by |
| --- | --- | --- |
| Hook bodies, delivery rows, the archive catalogue | this node's RocksDB files | **the volume** (EBS, LUKS, a CSI class with encryption): Ankusa does not encrypt bodies |
| Stored source specs: verify secrets, sink credentials, header values | the `s:` rows of that store, in every backup of it; or Redis (`source_store.type: redis`) | **Ankusa**, with `secrets.kek` (below) |
| Segments, backups, claim packs | the object store | **the object store**, asked to by `sse:` on every `put/4` (below) |

The startup log states it in one line, so an operator reads what a node
promises without reading config: `store files rely on volume encryption;
secrets envelope-encrypted with key <id>` (or `secrets are stored in plaintext
(no secrets.kek is configured)`; with `secrets.kek_provider` it names the
provider, since the key is resolved when the source store starts). Body-level
encryption is deliberately not offered: add it only if a compliance review
demands it.

**Secrets.** With a key-encryption key (`secrets.kek`, or a `kek_provider`) the
secret-bearing fields of a source spec are encrypted with AES-256-GCM before
`Ankusa.SourceStore.Persistent` writes the row (or `Ankusa.SourceStore.Redis`
the hash entry), and decrypted as the row is loaded. Which fields is not a
second list to maintain: it is the inverse of what the admin API hides
(`Ankusa.Admin.Redact`) — header values, anything under a key that is not on
its shown-key allowlist, a URL carrying credentials — so the type, topic,
region and signature header of a stored source stay readable and diffable
while `verify.secret` and a sink's password do not. A value is
`<<"AENC", 1, key_id_len, key_id, iv::96, tag::128, ciphertext>>` (base64 in
the JSON, behind `AENC:`), its key id authenticated with it
([`Ankusa.Crypto.Envelope`](https://hexdocs.pm/ankusa/Ankusa.Crypto.Envelope.html)).

- **A row no key opens refuses the boot** (`{:source_store_load_failed,
  {:secrets, source_id, reason}}`, and the log names the source). It is never
  skipped: a node that booted without it would `404` every hook for that
  source. A Redis node has the same rule at first boot; on a later reload it
  keeps its mirror and logs the entry.
- **Rows written before there was a KEK** load as they are, and
  `rewrap/1` encrypts them.
- **Rotation.** Put the new key in `secrets.kek` and the old one in
  `secrets.kek_previous` (keys that decrypt, never encrypt), roll the nodes,
  move every row (`Ankusa.SourceStore.Persistent.rewrap/1` or
  `Ankusa.SourceStore.Redis.rewrap/1` on a running node, or `mix
  ankusa.secrets.rewrap` on a stopped node's data dir with `ANKUSA_KEK` and
  `ANKUSA_KEK_PREVIOUS`), then drop the old key. A rewrap is synced in
  batches: an interrupted one leaves every row readable under one key or the
  other.
- **Backups.** Backed-up stores carry the same ciphertext, so restoring needs
  the same keys; keep them where the backup is not (a secrets manager, the
  provider's KMS), not on the volume.
- **Not covered.** Sources in the configuration file (`sources:`) are not
  stored and are not touched; they hold `${VAR}` references. A KEK is in
  memory in the node, as any key is.

**Object stores.** `sse:` in the options of an object-store adapter is sent on
every `PUT` it makes: `S3` as `x-amz-server-side-encryption: AES256 | aws:kms`
(+ `…-aws-kms-key-id`; signed with the request), `GCS` as
`x-goog-encryption-kms-key-name` (and the `kmsKeyName` upload parameter),
`Azure` as `x-ms-encryption-scope`, `OCI` as `opc-sse-kms-key-id`. Because
segments, backups and claim packs all reach the object store through that
adapter's `put/4` with the options of the store they are configured on, one
`sse:` per configured store covers them; a deployment that gives backups or
claims their own store (`backup.store`, `claim_check.stores`) sets it on each.
On GCS, Azure and OCI every object is encrypted at rest regardless, so
`type: aes256` sends nothing and only `kms` (a customer-managed key) changes
anything. A misspelled option, or `kms` without the `key_id` the provider
needs, is a boot error, never an unencrypted object. The test is
`sse header on put` in
`test/ankusa/store/backup_s3_integration_test.exs`: a segment, a backup and a
claim PUT recorded on their way to the emulator all carry the header.

## Scaling out

One node is one local store. To scale, run **N independent all-role
nodes behind a load balancer**. Each with its own data volume and its own
DLQ/admin API. Segment keys are `seg/<first_seq>-<last_seq>.seg` (with the
sibling `.idx` object), which name no instance or node, so two nodes writing
the same keys would silently overwrite each other's segments: give each node
its own `storage.key_prefix` (`node-a/`, `node-b/`) and they can share one
bucket, or give each its own bucket or LocalFS root. Claims are never
prefixed — pack ids carry 64 random bits, so every node's claims can share
one place and one gateway. See [`deployment.md`](deployment.md) for the
operational shape.

Under `wal: :none` none of this applies: there is no queue, no segment story
and no data volume — replicas are freely interchangeable, and the broker (or
whatever answers the sink) is the only shared state. See
[`architecture.md#4-stateless-ingest-fleet-wal-none`](architecture.md#4-stateless-ingest-fleet-wal-none).

## `Ankusa.BlobStore`

```elixir
@callback put(instance, key :: String.t(), data :: iodata(), opts :: keyword()) :: :ok | {:error, term()}
@callback get(instance, key, opts) :: {:ok, binary()} | {:error, term()}
@callback get_range(instance, key, offset, length, opts) :: {:ok, binary()} | {:error, term()}
@callback delete(instance, key, opts) :: :ok
@callback list(instance, prefix :: String.t(), opts) :: {:ok, [String.t()]} | {:error, term()}
```

Callers go through the facade, which names a **scope** on every call —
`Ankusa.BlobStore.put(instance, :segments | :backup, key, data)` and so on.
`:segments` resolves `storage.blob_store`. `storage.key_prefix` is applied
once, when the compactor names a new segment
(`Ankusa.BlobStore.object_key/3`), and the archive catalogue records the full
key, which every later read uses verbatim: changing the prefix moves new
segments only and never strands archived ones. `list/3` lists this node's
segments under its prefix and returns full keys. Claim packs bypass the
facade: `Ankusa.ClaimCheck.Stores` writes and reads them across
`claim_check.blob_stores` (`claim_check.stores` in YAML; failing over between stores), never prefixed.
Adapters see full keys and stay prefix-unaware.

`list/3` returns `{:ok, keys}` or `{:error, reason}` — a listing that failed
halfway is an error, never a shorter list — and every remote adapter follows
its store's pagination (S3 continuation tokens, GCS page tokens, Azure
markers, OCI `nextStartWith`) to the end.

`get_range/5` exists because the compactor never writes one object per
hook. Segments hold many records, and a single range `GET` reads exactly
one record's bytes back out.

**`:not_found` is part of the contract for every adapter**: `get/3` and
`get_range/5` return `{:error, :not_found}` for a missing key, never a
store-specific error (`:enoent`, an HTTP status). `Ankusa.ClaimCheck`
(see [`claim-check.md`](claim-check.md)) depends on this to map a missing
claim consistently regardless of which store is configured.

Two independent namespaces share one `BlobStore` by default and never
collide: `seg/...` (each segment written by `Ankusa.Storage.Compactor`, plus
its sibling `.idx` index object, under `storage.key_prefix`) and `claims/...`
(`Ankusa.ClaimCheck`, packed per tenant per dispatch batch, one object holding
many claims under `claims/tenant=<t>/dt=<day>/<pack_id>`, or
`claims/tenant=<t>/shard=<c>/dt=<day>/<pack_id>` with
`claim_check.key_layout: :sharded`; or in stores of their own with
`claim_check.blob_stores`). Retention differs per namespace too. See
[`claim-check.md#retention`](claim-check.md#retention).

| Adapter | Deps | Notes |
| --- | --- | --- |
| `BlobStore.LocalFS` | none | Default. Durable writes: temp file fsync, rename, directory fsync; `put` returns `{:error, reason}` instead of raising. `get_range` uses `:file.pread/3`, never slurps the whole segment. |
| `BlobStore.S3` | `aws_signature` + `req` | SigV4 signing via [`aws_signature`](https://hex.pm/packages/aws_signature), the implementation behind the official aws-elixir SDK, with HTTP through `Req`. Path-style addressing works unmodified against AWS, MinIO, Cloudflare R2, and the [floci](https://floci.io) emulator. `list/3` parses `ListObjectsV2` XML via stdlib `:xmerl`. Credentials: `:access_key_id`/`:secret_access_key` (+ `:session_token`), else `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`, else web identity (IRSA: `AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`, through STS `AssumeRoleWithWebIdentity`), else the EC2 instance profile (IMDSv2); temporary credentials are cached until shortly before they expire. |
| `BlobStore.GCS` | `req` | GCS JSON API. `:token_provider` opt (an MFA returning `{:ok, bearer_token}`) is required against real GCS. The adapter carries no OAuth2 dependency of its own; wire up whatever your deployment already uses (Goth, ADC). Unauthenticated against the `floci-gcp` emulator. |
| `BlobStore.Azure` | `req` | Azure Blob REST. Carries **no credential dependency**, the same stance as GCS: a pre-generated `:sas_token` (Shared Access Signature), or a `:token_provider` MFA, including the built-in `Ankusa.BlobStore.Azure.ManagedIdentity`, the best credential for a service running on Azure (no secret, short-lived Entra ID tokens from IMDS). No Shared-Key signing of its own. Unauthenticated against the `floci-az` emulator. |
| `BlobStore.OCI` | none | OCI Object Storage. The one adapter that signs its own requests, OCI has no bearer/SAS shortcut covering arbitrary `put`/`get`/`list`, using OTP's `:public_key` (RSA-SHA256 *Signature version 1*), no dependency. Two credential shapes: a static API key (`:tenancy_ocid`/`:user_ocid`/`:key_fingerprint`/`:private_key`), or the instance-principal / session-token output of the OCI SDK via `:key_id: "ST$<token>"` + `:private_key`. Signing is pinned against OCI's reference vectors in `test/ankusa/blob_store_oci_signing_test.exs`; `floci-oci` parses but never verifies the signature, so any locally generated key works there. |

**The HTTP adapters retry a transient failure** (`Ankusa.BlobStore.Retry`): a
transport error (refused, reset, timed out) or a `408`, `429`, `500`, `502`,
`503` or `504` is sent again after 200 ms, then after 1 s — three attempts in
all by default — and S3 and OCI re-sign each attempt. Anything else (`404`,
`403`, another `4xx`) is answered at once. The adapter option `:retries` is
the number of extra attempts (`2`; `0` for a single attempt); like
`:timeout_ms` it is Elixir config only. Each call is idempotent, a `PUT`
writing the same bytes to the same key, so a timed-out write that did land is
safe to repeat. A worst-case call is three timeouts plus the waits. `put`
hands the adapter's iodata to the HTTP client as is: a segment is not copied
into one binary first. `LocalFS` does neither: a POSIX error is not
transient.

**The HTTP adapters take `sse:`** — server-side encryption on every `put/4`
(`%{type: :aes256}`, or `%{type: :kms, key_id: key}`): see
[Encryption at rest](#encryption-at-rest) for what each provider receives.

```elixir
# S3 / MinIO / R2
config :ankusa,
  storage: %{
    blob_store:
      {Ankusa.BlobStore.S3,
       bucket: "ankusa-segments", region: "us-east-1",
       access_key_id: System.get_env("AWS_ACCESS_KEY_ID"),
       secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY")}
       # endpoint: "http://localhost:4566"  # only for MinIO/R2/floci; omit for real AWS
  }

# GCS
config :ankusa,
  storage: %{
    blob_store: {Ankusa.BlobStore.GCS, bucket: "ankusa-segments", token_provider: {MyApp.Auth, :gcs_token, []}}
  }

# Azure Blob Storage: a pre-generated SAS ...
config :ankusa,
  storage: %{
    blob_store:
      {Ankusa.BlobStore.Azure,
       account_name: "myaccount",
       container: "ankusa-segments",
       sas_token: System.get_env("AZURE_BLOB_SAS")}
       # endpoint: "http://localhost:4577/devstoreaccount1"  # only for floci-az; omit for real Azure
  }

# ... or, on Azure, the managed identity (system-assigned, no secret at all)
config :ankusa,
  storage: %{
    blob_store:
      {Ankusa.BlobStore.Azure,
       account_name: "myaccount",
       container: "ankusa-segments",
       # user-assigned? add: client_id: System.get_env("AZURE_CLIENT_ID")
       token_provider: {Ankusa.BlobStore.Azure.ManagedIdentity, :token, []}}
  }

# OCI Object Storage: signs its own requests with an API signing key
config :ankusa,
  storage: %{
    blob_store:
      {Ankusa.BlobStore.OCI,
       region: "us-ashburn-1",
       namespace: System.get_env("OCI_NAMESPACE"),
       bucket: "ankusa-segments",
       tenancy_ocid: System.get_env("OCI_TENANCY"),
       user_ocid: System.get_env("OCI_USER"),
       key_fingerprint: System.get_env("OCI_KEY_FINGERPRINT"),
       private_key: File.read!(System.fetch_env!("OCI_KEY_FILE"))}
  }

# ... or, on OCI compute/OKE, the instance principal (the OCI SDK does the
# IMDS → x509 federation; feed its output back here)
config :ankusa,
  storage: %{
    blob_store:
      {Ankusa.BlobStore.OCI,
       region: "us-ashburn-1",
       namespace: System.get_env("OCI_NAMESPACE"),
       bucket: "ankusa-segments",
       key_id: System.get_env("OCI_SESSION_TOKEN_ID"),   # "ST$<token>"
       private_key: File.read!(System.fetch_env!("OCI_SESSION_KEY_FILE"))}
  }
```

Local dev/test emulators via [floci](https://floci.io) (no cloud account):

```sh
# floci (S3, :4566) + floci-gcp (GCS, :4588) + floci-az (Azure, :4577) + floci-oci (OCI, :4599),
# buckets/containers auto-created
docker compose -f packages/ankusa/docker-compose.integration.yml up -d
(cd packages/ankusa && mix test --only integration)
docker compose -f packages/ankusa/docker-compose.integration.yml down -v
```

Or `mise run test:integration`, which does all three.

## `Ankusa.Codec` and segment format

```elixir
@callback encode([%{key: String.t(), payload: binary()}]) :: {segment :: binary(), index :: [index_entry]}
@callback decode_record(binary()) :: {:ok, binary()} | {:error, term()}
```

`Codec.Raw` (the only shipped codec) frames each record length-prefixed with
a per-record CRC32; `encode/1` packs many into one segment binary and
returns the byte offset + length of each, which is exactly what
the `get_range` callback needs.

## `Ankusa.Storage.Compactor`: how segments get written

A hook committed while the `:storage` role runs carries an *archive
obligation*. Each tick (default every `storage.interval_ms`, 1s) clears some:

1. Read archive obligations past the compactor's own position, in `seq` order,
   taking them until their stored sizes reach `storage.roll_bytes` (default
   16 MiB) — always at least one, so a hook larger than a segment still gets
   one. Sizes come from the obligation keys, so nothing is read and discarded
   to find out how much fits. A long backlog produces **several segments in
   one tick**: the tick repeats while obligations remain.
2. Encode those hooks into one segment via the configured `Codec`.
3. `PUT` the segment to the blob store under a deterministic key:
   `seg/<zero-padded first_seq>-<zero-padded last_seq>.seg`.
4. `PUT` the segment's index object, the same key with `.idx`, mapping each
   event id to `{offset, length, seq}` for range reads.
5. Write the segment's catalogue row to the store: key, index key, seq and id
   ranges, count and byte size.
6. Clear the batch's obligations. Each cleared obligation leaves a marker; when
   a hook's last obligation is gone, the hook is deleted.

A failed blob write ends the tick without crashing: nothing moves, and the
same hooks are written again under the same keys, after `storage.interval_ms`
doubled per consecutive failure (jittered, capped at 60 s). This is why
segments are never one-object-per-hook: PUT cost amortizes over however many
records landed in one tick, and archive-tier storage (which bills a minimum
object size) stays cheap at scale.

## Replay by id

`Ankusa.Storage.fetch/2` resolves an event id through the catalogue: the
segment rows whose id range could hold it (ids are time-ordered, so that is a
segment or two), then each candidate's `.idx` object, then a range read of
exactly that record's bytes from the blob store. It decodes the frame back
into the original `%Ankusa.Envelope{}` with its `seq`. Hooks a 0.3 node
archived resolve through the legacy locations imported from its index log. It
reads no queue state, so it works on a storage node with dispatch off.

```elixir
{:ok, envelope} = Ankusa.Storage.fetch(:default, event_id)
```
