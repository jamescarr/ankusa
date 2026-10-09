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
| `default` | the next-seq marker, migration markers, API-managed sources, rate-limit overrides, replay jobs (`j:<id>`) |
| `hooks` | each committed hook, keyed by its `seq` |
| `deliveries` | one delivery row per hook and sink |
| `index` | due / claimed / dead rows, archive obligations, cleared markers, claim-check refs, ingest dedupe keys (`?u`) and their expiry index (`?e`) |
| `archive` | the segment catalogue, and locations imported from a 0.3 node's index |
| `quarantine` | the quarantine pen: one summary and one body key per held envelope |

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
  sink index and module at ack time;
- an archive obligation, but only while the `:storage` role runs;
- the dedupe key and its expiry entry, for a hook with a dedupe key;
- the next-seq marker.

The batch is atomic: a commit either lands whole or nothing is acked, and a
failed commit makes the batcher answer `503 store_unavailable`. Seqs are
strictly increasing and never reused — gaps are allowed, because a failed
commit still consumes seqs. `[:ankusa, :commit, :stop]` reports `batch_size`,
`bytes` and `duration`. A hook whose source has no sinks while `:storage` is
off has no obligations at all: it is acked and stamped with a seq but not
stored.

### Durability, corruption, full disk

- One `sync: true` batch per commit: hundreds of hooks, one fsync. The boot
  line says it plainly: `[ankusa] store at <path>. Durable to power loss on
  THIS host only.` `kill -9` loses no acked hook; RocksDB recovers its own WAL.
  Local `BlobStore.LocalFS` writes are durable too (see below).
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
  one outlives the freed space.
- Stop an instance cleanly (supervisor shutdown, `docker stop`'s SIGTERM) and
  the store closes with it. SIGKILL keeps the data (RocksDB WAL recovery), but a
  node halted mid-write-load with the DB open can segfault at exit.

### Reclamation: a hook is deleted by its obligations

A hook's obligations are its delivery rows (pending or dead) and, only if
`:storage` ran at ack time, its archive obligation. When the last one clears,
the hook is deleted — whatever roles the node runs. So a node without the
archive (`ANKUSA_ROLES=edge,dispatch`) reclaims on delivery, and an archive
that is behind, off or on another node never blocks delivery.

### `:none`: no queue, no store hooks

`wal: :none` is the other ack path: no hook queue, no batcher, no compactor, no
dispatch pipeline, no DLQ. Ingest verifies, publishes to the source's sinks
inside the request (`Ankusa.Edge.Publish`), and answers `201` only once every
sink has confirmed. `c:Ankusa.Sink.durable?/1` is the promise that makes a
sink's `:ok` mean "something that outlives this node accepted it" — true for
every shipped sink except `Sink.Log` and `Sink.Redis` — and boot refuses a
`wal: :none` config in which a static source has no durable sink. One durable
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
not to losing the box. See "Scaling out" below.

```elixir
config :ankusa, wal: :disk   # the default
```

## Scaling out

One node is one local store. To scale, run **N independent all-role
nodes behind a load balancer**. Each with its own data volume and its own
DLQ/admin API. Each node also needs **its own bucket** (or its own LocalFS
root) for segments: segment keys are `seg/<first_seq>-<last_seq>.seg` (with
the sibling `.idx` object), which name no instance or node, and remote blob
stores ignore the `instance` argument, so two nodes sharing a bucket silently
overwrite each other's segments. See [`deployment.md`](deployment.md) for the
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
@callback list(instance, prefix :: String.t(), opts) :: [String.t()]
```

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
its sibling `.idx` index object) and `claims/...` (`Ankusa.ClaimCheck`, packed
per tenant per dispatch batch, one object holding many claims under
`claims/tenant=<t>/dt=<day>/<pack_id>`). Retention differs per namespace
too. See [`claim-check.md#retention`](claim-check.md#retention).

| Adapter | Deps | Notes |
| --- | --- | --- |
| `BlobStore.LocalFS` | none | Default. Durable writes: temp file fsync, rename, directory fsync; `put` returns `{:error, reason}` instead of raising. `get_range` uses `:file.pread/3`, never slurps the whole segment. |
| `BlobStore.S3` | `aws_signature` + `req` | SigV4 signing via [`aws_signature`](https://hex.pm/packages/aws_signature), the implementation behind the official aws-elixir SDK, with HTTP through `Req`. Path-style addressing works unmodified against AWS, MinIO, Cloudflare R2, and the [floci](https://floci.io) emulator. `list/3` parses `ListObjectsV2` XML via stdlib `:xmerl`. |
| `BlobStore.GCS` | `req` | GCS JSON API. `:token_provider` opt (an MFA returning `{:ok, bearer_token}`) is required against real GCS. The adapter carries no OAuth2 dependency of its own; wire up whatever your deployment already uses (Goth, ADC). Unauthenticated against the `floci-gcp` emulator. |
| `BlobStore.Azure` | `req` | Azure Blob REST. Carries **no credential dependency**, the same stance as GCS: a pre-generated `:sas_token` (Shared Access Signature), or a `:token_provider` MFA, including the built-in `Ankusa.BlobStore.Azure.ManagedIdentity`, the best credential for a service running on Azure (no secret, short-lived Entra ID tokens from IMDS). No Shared-Key signing of its own. Unauthenticated against the `floci-az` emulator. |
| `BlobStore.OCI` | none | OCI Object Storage. The one adapter that signs its own requests, OCI has no bearer/SAS shortcut covering arbitrary `put`/`get`/`list`, using OTP's `:public_key` (RSA-SHA256 *Signature version 1*), no dependency. Two credential shapes: a static API key (`:tenancy_ocid`/`:user_ocid`/`:key_fingerprint`/`:private_key`), or the instance-principal / session-token output of the OCI SDK via `:key_id: "ST$<token>"` + `:private_key`. Signing is pinned against OCI's reference vectors in `test/ankusa/blob_store_oci_signing_test.exs`; `floci-oci` parses but never verifies the signature, so any locally generated key works there. |

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
(cd packages/ankusa && mix test --include integration)
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
