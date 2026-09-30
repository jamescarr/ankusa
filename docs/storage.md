# Storage: WAL, segments, object stores

Two tiers, on purpose. The WAL is the fast, small, durable tier the ack
depends on under the default `wal.type: disk`. The object store is the cheap,
large, long-term tier. Segments roll off the WAL asynchronously, never blocking
an ack. See [`architecture.md`](architecture.md) for how this fits the request
path, and the [`wal: :none`](#none-no-wal-at-all) section below for the mode
that has none of it.

## `Ankusa.WAL`

```elixir
@callback append(server(), [entry()]) :: {:ok, [result()]}
@callback read(server(), after_seq :: non_neg_integer(), limit :: pos_integer()) :: [Envelope.t()]
@callback get_cursor(server(), name :: atom()) :: non_neg_integer()
@callback put_cursor(server(), name :: atom(), seq :: non_neg_integer()) :: :ok
@callback truncate_through(server(), seq :: non_neg_integer()) :: :ok
@callback stats(server()) :: map()
```

Contract every adapter must uphold:

- `append/2` is a **group commit**: given a list of records, write them all
  and issue a *single* commit, then return per-record results in order. Every
  record is written. Ingest does no deduplication, so a provider retry after
  a lost ack is stored again as its own record.
- A committed record gets a strictly increasing `seq`, and seq order **is**
  commit order: once a reader has observed seq `N`, no record with seq `≤ N`
  becomes visible later. Readers use `seq` only as a cursor; `0` means
  "nothing consumed yet." Values may have gaps.
- After a crash, replay must drop a torn trailing record (a write that
  started but never committed). No un-acked write is ever surfaced as
  durable.

### `:none`: no WAL at all

`wal: :none` is the other ack path: no log, no batcher, no compactor, no
dispatch pipeline, no DLQ. Ingest verifies, publishes to the source's sinks
inside the request (`Ankusa.Edge.Publish`), and answers `201` only once every
sink has confirmed. `Ankusa.Sink.durable?/2` is the promise that makes a
sink's `:ok` mean "something that outlives this node accepted it" — true for
every shipped sink except `Sink.Log` and `Sink.Redis` — and boot refuses a
`wal: :none` config in which a static source has no durable sink. One durable
sink is enough to pass that check, but *every* sink still has to confirm, so a
non-durable sink that cannot (Redis with no subscriber) turns every ingest
into a `503`. There is no log and no segment on this node, so there is nothing
to compact: the two-tier story in this document does not apply. See
[`delivery.md#direct-mode`](delivery.md#direct-mode) and
[`config-examples/direct.yml`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/config-examples/direct.yml).

### `WAL.DiskLog`: the default, single-node

Append-only, length-prefixed, CRC32-per-record binary log on local disk.
No external dependencies: OTP's `:file`, `:ets`, and `:erlang.crc32` only.

- One `:file.pwrite` + one `:file.datasync` (fsync) per batch: hundreds of
  hooks, one fsync.
- Replay validates every frame's CRC and truncates the file at the first
  torn/invalid one.
- Truncation is **logical first**: `truncate_through/2` records a durable seq
  floor in `<name>.truncated` and drops the affected index entries, so
  reclaiming a few records costs a few ETS deletes and never blocks appends.
  The file is rewritten only once the dead prefix passes `:rewrite_min_bytes`
  (default 64 MiB) and is at least as large as the live suffix it would copy.
  The floor is also what keeps `seq` from being reused: after a restart,
  allocation resumes at the floor, the persisted cursors, or the last replayed
  frame, whichever is highest, never at 1.
- Cursors and the truncation floor are written to a temp file, fsynced, then
  renamed, so a power loss leaves the old or the new file, never a torn one.
- Durable to process crash and power loss **on that box**, not to losing the
  box. It's one local file. Every WAL role (`edge`, `dispatch`, `storage`)
  reads that same file, so they must all run in **one** BEAM node; splitting
  them across processes or hosts is not supported. See "Scaling out" below.

```elixir
config :ankusa, wal: {Ankusa.WAL.DiskLog, []}   # the default; no opts required
# wal: {Ankusa.WAL.DiskLog, rewrite_min_bytes: 64 * 1024 * 1024}  # that IS the default
```

## Scaling out

One node is one `WAL.DiskLog` file. To scale, run **N independent all-role
nodes behind a load balancer**. Each with its own data volume and its own
DLQ/admin API. Each node also needs **its own bucket** (or its own LocalFS
root) for segments: segment keys are `seg/<first_seq>-<last_seq>.seg`, which
name no instance or node, and remote blob stores ignore the `instance`
argument, so two nodes sharing a bucket silently overwrite each other's
segments. See [`deployment.md`](deployment.md) for the operational shape.

Under `wal: :none` none of this applies: there is no WAL file, no segment
story, and no volume — replicas are freely interchangeable, and the broker (or
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
collide: `seg/...` (compaction, written by `Ankusa.Storage.Compactor`, every
hook) and `claims/...` (`Ankusa.ClaimCheck`, packed per tenant per dispatch
batch, one object holding many claims under
`claims/tenant=<t>/dt=<day>/<pack_id>`). Retention differs per namespace
too. See [`claim-check.md#retention`](claim-check.md#retention).

| Adapter | Deps | Notes |
| --- | --- | --- |
| `BlobStore.LocalFS` | none | Default. Atomic writes (temp file + rename). `get_range` uses `:file.pread/3`, never slurps the whole segment. |
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

One tick (default every `storage.interval_ms`, 1s):

1. Read WAL records past the compactor's own cursor in bounded chunks (256
   records per read), accumulating until their payloads reach
   `storage.roll_bytes` (default 16 MiB) or the WAL has nothing more to give.
   A long backlog therefore produces **several segments in one tick**, not one
   unbounded segment. Peak memory is a chunk plus a segment, however far
   behind a storage node fell.
2. Encode those records into one segment via the configured `Codec`.
3. `PUT` the segment to the blob store under a deterministic key:
   `seg/<zero-padded first_seq>-<zero-padded last_seq>.seg`.
4. Append one index row per record to `Ankusa.Storage.Index` (durable,
   append-only, fsynced before the cursor moves, on local disk regardless of
   which `BlobStore` is configured): `event_id`, `tenant_id`, `source_id`,
   `seq`, `segment_key`, `offset`, `length`.
5. Advance the compactor's durable cursor.
6. Truncate the WAL through `min(compactor_seq, dispatch_seq)`. **Never**
   past what dispatch has consumed yet, so at-least-once delivery survives
   compaction even if dispatch is lagging or down.

This is why segments are never one-object-per-hook: PUT cost amortizes over
however many records landed in one tick, and archive-tier storage (which
bills a minimum object size) stays cheap at scale.

## Replay by id

`Ankusa.Storage.fetch/2` looks an event id up through the index, range-reads
exactly its frame from the blob store, and decodes it back into the
original `%Ankusa.Envelope{}`, the read-side counterpart to compaction,
usable for building a replay/audit API or a dashboard without touching the
WAL.

```elixir
{:ok, envelope} = Ankusa.Storage.fetch(:default, event_id)
```
