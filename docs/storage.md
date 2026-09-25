# Storage: WAL, segments, object stores

Two tiers, on purpose. The WAL is the fast, small, durable tier the ack
depends on. The object store is the cheap, large, long-term tier — segments
roll off the WAL asynchronously, never blocking an ack. See
[`architecture.md`](architecture.md) for how this fits the request path.

## `Ankusa.WAL`

```elixir
@callback append(server(), [entry()]) :: {:ok, [result()]}
@callback read(server(), after_seq :: non_neg_integer(), limit :: pos_integer()) :: [Envelope.t()]
@callback get_cursor(server(), name :: atom()) :: non_neg_integer()
@callback put_cursor(server(), name :: atom(), seq :: non_neg_integer(), token :: pos_integer()) ::
            :ok | {:error, :fenced}
@callback truncate_through(server(), seq :: non_neg_integer(), token :: pos_integer()) ::
            :ok | {:error, :fenced}
@callback acquire_lease(server(), name :: atom(), holder :: String.t(), ttl_ms :: pos_integer()) ::
            {:ok, lease()} | {:error, {:held, String.t()}}
@callback renew_lease(server(), lease()) :: {:ok, lease()} | {:error, :lost}
@callback release_lease(server(), lease()) :: :ok
@callback stats(server()) :: map()
```

Contract every adapter must uphold:

- `append/2` is a **group commit**: given a list of records, write them all
  and issue a *single* commit, then return per-record results in order. Every
  record is written — the log has **no uniqueness constraint**. Two copies of
  the same event are two committed records with two `seq`s, each acked
  independently. Collapsing them is the idempotent receiver's job
  ([`delivery.md`](delivery.md)), not the log's, so nothing on the ack path has
  to read the log before it can write to it.
- A committed record gets a strictly increasing `seq`. Readers use it as a
  cursor; `0` means "nothing consumed yet."
- After a crash, replay must drop a torn trailing record (a write that
  started but never committed) — no un-acked write is ever surfaced as
  durable.

### `WAL.DiskLog` — the default, single-node

Append-only, length-prefixed, CRC32-per-record binary log on local disk.
No external dependencies — OTP's `:file`, `:ets`, and `:erlang.crc32` only.

- One `:file.pwrite` + one `:file.datasync` (fsync) per batch — hundreds of
  hooks, one fsync.
- Replay validates every frame's CRC and truncates the file at the first
  torn/invalid one.
- Truncation is **logical first** and lease-fenced: `truncate_through/3` records a durable seq
  floor in `<name>.truncated` and drops the affected index entries, so
  reclaiming a few records costs a few ETS deletes and never blocks appends.
  The file is rewritten only once the dead prefix passes `:rewrite_min_bytes`
  (default 64 MiB) and is at least as large as the live suffix it would copy.
  The floor is also what keeps `seq` from being reused: after a restart,
  allocation resumes at the floor, the persisted cursors, or the last replayed
  frame — whichever is highest — never at 1.
- Cursors and the truncation floor are written to a temp file, fsynced, then
  renamed, so a power loss leaves the old or the new file, never a torn one.
  The log keeps no dedup state at all: with no uniqueness constraint to
  enforce there is nothing to rebuild on start, and nothing to snapshot before
  truncation.
- Durable to process crash and power loss **on that box**, not to losing
  the box — it's one local file. See "Shared Postgres WAL" below for the
  fleet case.

```elixir
config :ankusa, wal: {Ankusa.WAL.DiskLog, []}   # the default; no opts required
# wal: {Ankusa.WAL.DiskLog, rewrite_min_bytes: 64 * 1024 * 1024}  # that IS the default
```

### Shared Postgres WAL

`WAL.Postgres` — the multi-node case.

Ships as the separate `ankusa_postgres` package (see
[`packaging.md`](packaging.md) for why). This is what a *fleet* of ingest
servers coordinates through — every node runs its own local `Postgrex` pool
against the same database; nodes never talk to each other directly.

```elixir
config :ankusa,
  wal: {Ankusa.WAL.Postgres,
        hostname: "localhost", port: 5432,
        username: "ankusa", password: "ankusa", database: "ankusa_prod",
        pool_size: 10}
```

**Group commit, translated to SQL.** `append/2` runs inside one
`Postgrex.transaction/2` (still exactly one `COMMIT`, one fsync-equivalent,
per batch):

1. **Take the instance's advisory lock** — `pg_advisory_xact_lock(hashtext(…))`
   for the instance, held to `COMMIT`, so commits land in the same order as the
   seqs they allocate (see `## Seq order`).
2. **Allocate the seqs before the insert** — one `nextval` per row, so each
   row's seq is known in advance and the results can be paired with the input
   *by position*. No `RETURNING`: Postgres does not promise its order for a
   multi-row statement, and with no uniqueness constraint two rows of one batch
   may be the same event with the same id, so the id cannot identify them
   either.
3. **Insert every row** — one `INSERT ... SELECT FROM unnest(...)`, all of it
   into `ankusa_wal`. Nothing is claimed, looked up or skipped.

A rolled-back transaction consumes sequence values it never keeps, which is one
reason `seq` may have small gaps.

Every row carries the envelope, and the envelope is the record: there is no
side table whose loss would change what a reader sees.

Every table carries an `instance` column, so one Postgres database can back
multiple `Ankusa.Instance`s — including the *same* instance name running on
many independent BEAM nodes, which is the actual point: coordination happens
through this shared, durable state, never through BEAM distribution. `seq` is
**not** reset per instance, but it's still strictly increasing and safe as a
cursor.

`WAL.Postgres` keeps no dedup state, deliberately — see
[`delivery.md`](delivery.md) for where idempotency lives instead. A database
migrated from the ledger-era schema keeps its `ankusa_wal_dedup` table and
`dedup_key` column (nothing here reads or writes them, and dropping a table an
operator's database holds is not the bootstrap's call), while the uniqueness
that era put on `ankusa_wal.event_id` is dropped on every boot: both halves of
that constraint say the same event cannot be appended twice, which is now
exactly what must work.

Local dev/test: `cd ankusa_postgres && docker compose up -d --wait && mix test`
(22 tests: the shared conformance suite, the commit-order test, lease fencing,
and the copy-appends-again contract).

## `Ankusa.BlobStore`

```elixir
@callback put(instance, key :: String.t(), data :: iodata(), opts :: keyword()) :: :ok | {:error, term()}
@callback get(instance, key, opts) :: {:ok, binary()} | {:error, term()}
@callback get_range(instance, key, offset, length, opts) :: {:ok, binary()} | {:error, term()}
@callback delete(instance, key, opts) :: :ok
@callback list(instance, prefix :: String.t(), opts) :: [String.t()]
```

`get_range/5` exists because the compactor never writes one object per
hook — segments hold many records, and a single range `GET` reads exactly
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
`claims/tenant=<t>/dt=<day>/<object_id>`). Retention differs per namespace
too — see [`claim-check.md#retention`](claim-check.md#retention).

| Adapter | Deps | Notes |
| --- | --- | --- |
| `BlobStore.LocalFS` | none | Default. Atomic writes (temp file + rename). `get_range` uses `:file.pread/3`, never slurps the whole segment. |
| `BlobStore.S3` | `aws_signature` + `req` | SigV4 signing via [`aws_signature`](https://hex.pm/packages/aws_signature) — the implementation behind the official aws-elixir SDK — with HTTP through `Req`. Path-style addressing works unmodified against AWS, MinIO, Cloudflare R2, and the [floci](https://floci.io) emulator. `list/3` parses `ListObjectsV2` XML via stdlib `:xmerl`. |
| `BlobStore.GCS` | `req` | GCS JSON API. `:token_provider` opt (an MFA returning `{:ok, bearer_token}`) is required against real GCS — the adapter carries no OAuth2 dependency of its own; wire up whatever your deployment already uses (Goth, ADC). Unauthenticated against the `floci-gcp` emulator. |
| `BlobStore.Azure` | `req` | Azure Blob REST. Carries **no credential dependency**, the same stance as GCS: a pre-generated `:sas_token` (Shared Access Signature), or a `:token_provider` MFA — including the built-in `Ankusa.BlobStore.Azure.ManagedIdentity`, the best credential for a service running on Azure (no secret, short-lived Entra ID tokens from IMDS). No Shared-Key signing of its own. Unauthenticated against the `floci-az` emulator. |
| `BlobStore.OCI` | none | OCI Object Storage. The one adapter that signs its own requests — OCI has no bearer/SAS shortcut covering arbitrary `put`/`get`/`list` — using OTP's `:public_key` (RSA-SHA256 *Signature version 1*), no dependency. Two credential shapes: a static API key (`:tenancy_ocid`/`:user_ocid`/`:key_fingerprint`/`:private_key`), or the instance-principal / session-token output of the OCI SDK via `:key_id: "ST$<token>"` + `:private_key`. Signing is pinned against OCI's reference vectors in `test/ankusa/blob_store_oci_signing_test.exs`; `floci-oci` parses but never verifies the signature, so any locally generated key works there. |

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

# Azure Blob Storage — a pre-generated SAS ...
config :ankusa,
  storage: %{
    blob_store:
      {Ankusa.BlobStore.Azure,
       account_name: "myaccount",
       container: "ankusa-segments",
       sas_token: System.get_env("AZURE_BLOB_SAS")}
       # endpoint: "http://localhost:4577/devstoreaccount1"  # only for floci-az; omit for real Azure
  }

# ... or, on Azure, the managed identity (system-assigned — no secret at all)
config :ankusa,
  storage: %{
    blob_store:
      {Ankusa.BlobStore.Azure,
       account_name: "myaccount",
       container: "ankusa-segments",
       # user-assigned? add: client_id: System.get_env("AZURE_CLIENT_ID")
       token_provider: {Ankusa.BlobStore.Azure.ManagedIdentity, :token, []}}
  }

# OCI Object Storage — signs its own requests with an API signing key
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
docker compose up -d          # floci (S3, :4566) + floci-gcp (GCS, :4588) + floci-az (Azure, :4577) + floci-oci (OCI, :4599), buckets/containers auto-created
mix test --include integration
docker compose down -v
```

## `Ankusa.Codec` and segment format

```elixir
@callback encode([%{key: String.t(), payload: binary()}]) :: {segment :: binary(), index :: [index_entry]}
@callback decode_record(binary()) :: {:ok, binary()} | {:error, term()}
```

`Codec.Raw` (the only shipped codec) frames each record length-prefixed with
a per-record CRC32; `encode/1` packs many into one segment binary and
returns the byte offset + length of each, which is exactly what
the `get_range` callback needs.

## `Ankusa.Storage.Compactor` — how segments get written

One tick (default every `storage.interval_ms`, 1s):

1. Read WAL records past the compactor's own cursor in bounded chunks (256
   records per read), accumulating until their payloads reach
   `storage.roll_bytes` (default 16 MiB) or the WAL has nothing more to give.
   A long backlog therefore produces **several segments in one tick**, not one
   unbounded segment — peak memory is a chunk plus a segment, however far
   behind a storage node fell.
2. Encode those records into one segment via the configured `Codec`.
3. `PUT` the segment to the blob store under a deterministic key:
   `seg/<zero-padded first_seq>-<zero-padded last_seq>.seg`.
4. `PUT` its **sidecar**, `seg/<first>-<last>.idx`, holding the same rows in the
   index file's framed format. This is what lets another storage replica — a
   standby taking the lease over, or a fresh one — build its own index for
   segments it never compacted (`Ankusa.Storage.Index.repair/1`).
5. Append one index row per record to `Ankusa.Storage.Index` (durable,
   append-only, fsynced before the cursor moves, on local disk regardless of
   which `BlobStore` is configured) — `event_id`, `tenant_id`, `source_id`,
   `seq`, `segment_key`, `offset`, `length` — and record the segment key as the
   local high-water mark (`segments/index.hwm`).
6. Advance the compactor's durable cursor, with the token of the `:storage`
   lease.
7. Truncate the WAL through `min(compactor_seq, dispatch_seq)` — **never**
   past what dispatch has consumed yet, so at-least-once delivery survives
   compaction even if dispatch is lagging or down — also with the lease token.
   A `{:error, :fenced}` at either step means the lease moved on: the node steps
   down after the segment it is writing. Segments are deterministic and
   idempotent, so re-writing one after a step-down is harmless, and the
   `put segment → sidecar → index → hwm → cursor` order is what makes a crash
   anywhere in there redo the work instead of skipping it.

This is why segments are never one-object-per-hook: PUT cost amortizes over
however many records landed in one tick, and archive-tier storage (which
bills a minimum object size) stays cheap at scale.

## Replay by id

`Ankusa.Storage.fetch/2` looks an event id up through the index, range-reads
exactly its frame from the blob store, and decodes it back into the
original `%Ankusa.Envelope{}` — the read-side counterpart to compaction,
usable for building a replay/audit API or a dashboard without touching the
WAL.

```elixir
{:ok, envelope} = Ankusa.Storage.fetch(:default, event_id)
```
