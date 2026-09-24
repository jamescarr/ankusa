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
@callback put_cursor(server(), name :: atom(), seq :: non_neg_integer()) :: :ok
@callback truncate_through(server(), seq :: non_neg_integer()) :: :ok
@callback stats(server()) :: map()
```

Contract every adapter must uphold:

- `append/2` is a **group commit**: given a list of records, write them all
  and issue a *single* commit, then return per-record results in order. A
  record whose `(tenant_id, source_id, dedup_key)` collides with an
  already-committed one comes back as `{:duplicate, existing_seq}` and is
  **not written** — the caller still acks `2xx`.
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
- Truncation is **logical first**: `truncate_through/2` records a durable seq
  floor in `<name>.truncated` and drops the affected index entries, so
  reclaiming a few records costs a few ETS deletes and never blocks appends.
  The file is rewritten only once the dead prefix passes `:rewrite_min_bytes`
  (default 64 MiB) and is at least as large as the live suffix it would copy.
  The floor is also what keeps `seq` from being reused: after a restart,
  allocation resumes at the floor, the persisted cursors, or the last replayed
  frame — whichever is highest — never at 1.
- Committed dedup keys live in an in-memory ETS set, keyed by `{tenant_id,
  source_id, dedup_key}`, rebuilt from the log on start. Because the log
  itself gets truncated after compaction, a **snapshot** of the dedup set is
  persisted to `<name>.dedup` before frames are dropped and reloaded before
  replay — dedup correctness survives compaction *and* restart even though the
  original records are long gone from disk. Cursors, the dedup snapshot, and
  the truncation floor are all written to a temp file, fsynced, then renamed,
  so a power loss leaves the old or the new file, never a torn one.
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

1. **Claim dedup keys** — one `INSERT ... ON CONFLICT DO NOTHING` against a
   permanent `ankusa_wal_dedup` ledger table, batched via `unnest/1`. Postgres
   takes a row lock on the conflicting index entry and blocks until the
   other writer's transaction resolves, so two nodes racing the same dedup
   key never double-claim it.
2. **Insert winners** — rows that had no dedup key, or won their claim, go
   into `ankusa_wal` (`RETURNING event_id, seq`). A losing row is never
   written here at all.
3. **Resolve losers' seq** — one lookup against `ankusa_wal_dedup`, which
   carries its own `seq` column (backfilled right after step 2) rather than
   joining back to `ankusa_wal`. That's deliberate: `ankusa_wal` rows get
   deleted by `truncate_through/2` once compacted, and a lookup that
   depended on the data row still existing would stop catching duplicates
   of an already-truncated event. The dedup ledger is **never** truncated —
   this is `WAL.DiskLog`'s persisted `.dedup` snapshot, just durable in the
   same database instead of a sidecar file.

Every row is correlated by the envelope's own `id` (a UUIDv7, always unique
regardless of dedup key), never by array/result position — Postgres doesn't
guarantee `RETURNING` order for a multi-row statement.

Every table carries an `instance` column, so one Postgres database can back
multiple `Ankusa.Instance`s — including the *same* instance name running on
many independent BEAM nodes, which is the actual point: `seq` may have
small gaps (a deduped row still consumes a sequence value) and is **not**
reset per instance, but it's still strictly increasing and safe as a
cursor.

Local dev/test: `cd ankusa_postgres && docker compose up -d --wait && mix test`
(10 tests, including concurrent-writer dedup races — N tasks racing the same
key, exactly one commits — and a truncation-survives-dedup regression).

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
store-specific error (`:enoent`, an HTTP status). `Ankusa.ClaimCheck.Direct`
(see [`claim-check.md`](claim-check.md)) depends on this to map a missing
claim consistently regardless of which store is configured.

Two independent namespaces share one `BlobStore` by default and never
collide: `seg/...` (compaction, written by `Ankusa.Storage.Compactor`, every
hook) and `claims/...` (`Ankusa.ClaimCheck`, only when something checks a
payload in). Retention differs per namespace too — see
[`claim-check.md#retention`](claim-check.md#retention).

| Adapter | Deps | Notes |
| --- | --- | --- |
| `BlobStore.LocalFS` | none | Default. Atomic writes (temp file + rename). `get_range` uses `:file.pread/3`, never slurps the whole segment. |
| `BlobStore.S3` | `aws_signature` + `req` | SigV4 signing via [`aws_signature`](https://hex.pm/packages/aws_signature) — the implementation behind the official aws-elixir SDK — with HTTP through `Req`. Path-style addressing works unmodified against AWS, MinIO, Cloudflare R2, and the [floci](https://floci.io) emulator. `list/3` parses `ListObjectsV2` XML via stdlib `:xmerl`. |
| `BlobStore.GCS` | `req` | GCS JSON API. `:token_provider` opt (an MFA returning `{:ok, bearer_token}`) is required against real GCS — the adapter carries no OAuth2 dependency of its own; wire up whatever your deployment already uses (Goth, ADC). Unauthenticated against the `floci-gcp` emulator. |

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
```

Local dev/test emulators via [floci](https://floci.io) (no cloud account):

```sh
docker compose up -d          # floci (S3, :4566) + floci-gcp (GCS, :4588), buckets auto-created
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
4. Append one index row per record to `Ankusa.Storage.Index` (durable,
   append-only, fsynced before the cursor moves, on local disk regardless of
   which `BlobStore` is configured) — `event_id`, `tenant_id`, `source_id`,
   `seq`, `segment_key`, `offset`, `length`.
5. Advance the compactor's durable cursor.
6. Truncate the WAL through `min(compactor_seq, dispatch_seq)` — **never**
   past what dispatch has consumed yet, so at-least-once delivery survives
   compaction even if dispatch is lagging or down.

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
