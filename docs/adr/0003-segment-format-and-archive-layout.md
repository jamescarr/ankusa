# ADR 0003: Segment format, index format and the dated archive layout

- Status: accepted, 2026-10-19 (written with its implementation: `DUR-8` is not a `[design-gate]` task, but it changes a persisted format and the object key layout, so ground rule 14 asks for the record)
- Plan: `DUR-8`
- Store format: 6 → 7 (`Ankusa.Store.Migrate` registry entry `{6, 7, Ankusa.Store.Migrations.SegmentRows}`)

## Context

Until now an archive segment was `<<len::32, crc32::32, payload>>` frames back to back under `seg/<first>-<last>.seg`, its index a bare `term_to_binary` map read with `binary_to_term/1`, its catalogue row (`?S`) only a key, seq and id ranges and a count:

1. **Nothing in a segment says what it is.** No magic, no version, no codec id, no whole-object checksum: a future codec or framing change cannot be told from the old one, and a truncated or overwritten object is a short read of valid-looking frames.
2. **The index is read with `binary_to_term/1`** (`storage.ex`, the replayer) from an object store that other tenants of the bucket, a compromised credential or a corrupted write can reach: it can intern atoms and build funs on a node that trusts it.
3. **The key says nothing about where or when.** `seg/<first>-<last>.seg` under a per-node prefix is found only through the local catalogue. A node that lost it (restore, replacement, a fresh node reading a shared bucket) cannot tell what is there without listing every segment ever written.
4. **`storage.roll_ms` is dead.** The compactor writes whatever is pending every tick; replay's `to >= now - (roll_ms + interval_ms)` rule assumed a roll the compactor never did.

## Decision

1. **Segment object, format 1**: `"ANKSEG" <> <<version::8, codec_id::8, count::32>>`, the codec's frames, `sha256(header <> frames)`. Index offsets are positions in the object (header included), so a range read is still one frame. `codec_id` is the codec's optional `id/0` (`Raw` = 1, none = 0: the row names the module). Format 0 (bare frames) stays readable; a newer format is refused. N reads N−1.
2. **Index object, format 1**: `"ANKIDX" <> <<version::8, count::32>>`, `term_to_binary(%{id => {offset, length, seq}})`, sha256. Decoded with `[:safe]` and a shape check; format 0 (bare term) is decoded the same way. (JSON was the alternative: an index is a map of up to ~10⁵ ids, and `[:safe]` plus a shape check costs less and keeps the legacy format one code path.)
3. **Layout**: `archive/v1/dt=<yyyy-mm-dd>/<node>/seg-<first>-<last>.{seg,idx}` and `_manifest.json` (a constant-size per-node-day summary), under `storage.key_prefix`. `dt` is the UTC day of the oldest hook's UUIDv7; `<node>` is `intake.node_id` else the host name. Day before node, so one LIST of a day's prefix finds every node's segments.
4. **Catalogue row** (`?S`, store format 7) gains `format`, `codec`, `node`, `dt`, `sha256`. The migration marks every existing row `format: 0`, 500 a batch, resumable; rows without the field are read as format 0 regardless.
5. **Finding without the catalogue**: `Storage.fetch/2` falls back to the id's day and the day before (skipping nodes by manifest, bisecting a node's segments by their indexes); `Archive.reconcile/2` rebuilds rows from the prefix (the boot restore reconcile is `node: :own, advance_seq: true`), settling only the archive obligations whose hook is the one the segment holds for that seq.
6. **Roll**: a backlog of `roll_bytes` rolls at once; the remainder (a partial segment) is written once its first obligation has waited `storage.roll_ms`; `storage.max_segments_per_tick` (16) caps one tick; `tick/1` flushes.

## Consequences

- A format 7 store cannot be opened by a format 6 image, and a format 6 node cannot read format 1 segments: the rollback boundary is the first start of a format 7 image.
- A segment lives in the directory of its *oldest* hook: a backlog segment spanning more than a day is found by the day scan only for its first two days (the catalogue finds all of them). An id without a UUIDv7 time has no day and is found only through the catalogue.
- The catalogue is keyed by first seq, which is per node: a reconcile that meets another node's segment with a first seq the store already has leaves it out (`conflicts`) and the day scan serves it.
- The compactor writes one more small object per segment (the manifest, constant size, deleted with the last segment of its node-day by the retention sweep).
- A hot-path-adjacent file set changes (`storage/*`, `store/*`, `queue/archive.ex`); none of it is on the request path, and the bench gate is the check.

## Proof

- `test/ankusa/codec_test.exs`: `versioned header` (golden format 0 and 1 vectors, tamper, truncation, newer version), `safe idx` (golden vectors, tamper, hostile atom, shape).
- `test/ankusa/storage_test.exs`: `dated layout`, `versioned header`, `safe idx`, `time roll`, `storage.max_segments_per_tick`, `reconcile other node`, `legacy seg/ layout`.
- `test/ankusa/store_upgrade_test.exs`: `the segment rows migration (format 6 -> 7)`, and every checked-in fixture (`store-v1` … `store-v7`) boots and serves its archive.
