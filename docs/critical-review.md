# Critical review: Ankusa from ingestion to consumers

Reviewed at commit `42b6f5a` (2026-10-01). Scope: the path a hook takes through every package (`Edge.Router` → `Ingest` → `Batcher` → `WAL.DiskLog` → `Storage.Compactor` / `Dispatch.Pipeline` → sinks and brokers → claim check → consumers in `sdk-elixir` and `examples/oban-consumer`), plus the supervision tree, the YAML loader and the admin surfaces. Nothing in the repository was changed. Runtime evidence comes from throwaway probes run against copies of `packages/ankusa` and `packages/ankusa_rabbitmq` outside the repo: on macOS (Elixir 1.20.4 / OTP 29, arm64), in Linux containers (`elixir:1.20.4-alpine`, OTP 29), and against a `rabbitmq:4-management-alpine` broker. All of them are reproduced in [Appendix A](#appendix-a-probes).

**Evidence tags.** **Reproduced**: observed in a probe during this review; output quoted. **Code**: traced through the source at the cited lines, not executed. **[INFERENCE]**: follows from the code or from a dependency's behaviour, not observed.

**Severity.** **Critical**: loses acked hooks, silently drops deliveries, or takes ingest down under realistic conditions. **High**: likely to hit production (stalls, unbounded growth, duplicate storms, security exposure, status codes that make providers give up). **Medium**: a real defect with a narrower trigger, or real operational pain. **Low**: hardening.

Paths below are relative to `packages/ankusa/lib/ankusa/` unless they start with a package or top-level directory.

## Verdict

The hot path is honest and fast. A request blocks in the batcher until the group commit's `fdatasync` returns, a full queue is a `503`, and nothing on that path answers `2xx` early. With 32 concurrent clients on a laptop the edge acked 14–16k small hooks per second, every one a `201` (probe 3, control run). The GCRA rate limiter (one ETS row per tenant, compare-and-swap through `:ets.select_replace/2`, no process on the hot path) is the best OTP in the tree.

Everything around that path falls apart, and it does so in the places a webhook receiver exists to get right:

- **Recovery destroys acked data.** Replay truncates the log at the first bad frame, discarding every acked record after it (W2). It also reads the whole log in one call and treats a failed read as an empty log: on macOS a WAL over 2 GiB, and on Linux one whose allocation is refused, is truncated to zero bytes at the next boot; otherwise a WAL larger than the container's memory limit OOM-kills every boot (W1). All reproduced.
- **Optional tiers take ingest down.** One expected `{:error, _}` from the object store crashes the compactor, and the flat `one_for_one` tree with the default budget of 3 restarts in 5 seconds rebuilds the whole instance (listener, batchers, WAL replay) every 4.5–6.5 s while the bucket is unreachable. With a 301 MB backlog the listener went dark six times in 30 s, each outage longer than the last (144 → 636 ms) as the untruncated WAL grew to 1.8 GB (S1). A full disk is worse: it tears a frame in the DLQ or the storage index, after which new dead letters silently vanish and the node no longer boots (S3). Reproduced.
- **One bad hook halts the node.** A sink returning `:error` instead of `{:error, reason}` crashes the pipeline on every poll; the instance and then its parent give up 3.3 s after the hook's `201` (D3). In a release that stops the VM, and the hook is still in the WAL at the next boot. Reproduced.
- **One tenant's dead sink stops every tenant's delivery.** Retries sleep inside tasks that hold global concurrency slots, and stuck envelopes fill the global admission window. Reproduced: a healthy tenant delivered 0 of 100 hooks in 15 seconds (D1).
- **Three paths answer `2xx` and then never deliver or dead-letter:** a source missing at dispatch time (D2), an unroutable RabbitMQ publish (B1: the broker confirmed it and kept nothing; reproduced), and a quarantined hook (`202`, and no code can ever read it back; E1). A fourth is dead-lettered but never deliverable: a non-UTF-8 `Content-Type` is acked `201`, rejected by every queue sink, and its DLQ entry makes every unfiltered replay fail (B8, D5; reproduced).
- **A stalled WAL answers `503` for hooks it then stores.** Every `503` sent while the WAL was unresponsive for 8 s or 20 s was committed and delivered anyway (W4). Reproduced.

The recurring defect is inverted assertiveness. The code is assertive where it should tolerate (`:ok = Ankusa.BlobStore.put(...)` crashes on a 503 the `BlobStore` contract says it can return) and tolerant where it must assert (`elem(:file.pread(fd, 0, size), 1)` turns a failed read into an empty log, then truncates the file to match). "Let it crash" is for states nobody expected. A 503 from S3 is expected; a failed read of the WAL at boot is the one place where crashing is the only correct answer.

Architecturally, the ceiling is one `GenServer` per node that owns every byte: appends, every reader's `pread` and decode, cursor and floor `fsync`s, and multi-gigabyte rewrites. Every role that touches the WAL must live in that node, so scale-out means N fully independent nodes (independent WALs, buckets, DLQs and source stores), and the features that look fleet-wide (the claim-check gateway, API-managed sources) do not compose with that shape.

## Top findings

| ID | Severity | Finding | Evidence |
|---|---|---|---|
| [W1](#w1) | Critical | A WAL one `pread` cannot return is truncated to 0 bytes at boot; otherwise boot needs RAM ≥ the WAL | Reproduced (macOS, Linux) |
| [W2](#w2) | Critical | One bad byte mid-log truncates every acked record after it | Reproduced |
| [S1](#s1) | Critical | An object-store outage rebuilds the whole instance, listener included, every 4.5–6.5 s | Reproduced |
| [S3](#s3) | Critical | A full disk tears the DLQ and the storage index: later dead letters vanish, and the node stops booting | Reproduced |
| [S4](#s4) | Critical | LocalFS segments and claim packs are never fsynced, yet the WAL floor moves past them | Code |
| [D1](#d1) | Critical | One tenant's dead sink stops delivery for every tenant | Reproduced |
| [D2](#d2) | Critical | Hooks whose source is gone at dispatch time are dropped silently | Code |
| [D3](#d3) | Critical | One hook to a sink with a non-conforming return value halts the node | Reproduced |
| [B1](#b1) | Critical | RabbitMQ confirms unroutable publishes; `durable?` says true | Reproduced |
| [O1](#o1) | Critical | One flat supervisor and one restart budget for every component | Code (S1, D3 reproduce it) |
| [W4](#w4) | High | A stalled WAL answers `503` for records it then commits | Reproduced |
| [E1](#e1) | High | Quarantine answers `202`; nothing can read the pen back | Code |
| [B8](#b8) | High | A non-UTF-8 `Content-Type`: `201`, never deliverable to a queue sink, and it breaks every unfiltered DLQ replay | Reproduced |
| [E3](#e3) | High | Unauthenticated POSTs mint Prometheus series: 20k requests → 280k series | Reproduced |
| [S7](#s7) | High | The compactor reads up to ~85× what it compacts, inside the WAL process | Reproduced (26× at 4 MB) |
| [W5](#w5) | High | Without `:storage` the WAL is never truncated, and the docs use that role set as the example | Code |
| [K1](#k1) | High | Headers stop at the queue boundary: header-borne provider ids cannot be deduped downstream | Code + docs |

## Where it breaks

```mermaid
flowchart LR
    P[Provider] -->|POST| R["Edge.Router<br/>E3 E4"]
    R --> I["Ingest<br/>E1 E2 E5 E8"]
    I -->|verify failed| Q[("Quarantine<br/>E1 E2 S3")]
    I --> B["Batcher<br/>E6 W4 W9"]
    B --> W[("WAL GenServer<br/>W1 W2 W3 W4 W5 W6")]
    W --> C["Compactor<br/>S1 S2 S3 S7"]
    C --> BS[("Blob store<br/>S4 S5 S6")]
    W --> D["Dispatch.Pipeline<br/>D1 D2 D3 D4 D6 D7"]
    D --> DLQ[("DLQ<br/>D5 S3")]
    D --> SK["Sinks<br/>B1-B10"]
    SK --> CO["Consumers<br/>K1 K2 K3"]
    CO -->|GET /v1/claims| G["Claim gateway<br/>C1 C2"]
    G --> BS
```

## 1. Ingestion

<a id="e1"></a>
### E1 · High · Code — Quarantine acks with `202`, and the pen is write-only

`edge/router.ex:76-77`, `edge/quarantine.ex:61-79`, `edge/ingest.ex:156-161`

A verification failure under `on_verify_failure: :quarantine` writes the full envelope to `quarantine/quarantine.log` and answers `202`. Providers treat any `2xx` as delivered and stop retrying. Nothing in the codebase reads that file back. Its only consumer is `Quarantine.recent/1`, an in-memory list of the last 200 summaries with body and headers dropped (`quarantine.ex:77-78`), empty after any restart, served by `GET /v1/quarantine`. There is no replay, no re-verify, no export. The verifiers accept exactly one `:secret` (`verifier/hmac.ex:247-253`; no multi-secret option exists), so rotating a secret is a hard cutover, and every hook in that window lands in a pen an operator can only recover by hand-decoding `term_to_binary` frames. `docs/delivery.md:459-460` says "a hook in the pen was never acked"; the provider received a `202`.

**Fix:** answer non-2xx (`503` with `Retry-After` lets the provider hold the event until the secret is fixed), or ship a re-verify/replay path that feeds the pen back through ingest. Add a `secrets: [current, previous]` rotation window to the verifiers.

<a id="e2"></a>
### E2 · High · Code — The quarantine bucket is global, its refusal is a `401`, and its log is unbounded

`edge/quarantine.ex:44-54,61-88`, `edge/ingest.ex:159`, `edge/router.ex:79-80`

The token bucket (100 burst, 20/s, hard-coded) is one field in one GenServer for the whole instance. Forged traffic above 20/s on any quarantine source drains it, after which real hooks on every other quarantine source get `{:rejected, {:quarantine_rate_limited, _}}` → `401 verification_failed`: a capacity limit reported as a permanent authentication failure. The bucket only slows the fill. Each token appends a full body (up to `max_body_bytes`, 8 MB) with no size cap, rotation or retention: up to 160 MB/s from unauthenticated senders, onto the volume the WAL lives on. That contradicts `architecture.md:157` ("a flood of forged requests can't fill the disk"). `:ok = :file.write(...)` and `:ok = :file.datasync(...)` (`quarantine.ex:74-75`) crash the process on `ENOSPC`, once per request, so four forged requests within five seconds exhaust the instance's restart budget (O1); the write that failed also leaves a torn frame behind (S3).

**Fix:** per-source buckets, `429`/`503` + `Retry-After` on exhaustion, a byte cap with rotation, and I/O errors handled as values.

<a id="e3"></a>
### E3 · High · Reproduced — Unauthenticated requests control Prometheus label cardinality and buffer full bodies

`metrics.ex:78-94`, `edge/ingest.ex:39-49`, `route_resolver.ex:46-58`, `edge/router.ex:41-57`

`ankusa.ingest.requests.total` and `ankusa.ingest.duration.seconds` are tagged with `source_id`, taken verbatim from the URL path segment, and the `[:ingest]` span wraps the source lookup, so unknown sources are counted under their own label. With the default `routes.enabled: false` and the server image's default `admin.enabled: true` (`ankusa_server/lib/ankusa_server/config.ex:374`), every distinct path mints 14 series in the reporter's ETS tables, and every scrape renders all of them. Probe 5: 20,000 POSTs to random paths, all answered `404`, grew `/metrics` from 0 to 280,000 `ankusa_ingest_*` series (28.9 MB per scrape) and VM ETS memory from 1,485 KiB to 26,734 KiB. Linear, unbounded, no authentication involved.

The same requests are expensive before they are refused. `read_body(length: max)` stops at `max_body_bytes`, so the `413` itself is honest, but every request, oversize ones included, may hold up to 8 MB before it is answered; `capture/2` reads the body before `Ingest` looks the source up; and `build_envelope/3` then runs `:binary.copy/1` on it (`ingest.ex:176`), a second full copy of a binary that is already standalone. Worst-case memory is bounded only by Bandit's connection limit.

**Fix:** label by source only after the lookup succeeds (an `unknown` bucket otherwise); reject on `Content-Length > max_body_bytes` before reading any of the body; look the source up before reading; copy only when `:binary.referenced_byte_size(body) > byte_size(body)`.

<a id="e4"></a>
### E4 · High · Code — Transient infrastructure failures are answered with `404`

`source_store.ex:60`, `edge/ingest.ex:42-45`, `source_store/persistent.ex:246-261`, `ankusa_redis/lib/ankusa/routes/store/redis.ex:420-426,442,564-577`

Providers treat `4xx` as permanent, and several disable an endpoint after repeated failures. Ankusa produces `404` for states that do not mean "this endpoint does not exist":

- `SourceStore.fetch/2` returns `{:ok, Source.t()} | :error`; it cannot express "unavailable". A database-backed store, which `multi-tenancy.md` recommends, can only say `:error`, which becomes `404`.
- `SourceStore.Persistent` quarantines `sources.json` (renames it to `.corrupt-<ts>` and boots empty) on *any* read result other than `:enoent`: `EACCES` after a UID change, `EIO`, or a `version` written by a newer release after a rollback. Every API-managed source then `404`s, and the next write persists a file containing only the new entry.
- The Redis route store follows the stored version *down*. A flushed Redis, or a failover to an empty replica, reads as version 1 with an empty table (`stored_version/1` maps a missing key to 1); every node adopts it within one tick, and with `routes.enabled` every hook is a `404`. `decode_rules(nil)` also resets the global IP rules to `default: :allow`.
- API-managed sources live in a node-local `sources.json` under `data_dir` (`source_store/persistent.ex:247`). Behind the load balancer the deployment docs prescribe, a source created through one node's admin API is a `404` on every other node.

**Fix:** a three-valued store contract (`{:ok, _} | :error | {:error, :unavailable}`, the last mapped to `503`); fail boot instead of quarantining on read errors that are not corruption; keep last-known-good routes and alarm when the namespace empties; a shared source store whenever more than one node serves ingest.

<a id="e5"></a>
### E5 · Medium · Code — The URL tenant is not bound to the source, and `accept_flag` spends budget

`edge/ingest.ex:52,68-93,117`

`tenant_id = Map.get(req, :tenant_id) || source.tenant_id` with only a format check. Under `RouteResolver.TenantPath`, `POST /webhooks/<any-tenant>/<source>` files the hook under any tenant's storage partition, claim-check namespace and rate-limit bucket. For sources with `Verifier.None` (the `Source.new/2` default) or `accept_flag`, an anonymous sender can throttle a tenant with traffic aimed at another tenant's source. Separately, `accept_flag` routes failed verifications through `admit/3` and charges the tenant's budget, contradicting the comment at `ingest.ex:75-78` that a forged flood is free.

<a id="e6"></a>
### E6 · Medium · Code — Ingest backpressure counts records, not bytes

`edge/batcher.ex:81-92`, `config.ex:28-36`

`max_queue` is 10,000 *records* per partition, two partitions by default: up to 20,000 blocked request processes, each holding a body of up to 8 MB. The dispatch side bounds bytes (`max_inflight_bytes`); the side that actually holds request memory does not.

<a id="e7"></a>
### E7 · Medium · Code — Direct mode publishes sequentially with no overall deadline

`edge/publish.ex:29-50`, `sink.ex:118-126`

Under `wal.type: none` the request process publishes to each sink in turn. Each has its own ~5 s timeout and `safe_deliver/4` adds none, so three slow sinks make a 15 s request, past GitHub's 10 s delivery timeout. The provider times out and retries while sink 1 already holds the hook and a later sink may still confirm late. Publish concurrently under one deadline shorter than the provider's timeout.

<a id="e8"></a>
### E8 · Medium · Code — An omitted HMAC secret is the empty key

`verifier/hmac.ex:247-253`

`Keyword.get(opts, :secret, "")`: a library user who forgets `:secret` gets a verifier that accepts any hook signed with the empty key, which anyone can compute. An explicit `nil` raises per request instead, a `500` loop. Validate at boot and refuse empty secrets.

## 2. Group commit and the WAL

<a id="w1"></a>
### W1 · Critical · Reproduced — Replay reads the whole WAL in one call, and a failed read truncates it to zero

`wal/disk_log.ex:414-421,446-447`, `wal/disk_log.ex:83-86`

```elixir
data = if size > 0, do: elem(:file.pread(fd, 0, size), 1), else: <<>>
```

`elem(_, 1)` of `{:error, :einval | :eio | :enomem}` is a bare atom. `parse/5` matches nothing on an atom, falls through to `_ -> {pos, next_seq}` (`disk_log.ex:446-447`) and returns `{0, 1}`; `init/1` then positions at 0 and calls `:file.truncate/1`. Every acked, undispatched hook is destroyed, and the only trace is the `:info` boot line `recovered 0 record(s)`. Nothing else about the read is bounded: the file comes back as one binary, so boot needs RAM at least the size of the WAL.

What the next boot does with the WAL (probe 2):

| Platform | WAL | Result |
|---|---|---|
| macOS (`pread` rejects counts above `INT_MAX`) | 2.3 GB, 2,200 acked hooks | `{:error, :einval}` → truncated to 0 bytes, 0 records |
| Linux container, `--memory=1g` | 2.3 GB, 2,200 acked frames | OOM-killed during replay, before anything is truncated; every boot repeats the same read, so the node never comes back |
| Linux container, `--memory=8g` | 2.3 GB, 2,200 acked frames | full read, all 2,200 recovered |
| Linux container, `ulimit -v` 4 GB or 8 GB | 5 acked frames, file extended (sparse) to 64 GiB | `{:error, :enomem}` → truncated to 0 bytes, 0 records |

The Docker VM reports `vm.overcommit_memory=1`; there, even the 64 GiB allocation was granted and the reader was OOM-killed instead. Wherever the allocation is refused (an address-space limit, as above, or strict overcommit [INFERENCE]), Linux takes the same truncate-to-zero branch as macOS. The Linux runs used verbatim copies of `frame/2`, `replay/3`, `parse/5` and `init/1`'s truncation in a dependency-free script.

The WAL reaches these sizes in situations the docs present as safe: a sink outage, a compactor that cannot write (S1: it crash-loops, and only the compactor truncates), or a role set without `:storage` (W5). With S1 in play, every instance rebuild replays the log again.

**Fix:** stream replay in bounded chunks with carry-over; match `{:ok, data}` and refuse to start on anything else.

<a id="w2"></a>
### W2 · Critical · Reproduced — The first bad frame truncates every acked frame after it

`wal/disk_log.ex:423-449`, `wal/disk_log.ex:83-86`

`parse/5` cannot tell a torn tail from corruption in the middle: a CRC mismatch (`437-440`) or a short frame (`442-443`) ends the scan, and `init/1` physically truncates there. Probe 1: ten acked records, one byte flipped in frame 3's payload. After restart only seqs `[1, 2]` remain and the file shrinks from 1,972 to 394 bytes. Eight acked records are destroyed, seven of them intact, and the only trace is the boot line's record count. The scan also covers the dead prefix still physically present below the truncation floor (the file is only rewritten past 64 MiB), so a bad sector in already-reclaimed data wipes the live suffix too. `storage.md:64-65` documents the behaviour as a feature ("truncates the file at the first torn/invalid one"); `architecture.md:151` describes it as dropping "a torn trailing frame". Neither says that every acked frame after a mid-log error goes with it.

The probe ran without persisted cursors, so its `next_seq=3` is not a seq-reuse finding: in a running deployment `next_seq` is at least every persisted cursor + 1 (`disk_log.ex:93-94`).

**Fix:** drop a tail only when no valid frame follows it (scan forward for the next magic with a valid CRC); otherwise refuse to start and name the damaged range. Skip CRC checks for frames at or below the floor.

<a id="w3"></a>
### W3 · High · Code — One process owns every byte, and reclamation fsyncs and rewrites on the append path

`wal/disk_log.ex:122-138,143-177,183-220,288-306,315-379,453-457`, `envelope.ex:62-64`

The `DiskLog` GenServer is the only path to the file, so it serializes:

- every append, including `term_to_binary(…, [:deterministic])` per record and a frame binary that copies the payload again;
- every reader: dispatch (up to 128 records per call) and the compactor (256) each get one `:file.pread/3` and one `binary_to_term/1` *per record*, inside the writer (S7 shows how many);
- every cursor persist (open, write, `fdatasync`, rename); dispatch's runs on every 200 ms poll that moved the watermark;
- every truncation: `truncate_through/2` persists the floor (write, `fdatasync`, rename; `disk_log.ex:207`) before the ETS deletes, so each compaction tick queues two fsyncs (the compactor's cursor, then the floor) ahead of waiting appends;
- the rewrite: once the dead prefix passes 64 MiB and is at least as large as the live suffix, `rewrite/3` copies the live suffix in 8 MiB chunks, synchronously, then `:ets.tab2list/1`s the whole index into the process heap to re-offset it. After a backlog drains, that suffix can be gigabytes.

`storage.md:66-68` says reclaiming "costs a few ETS deletes and never blocks appends". Even with no rewrite that is false: every reclaim is an fsync inside the WAL process, and every ingest ack waits behind whichever of these is running.

A hook's body is copied four times before it is durable: Bandit's read, `:binary.copy/1` (`ingest.ex:176`), `term_to_binary/2`, and the frame construction `<<…, payload::binary>>` (`disk_log.ex:456`); the compactor encodes it a fifth time. Serialization happens inside the WAL process only because `build_batch/2` writes the seq into the envelope before serializing it (`disk_log.ex:294-295`), although the frame header already carries the seq.

**Fix:** drop the seq from the payload and set it from the index key on read, so the batcher's task can serialize in parallel and hand the WAL iodata `[header, payload]` with the CRC computed separately; serve reads from a descriptor owned by a separate reader process (raw descriptors are bound to the process that opened them); move the rewrite off the append path, or make segments the unit of reclamation.

<a id="w4"></a>
### W4 · High · Reproduced — A stalled WAL answers `503` for records it then commits

`wal/disk_log.ex:122-135`, `edge/batcher.ex:105-125,182-188`, `wal.ex:20-23`

`append/2`, `read/3` and `put_cursor/3` are plain `GenServer.call/2`s with the default 5 s timeout; the batcher's own 15 s (`batcher.ex:54`) never covers the WAL. When the WAL stops answering for longer (an `fdatasync` stall, or a physical rewrite running inside `truncate_through/2`'s `handle_call/3`, `disk_log.ex:204-211` → `rewrite/3`), the batcher task's call exits, `safe_append/2` returns `{:error, {:timeout, _}}`, every caller in the batch gets `503`, `inflight` is cleared, and the next batch is sent. The timed-out `{:append, records}` message is still in the WAL's mailbox and commits when the WAL comes back. Since OTP 24 the late reply is dropped by the call's alias, but nothing cancels the server-side work.

Probe 9 made the WAL process unresponsive (`:sys.suspend/1`) under 32 clients:

| WAL unresponsive | `503`s sent | of those, committed and delivered anyway | instance / dispatch / compactor restarts |
|---|---|---|---|
| 8 s | 17 | 17 | 0 / 1 / 1 |
| 20 s | 64 | 64 | 0 / 2 / 2 |

Every `503` was a hook that was stored and delivered, so the provider's retry is a second copy. The WAL contract says the opposite (`wal.ex:20-23`: "A failed `append/2` makes none of its records visible"). Each timeout abandons another batch to the mailbox outside `max_queue`'s accounting, so the queue bound stops bounding memory for the length of the stall (up to one `max_batch` per partition per 5 s). The pipeline and the compactor do not catch the exits from their own `read`/`put_cursor` calls: each crashes once per 5 s timeout, too slow to trip the instance's restart budget but enough to kill in-flight deliveries every time.

**Fix:** carry a deadline in the request and drop expired batches server-side, or have the batcher wait for the in-flight commit instead of abandoning it; handle call timeouts as values in the pipeline and the compactor.

<a id="w5"></a>
### W5 · High · Code — WAL reclamation belongs to the `:storage` role

`storage/compactor.ex:200-206`, `instance.ex:198-204`, `docs/configuration.md:243,504-505`

`Compactor.write_segment/2` is the only caller of `WAL.truncate_through/2`, and the compactor runs only under `:storage`. `configuration.md` gives `ANKUSA_ROLES=edge,dispatch` as its example; with it the log is never truncated, grows until the volume is full, and meets W1 at the next boot. Even with `:storage`, truncation happens only right after a segment write, using the dispatch cursor sampled at that moment. A deployment that wants delivery without an archive must still archive every hook to reclaim its WAL.

**Fix:** reclaim through the minimum of the cursors of the roles that actually run, driven by whichever consumer advances.

<a id="w6"></a>
### W6 · Medium · Code — The in-memory index grows with the backlog

`wal/disk_log.ex:76,148,433`

One `{seq, {offset, len}}` row per live record in an `:ordered_set`, roughly 100 bytes each [INFERENCE]. A day-long backlog at 1,000 hooks/s is ~86M rows and ~8 GB of ETS, rebuilt by a full-file replay (W1) on every restart.

<a id="w7"></a>
### W7 · Medium · Code — No directory `fsync` anywhere

`wal/disk_log.ex:83,359,507-520`, `blob_store/local_fs.ex:16-23`, `source_store/persistent.ex:317-336`

The WAL file's creation, the rewrite's rename, the `.cursors`/`.truncated` renames, LocalFS segment and claim renames, and `sources.json` all rely on the filesystem to persist the directory entry. POSIX does not promise that without an `fsync` of the parent directory. For the WAL itself (a freshly created file, or a renamed rewrite followed by acked appends) that is an acked-data hole on filesystems that do not order directory metadata [INFERENCE].

<a id="w8"></a>
### W8 · Medium · Code — A corrupt sidecar crash-loops the WAL

`wal/disk_log.ex:486-498`

`binary_to_term(bin, [:safe])` on `.cursors` and `.truncated` has no error handling: a zero-length or damaged file raises in `init/1` on every start, and the instance gives up after three tries. Defaulting to 0 would be worse (seq reuse). Checksum the file and refuse to start with a message that names it.

<a id="w9"></a>
### W9 · Low · Code — The batcher's defensive `:DOWN` clause is unreachable

`edge/batcher.ex:127-139,169`

`Task.async/1` links the commit task to a batcher that does not trap exits, so a task killed by a signal takes the batcher down through the link before `:DOWN` can be handled. Blocked callers get exits (mapped to `503` at `ingest.ex:149-150`) rather than the reply the comment describes. `Dispatch.Pipeline` already uses `Task.Supervisor.async_nolink/2`; use it here too.

## 3. Compaction and object storage

<a id="s1"></a>
### S1 · Critical · Reproduced — Any object-store error crashes the compactor, and the whole instance follows

`storage/compactor.ex:99-108,181,198`, `instance.ex:54`

`write_segment/2` does `:ok = Ankusa.BlobStore.put(instance, key, segment)`, although the behaviour (`blob_store.ex:13-14`) and every remote adapter return `{:error, reason}` on a timeout, `5xx` or auth failure; `:ok = Index.append(...)` raises on `ENOSPC`. The `{:error, reason}` branch in `compact/2` that promises "segment is rewritten next tick" is reachable only from `put_cursor`. A store that fails fast (an S3 `403`, a refused connection) crashes the compactor once per 1 s tick; it shares one supervisor and one restart budget with everything else (O1), so the instance supervisor gives up at the fourth crash and its parent rebuilds the entire tree.

Probe 3, 32 clients for 15 s against a healthy edge with a nearly empty WAL:

| Object store | Requests | Outcomes | Instance rebuilt at |
|---|---|---|---|
| LocalFS (control) | 210,466 | 210,466 × `201` | never |
| returns `503` | 229,148 | 225,564 × `201`, 3,580 × `ECONNREFUSED`, 4 × `closed` | 7.5 s, 14.1 s |

What a rebuild costs is WAL replay, because Bandit starts only after `DiskLog.init/1`. Probe 10 seeded 301 MB first, then failed the store, with 4 clients posting and a sink that takes 100 ms:

| Measured over 30 s | Result |
|---|---|
| instance rebuilds | 6, every ~4.5 s |
| listener down per rebuild (ms) | 144, 183, 452, 483, 636, 602 (second run: 141, 168, 526, 393, 646, 636) |
| WAL size | 301 MB → 1,767 MB; nothing truncates while the compactor is down |
| redeliveries | 32 per rebuild (`dispatch.concurrency`) when the sink's side effect lands before it returns; 2 in total when it lands after |

Each rebuild replays the whole, growing WAL (W1) and kills every in-flight delivery. `terminate/2` persists the dispatch cursor on the way down, so only that in-flight set is redelivered, but a receiver that had already processed one of those requests gets it twice. `storage.md:82-83` admits that a full disk can crash-loop the compactor when the local blob store shares the volume. Nothing says that a remote store's `503` takes the listener with it, and `architecture.md:61-63` ("Take the compactor down: ingest keeps acking, the WAL grows, an alarm fires, nothing is lost") and the `Ankusa.Instance` module doc ("killing the storage or dispatch tree never stops the edge from acking") say the opposite. No alarm can fire either: there is no WAL-size metric (O5).

**Fix:** treat store errors as values, with exponential backoff in the compactor; never `:ok =` an I/O call in an optional tier.

<a id="s2"></a>
### S2 · High · Code — The storage index grows forever and is loaded whole on every compactor start

`storage/index.ex:54-63,76-89,116-123`

One row per hook ever compacted, appended forever to `segments/index.log`, never pruned. `open/1` reads the whole file into one binary, decodes every row into a list, and inserts them into ETS each time the compactor starts, including every restart in S1's loop. At 100 hooks/s that is 8.6M rows a day; at a few hundred bytes per row on disk and per row in ETS [INFERENCE: sizes estimated], gigabytes of file and of RAM per day, with no ceiling, on a node meant to run for months. A node without the table (no `:storage` role, or mid-restart) answers each lookup with a full-file scan.

**Fix:** a disk-backed index (per-segment sidecars or a sorted on-disk index) with retention tied to the segments it points into.

<a id="s3"></a>
### S3 · Critical · Reproduced — A full disk tears the DLQ, the index and the quarantine log for good

`durable_log.ex:69-81,112-117`, `storage/index.ex:54-63`, `storage/compactor.ex:55`

The DLQ, the storage index and the quarantine pen share `<<len::32, term>>` framing with no CRC and no tail repair. On `ENOSPC`, a raw `:file.write/2` leaves whatever fit on disk and returns `{:error, :enospc}`; `append_synced/2` then raises on `:ok = :file.write(...)`, and nothing truncates the torn tail on the next open. The first append after space is freed lands behind it, and the torn frame's length prefix swallows it:

- Probe 12 ran the real `Ankusa.DurableLog` on a 2 MB tmpfs in a Linux container. The third 300 KB DLQ append raised `{:error, :enospc}` and left 194,502 bytes of a torn frame. After the filler file was deleted, two more dead letters appended without error, and `DurableLog.read/2` returned only the first two entries. Dispatch advanced its cursor past those two, so their WAL copies are reclaimed: acked hooks, gone.
- When later appends supply more bytes than the torn frame claimed, `binary_to_term/2` decodes garbage and `read/2` raises instead (probe 11). In `segments/index.log` that raise happens in `Index.open/1`, inside `Compactor.init/1`, and the node does not boot: `failed to start child: {Ankusa.Instance, …}` ← `{Ankusa.Storage.Compactor, …}` ← `ArgumentError` from `binary_to_term(…, [:safe])`. It keeps not booting after the disk is cleaned.

The system produces the disk-full event on its own: the WAL without `:storage` (W5), the index (S2), the quarantine log (E2) and the never-pruned DLQ all grow without bound on the same `data_dir`. `storage.md:76-81` says a full disk "is reported, not fatal" and "Ingest resumes on its own once space frees"; for the DLQ and the index it does not.

**Fix:** a CRC per frame, truncation of a torn tail before the first append after open, and I/O errors handled as values.

<a id="s4"></a>
### S4 · Critical · Code — LocalFS writes are atomic but not durable, and the WAL floor moves past them

`blob_store/local_fs.ex:16-23`, `storage/compactor.ex:181-206`

`File.write!/2` then `File.rename!/2`, with no `fsync` of the file or its directory. The compactor then fsyncs the index rows, the compactor cursor and the WAL floor: durable metadata pointing at segment bytes that may still be only in the page cache, a window that reopens on every 1 s tick. Claim packs go through the same `put/4`, and dispatch publishes the ref and advances its cursor once it returns. A power loss inside the writeback window leaves:

- a floor past records whose segment does not exist, so the archive copy is gone and `Storage.fetch/2` cannot find them;
- for a claim-checked hook, a ref already on the broker pointing at a pack that never reached the disk, while the WAL copy is logically truncated. The gateway answers `404`, which the SDK classifies as permanent: the acked payload is lost for good.

`architecture.md:27-28` and `storage.md:84` say `WAL.DiskLog` + `BlobStore.LocalFS` survive power loss on the box.

<a id="s5"></a>
### S5 · Medium · Code — Segment keys collide across nodes, and there is no key prefix

`storage/compactor.ex:180`, `blob_store/s3.ex`

`seg/<first_seq>-<last_seq>.seg` names no node, seqs start at 1 on every node, and the S3/GCS/Azure/OCI adapters take a bucket but no key prefix, so every node needs its own bucket (`deployment.md:126-129`). That requirement is what breaks the claim-check topology (C1).

<a id="s6"></a>
### S6 · Medium · Code — The blob-store adapters are single-shot and lossy on listing

`blob_store/s3.ex:62-66,83-125`, `blob_store/{gcs,azure,oci}.ex`

One attempt per call, no retryable/permanent classification, and a 10 s default timeout for a 16 MiB whole-body PUT (`IO.iodata_to_binary/1` copies the segment first). `list/3` is unpaginated (S3 returns at most 1,000 keys) and returns `[]` on any error, so an outage is indistinguishable from an empty bucket. S3 credentials come only from options or `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` via `System.fetch_env!/1` (`s3.ex:104-105`): no session tokens, IRSA or instance roles, and a missing variable raises at the call site.

<a id="s7"></a>
### S7 · High · Reproduced — The compactor reads up to ~85× what it compacts, inside the WAL process

`storage/compactor.ex:7-9,29,120-165`

`collect/6` asks the WAL for `@read_chunk` = 256 records, `take_within_budget/4` keeps only what fits the remaining `roll_bytes` budget (16 MiB), and the next read restarts right after the last record kept. Everything else in the chunk is read, decoded and discarded, then read again for the next segment. Probe 13 committed 256 hooks of 4 MB and ran one tick: 52 segments, 56 `{:read, …}` calls, and 6,682 records (~26 GB) pulled through the WAL process to compact 256, a 26.1× amplification, in 5.2 s, while small-hook acks peaked at 410 ms. By the same arithmetic, 8 MB bodies (3 kept per 2 GB read) are ~85× and 1 MB bodies ~15× [INFERENCE: computed]. On a disk whose reads are not page-cached, one 1–2 GB read can outlast the 5 s call timeout, which exits the compactor (W4) [INFERENCE]. The module doc's "peak memory is a chunk plus one segment" is accurate, and that is the problem: a chunk is 256 bodies, up to 2 GB.

**Fix:** carry the unused tail of a read into the next segment, and size reads in bytes, not records.

## 4. Dispatch

<a id="d1"></a>
### D1 · Critical · Reproduced — One dead sink stops delivery for every tenant

`dispatch/pipeline.ex:217-237,403-431,455-466`, `sink/http.ex:66-69`

Two mechanisms, both global:

- **Concurrency slots.** `deliver/5` retries with `Process.sleep/1` inside the task, so a retrying delivery keeps its slot for the whole retry schedule. `Sink.Http` exports `ordering_key/2` and returns `nil` unless `ordered: true`, so the per-source fallback never applies to it, and a dead HTTP endpoint with 32 or more admitted hooks occupies all 32 slots.
- **The admission window.** With an ordering key (broker sinks, `Sink.Http` with `ordered: true`, any sink without `ordering_key/2`), the dead sink's lane runs one job at a time while its other envelopes wait in the lane queue. `remaining` holds every admitted envelope: those queued behind the stuck lane, and multi-sink envelopes whose healthy sink already finished. Once it reaches `max_inflight` (4,096) or `max_inflight_bytes` (128 MiB, about 128 stuck hooks at 1 MB bodies), `fill/2` stops reading the WAL for every source (`pipeline.ex:220-222`).

Probe 4 acked hooks for tenant `acme` (sink always returns `{:error, :connection_refused}`), then 100 hooks for tenant `globex` (healthy sink):

| Dead sink's lanes | acme hooks | globex delivered at 1 s / 5 s / 15 s |
|---|---|---|
| one per source | 4,200 | 0 / 0 / 0 |
| none (`Sink.Http` default) | 64 | 0 / 0 / 0 |

Under the default policy a dead hook gives up after 55–111 s of jittered backoff, ~83 s on average [INFERENCE: computed, not waited out]. Without lanes, globex starts once two waves of 32 dead hooks have given up: 2–4 minutes. With the lane, globex's first hook is seq 4,201, beyond the 4,096-envelope window, so it is read only after 105 dead hooks have each failed in turn: 1.6–3.2 hours. Draining all 4,200 takes about four days. While the dead sink keeps receiving traffic, the whole pipeline advances at the dead lane's give-up rate. `delivery.md:11-13,33-35` says "a slow or retrying sink only delays what actually has to wait for it" and "one failing sink never blocks delivery to the others".

**Fix:** retries as pipeline-owned timers (`Process.send_after/3` re-enqueues the job and the slot is free while it waits); per-sink concurrency caps and admission windows; a per-sink circuit breaker that parks a dead destination's work instead of burning attempts.

<a id="d2"></a>
### D2 · Critical · Code — A source that is gone at dispatch time silently drops its acked hooks

`dispatch/pipeline.ex:239-245,358-372`, `dispatch.ex:27-31`

`sinks_for/3` maps `SourceStore.fetch/2 → :error` to `[]`, and `admit/2` treats `[]` as "nothing to deliver: handled". `read_seq` advances; there is no DLQ entry, no log line, and no telemetry beyond the `completed` counter; the WAL is then truncated past the hook. No bug is needed to trigger it: an operator deletes or renames a source while its backlog is in the WAL (`DELETE /v1/tenants/:tenant/sources/:name`, `admin/router.ex:121-123`), or `SourceStore.Persistent` skips an entry its decoder now rejects (`persistent.ex:288-291`) or quarantines the whole file (E4). `Dispatch.replay/2` has the same fallback.

**Fix:** "no sinks configured" and "no such source" must be different outcomes. Hold or dead-letter the latter, or snapshot the sink list into the envelope at ack time.

<a id="d3"></a>
### D3 · Critical · Reproduced — One hook to a sink with a non-conforming return value halts the node

`dispatch/pipeline.ex:153-193,448-477`, `sink.ex:118-126`

`Sink.safe_deliver/4` turns raises, throws and exits into `{:error, _}` but returns every other value verbatim, and the `case` in `deliver/5` has clauses only for `:ok` and `{:error, _}`. A custom sink returning `:error`, `{:ok, meta}` or `nil` raises `CaseClauseError` in the task; the pipeline answers the `:DOWN` with `{:stop, {:delivery_task_crashed, _}}`, restarts from the cursor, re-reads the same envelope one poll later, and crashes again. Probe 8 sent one hook to such a sink under a parent supervisor shaped like `AnkusaServer.Application`'s: `201` at 61 ms, four instance rebuilds, and the top-level supervisor exited `:shutdown` at 3.26 s. In a release (`applications: [ankusa_server: :permanent]`) that stops the VM [INFERENCE: standard OTP behaviour for a permanent application; not run as a release], and the hook is still in the WAL at the next boot, so the node cannot stay up until someone removes the record or fixes the sink. `Sink.ordering_key/3` raising during `admit/2` (in the pipeline process itself) and `Message.check_in/2` returning an unexpected shape loop just as fast.

Not every crash in this family trips the budget. `DLQ.write/3` on a full disk (`DurableLog.append_synced/2` is `:ok = :file.write/2` and `:ok = :file.datasync/1`, run inline in `handle_info/2`) crashes only at give-up, 55–111 s after each restart: a livelock that redelivers the window and pins the watermark, not an instance kill. By then the WAL is answering `503` anyway, and the failed write has torn the DLQ (S3).

**Fix:** normalize unexpected returns to `{:error, {:bad_return, value}}`, wrap the whole task body, and treat a DLQ write failure as "hold the cursor", not a crash.

<a id="d4"></a>
### D4 · High · Code — The retry budget is ~83 s per hook; past that the WAL stops absorbing an outage

`retry_policy/exponential.ex:17-30`, `dispatch/pipeline.ex:458-476`, `sink/http.ex:58-60`

Defaults: base 100 ms, cap 30 s, 12 attempts, which is 111 s of sleeps without jitter and 55–111 s (mean ~83 s) with it. During an outage of length T, only jobs whose budget elapses inside T are dead-lettered: about 32 × ⌊T / 83 s⌋ with unordered sinks (a three-minute downstream deploy dead-letters ~64), about one per 83 s per lane with ordered ones. Everything else waits behind them (D1) and delivers after recovery. The costs are elsewhere: the dead-lettered hooks depend on a replay path that is broken (D5); the WAL, the component built to absorb outages, absorbs nothing past ~90 s per hook; and nothing classifies errors, so a `400` or an oversized message gets twelve attempts and `Retry-After` on a `429` or `503` is ignored.

<a id="d5"></a>
### D5 · High · Reproduced — DLQ replay is unsafe, non-idempotent, synchronous, and one hook breaks it

`dispatch.ex:18-37`, `admin/router.ex:97-99,438-466`

`replay/2` reads the entire DLQ file and then, inside the admin HTTP request process, calls `mod.deliver/3` directly (not `safe_deliver/4`) for every sink, discards the result, and increments the count regardless. A sink that is still down reports `replayed: N` with nothing delivered. Entries are never removed or marked, so a second call, or a proxy retrying a timed-out request, delivers everything again. An empty body replays the whole DLQ. There is no concurrency limit, no ordering, no retry policy.

A raise aborts the loop. Probe 14 dead-lettered three hooks while a queue sink was down, one of them carrying a non-UTF-8 `Content-Type` (B8), then brought the sink back:

| Call | Result | Publishes for [entry before the poison, poison, entry after it] |
|---|---|---|
| `Ankusa.Dispatch.replay/2` | raised `ErlangError` | [1, 0, 0] |
| `POST /v1/dlq/replay`, empty body | `500` | [2, 0, 0] |
| `POST /v1/dlq/replay`, again | `500` | [3, 0, 0] |

All three entries remained. Every unfiltered replay re-delivers everything ahead of the poison entry and never reaches anything behind it, which then needs an `:id`, `:source_id` or `:since` filter to recover. One unauthenticated POST to a source on the default `Verifier.None` (`source.ex:17`) is enough to break the documented recovery path.

**Fix:** replay through the pipeline (re-append to the WAL with a replay marker, or keep a DLQ cursor), so it inherits `safe_deliver/4`, ordering, retries and at-least-once bookkeeping.

<a id="d6"></a>
### D6 · High · Code — No delivery deadline: a hung sink pins a slot and the watermark forever

`dispatch/pipeline.ex:418-430,532-537`, `ankusa_kafka/lib/ankusa/sink/kafka.ex:74-80`

Nothing bounds a `deliver/3` call. A custom sink blocked in `GenServer.call(_, _, :infinity)`, a socket without a timeout, or Kafka's `:brod.produce/5` (which waits for the producer's "buffered" reply in a bare `receive` while the partition buffer is full during a broker brown-out [INFERENCE from brod's source]) holds a slot permanently. Its seq stays the smallest in `pending`, so the durable cursor never passes it, the WAL cannot be truncated beyond it, and a restart redelivers everything after it.

**Fix:** a per-attempt deadline enforced by the pipeline (`Task.yield/2` + `Task.shutdown/2`, or a timer that kills the task and counts an attempt).

<a id="d7"></a>
### D7 · Medium · Code — Progress is per envelope, not per sink, and the byte window overshoots

`dispatch/pipeline.ex:100-103,217-237,514-528`

`remaining` counts outstanding jobs per seq and the cursor is a single watermark. If sink A succeeded and sink B is retrying when the node restarts, A gets the hook again, and so does every envelope admitted after the stuck one: up to the 4,096-envelope window, per sink, on every deploy that lands during a retry. `max_inflight_bytes` (128 MiB) is checked only before a read of up to 128 envelopes, so one read of 8 MB bodies overshoots it eightfold.

<a id="d8"></a>
### D8 · Medium · Code — The watermark is invisible

`telemetry.ex`, `metrics.ex`, `dispatch/pipeline.ex:447-476,539-559`

Dispatch emits only `[:dispatch, :stop]` (with no sink or source tag) and `[:dispatch, :dlq]`. There is no gauge for cursor lag against `max_seq`, the age of the oldest undelivered hook, pending/running/retrying counts, window saturation, or cursor-persist failures (a `Logger.warning` only). D1, D6 and W5 all look like healthy throughput to an operator until the disk fills.

<a id="d9"></a>
### D9 · Low · Code — Dispatch polls

`dispatch/pipeline.ex:606-609`

A 200 ms `:poll` with no wake-up on commit adds ~100 ms of median latency and sends a read through the WAL process every tick, even when idle. A commit notification from the WAL to its readers would cost nothing.

## 5. Sinks and brokers

<a id="b1"></a>
### B1 · Critical · Reproduced — RabbitMQ confirms unroutable publishes, and `durable?` says that is durable

`ankusa_rabbitmq/lib/ankusa/sink/rabbitmq/connection.ex:94-114`, `sink.ex:103-108`

`AMQP.Basic.publish/5` is called without `mandatory: true`, and there is no `basic.return` handler. RabbitMQ's documentation is explicit: "For unroutable messages, the broker will issue a confirm once the exchange verifies a message won't route to any queue" ([Consumer Acknowledgements and Publisher Confirms](https://www.rabbitmq.com/docs/confirms)). Probe 15 ran the real `Sink.RabbitMQ` against `rabbitmq:4-management-alpine`: `durable?/2` → `true`; `deliver/3` to the exchange the sink itself declares, with no queue bound → `:ok`; a queue bound afterwards held 0 messages. Under `wal.type: disk`, dispatch then advances and the WAL is truncated: an acked hook gone with no DLQ entry. Under `wal.type: none` the provider is answered `201` for a message no queue holds. The connection's module doc says a confirm means the message "really was persisted by RabbitMQ", and the sink's own docs make queue binding the consumer's job, so the no-binding state is routine: a fresh deploy before the consumer has declared its queue, a routing key that matches nothing, a deleted queue.

**Fix:** `mandatory: true` with a `basic.return` handler that fails the publish (or an alternate exchange validated at connect), and `durable?/1` returning false until then.

<a id="b2"></a>
### B2 · High · Code — RabbitMQ publishes one message at a time per exchange, and still runs abandoned calls

`ankusa_rabbitmq/lib/ankusa/sink/rabbitmq/connection.ex:39-43,94-114`

Every delivery for an exchange goes through one GenServer whose `handle_call/3` publishes and then blocks in `wait_for_confirms(chan, 5_000)`. Throughput per exchange is one message per confirm round-trip, with all 32 dispatch slots, or in direct mode every request process, queued in one mailbox. Probe 15 measured 2,748 msg/s from 32 concurrent callers against a broker on loopback; that is an upper bound, since a network round-trip and the broker's disk sync for persistent messages both add to every message. Callers time out after 15 s, but their requests stay in the mailbox and are published later while the retry enqueues a fresh copy: duplicates beyond one per retry, and a mailbox that stays full after the broker recovers.

**Fix:** asynchronous confirms (track delivery tags, reply on `basic.ack`/`basic.nack`) with a bounded in-flight window and a channel pool.

<a id="b3"></a>
### B3 · High · Code — RabbitMQ never notices a dead channel

`ankusa_rabbitmq/lib/ankusa/sink/rabbitmq/connection.ex:63-87`

Only `conn.pid` is monitored. A channel-level error (the exchange deleted or redeclared with different arguments, `404`/`406`, or a broker-initiated channel close) kills the channel process while the connection stays up. `state.chan` keeps pointing at the dead pid, every publish lands in `catch :exit` and returns `{:error, {:publish_failed, _}}`, and every hook retries twelve times into the DLQ until the node restarts. Monitor the channel and reopen it.

<a id="b4"></a>
### B4 · Medium · Code — RabbitMQ funnels every delivery through a `DynamicSupervisor`

`ankusa_rabbitmq/lib/ankusa/sink/rabbitmq.ex:54-67`, `ankusa_rabbitmq/lib/ankusa/sink/rabbitmq/connection.ex:47-61,118-125`

There is no `whereis` fast path: every `deliver/3` calls `DynamicSupervisor.start_child/2`, spawning a process that fails registration with `already_started`, and `Connection.init/1` opens the AMQP connection synchronously. One unreachable broker blocks the supervisor, and every RabbitMQ delivery in the VM including healthy exchanges, for the connect timeout. Connections are keyed by `{instance, exchange}`, so two sinks with different URLs and the same exchange name share one connection.

<a id="b5"></a>
### B5 · Medium · Code — Kafka: abandoned produces still land, and one bad record kills a partition producer

`ankusa_kafka/lib/ankusa/sink/kafka.ex:69-81,135-153`

A `sync_produce_request/2` timeout does not cancel the record; it is written when the broker returns, while dispatch enqueues a fresh copy per retry. brod's producer exits on a non-retriable error such as `message_too_large` (reachable whenever `:inline_max_bytes` exceeds the topic's `max.message.bytes`), failing every co-buffered hook for that partition, while the poison record retries twelve times on its lane [INFERENCE from brod's source]. The producer is not idempotent (documented at `delivery.md:265-267`).

<a id="b6"></a>
### B6 · Low · Code — NATS deliberately skips `Nats-Msg-Id`

`ankusa_nats/lib/ankusa/sink/nats.ex:189-197`, `docs/delivery.md:335-338`

Retries of one delivery carry the same `env.id`; sending it as `Nats-Msg-Id` would collapse lost-ack retries inside JetStream's duplicate window for free. The stated reason (a DLQ replay must be a new publish) is served by suffixing replays, not by giving up broker-side dedupe for every retry.

<a id="b7"></a>
### B7 · Medium · Code — Lazy connects run inside one node-global `DynamicSupervisor` per adapter

`ankusa_nats/lib/ankusa/sink/nats.ex:219-240`, `ankusa_redis/lib/ankusa/sink/redis.ex:155-178`, `ankusa_kafka/lib/ankusa/sink/kafka/application.ex:14-21`

NATS (`Gnat.start_link/1` handshakes in `init`), Redis (`sync_connect: true`) and Kafka start connections on first delivery through a single named supervisor, so a down server serializes every waiting task's connect attempt and blocks unrelated connections. Connections are keyed by URL and never reaped; with API-managed sources their number is bounded only by what callers submit. These fixed names also contradict `architecture.md:220-221` ("There are no global process names anywhere in the framework").

<a id="b8"></a>
### B8 · High · Reproduced — A non-UTF-8 `Content-Type` is acked, never deliverable to a queue sink, and poisons replay

`edge/ingest.ex:180-182`, `sink/message.ex:55-78`

`Sink.Message.encode/3` puts the raw header into a map and calls `JSON.encode!/1`; Kafka, RabbitMQ, NATS and Redis all encode through it. Probe 6 sent `content-type: text/plain; charset=latin1; x=\xFF`: the edge answered `201 Created`, the envelope was in the WAL, and `encode/3` raised `ErlangError {:invalid_byte, 255}`. In dispatch that is not a crash: `safe_deliver/4` rescues it, the hook spends its retry budget, and it is dead-lettered (`term_to_binary/1` does not care about UTF-8). The damage comes after: no queue sink can ever take it, and its DLQ entry makes every unfiltered replay fail (D5, probe 14). Under `wal.type: none` the provider gets `503` forever. Validate or escape header-derived strings at the edge, or carry them as bytes.

<a id="b9"></a>
### B9 · Medium · Code — `Sink.Http` neither signs nor bounds

`sink/http.ex:38-61`, `http_client.ex`

Outbound requests carry the `x-ankusa-*` identity headers and the operator's static headers, but no HMAC over body and timestamp, so the receiver can neither authenticate Ankusa nor bound replays (K2). Every non-2xx, including `400`, `404` and `410`, gets the full retry policy; `Retry-After` is ignored; the response body is read whole, with no size cap, before the status is checked.

<a id="b10"></a>
### B10 · Medium · Reproduced — Most lockfiles pin a `mint` with published advisories

`packages/ankusa/mix.lock` and others

`mix deps.get` in a copy of `packages/ankusa` flagged `mint 1.10.1` with three advisories: EEF-CVE-2026-91043 (High: HPACK-indexed cookie fields bypass `max_header_list_size` and exhaust client memory), EEF-CVE-2026-94194 and EEF-CVE-2026-92103 (Medium). The same version is locked in `ankusa_server` (the Docker image), `ankusa_kafka`, `ankusa_nats`, `ankusa_rabbitmq`, three examples and `tools/loadgen`; `ankusa_redis` and `sdk-elixir` already lock `1.11.0`, which the same check does not flag. Mint sits under Finch/Req, which every HTTP sink and remote blob store uses, and the HTTP sink talks to operator- or API-supplied URLs. Bump it and run `mise run deps`.

## 6. Claim check

<a id="c1"></a>
### C1 · High · Code — The documented multi-node claim-check topology cannot work

`claim_check.ex:271-279,310-314`, `docs/architecture.md:288-331`, `docs/deployment.md:126-129`

Claims are written through `config.storage.blob_store`, the same store as segments, and refs (`urn:ankusa:claim:v1:<tenant>:<claim_id>`) name no node or bucket. Segments force one bucket per node (S5). Topology 3 draws N ingest nodes writing claims into one object store and one `:claim_check` gateway redeeming them. With per-node buckets, a gateway configured with node 1's bucket answers `404 not_found` for every claim node 2 wrote, and the SDK classifies `404` as permanent: the consumer dead-letters the payload. With a shared bucket, the nodes overwrite each other's segments.

**Fix:** give claims their own store with a node-independent namespace, and validate the combination at boot.

<a id="c2"></a>
### C2 · Medium · Code — The gateway buffers whole claims, mislabels missing keys, and leaks store errors

`claim_check/router.ex:30,39-48,63-68`, `claim_check.ex:271-279`

Each `GET` reads the full claim into one binary before `send_resp/3`: no streaming, no `Range`, no `HEAD` (a proxy's `HEAD` gets a `404`, which clients treat as permanent), no in-flight limit. Any store error other than `:not_found` becomes a `503` with `reason: inspect(reason)` in the body; for S3 that is the raw error XML (`SignatureDoesNotMatch` bodies include the access-key id and the canonical request), served to unauthenticated callers. S3 answers `403` for a missing key when the credential lacks `s3:ListBucket`, so an expired claim behind a least-privilege gateway is a `503` that workers retry forever. Responses carry `Cache-Control: public, max-age=31536000, immutable`, inviting shared caches to keep tenant payloads.

<a id="c3"></a>
### C3 · Low · Code — Fallback check-ins are dated by `received_at`

`sink/message.ex:87-91`, `claim_check/sweeper.ex:62-91`

When a pack fails, each job checks its body in under a pack id derived from the envelope's receive time. After a backlog older than `retention_days`, the claim lands in a `dt=` partition the next sweep deletes while its message is still queued. `retention_days` is not validated (0 or a negative value deletes yesterday's or today's partition), and `File.rm_rf!/1` raises on the first error, skipping the rest of the sweep.

<a id="c4"></a>
### C4 · Low · Code — Refs are bearer capabilities with partly derivable ids

`sink/message.ex:87-89`

Refs carry no MAC. Batch packs use 64 random bits, but single-claim packs derive their entropy from `sha256(env.id)` and `received_at`, so anyone who knows a hook id and its receive time can construct the gateway URL. The gateway has no authentication by design, which leaves these ids as the only protection.

## 7. Consumers

<a id="k1"></a>
### K1 · High · Code + docs — Original headers stop at the queue boundary

`sink/message.ex:57-65`, `sink/http.ex:41-48`, `docs/delivery.md:144-161`

The WAL stores every request header. The wire format (`v`, `id`, `source_id`, `tenant_id`, `received_at`, `content_type`, `size`, body or claim) carries none of them, and `Sink.Http` forwards none. Ingest does no deduplication and mints a new `id` for every provider retry, so the docs tell consumers to dedupe provider retries on "the provider's own event id in the body". For providers whose delivery id lives only in a header (GitHub's `X-GitHub-Delivery`, Standard Webhooks' `webhook-id`, Svix's `svix-id`, Shopify's `X-Shopify-Webhook-Id`), that is impossible downstream: provider retries arrive as distinct hooks with no shared key. Consumers also cannot re-verify the provider's signature, so authenticity rests entirely on ingest. `delivery.md:157-161` documents the drop; it is still the wrong trade for a system whose consumer contract is "be idempotent".

**Fix:** carry an allowlisted set of headers (or all of them) in the message.

<a id="k2"></a>
### K2 · Medium · Code — The HTTP hand-off is unauthenticated on both ends

`sdk-elixir/lib/ankusa/sdk/receiver.ex:1-12`, `sink/http.ex:38-48`

`Ankusa.SDK.Receiver` "verifies nothing itself" and accepts any POST carrying `x-ankusa-id`. `Sink.Http` signs nothing, the SDK ships no constant-time compare, HMAC or timestamp-skew helper, and `integrations.md` gives no guidance. Anyone who can reach the receiver injects hooks the application treats as verified. Sign outbound requests (ingest already implements the Standard Webhooks scheme) and verify them in the SDK.

<a id="k3"></a>
### K3 · Medium · Code — The Oban example's dedupe does not survive production

`examples/oban-consumer/consumer_app/lib/ankusa_example/consumer/router.ex:84`, `examples/oban-consumer/consumer_app/lib/ankusa_example/consumer/webhook_worker.ex:19-33`, `examples/oban-consumer/consumer_app/config/runtime.exs:10`

Jobs are unique on `ankusa_id` with `period: :infinity`, which holds only while completed jobs stay in `oban_jobs`. The config runs no `Pruner` (only `Lifeline`), so the table grows forever, and adding the standard pruner silently re-enables duplicate jobs for later redeliveries. The worker's `ON CONFLICT … deliveries = deliveries + 1` counts duplicates instead of skipping the business effect, and the full body is stored in job args. As a reference implementation, it teaches the pattern that fails.

<a id="k4"></a>
### K4 · Low · Code — Claim redemption treats `429` and `408` as permanent

`sdk-elixir/lib/ankusa/sdk/claim_check.ex:79-87`, `conformance/cases/redeem.json:221-233`

Every 4xx except 404 is `ClaimRejectedError` with `retryable: false`, and the shared conformance vector pins `429 → retryable: false`, so every SDK dead-letters a message when a proxy in front of the gateway throttles.

<a id="k5"></a>
### K5 · Low · Code — Conformance covers refs and status codes, not the wire format

`conformance/features.json`

There is no operation for decoding the queue message (`v`, `body_base64` versus `claim` + `sha256`, an unknown `v`, a missing body), no integrity check for inline bodies (no digest is carried and `size` is never compared with the decoded body), and nothing on id-based dedupe or a tenant mismatch between ref and message. The eight SDKs agree on HTTP classification; nothing proves they agree on the message they all consume.

## 8. OTP structure, security, operations

<a id="o1"></a>
### O1 · Critical · Code (consequences reproduced in S1 and D3) — One flat supervisor, one restart budget, no failure domains

`instance.ex:42-54`, `application.ex:10-14`, `ankusa_server/lib/ankusa_server/application.ex:45-48`

Metrics, WAL, source store, routes, quarantine, rate limiter, batcher supervisor, up to four Bandit listeners, dispatch, compactor and sweeper are siblings under one `one_for_one` supervisor with the default intensity of 3 restarts in 5 seconds, and the server wraps that instance in another supervisor with the same default. Whether a failure takes the listener down depends only on how fast its process re-crashes:

| Path | Re-crash cadence | Outcome |
|---|---|---|
| sink returns a non-conforming value (D3) | one poll (~200 ms) after each restart | instance and parent give up in 3.3 s; a release halts the VM (reproduced) |
| object store fails fast (S1) | one 1 s tick after each restart | instance rebuilt every 4.5–6.5 s; the parent survives; the listener flaps (reproduced) |
| torn `index.log` (S3), corrupt WAL sidecar (W8) | immediately, in `init/1` | the node does not boot (index case reproduced) |
| quarantine write on a full disk (E2) | per forged request | four requests in 5 s rebuild the instance (Code) |
| WAL call timeouts (W4) | once per 5 s timeout, per process | pipeline and compactor restart; the instance survives (reproduced) |
| DLQ write on a full disk (D3) | at give-up, 55–111 s after each restart | livelock that pins the watermark; no instance kill (Code) |

Nothing models dependencies either: the WAL is everyone's root dependency, yet while it is unavailable, dispatch's and the compactor's calls exit and crash them, spending the same budget.

**Fix:** shape the tree as failure domains. A `rest_for_one` root of `[WAL, edge subtree]`; dispatch and storage in their own subtrees with their own budgets; workers that talk to external systems handling errors as values with backoff instead of crashing.

<a id="o2"></a>
### O2 · High · Code — Unauthenticated control planes on every interface, leaky redaction, secrets in crash reports

`instance.ex:227-232`, `ankusa_server/lib/ankusa_server/config.ex:366-377`, `admin/redact.ex:37,61-67`, `ankusa_server/Dockerfile:52`

- The admin API (4002), route management (4003) and the claim gateway (4001) bind all interfaces: `bandit_child/3` passes no `ip:`, and neither `Ankusa.Config` nor the YAML schema has one. The server image enables admin by default and `EXPOSE`s all four ports. Admin can replay the DLQ, read the config, lift rate limits, flip IP rules, and, with `source_store.type: persistent`, create or repoint sources and their sinks.
- Redaction is a key-name denylist (`secret password secret_access_key token sasl nkey_seed`) that misses shipped credential options (Azure's `sas_token`, OCI's `private_key`) and URL-embedded tokens outside userinfo; `GET /v1/config` serves them.
- No module defines `format_status/1` (grep finds none). `Dispatch.Pipeline` keeps the whole `%Ankusa.Config{}` (source secrets, SASL passwords, broker URLs) in its state and stops itself deliberately on task crashes, so every such crash report prints them.

**Fix:** default to loopback and add an `ip` option; invert redaction to an allowlist; implement `format_status/1` wherever config lives in state.

<a id="o3"></a>
### O3 · Medium · Code — Health is a constant

`edge/router.ex:31-33`, `admin/router.ex:55-64`, `ankusa_server/Dockerfile:54-55`

Both `/health` endpoints return `200` without touching the WAL, the disk or any child. A full volume (every hook a `503`) or a crash-looping WAL leaves the container healthy in the load balancer's eyes. The image's `HEALTHCHECK` targets the admin port, so `admin.enabled: false` in YAML makes a working node unhealthy. Add a readiness check that reflects WAL writability and free space.

<a id="o4"></a>
### O4 · Medium · Code — Numeric config is type-checked, not range-checked

`ankusa_server/lib/ankusa_server/config.ex`, `edge/batcher_supervisor.ex`, `dispatch/pipeline.ex:405`

`batcher.partitions: 0` makes `:erlang.phash2(key, 0)` raise on every request, so every hook gets a `500`. `dispatch.concurrency: 0` starts nothing, pins the cursor, and lets the WAL grow while ingest keeps answering `201`. `check-config` passes both.

<a id="o5"></a>
### O5 · Medium · Code — The metrics an operator needs do not exist

`metrics.ex:71-187`

There are counters and histograms of events only: no gauges for WAL bytes or records, cursor lag (dispatch or compactor), oldest undelivered age, DLQ size, quarantine size, retrying jobs or window saturation. WAL stats exist only as `GET /v1/wal` (a call into the WAL process), and the DLQ count only by reading the whole DLQ file. The "alarm fires" in `architecture.md:62` has nothing to fire on.

<a id="o6"></a>
### O6 · Medium · Code — Route mutations republish the whole snapshot through `:persistent_term`

`routes/snapshot.ex:10-14,225`, `routes/store/ets.ex:8-10`, `routes.ex:581-595`

Every route mutation (ETS store), and every version change each node observes (Redis store), rebuilds and re-sorts every route and calls `:persistent_term.put/2` with a changed value. Each such put starts a global GC pass that copies the old snapshot into every process still referencing it, in-flight ingest requests included [INFERENCE: documented `persistent_term` semantics]. The module doc assumes "writes are rare (route changes)"; API-driven provisioning breaks that assumption: 10,000 route writes are O(n²) rebuild work in the store process plus 10,000 global GC passes on every node. A `put/2` with an equal value is a no-op, so instance restarts do not trigger this. An ETS table with a versioned swap fits an API-mutable table.

<a id="o7"></a>
### O7 · Low · Code — Smaller operational defects

- `ankusa_server/lib/ankusa_server/gcs_token.ex:55-64`: `Req.get/2` without `retry: false`, so Req's default retries stretch a 1 s/5 s metadata fetch to 10–15 s, with no single-flight across concurrent callers.
- `ankusa_server/lib/ankusa_server/config.ex:114`: `${lowercase_name}` does not match the interpolation regex and silently becomes a literal secret. Reject any leftover `${`.
- `application.ex:10-14`: `Ankusa.Registry` is a `one_for_one` sibling of the instance. If the Registry restarts, every `via` lookup fails while the instance's processes keep running unregistered.
- The release ships no `vm.args`/`env.sh` (scheduler counts under CPU quotas, busy-wait), and `RELEASE_DISTRIBUTION=none` (`ankusa_server/Dockerfile:44`) removes `bin/ankusa remote`, so a live container cannot be inspected.

## 9. Docs that promise more than the code delivers

| Claim | Where | Reality |
|---|---|---|
| "Never return `2xx` until the hook is durably accepted." | `architecture.md:5` | `202` for quarantine with no way back (E1); `201` then a silent drop for unknown sources (D2) and unroutable RabbitMQ publishes (B1); `201` for a non-UTF-8 header no queue sink can ever take (B8). |
| Replay "drops a torn trailing frame" / "truncates the file at the first torn/invalid one". | `architecture.md:151`, `storage.md:64-65` | The second is accurate, and it is the bug: every acked frame after a mid-log error goes too (W2). A failed read drops the whole log (W1). |
| Reclaiming "costs a few ETS deletes and never blocks appends". | `storage.md:66-68` | Each reclaim is a floor write, `fdatasync` and rename inside the WAL process; a rewrite blocks for the whole copy (W3). |
| A failed write "is reported, not fatal … Ingest resumes on its own once space frees." | `storage.md:76-81` | A full disk tears the DLQ and the index: later dead letters vanish and the node stops booting (S3). |
| "Take the compactor down: ingest keeps acking, the WAL grows, an alarm fires, nothing is lost." | `architecture.md:61-63`, `Ankusa.Instance` module doc | A store error rebuilds the whole instance every 4.5–6.5 s (S1); there is no WAL-size metric (O5). |
| "A slow or retrying sink only delays what actually has to wait for it." / "One failing sink never blocks delivery to the others." | `delivery.md:11-13,33-35` | 0/100 delivered for a healthy tenant (D1). |
| Quarantine: "a flood of forged requests can't fill the disk". | `architecture.md:157`, `delivery.md:442-444` | An unbounded log with a ~160 MB/s ceiling (E2). |
| "A hook in the pen was never acked." | `delivery.md:459-460` | The provider received a `202` (E1). |
| `WAL.DiskLog` + `BlobStore.LocalFS` survive power loss on the box. | `architecture.md:27-28`, `storage.md:84` | LocalFS segments and claims are never fsynced, while the WAL floor moves past them (S4). |
| A confirm means the message "really was persisted by RabbitMQ". | `ankusa_rabbitmq/lib/ankusa/sink/rabbitmq/connection.ex:7-9` | A publish no queue receives is confirmed too (B1). |
| "There are no global process names anywhere in the framework." | `architecture.md:220-221` | Every adapter registers fixed node-global supervisors (B7). |
| One claim-check gateway serves N ingest nodes. | `architecture.md:288-331` | Per-node buckets make other nodes' claims `404` (C1). |
| "The dynamic store itself isn't shipped yet." | `multi-tenancy.md:127` | `SourceStore.Persistent` and tenant source CRUD ship. |
| `ANKUSA_ROLES=edge,dispatch` as the example override. | `configuration.md:243,504-505` | That role set never truncates the WAL (W5). |

## 10. What is done well

- **The ack path.** Request processes block until the group commit's `fdatasync` returns; replies go out per record, in order; a failed commit discards its partial tail on a fresh descriptor (`discard_tail/1`) instead of building on one that just failed `fsync`; overload is a `503`, not an unbounded queue. In the probes, 32 clients on a laptop got 14–16k acks per second, every one a `201`.
- **Seq monotonicity across reclamation.** `next_seq` is the maximum of the last frame, the persisted floor and every cursor, and the floor and cursors are written tmp → `fdatasync` → rename.
- **The rate limiter.** GCRA, one public ETS row per tenant, `:ets.select_replace/2` as compare-and-swap, a sweep that bounds memory, charged after verification, no process on the hot path.
- **Dispatch internals.** `Task.Supervisor.async_nolink/2` + `trap_exit` + a `terminate/2` that persists the cursor; a watermark that never passes unfinished work; per-key lanes; the measured, commented fix in `start_jobs/1` that stops every spawn from copying the run queue into the task.
- **The cursor survives a rebuild.** Across six instance rebuilds in probe 10, `terminate/2` persisted the dispatch cursor each time, so only the in-flight set (32 per rebuild) was redelivered.
- **Shutdown order.** Children stop in reverse start order: listeners drain before batchers, batchers stop before the WAL.
- **Claim check.** A strict ref grammar (the claim-check reviewer probed traversal and integer abuse; neither is possible), packs written before refs are published, sha256 verified end to end in core and in the SDK.
- **Routes.** No per-request regex; a ~150 µs worst-case miss over 10k patterns (the routes reviewer's throwaway measurement); version-checked atomic Redis writes; a snapshot that keeps serving through a Redis outage.
- **Honest prose where it exists.** The boot line says "Durable to power loss on THIS host only"; direct mode's trade-offs, the header drop and the LocalFS full-disk crash loop are written down; `Sink.Log` and `Sink.Redis` declare `durable?/1` false.
- **A real crash test.** `loss_test.exs` hard-kills the instance (`Process.exit(pid, :kill)`) and checks recovery.

## 11. What to fix, in order

**P0: stop losing acked hooks and stop taking ingest down**

1. WAL replay: stream in bounded chunks, assert `{:ok, _}`, refuse to start on mid-log corruption, drop only a true tail, log what was dropped (W1, W2, W8).
2. Supervision: failure domains with their own budgets, WAL and edge under `rest_for_one`, external-dependency errors as values with backoff in the compactor, pipeline, quarantine and DLQ paths (O1, S1, D3, E2, W4).
3. Dispatch: an unknown source is held or dead-lettered, never completed (D2); sink returns are normalized (D3); replay goes through the pipeline and `safe_deliver/4` (D5).
4. `DurableLog`: a CRC per frame and torn-tail repair on open (S3).
5. LocalFS: `fsync` the file and its directory before the floor or a ref moves past it (S4).
6. RabbitMQ: `mandatory` with `basic.return`, channel monitoring, `durable?/1` false until both exist (B1, B3).
7. Quarantine: no `2xx` without a replay path; per-source buckets; `503` on exhaustion; a byte cap (E1, E2).
8. Dispatch scheduling: retries as pipeline-owned timers that free the slot, per-sink concurrency and windows, per-attempt deadlines, circuit breakers (D1, D4, D6).
9. Reclaim the WAL from whichever cursors exist, not only the compactor's (W5).

**P1: scale and operability**

10. Move serialization out of the WAL process; separate the read path; take the rewrite off the append path; deadlines carried in the request instead of bare call timeouts (W3, W4); compactor reads sized in bytes with carry-over (S7).
11. A bounded, disk-backed storage index (S2).
12. Directory `fsync` for every create and rename (W7).
13. Bounded metric labels, lag/age/size gauges, a real readiness endpoint (E3, D8, O3, O5).
14. Loopback by default, allowlist redaction, `format_status/1` (O2).
15. Carry original headers in the message; sign `Sink.Http`; verify in the SDK (K1, K2, B9).
16. A claims store independent of the segment store; streamed gateway responses (C1, C2).
17. Validate header bytes at the edge (B8); reject on `Content-Length` before reading (E3); range-check numeric config (O4); bump `mint` (B10).

**P2:** the remaining Medium and Low findings.

## Appendix A: probes

Probes ran from copies of `packages/ankusa` (and, for probe 15, `packages/ankusa_rabbitmq`) at `42b6f5a` in `/tmp`, as `MIX_ENV=test mix run probe/<name>.exs` on macOS (Darwin 25.6) arm64 with Elixir 1.20.4 / OTP 29, unless marked Linux. The Linux probes ran in `elixir:1.20.4-alpine` (OTP 29, aarch64) on Docker 29.4 with a 16 GB VM; probe 15 used `rabbitmq:4-management-alpine`. Nothing in the repository was modified. Snippets are condensed; every line that matters to the result is shown, and log lines drop their timestamps and the temp path.

### Probe 1: mid-log corruption (W2)

```elixir
config = Ankusa.Config.new(instance: :p, data_dir: dir, port: 0, roles: [:edge])
Ankusa.put_config(config)
{:ok, wal} = Ankusa.WAL.DiskLog.start_link(instance: :p, config: config)

for i <- 1..10 do
  env = %Ankusa.Envelope{id: "e#{i}", source_id: "s", received_at: 0, method: "POST",
                         path: "/", headers: [], body: "payload-#{i}"}
  {:ok, [{:committed, _}]} = Ankusa.WAL.append(:p, [%{envelope: env}])
end

GenServer.stop(wal)
# frame = 20-byte header (len at bytes 16..19) + payload; flip one byte 5 bytes into frame 3's payload
{:ok, _} = Ankusa.WAL.DiskLog.start_link(instance: :p, config: config)
Ankusa.WAL.read(:p, 0, 100) |> Enum.map(& &1.seq)
```

```text
before restart: 10 acked records, file_bytes=1972, flipped 1 byte at offset 419 (frame 3 payload)
[info] [ankusa] DiskLog WAL at …/wal/ankusa.wal: recovered 2 record(s), next_seq=3. Durable to power loss on THIS host only.
after restart:  readable seqs=[1, 2] file_bytes=394
```

### Probe 2: WAL size at boot (W1)

macOS, the real `Ankusa.WAL.DiskLog`:

```elixir
body = :binary.copy("x", 1_048_576)
for b <- 1..22 do
  {:ok, _} = Ankusa.WAL.append(:p, for(i <- 1..100, do: %{envelope: env(b, i, body)}))
end
GenServer.stop(wal)
{:ok, _} = Ankusa.WAL.DiskLog.start_link(instance: :p, config: config)
Ankusa.WAL.stats(:p)
```

```text
before restart: records=2200 next_seq=2201 file_bytes=2307294359
[info] [ankusa] DiskLog WAL at …/wal/ankusa.wal: recovered 0 record(s), next_seq=1. Durable to power loss on THIS host only.
after restart:  records=0 next_seq=1 file_bytes=0
```

The `pread` limit behind it, from a sparse file of 2 GiB + 10 bytes:

```text
pread 1073741824: ok, 1073741824 bytes
pread 2147483647: ok, 2147483647 bytes
pread 2147483648: {:error, :einval}
```

Linux: a dependency-free script with verbatim copies of `frame/2` (`disk_log.ex:453-457`), `replay/3` (`414-421`), `parse/5` (`423-449`) and `init/1`'s position-and-truncate (`83-86`). The only change prints the raw `pread` result before `elem(_, 1)` takes it apart.

```text
$ docker run --memory=1g …  elixir wal_replay_linux.exs real …
real: wrote 2200 acked frames, file is 2306911200 bytes
Killed

$ docker run --memory=8g …  elixir wal_replay_linux.exs real …
real: wrote 2200 acked frames, file is 2306911200 bytes
  pread(0, 2306911200) -> {:ok, <<2306911200 bytes>>}
  after init: recovered 2200 record(s), next_seq=2201, file is 2306911200 bytes

$ docker run --memory=8g …  elixir wal_replay_linux.exs sparse …            # 5 frames, then a 64 GiB hole
Killed

$ docker run --memory=8g …  sh -c 'ulimit -v 4000000; elixir wal_replay_linux.exs sparse …'   # same with 8000000
sparse: wrote 5 acked frames, file is 68719476736 bytes
  pread(0, 68719476736) -> {:error, :enomem}
  after init: recovered 0 record(s), next_seq=1, file is 0 bytes

$ docker run … cat /proc/sys/vm/overcommit_memory
1
```

### Probe 3: object-store outage, small WAL (S1, O1)

```elixir
defmodule Probe.DownStore do
  @behaviour Ankusa.BlobStore
  def put(_i, _k, _d, _o), do: {:error, {:status, 503, "SlowDown"}}
  def get(_i, _k, _o), do: {:error, {:status, 503, "SlowDown"}}
  def get_range(_i, _k, _off, _len, _o), do: {:error, {:status, 503, "SlowDown"}}
  def delete(_i, _k, _o), do: :ok
  def list(_i, _p, _o), do: []
end

config = Ankusa.Config.new(instance: :probe_cc, data_dir: dir, port: port,
  roles: [:edge, :dispatch, :storage],
  source_store: {Ankusa.SourceStore.Static, sources: %{"acme" => %{sinks: []}}},
  storage: %{blob_store: {Probe.DownStore, []}})   # control run: {Ankusa.BlobStore.LocalFS, []}

# the same shape as Ankusa.Application: one_for_one, default intensity
{:ok, sup} = Supervisor.start_link([{Ankusa.Instance, config}], strategy: :one_for_one)
# 32 tasks POST {} to /webhooks/acme in a loop for 15 s (Req, retry: false);
# a watcher samples Supervisor.which_children(sup) every 20 ms and records pid changes
```

```text
store: LocalFS (control)
requests: 210466  outcomes: %{201 => 210466}
Instance supervisor transitions (ms since start): []

store: down (503)
requests: 229148  outcomes: %{201 => 225564, {:error, :closed} => 4, {:error, :econnrefused} => 3580}
Instance supervisor transitions (ms since start): [{7520, :up}, {14123, :up}]
```

Each transition is a new `Ankusa.Instance` pid: the whole tree torn down and rebuilt. An earlier run of the same probe rebuilt it at 7.2 s and 13.7 s.

### Probe 4: cross-tenant dispatch stall (D1)

```elixir
defmodule Probe.DeadSink do
  @behaviour Ankusa.Sink
  def deliver(_env, _ctx, _opts), do: {:error, :connection_refused}
  # :lane = one lane per source; :none = nil, like Sink.Http's default
  def ordering_key(env, opts),
    do: if(opts[:mode] == :none, do: nil, else: {env.tenant_id, env.source_id})
end

defmodule Probe.CountSink do
  @behaviour Ankusa.Sink
  def deliver(_env, _ctx, _opts), do: (:ets.update_counter(:probe_live, :n, 1); :ok)
  def ordering_key(_env, _opts), do: nil
end

sources = %{
  "dead" => %{tenant_id: "acme", sinks: [{Probe.DeadSink, [mode: mode]}]},
  "live" => %{tenant_id: "globex", sinks: [{Probe.CountSink, []}]}
}
# roles [:edge, :dispatch]; ingest N hooks to "dead" via Ankusa.Edge.Ingest.ingest/2,
# then 100 to "live"; read the counter at 1 s, 5 s and 15 s
```

```text
mode=lane: acked 4200 hooks for tenant acme (sink down), then 100 for tenant globex (sink healthy)
t=1s globex delivered: 0/100
t=5s globex delivered: 0/100
t=15s globex delivered: 0/100
mode=none: acked 64 hooks for tenant acme (sink down), then 100 for tenant globex (sink healthy)
t=1s globex delivered: 0/100
t=5s globex delivered: 0/100
t=15s globex delivered: 0/100
```

### Probe 5: metric label cardinality (E3)

```elixir
config = Ankusa.Config.new(instance: :probe_metrics, data_dir: dir, port: port,
  roles: [:edge, :dispatch, :storage], admin: %{enabled: true, port: admin_port})
# 20,000 POSTs to /webhooks/scan-<i>-<unique>, 32 concurrent; scrape /metrics before and after
```

```text
20000 POSTs to random unknown sources -> %{404 => 20000}
ankusa_ingest_* series in /metrics: 0 -> 280000
/metrics body: 0 B -> 28874499 B
VM ETS memory: 1485 KiB -> 26734 KiB
```

### Probe 6: non-UTF-8 `Content-Type` (B8)

```elixir
# raw TCP to the edge (roles [:edge], source "acme" with default sinks)
"POST /webhooks/acme HTTP/1.1\r\nhost: x\r\ncontent-type: text/plain; charset=latin1; x=" <>
  <<0xFF>> <> "\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}"

[env] = Ankusa.WAL.read(:probe_ct, 0, 10)
Ankusa.Sink.Message.encode(env, %{instance: :probe_ct}, 262_144)
```

```text
edge answered: HTTP/1.1 201 Created
WAL envelope content_type: <<116, 101, 120, 116, 47, 112, 108, 97, 105, 110, 59, 32, 99, 104, 97, 114, 115, 101, 116, 61, 108, 97, 116, 105, 110, 49, 59, 32, 120, 61, 255>>
Sink.Message.encode -> {:raised, %ErlangError{original: {:invalid_byte, 255}, reason: nil}}
```

### Probe 7: dependency advisories (B10)

```text
$ mix deps.get        # in a copy of packages/ankusa; the lock is unchanged (aliases and URLs omitted)
  mint 1.10.1 VULNERABLE!
    EEF-CVE-2026-94194 (MEDIUM)  Mint HTTP/1 client applies chunked framing when chunked is not the final transfer coding, enabling response smuggling through intermediaries
    EEF-CVE-2026-91043 (HIGH)    HPACK-indexed cookie fields in Mint HTTP/2 responses bypass max_header_list_size and exhaust client memory
    EEF-CVE-2026-92103 (MEDIUM)  Mint HTTP/2 client buffers oversized frames up to 16 MiB before enforcing max_frame_size
```

The same command in a copy of `packages/sdk-elixir` (locking `mint 1.11.0`) reported no advisories.

### Probe 8: a sink with a non-conforming return value (D3)

```elixir
defmodule Probe.BadReturnSink do
  @behaviour Ankusa.Sink
  def deliver(_env, _ctx, _opts), do: :error
  def ordering_key(_env, _opts), do: nil
end

# roles [:edge, :dispatch], source "acme" -> Probe.BadReturnSink
# a trap_exit process starts the parent, shaped like AnkusaServer.Application's supervisor:
{:ok, sup} = Supervisor.start_link([{Ankusa.Instance, config}], strategy: :one_for_one, name: Probe.TopSup)
# then: one POST /webhooks/acme; sample which_children(Probe.TopSup) every 5 ms; wait for {:EXIT, sup, _}
```

```text
POST /webhooks/acme -> 201 at 61 ms
Ankusa.Instance pids seen: 4
top-level supervisor exited with :shutdown at 3260 ms
```

### Probe 9: an unresponsive WAL (W4)

```elixir
# roles [:edge, :dispatch, :storage]; source "acme" -> a sink that records each env.id it receives
wal = GenServer.whereis(Ankusa.via(inst, :wal))
# 32 clients POST continuously, recording every response; after 2 s:
:ok = :sys.suspend(wal)
Process.sleep(stall_ms)
:ok = :sys.resume(wal)
# after the clients stop and dispatch drains: committed = WAL next_seq - 1, compared with 201s sent
```

```text
WAL unresponsive for 8000 ms; 32 clients
responses: %{201 => 124149, 503 => 17}
records committed to the WAL: 124166; 201s sent: 124149; committed although the client was not sent 201: 17
distinct hooks delivered to the sink: 124166
max WAL mailbox during the stall: 8
restarts during the run: instance=0 dispatch=1 compactor=1

WAL unresponsive for 20000 ms; 32 clients
responses: %{201 => 119725, 503 => 64}
records committed to the WAL: 119789; 201s sent: 119725; committed although the client was not sent 201: 64
distinct hooks delivered to the sink: 119789
max WAL mailbox during the stall: 14
restarts during the run: instance=0 dispatch=2 compactor=2
```

### Probe 10: object-store outage with a backlog (S1)

```elixir
# phase 1: roles [:edge] only (nothing reads or truncates); 3,000 x 100 KB via Ingest.ingest/2
# phase 2: roles [:edge, :dispatch, :storage], Probe.DownStore (probe 3),
#          source "acme" -> a sink that sleeps 100 ms and counts deliveries per env.id
#          (COUNT_AT=start counts before the sleep: a receiver that processed the request)
# for 30 s: 4 clients POST 10 KB bodies; a prober tries a TCP connect every 2 ms
```

```text
seeded WAL: 301 MB, 3000 hooks
listener down windows (start ms, duration ms): [{4057, 144}, {8248, 183}, {12478, 452}, {16991, 483}, {21528, 636}, {26211, 602}]
deliveries: 7959 for 7957 distinct hooks (2 redeliveries)
WAL after 30 s: 1767 MB

COUNT_AT=start:
listener down windows (start ms, duration ms): [{4059, 141}, {8242, 168}, {12458, 526}, {17046, 393}, {21488, 646}, {26190, 636}]
deliveries: 8201 for 8009 distinct hooks (192 redeliveries)
WAL after 30 s: 1791 MB
```

### Probe 11: a torn `DurableLog` frame, and booting on it (S3)

```elixir
alias Ankusa.DurableLog
:ok = DurableLog.append(a, %{row: 1}, sync: true)
torn.(a, <<100::32, "0123456789">>)          # header claims 100 bytes, 10 present
:ok = DurableLog.append(a, %{row: 3}, sync: true)
:ok = DurableLog.append(a, %{row: 4}, sync: true)
DurableLog.read(a)
# same with <<20::32, "0123456789">>, then the same shape in segments/index.log:
:ok = Ankusa.Storage.Index.append(config, [row.(1), row.(2)])
torn.(index, <<40::32, :binary.copy("z", 30)::binary>>)
:ok = Ankusa.Storage.Index.append(config, [row.(3), row.(4)])
Supervisor.start_link([{Ankusa.Instance, config}], strategy: :one_for_one)   # roles [:edge, :dispatch, :storage]
```

```text
torn frame claiming 100 bytes, then rows 3 and 4 appended: read -> [1]
torn frame claiming 20 bytes, then rows 3 and 4 appended: read -> {:raised, ArgumentError, "errors were found at the given arguments:"}
** (EXIT) shutdown: failed to start child: {Ankusa.Instance, :probe_torn}
    ** (EXIT) shutdown: failed to start child: {Ankusa.Storage.Compactor, :probe_torn}
        ** (EXIT) an exception was raised:
            ** (ArgumentError) errors were found at the given arguments:
  * 1st argument: invalid or unsafe external representation of a term
                :erlang.binary_to_term(<<122, 122, …, 0, 0, 0, 135, 131, 116, 0, 0, 0, 8>>, [:safe])
```

### Probe 12: `ENOSPC` on Linux (S3)

The real `packages/ankusa/lib/ankusa/durable_log.ex` (it has no dependencies), loaded with `Code.require_file/1` in `docker run --tmpfs /data:size=2m … elixir:1.20.4-alpine`. A 1.3 MB filler file leaves ~700 KB; 300 KB DLQ records are appended with `sync: true` until one raises; the filler is deleted; two more records are appended.

```text
appended [1, 2]; append 3 raised: no match of right hand side value:
    {:error, :enospc}
dlq.log is 794624 bytes; complete frames end at 600122; torn tail = 194502 bytes
after freeing space and appending after-1, after-2: DurableLog.read -> ["e1", "e2"]
```

### Probe 13: compactor read amplification (S7)

```elixir
# roles [:edge, :storage], storage: %{interval_ms: 0}; 256 x 4,000,000-byte hooks via Ingest.ingest/2
1 = :erlang.trace(wal_pid, true, [:receive, {:tracer, tracer}])   # tracer counts {:"$gen_call", _, {:read, after, limit}}
# one client acks small hooks in a loop meanwhile
{:ok, segments} = GenServer.call(Ankusa.via(inst, :compactor), :tick, :infinity)
```

```text
256 hooks x 4 MB committed; one compactor tick
segments written: 52; WAL {:read, …} calls: 56
4 MB records read out of the WAL: 6682 (26.1x the 256 compacted; ~26 GB)
tick took 5208 ms
small-hook ack latency during the tick: n=525 p50=1 ms max=410 ms
```

### Probe 14: one non-UTF-8 hook in the DLQ (D5, B8)

```elixir
# A sink shaped like Kafka/RabbitMQ/NATS/Redis: Ankusa.Sink.Message.encode/3, then "publish".
# dispatch: %{retry: {Ankusa.RetryPolicy.Exponential, max_attempts: 1}}, so hooks dead-letter at
# once instead of after ~83 s; admin enabled.
# broker down: POST hook 1 (JSON), hook 2 (raw TCP, content-type with 0xFF), hook 3 (JSON)
# broker up:   Ankusa.Dispatch.replay(inst); POST /v1/dlq/replay (empty body), twice
```

```text
dead-lettered while the broker was down: 3 entries (hook 2 = non-UTF-8 content-type)
Ankusa.Dispatch.replay(inst) -> {:raised, ErlangError}; publishes per hook [1, 2, 3]: [0, 0, 1]
POST /v1/dlq/replay (empty body) -> 500; publishes per hook [1, 2, 3]: [0, 0, 2]
POST /v1/dlq/replay again -> 500; publishes per hook [1, 2, 3]: [0, 0, 3]
DLQ entries afterwards: 3
```

Dispatch finished hook 3 first, so the DLQ order was 3, 2, 1: hook 3 sits ahead of the poison entry and is republished on every attempt; hook 1 sits behind it and is never reached.

### Probe 15: RabbitMQ (B1, B2)

```elixir
# broker: rabbitmq:4-management-alpine on 127.0.0.1:5679; the real Ankusa.Sink.RabbitMQ
opts = [url: "amqp://guest:guest@127.0.0.1:5679", exchange: "ankusa.review"]
Ankusa.Sink.durable?(Ankusa.Sink.RabbitMQ, opts)
Ankusa.Sink.RabbitMQ.deliver(env.(1), ctx, opts)       # no queue bound yet
# bind "review.q" to the exchange with "#", count its messages; then 2,000 deliveries from 32 tasks
```

```text
durable?/2 for Sink.RabbitMQ: true
deliver/3 with no queue bound to the exchange: :ok
messages in a queue bound afterwards: 0 (the confirmed hook is gone)
2000 deliveries from 32 concurrent callers: %{ok: 2000} in 727 ms (2748 msg/s); queued: 2000
```

## Appendix B: method

Seven parallel slice reviews covered the edge, the WAL and storage, dispatch, the claim check, routes and admin, adapters and consumers, and supervision and operations, across every package. Every finding above was re-read against the cited lines before inclusion; findings that reading did not support were dropped. A second pass re-ran or added a probe for every runtime claim (including the `DurableLog` torn-tail case a reviewer first reported), checked crash cadences against the restart budgets instead of assuming them, and corrected several first-pass statements: the lane-mode stall projection, the retry-budget dead-letter count, which hooks are dead-lettered, and the platform scope of the 2 GiB replay wipe. Runtime claims are limited to the probes in Appendix A; everything else is tagged Code or [INFERENCE]. The measurements quoted in section 10 from the routes and claim-check reviewers are theirs.
