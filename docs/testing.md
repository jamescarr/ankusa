# Testing

Every package's test suite runs from its own directory under `packages/`;
there's no single test runner spanning them, because each has a genuinely
different infrastructure dependency (none, RabbitMQ, Redpanda, NATS, Redis).
`mise run check:package <pkg>` runs one package's full CI check (format,
warnings-as-errors, credo, dialyzer, sobelow, tests with a coverage floor,
docs), starting and stopping that package's own `docker-compose.yml` around the
suite; `mise run check` runs all of them. The
eight client SDKs are also checked against one shared, language-neutral vector
suite — `mise run check:conformance`, below.

## Static analysis and coverage gates

`check:package` runs these on every Mix package (sobelow only on the two Plug
applications) between `mix compile --warnings-as-errors` and the tests:

| Gate | What fails it | Config |
| --- | --- | --- |
| `mix credo --strict` | any issue | [`.credo.exs`](../.credo.exs): the default checks, except `Design.AliasUsage` is off and the nesting and cyclomatic-complexity ceilings sit at the worst function that exists today (5 and 20) |
| `mix dialyzer` | any warning (a wrong `@spec`, an unreachable clause, a call that can never succeed) | PLTs live in `_build/plts`; CI caches `_build` keyed on the toolchain pins and `mix.lock`, so a PLT rebuilds only when one changes |
| `mix sobelow --exit --skip` | any finding in `ankusa` core or `ankusa_server` | a deliberate finding is marked in the code with `# sobelow_skip ["<Check>"]` and a line saying why it is safe; `--skip` is what makes sobelow read those marks |
| `mix test --cover` | line coverage under the package's `test_coverage` threshold | `85` for `ankusa` core (shared test helpers excluded), `80` for every other package, in each `mix.exs` |

The test run prints its ExUnit seed; in CI `check:package` also appends it to the
job's summary (`$GITHUB_STEP_SUMMARY`), so an order-dependent failure replays with
`mix test --seed <n>`. A new adapter scaffolded by `mise run new:adapter` ships
the same dependencies, PLT paths and coverage floor.

## `ankusa` core: `mix test`

```sh
mise run check:package ankusa         # no external infra needed
mise run test:integration             # +22 tests against the floci emulators (see below)
```

`test/ankusa/tracing_test.exs` (OPS-1) is the proof that a hook is one trace: it
starts the OpenTelemetry SDK with the in-memory `:otel_exporter_pid` and asserts
`one trace across stages` (ingest, verify, commit and dispatch spans share the
provider's trace id, the attempt links to the ingest span, the delivered
`traceparent` is the ingest span's), and, with no SDK, that a `traceparent` reaches
the queue message and `Sink.Http` unchanged. It tears the SDK down, and the
tracers it parked in `:persistent_term`, so no other test sees it.

`test/ankusa/admin/router_test.exs` (OPS-2) has `runtime level`: `PUT /v1/log-level`
is `401` with no credential and `403` without `config:write` + `tenant:*`, `400`
for a body that is not `{"level": <level>}`, changes `Logger.level()` at once and
answers the old level, leaves an audit row (`log_level.put`, the caller as the
actor), and changes nothing when the row cannot be written. The VM's log level is
global, so the test restores it. `ankusa_server`'s `test/ankusa_server/log_test.exs`
is the proof of the JSON log: `json line` boots a `wal.type: none` edge whose sink
refuses every hook, sends a request with a `traceparent`, and parses the line the
refusal wrote with `AnkusaServer.LogFormatter` (`source_id`, `tenant_id`, `hook_id`,
`sink`, `trace_id`, `instance`, one line, a timestamp); it also covers the ids of
one request not leaking onto the next on the same connection process, a delivery
attempt's correlation, the fixed key order, and the redaction of
`secret|token|password|authorization|cookie` keys in metadata and reports.

The always-on tests cover:

- **Stored-header policy** (`stored_headers_test.exs`, `edge_test.exs`,
  `storage_test.exs`): `stored_headers_test.exs` covers drop, hash, case
  folding, the untouched fast path and the config and source validation;
  `edge_test.exs` (`stored-header policy`) posts a signed hook carrying
  `authorization` and `cookie`, checks it verifies and that neither the stored
  hook, its raw store row, a sink message nor a quarantine entry holds them, a
  source override, the dedupe key read before a header is dropped and the
  `dropped_headers` telemetry count; `storage_test.exs` (`segment has no
  authorization header`) checks the archive segment and claim pack objects.
- **Ingest dedupe and metrics** (`dedupe_test.exs`, `metrics_test.exs`):
  `dedupe_test.exs` covers a provider retry collapsing to the original id with
  `duplicate: true`, a key past its TTL committing again, two entries sharing
  a key in one batch, per-tenant keys and the writer's expiry sweep;
  `metrics_test.exs` covers the scrape's bounded labels and series, and the
  cardinality policy (`metrics.detail`, `metrics.max_series_per_metric`):
  "bounded series under 10k sources" (10 000 sources, one request each: the
  ingest counter keeps at most the ceiling, the rest is one `other` series that
  still sums to 10 000, and a scrape takes under 100 ms), the series set under
  `detail: :none` being identical for one tenant and for forty, `tenant_id`
  present and `source_id` absent by default, the gauges dropping what the
  ceiling refuses, and a concurrent test that admission never exceeds the
  ceiling or leaks a slot.
- **Verifier hardening** (`verifier_test.exs` "tolerance range and weak hashes",
  `metrics_test.exs`, `ankusa_server` `config_test.exs` "tolerance range"):
  `tolerance` 0–3600 validates and 3601, a negative or a non-integer is
  `:bad_tolerance` from `validate_opts/1` and every `verify/2`; a source
  declared with 3601 refuses to boot and names itself; the YAML loader names
  `sources.<id>.verify.tolerance_seconds`; no preset uses SHA-1; a `sha1`
  inline scheme boots with one warning and a SHA-1 source (and no other)
  appears on `ankusa_verifier_weak_hash_total`. `mise run lint:pins` is the
  gate for action SHAs and image digests (what runs where:
  [`releasing.md`](releasing.md#supply-chain-pins-sbom-provenance-signature)).
- **Verifier breadth** (`verifier_test.exs` "Asymmetric", "Token", "ClientCert"
  and "validate_config!/1 for the asymmetric, token and client-certificate
  verifiers", `edge_test.exs` "verifier breadth", `instance_test.exs` mutual
  TLS, `ankusa_server` `config_test.exs` "type standard_webhooks_asym" through
  "/v1/config shows where a credential travels", `conformance/cases/signature.json`
  `webhook.verify_signature.asymmetric`): the Standard Webhooks `v1a` reference
  message verifies under `Verifier.Asymmetric` (the same bytes are the
  conformance case, which the suite reads), and a wrong key, a stale or future
  timestamp, a tampered body or id and a signature of the wrong length fail; a
  rotation list verifies with any key; at most 8 right-length signatures are
  tried; RSA verifies under a PEM, a PKCS#1 PEM or a bare DER key and refuses a
  1024-bit one; a token verifies in each of its four locations, rotates, never
  matches an empty request and is never stored; a client-certificate source
  accepts only a pinned fingerprint or subject, rejects a request with no
  certificate, another CA-signed certificate, or a certificate header, and over
  a real mutual-TLS listener takes the pinned client and refuses the rest; a
  source with an unusable key, secret or pin, or a certificate source on a
  listener that asks for no certificate, refuses to boot naming itself; the
  YAML loader names the key; `/v1/config` hides every `secret` and `public_key`.
- **Store and queue** (`store_test.exs`, `queue_test.exs`): the RocksDB store
  applies one batch across column families (puts, deletes, range deletes) and
  refuses an unreadable or missing store with `:store_unavailable` rather than
  reporting it empty; `fold/6` is half-open, ordered and never yields a
  sentinel; a corrupt blob or SST block fails the scan and the point read
  instead of silently shortening results, and a damaged WAL record refuses to
  open while a torn tail drops cleanly; `reopen/1` republishes working handles
  and keeps every committed key, a store left closed by a failed reopen opens
  again by itself, and a reported write failure reopens the store at most once
  per interval; a reported write failure emits `[:store, :write_failed]` with a
  class from a fixed set (`failure_class/1`) that `ready/1` and `/ready` name, a
  store that cannot reopen emits `[:store, :reopened]` and `[:store, :unavailable]`
  per try with the outage growing, retries on a doubling, jittered wait
  (`reopen backoff`), and starts the backoff over once it opens
  (`metrics_test.exs` scrapes `ankusa_store_write_failures_total`,
  `ankusa_store_reopen_total`, `ankusa_store_unavailable_seconds` and
  `ankusa_store_wal_replay_seconds`, and `health_test.exs` the `reasons.store`); the database opens with the recovery, blob and buffer
  settings asked for (read back from the `OPTIONS` file RocksDB writes, since
  RocksDB keeps its default for a value it cannot parse). The queue writer
  assigns dense, strictly
  increasing seqs (the hook's seq comes from its key); seqs never repeat after
  every hook is delivered, reclaimed and the instance restarts; a commit while
  the store is down is refused and recovers with a higher seq; and
  `[:commit, :stop]` measures successful commits only. Encodings
  (`envelope_test.exs`, `queue_test.exs`, `dispatch_test.exs`): a hook and a
  delivery row read in the bare v0 form and in v1 (`"AE"`/`"AD"` + version
  byte); a field a newer node added to an envelope decodes (the test splices
  in a key whose atom this VM never interned, which `binary_to_term(_, [:safe])`
  would reject) and an added row key survives a rewrite; an unknown version
  is an error value, and the pipeline dead-letters such a hook or row with
  that reason and lists it in the DLQ instead of raising.
- **Backup and restore** (`store/backup_test.exs`, `lifecycle_test.exs`): a
  backup, a wiped `data_dir` and a restart bring back every acked hook;
  retention keeps `backup.keep` backups and what they reference; a second node
  on one prefix is refused. `verify detects corruption` flips a byte, shortens,
  deletes a file and breaks a manifest, and sees each (`Backup.verify/2` writes
  nothing: no `put`), and the verify job reports every target on
  `[:ankusa, :backup, :verify]` (`ankusa_backup_verify_failed_total` in
  `metrics_test.exs`). `manifest signature` refuses an unsigned, mis-signed or
  tampered manifest at restore (and accepts a rotated key list, and no key at
  all with a warning); a backup of a newer store format is refused.
  `final checkpoint on stop` stops an instance cleanly and restores
  `LATEST` — newer than the last periodic backup — with every acked id; a final
  backup slower than `backup.final_timeout_ms` is abandoned without holding the
  stop. `secondary restore` loses the primary bucket and restores every acked
  id from the secondary, falls back when the primary fails its checksum, and
  refuses with the primary's reason when both do; a failing secondary fails the
  run without making the primary upload again. `/ready` says `backup: stale`
  past `backup.max_age_ms` and is `503` only with `require_fresh`. In
  `lifecycle_test.exs`, `opens a new incarnation` restores a backup taken
  before two published events and shows `io.ankusa.instance.restored` first,
  and the re-issued sequence numbers under different event ids.
- **Migration** (`store_migrate_test.exs`): a 0.3 data dir imports once — only
  what 0.3 had not finished (undelivered hooks deliver, unarchived hooks
  archive), dead letters carry over and replay to the current sinks, the
  quarantine pen (whose 0.3 entries a `quarantine` replay job still releases
  under their original ids), API sources, rate-limit overrides and the segment
  index all carry over, and the next seq clears everything imported. An artifact that
  cannot be trusted stops the boot: damage in the middle of the 0.3 WAL with acked
  frames after it, or a cursor file 0.3 did not write, refuses to start rather
  than guess; a torn final frame imports every complete frame before it. An
  artifact that shows up after the first boot is never imported over live
  data, and under `wal: :none` the queue artifacts are left for a node that
  has a queue.
- **Store upgrade** (`store_upgrade_test.exs`): current code boots every
  checked-in fixture `test/fixtures/store-v<n>.tar.zst` (a store written at
  format `n`, packed by `mix ankusa.store.fixture <path>` at the release that
  introduced it) and finds its pending and dead-lettered hooks, quarantine pen,
  API sources, archived hooks by id (`Storage.fetch/2`) and ingest dedupe keys.
  A new format ships its migration in the store's migration registry and its
  fixture in the same PR. An unversioned store is stamped format 1; a store
  newer than the release refuses to open naming both versions (and so do
  `mix ankusa.store.migrate` and its `--dry-run`); a migration killed after four
  of ten batches resumes at the fifth, never repeating a committed batch; a
  registry with a gap refuses rather than skip a format.
- **Audit trail** (`test/ankusa/admin/audit_test.exs`): every mutating route of
  the admin and route-management APIs leaves exactly one row (actor, client
  address, request id, keyed hashes, revision), and a request that changes
  nothing leaves none; `audit row survives kill` runs
  `test/support/audit_harness.exs` in a child BEAM that makes every kind of
  change and is `kill -9`ed on the response to the last one, then reopens the
  store alone; `no secret in audit` scans every stored and served row for the
  secrets the changes carried; `filters` covers `since`/`until`/`actor`/
  `action`/`tenant`/`cursor`/`limit`, tenant visibility and the 401/403 cases;
  the audit process's own tests cover commit order under concurrency, the
  pending row a crash leaves (recorded `interrupted` at the next boot), a
  caller that dies mid-change, retention pruning, and row versions. The format
  2 fixture (`store-v2.tar.zst`) carries audit rows, which
  `store_upgrade_test.exs` reads back; the format 1 fixture, upgraded, gives
  every source revision 1.
- **Tenant key layout and counters** ([ADR 0001](https://github.com/jamescarr/ankusa/blob/main/docs/adr/0001-tenant-key-layout.md);
  `queue/deliveries_test.exs`, `store_upgrade_test.exs`, `metrics_test.exs`,
  `dispatch_test.exs`, `admin/router_test.exs`, `loss_test.exs`):
  `counters equal a fold after any sequence of transitions` (a property test
  over ingest, claim, retry, deliver, dead-letter, revive, recover, expand,
  purge and archive settle) and `concurrent writers do not lose counts` pin the
  per-tenant counters to the index; `no due row without a marker` and `cleanup
  does not lose a marker to a concurrent enqueue` pin the active-tenant index;
  `tenant ordinal is assigned once and never reused` and the `tenants.max`
  cases pin the ordinals; a source-grep test keeps the tenant-keyed constructors
  inside `queue/deliveries.ex`. The format 3 fixture (`store-v3.tar.zst`, from
  `mix ankusa.store.fixture`) is booted like the others, and the migration's own
  cases cover the v2 → v3 upgrade with counters equal to a fold, a kill before
  every step (rows and counters equal an uninterrupted run), each ordinal
  allocated once, hooks with no tenant or envelope filed under `default`, a
  format 2 release refusing a format 3 store, and a restored format 2 backup
  migrating on boot. `queue gauges read counters` traces the store reads of
  `Gauges.measure/1` (no scan of a queue family) and `tenant scoped dlq reads
  one range` those of a tenant's DLQ listing (one range scan, a hook read only
  for the returned page); the traces run the code in a traced child process,
  because a process's calls to itself deliver no trace messages
  (`Ankusa.TestHelpers.store_calls/1`). `a tick visits only tenants with work`
  puts 1 000 tenants in the queue with 3 due and reads the scan's
  `active_tenants` gauge. `bench/gauges_bench.exs` and
  `bench/dispatch_tick_bench.exs` are self-asserting and run by `bench:gate`
  (see [Perf gate](#perf-gate-mise-run-benchgate)).
- **HTTP adapters** (outbound): the SigV4 signing `BlobStore.S3` puts on the
  wire, pinned against AWS's published reference signatures and against the
  request `Req.Test` captures; the RSA-SHA256 *Signature version 1* signing
  `BlobStore.OCI` puts on the wire, pinned against OCI's published reference
  signature (computed with OpenSSL) and reconstructed from captured requests;
  the `BlobStore.Azure` request plumbing (Put Blob headers, SAS appending vs.
  bearer `:token_provider`, `get_range` windows, `list` XML, `:not_found`); the
  `Ankusa.BlobStore.Azure.ManagedIdentity` token provider (IMDS fetch shape,
  caching, expiry-window refresh); `Sink.Http` forwarding, status mapping, the
  signed-by-default rule (a sink with neither `secret` nor `allow_unsigned: true`
  fails `validate_opts!/1` with "requires secret", and a delivery for one that
  skipped it is dead-lettered unsent), and the
  rule that a redirect is reported rather than followed; the shared
  `Ankusa.HttpClient` allowlist; the egress policy (`Ankusa.HttpClient.Egress`:
  loopback, metadata, private ranges, mapped IPv6 and odd IPv4 spellings,
  DNS rebinding including an answer that moves between attempts, `allow_cidrs`
  and `allow_hosts`, and that an `http` delivery really connects to the checked
  address over a socket) and TLS verification against a real `https` listener
  with a private CA (untrusted, `cacertfile`, hostname mismatch,
  `allow_insecure_tls`, `verify_none` refused), and computed per-tenant
  destinations (`computed url per tenant`, `computed url denied` — a private
  address fails closed on its own attempt, a rebinding name is re-checked per
  attempt — per-tenant headers and secrets, and the bounded pool against a real
  `https` listener: static sinks stay off it, an idle origin is reaped, a new
  origin is refused at the ceiling).
- **Secrets at rest** (`crypto_test.exs`, `source_store_persistent_test.exs`,
  `secrets_rewrap_task_test.exs`, `blob_store_sse_test.exs`): the
  `Ankusa.Crypto.Envelope` value layout (a flipped bit in the IV, tag or
  ciphertext, or a key id relabelled to another key's, is `:tampered`; every
  encryption has its own IV), key parsing and rotation (`kek_previous` decrypts
  and never encrypts; a failing or raising provider is an error value that does
  not echo what it raised), and which fields of a source spec are encrypted
  (exactly the ones `Ankusa.Admin.Redact` hides, a rotation list element by
  element, identity and structure left readable). `no plaintext at rest`: the
  stored row read raw, and every file under the data dir including the WAL,
  hold none of a spec's secrets, and a restart decrypts; the same scan finds
  them in a store with no KEK, so it is not vacuous. A row no key opens (no
  KEK, a key it was not given, an altered ciphertext, a KEK provider that
  fails) refuses the boot and names the source. `rewrap`: a rotation, a dry
  run that writes nothing, 700 rows over three batches, then the old key
  dropped and no longer opening the rows; `mix ankusa.secrets.rewrap` runs the
  same runbook against a stopped node's data dir. `sse` on S3, GCS, Azure and
  OCI is the header the adapter puts on the wire (`Req.Test` captures it; S3's
  is in `SignedHeaders`), only on a `PUT`; a bad option is a boot error and
  `put/4` raises rather than writing an unencrypted object.
- **Edge**: accept/verify/quarantine/load-shed/oversize, shedding with
  `503` once the batcher's queue fills while a commit is in flight, pluggable
  route resolvers (`Path` and `TenantPath`), and that the same body posted
  twice is stored twice (two ids, two stored hooks). The quarantine bucket is
  per source (`429 quarantine_rate_limited` with `Retry-After` on one source
  leaves another's `202`), and a full pen is `503 quarantine_full` that evicts
  nothing, frees room on a purge, and still counts its bytes after a restart.
  A hook refused for room gives its token back, concurrent charges on one
  source's bucket never pass its `burst`, and with the pen suspended 1 000
  writes wait (`pending`) and the 1 001st is `503 overload` that counts nothing,
  after which every waiting write lands and the pen's byte count is the sum of
  its entries' sizes. `bench/quarantine_bench.exs` (self-asserting, not in the
  gate) paces 5 000 forged hooks/s over 500 sources for 10 s and exits 1 unless
  their p99 ack latency is under 50 ms, a clean source's p99 beside them is
  within 2× of its solo p99 (or under 25 ms), and the byte count is exact.
  Admission (`edge_rate_limit_test.exs`): the per-address limit answers `429`
  over a real socket before a declared 1 000-byte body is read, an IPv6 address
  is its `/64`, `X-Forwarded-For` counts only behind a trusted proxy, an address
  refused there has not spent the node's bucket, the per-node and per-source
  byte limits (declared lengths before the read, undeclared ones after it), a
  tenant's byte quota spent by verified hooks only, its `max_body_bytes` as
  `413` before the read, and its queue share: with the writer suspended a
  tenant past `max_queue` gets `429 tenant_overload` (never `503`) while
  another tenant still gets `201`, counters never go negative, and 200
  concurrent charges admit exactly the share.
  A commit is never
  abandoned: a record still buffered at its deadline behind a stuck commit is
  answered `503` and never stored, the writer refuses a batch whose deadline
  passed before it could start, a commit stuck longer than the old 5 s writer
  timeout is waited out and every `201` is stored, a commit task killed from
  outside while its call waits for the writer fails only its own batch and
  leaves nothing of it stored, and an `:infinity` timeout never expires while a
  finite one beside it does. The batcher's status shows how many records it
  holds, never their sinks. `edge_direct_test.exs`
  covers the other ack path (`wal: :none`): the `201` body is exactly
  `{id, status}`, a sink sees the envelope before the response, a refusing,
  raising or bad-returning sink is `503` with `Retry-After` and is called
  exactly once, no queue writer runs and no hook is stored, an oversized
  body reaches the sink with `ctx.claim`, and a claim check whose blob store
  exits or returns garbage is a `503` before any sink runs.
- **Failure domains** (`instance_test.exs`): a dispatch subtree that exhausts
  its restart budget leaves the instance, edge supervisor, batcher and store
  processes untouched, still acks hooks during the outage and delivers them
  when the subtree comes back; a restart that fails is retried with a longer
  delay while the manager stays up; a `Registry` partition crash rebuilds the
  instance with every process registered again; the status
  (`:sys.get_status/1`, what a crash report prints) of each process that holds
  sink options shows none of them; and neither does the crash report of a
  batcher or source store that dies handling a hook or a source write.
- **Listeners** (`listener_test.exs`, `instance_test.exs`, `edge_test.exs`,
  `config_test.exs`): the limits `listener` sets reach Bandit and Thousand
  Island (connection cap spread over acceptors, header limits, HTTP/2 off); a
  raw socket that dribbles a request head or says nothing is closed after
  `read_timeout_ms`; a body that has not arrived within `request_deadline_ms`
  is a `408`; a keep-alive connection closes after `max_requests_per_connection`;
  a TLS edge with `verify: :verify_peer` serves the pinned client certificate
  and refuses a client with none or one a different CA signed (the fixtures in
  `test/fixtures/tls/`, regenerated by `gen.sh`); every other listener takes
  its own `tls` block; the metrics listener serves only `/metrics`, `/health`
  and `/ready` and answers `404` to the operator API's routes. The image
  smoke drill (`mise run docker:smoke`) repeats the mutual-TLS check with
  `openssl s_client` and `curl` against the built image.
- **Secret sources** (`source_store_static_test.exs` in core;
  `ankusa_server/test/ankusa_server/secrets_test.exs`): `SourceStore.Static.publish/2`
  swaps a running instance's sources, names the ids that changed, and applies
  nothing when one spec is refused; `${file:}` and `secret_file:` read a file
  (one trailing newline removed) and a missing, oversized, relative or
  doubly-given file fails the load by dotted key, never with a value; `${sm:}`
  resolves through a loopback stub of Vault KV v2 and AWS `GetSecretValue`
  (SigV4 header, token header, a JSON field, a binary secret) and a missing or
  unreachable secret fails the boot naming the reference; a signed POST
  verifies with the new secret after a file swap and a reload while the old one
  stays valid until it leaves the list; a manager outage or a deleted file at
  reload keeps the last value and logs a warning; a reload that cannot be
  applied changes nothing; an API-submitted spec cannot name a file.
- **Dispatch** (`dispatch_test.exs`): a hook reaches every sink of its source
  exactly once; a failing sink is retried until it succeeds; a failing sink's
  retries do not hold the slots a healthy source needs (D1); one dead row per
  exhausted sink, with the failing sink named in the stored reason, and the
  hook kept until every obligation clears; replay re-delivers a dead hook once
  and then reclaims it (D5); a raising sink and a bad-returning sink are
  dead-lettered, not fatal to dispatch; hooks whose source was deleted are
  dead-lettered after a restart (D2); a claimed row is retried after a
  restart while a delivered row is not (D7); a row that became visible below
  the scan floor (a stalled commit) is delivered when its wake arrives; and a
  window that drained is refilled even when housekeeping flushes the outcomes
  first. Ordering is asserted only for `ordered: true` sinks (below). `replay_test.exs` covers
  the replay jobs, including a `quarantine` job that leaves a hook failing the
  current secret in the pen, releases it once the secret is rotated (past the
  timestamp tolerance: the window is judged at receive time) with its original
  id and the job's `replay_id`, and finds nothing left on a second run.
- **Ordering lanes (DEL-1.b, ADR 0004)** (`dispatch/ordering_test.exs`,
  `source_ordering_test.exs`, `queue/deliveries_test.exs`,
  `store_upgrade_test.exs`): `ordered lane` (100 hooks for one key with a
  mid-stream retry arrive in order, one attempt of the lane in flight at a
  time), `other keys concurrent` (a slow lane holds no slot, window room or
  process), `survives restart` (a busy lane, a retrying head and their members),
  `dead head releases lane`, `replay respects lanes`, `keyless lane`, `key
  missing counter`, `racing writer` (a member committed between a release's
  read and its write is promoted by the sweep) and the `lane invariant`
  property (random ingest, claim, retry, delivered, dead, revive and purge:
  every `?O` has an `?H` or `?P`, every `?H` names one row of `?D`/`?F`,
  counters equal a fold). `source_ordering_test.exs` is the option grammar and
  lane resolution; the 7 → 8 migration and the `store-v8` fixture (a busy lane,
  a lane whose head waits out a retry, a keyless lane) are in the upgrade test.
- **Dispatch, retry horizon and error classes (DEL-2)** (`dispatch_test.exs`,
  `retry_policy_exponential_test.exs`, `retry_policy_test.exs`,
  `sink/{sqs,google_pubsub,http}_test.exs`): a row 7 h in with 200 attempts
  keeps retrying while one past `max_elapsed_ms` is dead-lettered by its next
  failure; no retry is scheduled past the horizon (a property test over
  generated policies and failure sequences); a real outage with
  `max_elapsed_ms: 1500` and no attempt limit dead-letters at the horizon, not
  at an attempt count; a `Retry-After` is capped by `dispatch.max_retry_after_ms`
  and the horizon; a per-source `retry:` is honoured across a restart of a
  persistent source store, and the row's `first_attempt_at` survives one; a row
  parked behind an open breaker is due by its horizon and dead-lettered past it;
  a `{:permanent, {:config, _}}` failure emits
  `[:ankusa, :dispatch, :config_error]` (and `metrics_test.exs` counts it); SQS
  and Pub/Sub auth/queue/topic errors are config errors, throttles a jittered
  `{:retry_after, ms, _}`; HTTP `409` is transient, `425` a pause, `451`
  permanent. `claim_check_test.exs`, `admin/router_test.exs` and
  `source_store_persistent_test.exs` hold the horizon against
  `claim_check.bucket_expiry_days`: boot refuses a configured or stored source
  that outlasts it, and an admin write of one is `422
  retry_exceeds_claim_expiry`.
- **Dispatch, per tenant** (`dispatch/isolation_test.exs`, `describe "per-tenant
  scheduling"`): `tenant fairness` runs a tenant at 99 times another's load
  behind a slow sink and asserts the light tenant's p99 delivery lag is within
  2× of its solo p99 (floor 25 ms), the heavy tenant never holds more claimed
  rows than its window and never runs more attempts than its concurrency; a
  page is split across the active tenants instead of going to the one with the
  oldest rows; a breaker, and `sink_concurrency`, are per tenant (one tenant's
  failing or slow sink parks or holds nothing of another's); a tenant's own
  `dispatch.tenants` window caps what it claims, and
  `ankusa_dispatch_tenant_inflight` reports it and then reports 0. The new
  keys' ranges are in `config_test.exs`. `bench/tenant_fairness_bench.exs`
  (`MIX_ENV=test mix run bench/tenant_fairness_bench.exs`, not part of
  `bench:gate`) runs the same 99:1 load through the edge and prints both
  tenants' p99.
- **Tenant erase** (`tenant_test.exs`, `tenant_s3_integration_test.exs`):
  `erase leaves no trace` fills a tenant and its neighbour with sources, routes,
  an override, delivery rows in every state, held hooks, dedupe keys and claim
  objects, erases one, and scans every column family, the claim store and the
  edge's tables for its id and ordinal: nothing is left, the counters equal a
  fold of the rows, and the neighbour is untouched. A lifecycle consumer is
  told in order (`deleted` of each source and route, then `tenant.deleted`) and
  a replay of the window emits nothing else of the tenant.
  `a running attempt cannot put a row back` erases while a slow sink's attempts
  are running and asserts no row, counter or marker remains, even after the
  retry would have been due. A tenant being erased takes no hook (`503
  tenant_erasing`, the quarantine pen too) and one that was erased is a new
  tenant. `a fetch of an erased hook is gone` archives hooks, erases, and
  asserts `Storage.fetch/2` is `{:error, :gone}` for them and not for the
  neighbour's, and that an archive replay skips them. `resumes after kill -9`
  runs the erase in a child BEAM, kills it with `kill -9` in the `queue` stage
  while its sink never answers, reopens the store, and asserts the tenant is
  still closed, the job runs to `erased`, one `tenant.delete` audit row and the
  `tenant.deleted` event exist, and the neighbour's hooks are all still
  stored. The request cases cover a wrong `confirm`, `default`, a bad name, a
  configured source, and the admin routes (`202`, the status, the audit actor);
  `admin/router_auth_test.exs` has the `401`/`403` cases, a token for the
  tenant itself included. The S3 test puts claim objects for two tenants in
  floci, erases one, and asserts its prefix is empty and the other's objects
  remain.
- **Filesystem durability** (`fsync_test.exs`): the helpers return an error
  for a path they cannot open, write or rename, never raise, and `write_file`
  replaces content in one step and leaves no temp file. `mkdir_p/2` fsyncs the
  parent of every directory from the root down to the target on every call,
  even when the directories already exist (traced, in order). The fsync order
  on disk is checked by `strace` on Linux, not in the suite.
- **Storage** (`storage_test.exs`): compaction round-trips every hook
  byte-for-byte with its seq, and a tick writes one segment plus one index
  object; a hook survives one cleared obligation and is reclaimed once both
  clear, in either order; without the `:storage` role a delivered hook is
  reclaimed straight away (W5); `roll_bytes` caps a segment's size; a failing
  blob store fails the tick without crashing it and the next tick retries; and
  the catalogue lives in the store, so fetch survives an instance restart. The
  segment format and archive layout (DUR-8): `codec_test.exs` holds golden
  vectors of the segment (formats 0 and 1) and index (formats 0 and 1) objects
  and refuses tampered, truncated, newer and hostile ones (`versioned header`,
  `safe idx`: an index naming an atom the node never saw is refused, not
  interned); `storage_test.exs` pins the `archive/v1/dt=<day>/<node>/` layout and
  its manifest, a segment or index altered on disk (not served), `time roll` (a
  partial segment waits `storage.roll_ms`, a backlog of `roll_bytes` does not,
  `tick/1` flushes), `storage.max_segments_per_tick`, `reconcile other node` (a
  second node serves the first's segments by day and reconciles them, never
  settling its own obligations) and the legacy `seg/` layout.
- **Claim Check** (`test/ankusa/claim_check/`, `test/ankusa/dispatch/claim_check_test.exs`):
  reference parsing (URN grammar, tenant and ULID claim-id rejection, date
  partitions from the pack id's timestamp, claim ids locating their pack and
  position); pack building (index rows, ZIP offsets, manifest, standard-reader
  round-trip); pack/redeem round-trips over `LocalFS`; tampered-object
  integrity detection; `404` for a claim id past the end of its pack's index;
  packing per tenant per batch with `pack_max_bytes` splitting and
  failure isolation; check-in-once (a fat hook on two sinks and a failing
  retry writes one object); `validate_config!/1` boot-time rejections; the
  `:claim_check` role's read-only HTTP API; the gateway's integrity check
  (`router_test.exs`, "integrity": a flipped body byte or an unparsable
  manifest is `502 integrity_mismatch`, `integrity_check: :off` serves and
  sends no digest, `x-ankusa-sha256` on `200`/`206` only, 100 concurrent
  `HEAD`s of one claim cause one store read and 100 `GET`s three — index,
  manifest, claim — and the manifest's digests in `pack_test.exs`); and the
  `LocalFS` retention sweeper deleting whole `dt=` day partitions (`sweeper_test.exs`,
  "bounded sweep": 10 000 partitions over 100 tenants, one tenant per tick, a
  restart resuming after the persisted cursor, a tenant the budget cut short
  staying the next tick's); a claim batch where an upload process dies or a store
  raises answering `unavailable` for that pack's items only, and the pack limits
  in `split/3` (`batch_test.exs`, "batch exit handled"). The gateway,
  sweep and archive series are in `metrics_test.exs` ("gateway metrics",
  "archive metrics") and the compactor's failure and age events in
  `storage_test.exs`.
- **Property tests** (`test/ankusa/property/`, [StreamData](https://hexdocs.pm/stream_data),
  5 000 runs per property by default; `PROPERTY_RUNS=50000 mix test
  test/ankusa/property` for a long soak; generators in
  `test/support/generators.ex`). Each one states what a parser or codec must
  hold for *any* input, not for the examples someone thought of: a claim ref
  parses back to itself, and whatever `Ref.parse/1` accepts is the canonical
  spelling; a pack built from 1–600 claims of 0–1 MiB reads back byte for byte
  by `Pack.locate/2` and by a standard ZIP reader (and the 65 532-claim
  ceiling builds, one more is refused); `Envelope.from_binary/1` inverts
  `to_binary/1,2` field for field in both encodings and never raises on
  arbitrary bytes; `Ingest.check_headers/1` never raises and
  agrees with the byte-class definition on raw bytes; `Net.parse_cidr/1` is an
  error or the exact range its prefix names, with the IPv4-mapped refusal;
  the 0.3 import's frame scanner forgives a torn tail (every whole frame
  imports), refuses the boot with a named error when an acked frame sits past
  damage (bit flips in the payload, CRC, length, magic or version, zeroed
  headers), and never raises on arbitrary bytes as a WAL, term log or
  cursor/floor sidecar (a CRC-valid frame around a payload that is not an
  envelope still raises, by design: `Store` turns that into a refused boot);
  `Verifier.Hmac` never raises for any scheme, template, header list or
  secret, verifies a request signed per its scheme under every key of a
  rotation list, and refuses a tampered body, a wrong key and a stale
  timestamp; `Dedupe.key/2` never raises, clamps to 200 bytes, and follows a
  JSON path to exactly the string or integer at its end. The corpus under
  `test/fixtures/corrupt_frames/` (see its `README.md`) is torn, bit-flipped and
  zeroed 0.3 frames and unreadable sidecars, each of which has to make the
  import return `{:error, _}`. A counterexample is fixed in the same change and
  its shrunk case kept as an example test. The first runs found four:
  `hash: sha1` raising in `:crypto.mac/4` and an invalid inline scheme raising
  out of `verify/2` (both pinned in `verifier_test.exs`), a pack of 65 535
  entries that `:zip` cannot open (the ceiling test above), and a torn WAL
  header zero-extended into a "frame" of length 0 (`store_migrate_test.exs`).
- **Route management** (`test/ankusa/net_test.exs`, `net/client_ip_test.exs`,
  `routes/matcher_test.exs`, `routes_test.exs`, `edge_route_guard_test.exs`,
  `routes/router_test.exs`): example-based (CIDR parsing is also covered by the
  property tests above). `cidr` (the
  dependency) does the prefix bit math, so `net_test.exs` pins the small surface
  Ankusa keeps on top of `:inet` — address parsing, canonical rendering, the
  IPv4-mapped-to-IPv4 normalization, and `parse_cidr/1`, where junk is an
  `:error` rather than a raise and an IPv4-mapped *range* is rejected because a
  normalized address could never match one.
  `net/client_ip_test.exs` walks the forwarded chain entry by entry: the header
  is read only from a trusted peer, every `x-forwarded-for` value is joined in
  order, the chain is walked right to left, an entry with a port or IPv6 brackets
  is read, an unreadable entry before the client denies the request, and no
  header falls back to the peer. `routes/matcher_test.exs` covers the
  path-pattern grammar and every negative case (`%2F`, `.`/`..`, a wildcard that
  is not last). `routes_test.exs` covers the ETS store's cap, `authorize/4`'s
  decision matrix (including a route's own rules replacing the global list, and
  a global deny beating them), the decision cache, the dry run's rule and scope
  reporting, telemetry, and CRUD. `edge_route_guard_test.exs` drives the guard
  with real requests and asserts its store effects (a rejected request writes
  **nothing**); `routes/router_test.exs` drives the management API over both
  `Plug.Test.conn` and a real socket, plus its 401/403 cases (`admin/router_auth_test.exs`
  does the same for every `Ankusa.Admin.Router` route; `admin/auth_test.exs` covers
  tokens, JWTs against a stub JWKS endpoint, and client certificates). `routes/router_openapi_test.exs` is the
  contract test for that API: it drives every documented operation from the
  examples in `priv/openapi/admin.v1.yaml` — path parameters, query parameters,
  and request bodies included — and drives every documented error response with
  a request that produces it, running each through the real router and checking
  the status, schema, and field names against the document, plus the documented
  method matrix, 404s for the near-misses of the documented surface, and every
  example against its own schema, so the spec and the code cannot drift apart.
- **A loss checker** (`loss_test.exs`): the one test here that runs a separate
  OS process. It starts a child BEAM that owns the same store as the test's
  instance and ingests concurrently, waits for 500 printed acks, then
  `kill -9`s the child — no clean shutdown, no flush beyond the `sync: true`
  commit that preceded each ack — reopens only the store over the same data
  dir and proves every acked id reads back. Zero tolerance: this is the test
  that actually backs the `wal.type: disk` half of the core invariant claim in
  [`architecture.md`](architecture.md), not just
  the description of it. The `wal.type: none` half is backed by
  `edge_direct_test.exs`, which asserts the response really does wait for the
  sink's confirm — and is a `503` when that confirm never comes. A full disk,
  a failing disk and a power cut are the crash drill's, below.
  `loss_test.exs` also runs the harness with `TENANTS=4` (four sources, each
  owned by a tenant and with a sink): after the `kill -9`, every hook has one
  due row under its tenant's ordinal, `pending_for_tenant/2` agrees, and the
  stored per-tenant counters equal a fold of the index (`counters equal the
  index after kill -9`).
- **Lifecycle events** (`lifecycle_test.exs`, `replay_test.exs`): through a real
  instance, a source's create/update/delete and a route's
  create/replace/patch/delete each deliver one CloudEvent to the configured
  sink, the source's secret arrives redacted, a refused change emits nothing,
  and the store assigns no seq to an event (it is not a hook). The outbox:
  `survives kill` runs `test/support/lifecycle_harness.exs` in a child BEAM
  whose only sink never confirms, creates a source, and is `kill -9`ed the
  moment `SourceStore.put/6` returns; the reopened store delivers
  `source.created`. `ordered by revision` puts three rapid updates through a
  sink that is slower the lower the revision and sees them in order;
  publishing the same row twice gives the same event `id`, body and
  idempotency key (a replay adds `replayid` and nothing else); a publisher
  killed before its cursor moved delivers again with the same `id`; the row
  holds no entity (it sits beside it, redacted); what changed while lifecycle
  was off is not announced. A pending row of a change nobody saw finish is
  settled from the subject's state: applied is announced, not applied is not,
  neither is announced as a state refresh, a deletion that was applied carries
  the entity the row recorded, and a row left across a restart is settled at
  the next boot. `lifecycle replay` re-emits a window (`since`/`until`/
  `tenant`) under the job's id with the original event ids, an empty window
  finishes, a node with no lifecycle sink refuses the job. A refusing sink is
  retried until it confirms; when the retries run out the event is dropped
  and counted but the change stands; a broken sink (one that raises, or
  returns neither `:ok` nor an error) never keeps another from receiving the
  event or crashes the publisher; with no lifecycle sinks there is no
  publisher. `ankusa:lifecycle` is a `404` at ingest, an event over a sink's
  inline threshold is claim-checked under a valid tenant and redeems to the
  event, and each invalid lifecycle config is refused at boot. The Redis
  route and source stores (`ankusa_redis`, `redis_lifecycle_test.exs`, needs
  the package's compose Redis): a child BEAM leaves a pending row for each
  place a `kill -9` can land (change applied, not applied, state changed
  elsewhere) and the reopened node announces the applied ones and the current
  state, and nothing for the one that never happened.
- **The AsyncAPI document** (`async_api_test.exs`, plus `GET /asyncapi.json` in
  `admin/router_test.exs`): sources on one address share a channel, sinks
  without a channel are absent, a computed address is a channel of its own, the
  tenant is fixed in a message only when the route resolver takes it from the
  source, ids that collide once sanitized stay distinct, lifecycle sinks add a
  channel naming the CloudEvent schema, and every document `async_api_spex`
  validates. The adapters' `*_describe_test.exs` pin what each sink advertises
  (and that no credential reaches it) without a broker.

The object-store transport has its own unit suites against `Req.Test` stubs, no
emulator needed: `blob_store_retry_test.exs` (full-jitter retries, `Retry-After`
honoured and `{:throttled, seconds}` when it does not fit, the one operation
deadline, the per-bucket breaker on failures and on p99 latency),
`blob_store_conditional_test.exs` (`put_if_absent` on every adapter),
`blob_store_checksum_test.exs` (digest headers on `PUT`, `{:error, :integrity}` on a
corrupted `GET`), `blob_store_multipart_test.exs` (a 16 MiB pack under a
1 MiB/s-link stub that times out any single request over 10 MiB, so only parts
get through), `blob_store_credentials_test.exs` (GCS metadata server, Azure
managed identity and OCI instance principal tokens refreshed on a `401`),
`blob_store_pool_test.exs` (one Finch pool per endpoint, `pool_size` and
`pool_timeout` against a real slow endpoint, option ranges) and the "conditional
pack writes" cases of `claim_check_test.exs` (a collision gets a fresh id with the
original intact; `Retry-After: 3600` returns inside the deadline).

The outbound connection pools (`Ankusa.HttpClient.Pools`) are tested against real
listeners in `http_client_test.exs`: `pool reaped` (an idle pool is closed by the
sweep, with its connections; one serving a request is not), `http2` (ALPN against
a private-CA TLS listener, and `protocols: [:http1]` and a sink's `pool:` override
forcing HTTP/1.1), the two ceilings (`max_pools` closes the least recently used
idle pool; `max_pools_per_tenant` closes only the tenant's own), the refusal
(`{:unavailable, :pool_limit}` with every pool serving a request, retryable), a
deleted source's pools, the sizes and ranges, and the boot warnings.
`sink/http_test.exs` proves the pinning on pooled connections: every connection a
pool opens goes to the address the egress check validated (a name that resolves
nowhere else still connects, verified against the name), and a name that turns
private between two deliveries through one pool is denied on the second, before
the pool is touched. `bench/pools_bench.exs` (`MIX_ENV=test mix run
bench/pools_bench.exs`, under a minute; raise `ulimit -n` first, as it holds
about 2 100 descriptors at its peak) sends one request to each of 10⁵ origins at
the dispatch concurrency and checks that live pools never pass `max_pools`,
nothing is refused, and no pool or socket is left after the sweep (it prints the
peak pools, client sockets, file descriptors and RSS, and the same after the
sweep). It also compares the owned pool with Req's default at one origin
(median of five alternating rounds): the 100 ms sink must keep 90 % of the
default's requests per second; the zero-latency sink's ratio is printed and not
gated, since there it measures the pool's per-request bookkeeping alone.

The `:integration`-tagged tests (`test/ankusa/blob_store_s3_test.exs`,
`blob_store_gcs_test.exs`, `blob_store_azure_integration_test.exs`,
`blob_store_oci_integration_test.exs`) exercise `BlobStore.{S3,GCS,Azure,OCI}`
against real running emulators: put/get round-trip, `get_range` byte-slicing,
`:not_found`, `list`+`delete`, `put_if_absent` on an existing key, and a 16 MiB
multipart round-trip. `store/backup_s3_integration_test.exs` runs a
store backup and restore through floci S3, and `sse header on put`: a segment,
a backup and a claim `PUT`, recorded on their way to the emulator, all carry
`x-amz-server-side-encryption`. `sink/sqs_integration_test.exs`
delivers through `Sink.SQS` to floci's SQS and reads the queue back: the
`Ankusa.Sink.Message` body, the message attributes and FIFO group, one stored
message for a retried FIFO delivery and a second for a replay, a missing queue
as an `{:sqs, 400, _, _}` error, and a standard queue. Through the batcher:
`sqs batch partial failure` (ten FIFO deliveries in one `SendMessageBatch`, the
emulator refuses the entry without a group, exactly that delivery is permanent
and the other nine are stored), `sqs batch order` (one group's messages arrive
in the order they were admitted) and a standard queue's batch.
`sink/google_pubsub_integration_test.exs` publishes through
`Sink.GooglePubSub` to floci-gcp's Pub/Sub and pulls the subscription back:
the `Ankusa.Sink.Message` data and attributes, an ordering key, a missing
topic as a transient error, and `pubsub batch`: 50 deliveries are one `publish`
(one `[:ankusa, :sink_batch, :flush]` event) and all 50 arrive. The unit tests
(`sink/google_pubsub_test.exs`) hold the bisection: one invalid message among
999 good ones is one dead entry and 999 delivered, within `1 + 2·⌈log₂ 1000⌉`
requests. Excluded by default
(`test_helper.exs`:`ExUnit.start(exclude: [:integration])`) because they
need live infra:

```sh
mise run test:integration
```

It starts `packages/ankusa/docker-compose.integration.yml` (floci S3 and SQS on
:4566, floci-gcp (GCS and Pub/Sub) on :4588, floci-az on :4577, floci-oci on :4599), waits for
the bucket/container bootstrap, runs `mix test --only integration` (the tagged tests alone; the rest is
`check:package ankusa`'s) in
`packages/ankusa`, and tears the emulators down. CI runs it as the
`core integration (floci)` job.

Every floci emulator runs in `persistent` storage mode (the images default to
in-memory) with its state on a named Docker volume, so it survives an emulator
restart; the task's `down -v` removes those volumes, so each run starts empty.
The example compose files that use floci do the same.

## `async_api_spex`: `mix test`

```sh
mise run check:package async_api_spex    # 62 tests + the standalone scaffold, no external infra needed
```

Covers: a document declared with `use AsyncApiSpex.Schema` / `Message` encoding
to the exact AsyncAPI 3.0 JSON (lowerCamel keys, no nulls, `$ref`s into
`components`, `x-` extensions inlined), two modules claiming one component name
raising, each validator rule failing with one error that names its JSON path,
bad macro options raising at compile time, `AsyncApiSpex.Plug.RenderSpec`
answering `application/asyncapi+json`, `AsyncApiSpex.Plug.Viewer` answering an
HTML page that loads the document (with a `</script>` in its config unable to
close the page's script block), and `mix async_api_spex.gen` writing a
decodable file. The decorators (`Schema` `fields:`, `use AsyncApiSpex.Channel`,
`use AsyncApiSpex.Spec`) are covered by the README's shop example producing the
exact server, channel, operation, message, and schema JSON, `otp_app:` discovery,
server and message sharing and conflicts between channel modules, a struct
schema that references itself, channel options that fail only when the
document is built (a server host that evaluates to `nil`, colliding message
keys), and the main compile-time option errors.

After the suite, `packages/async_api_spex/scripts/standalone.sh` proves the
library outside this repo: it fails if `lib/` mentions Ankusa, then scaffolds a
fresh `mix new` app depending on only the checkout (by path) and `plug`,
decorates a producer and a consumer that share a Kafka server, assembles the
document with `otp_app:`, and checks it validates, that `RenderSpec` and the
`Viewer` answer from a `Plug.Router`, and that `mix async_api_spex.gen` writes
it, all under `mix compile --warnings-as-errors`. `KEEP=1` leaves the scaffold
in place.

## `ankusa_rabbitmq`: `mix test`

Same pattern: every test needs live RabbitMQ (on :5673 AMQP, :15673
management UI):

```sh
mise run check:package ankusa_rabbitmq   # 11 tests
```

Seven tests are in `rabbitmq_test.exs` and four in `rabbitmq_describe_test.exs`.
Covers: inline-payload publish + decode, fat-payload claim check-in (message
carries a claim reference and its sha256, the claim round-trips through `Ankusa.ClaimCheck.redeem/3`
against a real `BlobStore`), routing key as both a static string and a
function, `{:error, {:unroutable, key}}` for a mandatory publish with no bound
queue, a broker-closed channel being reopened with the exchange re-declared,
`{:error, :nacked}`, and an unreachable broker asserting
`{:error, :not_connected}` (not `:econnrefused`, and not a hang).

## `ankusa_kafka`: `mix test`

Same pattern, against Redpanda on :19092:

```sh
mise run check:package ankusa_kafka      # 10 tests
```

`KAFKA_BROKERS` (default `localhost:19092`) points the suite at another
broker. Each test creates its own topic in `setup` and deletes it in
`on_exit`, and records are read back with `:brod.fetch/4`, so no consumer
group is involved. Covers: inline record round-trip (value, key, headers,
event timestamp), fat record claim check-in plus redeem, `:key` as a static
string and as a function, an unknown topic being an error that is **not**
auto-created, and an unreachable broker failing within `produce_timeout_ms`
instead of hanging.

brod's `crc32cer` NIF compiles from source, so the first `mix deps.compile`
needs a C toolchain and CMake ≥ 3.16; `.mise.toml` pins CMake, so
`mise install` covers the second.

## `ankusa_nats`: `mix test`

Same pattern, against NATS with JetStream enabled (on :4223 client, :8223
monitoring):

```sh
mise run check:package ankusa_nats       # 9 tests
```

`NATS_SERVERS` (default `localhost:4223`) points the suite at another server.
Each test creates its own stream with `Gnat.Jetstream.API.Stream.create/2`
(`subjects: ["ankusa.test.<n>.>"]`, memory storage) and deletes it in
`on_exit`, so nothing depends on a stream being pre-provisioned. Covers:

- an inline message: the subject it landed on, the nine headers asserted in
  `nats_test.exs` (`nats-msg-id`, `ankusa_id`, `ankusa_source_id`,
  `ankusa_tenant_id`, `ankusa_idempotency_key`, `ankusa_message_version`,
  `content_type`, `ankusa_dedupe_key`, `ankusa_replay_id`), the
  `Ankusa.Sink.Message` body, **and** `Gnat.Jetstream.API.Stream.info` reporting
  the message as stored, which is what proves the `:ok` came from JetStream's
  publish ack rather than from a successful socket write;
- a fat payload checked in through `ClaimCheck`, the message carrying a
  claim reference that redeems to the original bytes;
- `:subject` as a static string and as a 1-arity fun;
- a subject **no stream covers** being `{:error, :no_stream}`, with
  `Gnat.Jetstream.API.Stream.list` confirming no stream was created for it:
  the sink never creates one;
- a publish the stream itself refuses (`max_msg_size` exceeded) being an
  error, not a stored hook. JetStream answers that with `"seq": 0` *and* an
  `"error"` in the same ack, so this test is the one that pins the sink's
  error-first ack parsing;
- an unreachable server failing fast (`:econnrefused`, not a hang).

gnat is pure Elixir, so unlike `ankusa_kafka` this suite needs no C
toolchain.

## `ankusa_redis`: `mix test`

Same pattern, against Redis on :6399:

```sh
mise run check:package ankusa_redis      # 29 tests
```

`REDIS_URL` (default `redis://localhost:6399`) points the suite at another
server. Two instances in one VM share one namespace, which is how the suite
tests multi-node behaviour without a cluster: CRUD round-trips through
`Ankusa.Routes`, the cap is enforced against the shared hash, a write on one
node reaches the other over pub/sub **without** waiting for a tick, a raw write
with no broadcast is still picked up by the tick, a Redis error on a write is
`:store_unavailable` and leaves the local mirror untouched, and a boot against a
Redis that is not there (or against a corrupted definition) fails instead of
starting a node that would deny everything. The suite deletes only the keys under
its own namespace (`ankusa:routes:test`) between tests — never `FLUSHDB`, which
would wipe a Redis you happen to share with it.

The Redis *source* store has its own suite
(`test/ankusa/source_store/redis_test.exs`). Its secrets-at-rest cases: with
`secrets.kek` the hash entry holds no plaintext secret while every node with
the key reads it (and an update that omits the secret merges the decrypted
copy); a node with no key for an entry refuses its first boot and the log names
the entry; a `kek_provider` that fails refuses it too; `rewrap/1` moves
plaintext and previous-key entries to the current key, writes nothing and moves
no version when there is nothing to move, and a node with only the new key then
reads everything.

`test/ankusa/rate_limiter/store/redis_test.exs` (12 tests) covers the fleet store
(`Ankusa.RateLimiter.Store.Redis`, TEN-6) with two instances in one VM on one
namespace standing in for two edge nodes. **`fleet limiter`**: two nodes enforce
one tenant's 10 rps combined (the burst plus what refilled, where per-node
buckets would admit double), the byte bucket and a `429`'s `Retry-After`, and an
override change resetting the fleet bucket. **`fleet dedupe`**: the same event
key posted to each node is one stored hook (the second answers `201 duplicate`
with the first's id); a claim whose commit failed (`wal: none`, refusing sink) is
released so the retry on the other node stores the hook; a claim still pending
is `409 dedupe_pending` and stores nothing until it expires; a promoted key is
`committed:<id>` for the source's TTL. **`degrades without redis`**: Redis behind
a TCP forwarder that is killed mid-run, then restored — every answer is a `201` or
the node's own `429`, never a `5xx`, the breaker opens and
`ankusa_fleet_limiter_degraded` reads `1`, and the half-open probe closes it; and
a forwarder that delays every reply 500 ms, where the ack p99 over 200 hooks
stays under 150 ms and the breaker is open. A flushed script cache is answered
with `EVAL`. The stub-store counterpart, which needs no Redis,
is core's `edge_fleet_test.exs`: one store call per hook, the two-phase claim,
the breaker's open, half-open and failed-probe transitions, and the fallback.

`test/ankusa/sink/redis_test.exs` (6 tests) covers `Ankusa.Sink.Redis` against
the same server, subscribing with `Redix.PubSub` in the test process: an inline
message arriving on the channel with the `Ankusa.Sink.Message` body, a fat
payload checked in through `ClaimCheck` and the message carrying a redeemable
claim, `:channel` as a static string and as a 1-arity fun, a publish to a
channel with no subscriber being `{:error, :no_subscribers}`, `durable?` pinned
— including `Ankusa.Queue.validate_config!` refusing a
`wal: none` source whose only sink is this one — and an unreachable server
failing fast (`:econnrefused`, not a hang). Every publish in the suite goes to
a per-test unique channel and instance, so tests never see each other's
messages.

## Broker fault injection: toxiproxy (`*_faults_test.exs`)

Each broker adapter's suite has a fault suite beside its happy-path one
(`ankusa_rabbitmq/test/ankusa/sink/rabbitmq_faults_test.exs`,
`ankusa_kafka/…/kafka_faults_test.exs`, `ankusa_nats/…/nats_faults_test.exs`,
`ankusa_redis/…/redis_faults_test.exs`). The adapter's `docker-compose.yml` runs
a [toxiproxy](https://github.com/Shopify/toxiproxy) service beside the broker,
the sink under test is pointed at the proxy instead of the broker, and the test
adds latency, drops or a cut connection to the proxy through its HTTP API, or
`docker compose kill`s the broker itself and starts it again. The test's own
reads (what did the broker retain?) go straight to the broker.

| Package | Proxy listens on | Toxiproxy API | Broker killed (`docker compose kill`) |
| --- | --- | --- | --- |
| `ankusa_rabbitmq` | `:5674` → `rabbitmq:5672` | `:8475` | `rabbitmq` |
| `ankusa_kafka` | `:19093` → `redpanda:9093` (a `proxied` listener that advertises the proxy, so a client bootstrapped through it stays on it) | `:8476` | `redpanda` |
| `ankusa_nats` | `:4224` → `nats:4222` | `:8477` | `nats` (streams in the suite use file storage so they outlive it) |
| `ankusa_redis` | `:6400` → `redis:6379` | `:8478` | `redis` |

`TOXIPROXY_URL` and the adapter's `*_PROXY_*` variable (named in each suite's
`@moduledoc`) point a suite at another toxiproxy. The shared harness
(`Ankusa.Toxiproxy`, `Ankusa.BrokerFaults`) is
`packages/ankusa/test/support/broker_faults.exs`; each adapter's
`test_helper.exs` loads it and core's suite never does.

The contract every suite asserts: **`deliver/3` answers `:ok` only for a message
the broker retained, every other answer is a transient error (never
`{:permanent, _}`), and a row dispatch retries lands once the broker is back.**
An error does not mean "not stored" (a confirm that was lost, or arrived after
the caller's deadline, is a stored message: delivery is at-least-once), so each
suite also reads the broker to show that the message of a failed attempt is
there, and what the retry added. Four scenarios, each in every suite:

- **the broker killed with a publish in flight.** RabbitMQ: `max_inflight`
  publishes (4) wait on their confirms, a fifth is `{:error, :busy}`, the broker
  is killed; every answer is `:ok` (only for what the broker had kept across the
  kill) or a retryable error, and the retries arrive. Kafka and NATS: the
  acknowledgement is held in the proxy while the broker is killed; the delivery
  fails retryably, the retry lands (NATS: the stream holds the message once,
  `Nats-Msg-Id`). Redis: the `PUBLISH` is held in the proxy when the server
  dies, so no subscriber ever receives it; the delivery fails retryably and the
  retry reaches a fresh subscriber.
- **the per-publish timeout expiring mid-flight** (`confirm_timeout_ms`,
  `produce_timeout_ms`, `publish_timeout_ms`): the reply is delayed past the
  timeout, the answer is that timeout's error, and the broker holds the message
  anyway.
- **an acknowledgement lost with its connection** (RabbitMQ's confirm, Kafka's
  produce ack, JetStream's publish ack, Redis's `PUBLISH` reply): the broker
  stores the message, the proxy drops the reply and cuts the connection; the
  answer is a retryable error, the broker holds the message, and the retry is
  answered `:ok`.
- **latency above the attempt deadline.** The sink runs inside a real dispatch
  pipeline (`dispatch.attempt_timeout_ms` 1 s) against a reply delayed to 3 s:
  the attempt is killed and counted as failed, the row is **not** dead-lettered
  and stays due, and once the latency is gone dispatch delivers it (NATS: the
  stream holds it once).

`examples/fleet-e2e/run.sh`'s chaos phase adds the whole-fleet version: RabbitMQ
restarts at +36 s under the paced load, and the run still ends `missing=0`
(below).

## SDK conformance: one harness across the SDKs

`packages/sdk-python`, `packages/sdk-typescript`, `packages/sdk-rust`,
`packages/sdk-ruby`, `packages/sdk-go`, `packages/sdk-php`,
`packages/sdk-elixir`, `packages/sdk-java`, and `packages/sdk-clojure` are the
nine client SDKs.
Rather than hand-write each one's edge-case tests, all nine are checked
against the same language-neutral vectors in `conformance/`: a feature manifest
(`features.json`), JSON cases (`cases/*.json`), and a native runner per SDK
(`packages/sdk-python/tests/test_conformance.py`,
`packages/sdk-typescript/src/conformance/conformance.test.ts`,
`packages/sdk-rust/tests/conformance.rs`,
`packages/sdk-ruby/test/conformance_test.rb`,
`packages/sdk-go/conformance_test.go`,
`packages/sdk-php/tests/Conformance/ConformanceTest.php` — the PHP runner
stands up PHP's built-in server on a free port to answer as the mock gateway —
`packages/sdk-elixir/test/conformance_test.exs`, whose runner hand-writes a
`:gen_tcp` gateway so it can abandon a delayed response mid-flight the way the
timeout vectors require,
`packages/sdk-java/src/test/java/io/github/jamescarr/ankusa/conformance/ConformanceTest.java`,
a JUnit Jupiter `@TestFactory` that starts the JDK's own
`com.sun.net.httpserver.HttpServer` for the mock gateway, and
`packages/sdk-clojure/test/ankusa/sdk/conformance_test.clj`, which registers
one `clojure.test` var per case and uses the same JDK `HttpServer` as the mock
gateway).

```sh
mise run check:conformance    # validate conformance/, check core's encoder, run every SDK
```

The checker (`conformance/check.mjs`) fails if a `packages/sdk-*` directory is
unregistered, a feature has no cases, or a case names an unknown feature or
operation; then it regenerates the producer vectors and runs each SDK's runner
in that package's directory. Each runner also runs inside its own
`mise run check:package <pkg>`. Adding a feature to `features.json` makes every
SDK fail until it implements the feature; the vector format and runner contract
are in `conformance/README.md`.

The vectors are decoder-only except one file. `cases/producer.json`
(`message.producer`) is generated by `mix ankusa.conformance.gen` from
`Ankusa.Sink.Message.encode/3` (core's `conformance_gen_test.exs` asserts the
checked-in file is what the task writes), and `check.mjs` runs the same task
into a temporary file and fails when the two differ. So a field added to the
encoder without a vector update fails `check:conformance`, naming the message
keys that drifted, and once the vectors are regenerated every SDK has to decode
what the encoder now writes. `message.versions` and `message.decode.v2_additive`
pin the version range (1 and 2) every SDK accepts and that a `v: 2` message is
decoded like a `v: 1` one; `lifecycle.decode` covers `decode_lifecycle`.

`webhook.require_signature` (`cases/receive.json`) is the receiver half of
signed-by-default: every SDK's `receive_delivery` refuses an unsigned delivery
unless told `allow_unsigned`, verifies a signed one, and judges the signature
before the hook id. The Elixir `Ankusa.SDK.Receiver` plug is tested on top of it
(`receiver_test.exs`: it refuses to start without a secret, answers `401` to an
unsigned POST).

## Verifying the worked example

[`examples/rabbitmq-consumer/`](https://github.com/jamescarr/ankusa/tree/main/examples/rabbitmq-consumer/) isn't a Mix
test suite. It's verified by actually running it:

```sh
cd examples/rabbitmq-consumer
docker compose up --build
curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' -d '{"id":"evt_1"}'
docker compose logs worker     # confirm the hook printed
docker compose down -v
```

[`examples/kafka-sqs-consumer/`](https://github.com/jamescarr/ankusa/tree/main/examples/kafka-sqs-consumer/) is verified
the same way, plus its three failure drills (bridge down, worker down, poison
claim, each with its own `docker compose` commands and expected output in
that example's README). After it's up, one small and one fat hook exercise
both payload paths end to end:

```sh
cd examples/kafka-sqs-consumer
docker compose up --build -d --wait
curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' -d '{"id":"evt_1"}'
python3 -c "import json;print(json.dumps({'id':'evt_2','items':[{'n':i} for i in range(2000)]}))" \
  | curl -XPOST localhost:4000/webhooks/demo -H 'content-type: application/json' --data-binary @-
docker compose logs worker   # via=inline, then via=claim:<id>
docker compose down -v
```

[`examples/jetstream-k8s/`](https://github.com/jamescarr/ankusa/tree/main/examples/jetstream-k8s/)
is a manual drill on a real `kind` cluster. CI only does what
`mise run check:examples` does for it: builds and typechecks the consumer,
compiles and format-checks the control panel, and runs `helm lint` /
`helm template` on the
chart. The cluster itself is brought up by hand, and the drills in that
example's README (autoscaling, the claim path, killing pods, cutting
connections with Istio `AuthorizationPolicy`, gateway faults) are the test:

```sh
examples/jetstream-k8s/run.sh
curl -i -XPOST localhost:8080/webhooks/demo -H 'content-type: application/json' \
  -H 'x-demo-event-id: v1' -d '{"id":"evt_1"}'     # 201 through the Envoy gateway
open http://localhost:8080/                          # the control panel
examples/jetstream-k8s/run.sh down
```

Follow `ankusa_rabbitmq`/`ankusa_kafka`/`ankusa_nats`/`ankusa_redis`: a
`docker-compose.yml` for the real
dependency, and tests that
hit the real thing. A mock proves your code calls a mock correctly; it
proves nothing about whether a hand-rolled protocol implementation (SQL,
AMQP, SigV4, whatever) is actually right. Every adapter in this repo that
talks to external infrastructure is tested against a real instance of that
infrastructure, not a stand-in for it. That standard applies to new
adapters too.

## Crash-consistency drill: `mise run test:crash`

`loss_test.exs` proves the commit order against a `kill -9`. The drill proves
what that cannot: a full disk, a failing disk, and a power cut. It needs
`docker`, runs the same child-BEAM harness
([`crash_harness.exs`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa/test/support/crash_harness.exs),
shared with `loss_test.exs`) in the `elixir:<version>-alpine` image the server
builds with, and prints one line:

```sh
mise run test:crash
# acked=14940 recovered=14940 missing=0 enospc_503=1 resumed=true
```

- **ENOSPC** (`mise run test:crash`, CI job `crash`): `/data` is a 192 MiB
  tmpfs. The harness ingests `RATE` (default 1000) hooks/s of `BODY_BYTES`
  (default 10 KiB) each; at +5 s a filler file takes every free byte. RocksDB
  pre-allocates its WAL (about 70 MiB here), so the node keeps acking until
  that is used up: the filler stays until a POST has answered
  `503 store_unavailable` and `/ready` `503` (the store then reopens itself and
  may serve again for a while, so the 503s come in bursts). Then the filler is
  deleted, and a `201` and `/ready` `200` must come back on their own. The
  instance is `kill -9`ed under load once it has served again for 5 s and at
  least 20 s into the run, the store is reopened with `Ankusa.Store` alone,
  and every id the harness printed as acked must read back (`missing=0`). The
  volume is 192 MiB because the load (10 MB/s) refilled a 96 MiB one within
  seconds of the resume, and a node killed on a full disk cannot open its store.
- **EIO and power loss** (`scripts/drills/flakey.sh`): `/data` is ext4 on a
  device-mapper device over a loop device. Two 2 s windows (+5 s, +10 s) where
  every I/O fails, with the same `503`/resume assertions, then at +20 s a power
  cut: the device fails every I/O, the instance is killed, the filesystem is
  unmounted, repaired and remounted, so anything not fsynced is gone. A tmpfs
  cannot show a missing fsync, because `kill -9` loses nothing the kernel
  already holds (with `Queue.Writer` patched to `sync: false` the tmpfs drill
  still reports `missing=0`); this variant can: the same patch gives
  `acked=9344 recovered=5600 missing=3744` and exit status 1. It needs `docker run
  --privileged` with loop and device-mapper support in the Docker host's
  kernel and exits `2` where it has neither. `release:preflight` runs it
  before a core tag; CI runs it as the job `crash-powerloss` (GitHub's
  `ubuntu-latest` allows it; on a runner that does not, the job reports a
  skip). Where no privileged Docker host exists, it is a manual pre-release step.
  The windows use device-mapper's `error` target rather than `dm-flakey`, a
  module some kernels lack; the fault is the same, I/O errors for 2 s.
  A window fails the file data area, not the whole device: the volume is made
  with `packed_meta_blocks`, which puts the journal, bitmaps and inode tables
  in one run at the start, and only the blocks after it get the `error` target
  (the power cut still fails everything). With the whole device failing, the
  journal's writes failed too, and a journal that cannot be written is aborted:
  ext4 then remounts the volume read-only for good (`Aborting journal on
  device`, `Remounting filesystem read-only`, whatever `errors=` says), which
  no application can reopen a store on, so `the node did not resume` depended
  on whether a journal commit happened to land in a window. A drill that still
  fails that way prints the kernel's filesystem lines and says the volume went
  read-only.
  The other half of the fix is in the store: a write failure asks for a reopen,
  but the reopen (close, then an open whose recovery flushes the WAL to an SST)
  now waits until a synced file write next to the database works
  (`ankusa_store_reopen_total{result="deferred"}`). Without the wait, with the
  fault limited to the data area, a reopen that started inside a window was
  followed in one of two runs by a store no later open accepted (`Corruption:
  missing start of fragmented record`), and the node never resumed; with it,
  eight consecutive runs passed.
- **What the drills assert about the store's failure reporting.** While the
  fault is on, `/ready` answers `503` with a `reasons.store` (`no_space` for the
  tmpfs, `io_error`/`read_only` for the device), and after the node resumed the
  metrics listener (`METRICS_PORT`, 4001 in the drill) must show
  `ankusa_store_write_failures_total{class}` and `ankusa_store_reopen_total{result}`;
  the drill prints every `ankusa_store_*` line, and on a failure the harness's
  own log (`LOG_LEVEL`, stderr) and the kernel's filesystem lines.

`RATE`, `BODY_BYTES`, `FAULT_AT` and `KILL_AT` (seconds) tune a run. Compiled
deps are kept in `.cache/crash`, so only the first run builds RocksDB.

## Core bench: `bench/core_bench.exs`

An in-process ingest → dispatch bench for core alone: no HTTP hop, no consumer,
no Kubernetes.

```sh
N=20000 CONCURRENCY=256 SINK_LATENCY_MS=5 mise run bench
```

It ingests `N` hooks through `Ankusa.Edge.Ingest` at `CONCURRENCY` workers, then
waits for a sink that sleeps `SINK_LATENCY_MS` per delivery to receive every
acked envelope. One JSON line plus a table; exit code is non-zero if anything
acked never arrived. It runs under `MIX_ENV=test` because core's
`config/config.exs` autostarts the default instance on :4000 outside `:test`.

The bench reports `ingest_per_s`, `p50_ms`, `p99_ms` (ingest latency),
`end_to_end_per_s`, `drain_s` and `missing`, and exits non-zero if anything
acked never arrived. The numbers are machine-dependent and move with the
pipeline, so run it on your own hardware rather than reading a figure here.

### Dual commit: `wal.type: dual`

[ADR 0002](https://github.com/jamescarr/ankusa/blob/main/docs/adr/0002-dual-commit-intake-log.md) lists every case; each half of the
commit is driven alone with `Ankusa.Intake.Memory` (a file-backed log with
injectable latency, per-call failures, a lost ack, a held publish) or by taking the
store down, and asserts on what a consumer sees (the `201`, the stored hooks, the
sink's deliveries, the log's records):

- `queue_dual_test.exs`: `no ack without intake confirm`, the max-not-sum
  latency, a log that fails after the local commit (one hook id per dedupe key,
  one delivery, the retry is `intake_pending` until the reconciler completes
  the hook and `duplicate` after), a store that fails after the publish, both
  halves failing, a lost ack republished and applied once, the refusals before
  either half (open breaker, pending ceilings, record too large), the breaker's
  open / half-open / close cycle and that it never degrades to disk-only, the
  router's `503`s and `Retry-After`, the fleet claim staying pending, the
  startup lines, the metric series.
- `intake/reconciler_test.exs`: the log half, the local half, a local half that
  already landed is not reapplied, a stuck entry releases its claim, a permanent
  failure is stuck and kept, `hold` against a finished entry, claims promote only
  when both halves are confirmed.
- `intake/recovery_test.exs`: replay from `m:intake_offset` with the original seqs,
  duplicate and reordered records, a seq collision re-sequenced, republishing the
  local tail (`disk` to `dual`), unknown versions, kinds and sources, a truncated
  log, a node id mismatch, `Recovery.run_offline/2` (what `mix ankusa.recover`
  runs) twice and with `--dry-run`, a clean stop leaving the offset at the log
  head, and the offset staying below a record only the log holds.
- `config_test.exs` (every `intake.*` range and the cross-key rules, the
  `/v1/config` redaction), `health_test.exs` (`intake` ok, failing as `down`, failing as `recovering`).
- `health_test.exs` (OPS-5): the readiness checks and their gating. A
  direct-mode (`wal: :none`) instance whose sink fails is `503` with `sinks:
  failing` and ready again once the sink publishes (or after
  `listener.ready_sink_error_window_ms`); a data dir under `listener.ready_min_free_bytes` is
  `503` with `disk: failing` (`unmeasured` without `:os_mon`); a backlog over
  `queue.max_oldest_due_ms` is `backlog: failing` with a `200`; a check that
  reports `draining` gates, and one that raises is `failing` (reason `error`)
  without crashing the probe. `backup_test.exs` covers `backup` ok/failing and
  `require_fresh`; `edge_test.exs` the `/ready` body over HTTP;
  `admin/router_test.exs` that `/v1/config` shows the new keys.

`bench/intake_bench.exs` (`MIX_ENV=test mix run bench/intake_bench.exs`, also run
by `mise run bench:gate`) compares `wal: :dual` with `wal: :disk` in one VM:
`Ankusa.Intake.Memory` confirms after 5 ms, one client measures the ack p50 over
200 requests with 1 KB bodies, 512 clients measure throughput over 6 000 (the best of three rounds), and the log's mean batch is printed. It passes only if the
dual p50 is within `max(disk p50, 5 ms) + 5 ms` (the parallel commit costs the
larger of fsync and confirm, not their sum), the dual throughput is at least
10 000 req/s (the plan's per-node ingest SLO, absolute, so a slow disk cannot pass a ratio at half of it) and at least 0.3 x the disk's (a regression guard;
ADR 0002 asked for 0.8 x, which the dual path cannot reach where fsync is near free: 512 clients in a closed loop with one
commit in flight per batcher partition make the mean batch about 128 and cap the
dual mode at `clients / (2 x (confirm + overhead))`, while the disk mode is
CPU-bound; the ADR's Amendments carry the measurement, and the group commit
of DUR-4 is what lifts the ceiling).

The Kafka adapter is tested against real brokers, in
[`packages/ankusa_kafka/test/ankusa/intake/kafka_test.exs`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_kafka/test/ankusa/intake/kafka_test.exs)
(the package's `docker-compose.yml` has a three-broker KRaft cluster beside Redpanda,
and the tests stop and restart brokers with `docker compose`): a publish is confirmed
at quorum and its position is the broker's offset; one broker down still confirms and
two down is `unavailable`; `verify/1` refuses `min.insync.replicas` below 2, a
replication factor below 3, unclean leader election, a retention below twice
`backup.max_age_ms` and a `max.message.bytes` below `max_record_bytes`; the node's
partition survives a partition-count change; replay filters by node, skips unknown
kinds and versions and returns a duplicate record twice; broker error codes map to the four
classes. `bench/partition_bench.exs` measures what one partition carries.
`mix ankusa.recover`'s option parsing and the `--allow-gap` audit row are in
`intake/recovery_test.exs`; the server's `wal.type: dual` and `intake.*` keys, the
`ANKUSA_WAL_TYPE` and `ANKUSA_INTAKE_NODE_ID` overrides and the credentials' redaction
are in `ankusa_server`'s `config_test.exs`.

The fleet drill is the end-to-end proof:
`examples/fleet-e2e/run.sh --wal dual --kill-node` loses `ankusa-a` **and its volume** under
1 000 req/s with RabbitMQ paused (so the acked hooks are undelivered), starts a
replacement with the same node id, stops the three intake brokers (every request
`503 intake_unavailable`, none acked, `/ready` 503) and restores the node from the
secondary backup alone with `recover`: `missing=0`, `duplicate_hooks=0`.
`--wal disk --kill-node` runs the same host loss and **must lose** the hooks acked
since the last backup, so the drill proves the difference. See
[`examples/fleet-e2e/README.md`](https://github.com/jamescarr/ankusa/blob/main/examples/fleet-e2e/README.md).

### Group commit

- `queue_test.exs` (`group commit`): calls queued behind a suspended writer are one
  synced commit (`[:ankusa, :commit, :stop]` `batch_size`) with dense seqs and each
  caller's replies in its own order; a call that would pass
  `batcher.group_commit_max_records` or `group_commit_max_bytes` starts the next group.
- `dedupe_test.exs` (`the same key on two batcher partitions`): the same dedupe key
  posted at once to two partitions, with the writer suspended so both read it as
  absent, yields one hook, one delivery row and a `duplicate` with the first hook's
  id, whether the two calls merge into one group or commit as two; a key the sweeper
  deleted is not remembered as live. `queue_dual_test.exs` has the `wal: :dual`
  case (the retry meets an unconfirmed original: `intake_pending`).
- `queue_test.exs` (`the WAL fsync is pipelined`): a group is in the store (a reader
  sees it) and not acked, not durable (`Syncer.durable_seq/1`) and not due
  (`Syncer.visible/2`) while its fsync runs; group N+1 is applied, with dense seqs,
  while group N syncs; a failed fsync (the store taken away under it) answers the
  group with an error, fences the writer, deletes the group's rows once the store is
  back, and the provider's retry (same dedupe key) is stored once.
  `dispatch_test.exs` (`a group applied but not yet fsynced is not delivered`) holds the
  fsync with a telemetry handler on `[:ankusa, :wal_sync, :start]` and asserts the sink
  gets nothing until it returns; `queue_dual_test.exs` (`no ack before the local
  fsync, even once the log has confirmed`) does the same under `wal: :dual`.

`bench/store_commit.exs` (`MIX_ENV=test mix run bench/store_commit.exs`) drives
`Ankusa.Edge.Batcher.commit/4` with 10 KB bodies over 4 partitions and makes the
WAL fsync cost 1, 5 and 10 ms (a telemetry handler on `[:ankusa, :wal_sync, :start]`
sleeps in the syncer before its `sync_wal`). Throughput: 256 closed-loop clients,
grouped against ungrouped
(`group_commit_max_records: 1, max_commits: 1, group_commit_delay_ms: 0`: one write
per partition batch), printing records/s, groups applied/s, fsyncs/s, mean group,
groups per fsync, the writer's mean apply time and ack p50/p99. Burst: 1 000 req/s
open loop, the writer stalled for one second, then drained while arrivals continue.
It passes only if grouped is at least 1.5 x ungrouped at 5 ms, the writer applies at
least 1.2 groups per fsync at 5 ms (a writer that waits for each fsync cannot apply
more than one: the fsync is off its critical path), and the ack p99 of the requests
that arrived after the stall is at most 50 ms (the SLO; the group cap is what keeps a
backlog from becoming one 64 MiB commit).

### Perf gate: `mise run bench:gate`

A hot-path change must not make ingest slower unnoticed, so
[`bench/gate.exs`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa/bench/gate.exs)
runs the bench for `BODY_BYTES` 512 and 10240 (`N=20000`, `CONCURRENCY=256`,
`SINK_LATENCY_MS=5`) and for 512 bytes at `CONCURRENCY=32`, three runs each,
takes the median of every metric per cell (`512B`, `10240B`, `512B-c32`,
`512B-ordered`),
writes `packages/ankusa/bench/last.json`, and compares it with the checked-in
[`bench/baseline.json`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa/bench/baseline.json)
(`{"512B": {"ingest_per_s", "p50_ms", "p99_ms", "end_to_end_per_s"}, "10240B":
{...}, "512B-c32": {...}, "512B-ordered": {...}, "runner": "<class>"}`). It prints a table with the
delta per metric and fails when a cell's `p99_ms` is above 1.2× the baseline or
its `ingest_per_s` below 0.8× it:

```text
FAIL p99_ms 512B-c32: 45.10 > 1.2×21.60 (limit 25.92)
```

The `-ordered` cell is the `512B` cell with the source's sink `ordered: true`
(`ORDERED=1`: 64 keys, so 64 lanes run at once; ADR 0004): the cost of the lane
key at ingest (`ankusa_dispatch_lane*`), the member put and the sweep. Measured on
one machine (`BENCH_REBASELINE=1`) it runs within 3 % of `512B` on `ingest_per_s`
(27 596 vs 28 313) and no slower on `p99_ms` (15.4 vs 17.2 ms); its checked-in
entry is the `github-linux-x64` `512B` entry with `ingest_per_s` scaled by that
ratio (22 772), p99 unchanged. An unordered source pays one counter read per scan
(`lane_members == 0` gates the sweep): the three older cells did not move.

The `-c32` cell exists because of Little's law. At 256 workers the pipeline is
throughput-bound and latency is simply 256 ÷ throughput, so a fixed delay in
the request path (say a `Process.sleep(2)` in `Ingest.ingest/2`) changes
neither gated number; the 256-worker cells catch CPU cost per hook as lost
`ingest_per_s`. At 32 workers latency is the service time, so the same delay
shows up in `p99_ms`.

A p99 over 20 000 samples is noisy on a shared machine, and noise only adds
latency and takes throughput away. A cell that is over a limit after its
three runs is therefore run three more times and judged on its best run of the
six (lowest `p99_ms`, highest `ingest_per_s`): a real regression raises every
run, noise rarely does. `last.json` and the table show those best-run numbers
for such a cell.

Numbers only compare on the machine that recorded them. Hosted CI runners of
one class differ by a factor of two in speed from job to job (the same code
ran at 23 000 and at 50 000 hooks/s on two `ubuntu-latest` runners), so the CI
job does not compare with `baseline.json`: it measures the pull request's base
commit on its own runner first (`BENCH_REBASELINE=1 BENCH_BASELINE=<temp
file>` over the base's `lib/`, `priv/` and `config/`, with this change's bench
scripts), then gates the change against that. When the pull request changes
`mix.exs` or `mix.lock`, the base's copies are checked out with its sources and
`mix deps.get` materialises the base's own dependency set before the
measurement (the branch's are restored afterwards), so a dependency change is
also measured base-against-branch on one runner rather than against a baseline
recorded on another one. It falls back to the checked-in `baseline.json` only
when there is no base commit to measure. `baseline.json` (`runner:
github-linux-x64`) is the reference for local runs on a comparable machine; a
run on another runner class prints a `WARN` (a laptop is usually faster than
the CI runner, so it passes; a slower one can fail without a regression).
`BENCH_RUNS`, `BENCH_CONFIRM`, `BENCH_RUNNER`, `BENCH_BASELINE` and
`BENCH_REBASELINE` are documented in the script's header.

[`bench/tenant_fairness_bench.exs`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa/bench/tenant_fairness_bench.exs)
is the fairness bench of TEN-3, run by hand: it prints a light tenant's p99
delivery lag alone and beside a tenant at 99 times its load whose sink is slow,
and the heavy tenant's peak claimed rows and running attempts, and exits 1 if
the light tenant's p99 is more than 2× its solo p99 (or over 25 ms) or the
heavy tenant passes its window.

[`bench/dedupe_bench.exs`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa/bench/dedupe_bench.exs)
is the DUR-3 bench, run by hand (`KEYS=100000000 MIX_ENV=test mix run
bench/dedupe_bench.exs`, about 30 minutes and 13 GB of disk at 10⁸ keys; the
default `KEYS` is 10⁶): it fills a store with `KEYS` dedupe keys, once with the
settings from before DUR-3 (no bloom filter) and once with the defaults, and
prints the p50 and p99 of a `multi_get` of 256 fresh keys and of the
`sync: true` commit after it, each at the default block cache and at
`CACHE_BYTES`. It prints numbers and does not gate; the 10⁸-key results are in
[`storage.md`](storage.md#rocksdb-tuning-and-the-index-filters). The settings
themselves are checked by `store_test.exs` (`effective settings`), which reads
them back from the `OPTIONS-*` file.

`mise run bench:gate` also runs four self-asserting scripts first, with no
baseline entry (the fourth, [`bench/intake_bench.exs`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa/bench/intake_bench.exs), is under [Dual commit](#dual-commit-wal-type-dual)):
[`bench/gauges_bench.exs`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa/bench/gauges_bench.exs)
times `Ankusa.Metrics.Gauges.measure/1` and
[`bench/dispatch_tick_bench.exs`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa/bench/dispatch_tick_bench.exs)
times a dispatch scan (`Deliveries.active_tenants/3` plus `due_for_tenant/6`
for each tenant named), each against 10³, 10⁴, 10⁵ and 10⁶ queued rows over 10
tenants and against 10, 10³ and 10⁵ tenants of which 8 have rows due, while a
process commits 3 000 batches of counter merges a second. A cell is the 10th
percentile of 1 000 ticks, the lowest of three rounds (a tick is a fraction of
a millisecond, so a median moves by a factor of two with whatever else the
machine does; noise only adds time, and a growth with the queue raises every
sample). A script exits 1 unless its slowest cell is within 2× of its fastest
(counted as at least 1 ms) and under 50 ms ([ADR 0001](https://github.com/jamescarr/ankusa/blob/main/docs/adr/0001-tenant-key-layout.md),
as amended in TEN-1.b: a store a thousand times bigger costs a little more per
seek, on a CI runner the gauge tick went from 0.27 ms at 10⁵ rows to 0.59 ms at
10⁶, while a tick that grew with the queue would cost tens of milliseconds or
more at the top of either axis).
[`bench/routes_bench.exs`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa/bench/routes_bench.exs)
publishes 10⁶ routes over 10⁵ distinct first segments (10⁵ routes in the gate:
`ROUTES_BENCH_N`) and exits 1 unless the first publish and a reload each take
under 60 s (scaled below 10⁶), a request no route matches is decided in under
1 ms at p99 with the decision cache out of the picture (`Routes.dry_run/2`),
one create and one list page cost no more than 3× (plus 5 ms: a create is a
synced audit write) what they cost at 10³ routes, and a route costs no more
than the per-route estimate `routes.memory_budget_bytes` is checked with. Its
Redis half (boot and reload through `HSCAN`) is
`packages/ankusa_redis/bench/routes_load_bench.exs`. A rebaseline run
(`BENCH_REBASELINE=1`) skips them: it measures the
base commit's sources, which have neither the layout nor these APIs.

CI runs it as the `perf gate` job in `ci.yml`: on every push to `main`, and on
a pull request whose diff touches a hot-path file: anything under
`packages/ankusa/lib/ankusa/{edge,queue,store,dispatch,verifier,routes}/`,
`store.ex`, `queue.ex`, `dedupe.ex`, `envelope.ex`, `http.ex`,
`route_resolver.ex`, `sink/message.ex`, `sink/http.ex`, or the gate's own files
(`packages/ankusa/bench/`, `.mise/tasks/bench/`, `ci.yml`). The job always
reports a status (it can be a required check); on a PR that touches none of
those paths its steps are skipped. The run's `last.json` and the base
commit's numbers are attached as the `bench-last` artifact. If the gate goes
red on a change that touches no hot-path code, re-run the job once; a real
regression fails again.

A hot-path PR states before/after numbers in its description. The checked-in
baseline changes only in a pull request titled `perf: rebaseline <reason>`
that carries the before/after table: copy the `bench-last` artifact's cells
and `runner` into `baseline.json`, or run `BENCH_REBASELINE=1 mise run
bench:gate` on a `github-linux-x64` machine.

### Soak: `mise run bench:soak`

[`scripts/soak.sh`](https://github.com/jamescarr/ankusa/blob/main/packages/ankusa_server/scripts/soak.sh)
runs the built image (the baked demo source, a `log` sink) under
[`tools/loadgen`](https://github.com/jamescarr/ankusa/blob/main/tools/loadgen)
at `RATE` (200) req/s for `DURATION` (3600) seconds. Every `SAMPLE_INTERVAL`
(60) seconds it saves `/metrics` and the container's total RSS, then waits for
the queue to drain and fails when `missing > 0` (accepted hooks minus
`ankusa_dispatch_deliveries_total{result="ok"}`) or when RSS grew by more than
5 %: the lowest sample in the last sixth of the run against the lowest in the
second sixth (the container's RSS is a sawtooth that climbs about 10 % and
drops back every few minutes; the floor is the live set a leak would raise).
It also fails when the largest `/metrics` scrape of the run holds more than
`MAX_SERIES` (10 000) lines (`soak.json`'s `metrics_lines`): the per-node series
budget of the [cardinality policy](configuration.md#metric-cardinality).
It also samples `ankusa_dlq_rows` and `ankusa_claim_leases` and fails when the
highest sample of the last sixth of the run is above the highest of the second
sixth: with a healthy sink both stay at zero, and a retention gap shows as growth
(`soak.json`'s `dlq_rows_early`/`dlq_rows_late`, `claim_leases_early`/`claim_leases_late`).
The same property is proved without a container by the sink-down drill in
`packages/ankusa/test/ankusa/queue/deliveries_test.exs` (`dlq reaper: the sink-down
drill`): a sink that never answers for `DRILL_DAYS` (6) days at `DRILL_HOOKS` (20)
hooks a day, each with a claim pack and a lease, then one retention tick; the
DLQ, hooks, leases, claim packs and segments left must equal those of a three-day
outage, whatever the outage length. The rate is scaled down: the drill proves the
footprint is bounded by retention, not the throughput of 6 h at 10 k/s.
A run shorter than about ten minutes is still in warm-up and fails this check;
`MAX_RSS_GROWTH=1` turns it off for a quick smoke of the script. Samples,
scrapes, the loadgen report and `soak.json` land in
`soak-out/`. The nightly
[`soak.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/soak.yml)
runs it (and `workflow_dispatch` takes a shorter `duration`); locally,
`DURATION=600 mise run bench:soak` is a ten-minute check, and
`DURATION=120 MAX_RSS_GROWTH=1 mise run bench:soak` a smoke of the script.

## Fleet end to end (compose, nightly)

[`examples/fleet-e2e/run.sh`](https://github.com/jamescarr/ankusa/blob/main/examples/fleet-e2e/run.sh)
is the automated multi-node proof: the whole path a provider's hook takes, with
more than one Ankusa node, in plain Docker Compose (no `kind`):

```text
loadgen -> nginx (round robin) -> ankusa-a / ankusa-b -> RabbitMQ -> consumer -> Postgres
                                   |  (own store, own segment prefix)             |
                                   +-- Redis: routes, API-managed sources         |
                                   +-- MinIO bucket <- claim-check gateway <------+ (fat hooks)
```

Both nodes read their route definitions and sources from Redis (a path nobody
defined is a `404` on either), write segments under their own
`storage.key_prefix` of one bucket, and check fat bodies in to it. The
`claim-check`-role node is the only reader of that bucket; the consumer holds no
storage credentials. It records every hook in `processed_webhooks`, keyed by the
message's `idempotency_key`, which is the table `mix loadgen.verify` reads, so
the verifier is the one the kind drill uses.

```sh
examples/fleet-e2e/run.sh
# verify: missing=0 sha_mismatches=0 extra_deliveries=<n> claim_redeemed_cross_node=true duplicate_hooks=0
```

Phases: **steady** (paced, through the load balancer); **fat hooks** (a 20 KB
body POSTed to each node directly must reach the consumer through a claim, with
its sha256 intact: `claim_redeemed_cross_node`); **chaos** (the same load, with
`ankusa-a` killed at +10 s and started at +20 s, one event delivered to node b
and then *retried on node a*, the consumer killed at +30 s, and RabbitMQ
restarted at +36 s with the load still being acked, DEL-7: the nodes' publishes
in flight fail and are retried, and the consumer starts afresh once the broker
accepts connections); **burst**
(closed loop). Each phase is verified; the run fails on any `missing`,
`sha_mismatches` or `duplicate_hooks`.

`extra_deliveries` counts consumer receipts beyond the first per idempotency
key, and has two components. **Duplicate hooks** are one provider event stored as
two hooks (two ids): the example runs `dedupe.scope: fleet`, so the retry on node
a is answered with node b's hook id and none is created. `duplicate_hooks`
(distinct hook ids per idempotency key minus one, from the consumer table's
`hook_ids`) reports that component alone, and the run fails unless it is `0`
(`TEN-6` in
[`ANKUSA_HARDENING_PLAN.md`](https://github.com/jamescarr/ankusa/blob/main/ANKUSA_HARDENING_PLAN.md)).
**Broker redeliveries** are what RabbitMQ delivers again after the consumer kill:
the same hook, at-least-once, which ingest cannot remove and the chaos phase
exists to provoke. They stay in `extra_deliveries`, which is reported and
bounded by the delivery contract, not asserted to be zero.

CI: [`.github/workflows/e2e.yml`](https://github.com/jamescarr/ankusa/blob/main/.github/workflows/e2e.yml)
runs it nightly, on `workflow_dispatch`, on every release tag, and on a pull
request that touches the example, beside `mise run e2e` (the kind drill below).
The load generator's reports are uploaded as the `fleet-e2e-reports` artifact.
`RATE`, `DURATION` (at least 40), `BURST_SECONDS`, `CONCURRENCY`, `KEEP=1` and
`BUILD_ANKUSA=0` (use the image named by `ANKUSA_IMAGE`) tune a run.

## Load and end-to-end (kind + Oban)

[`examples/oban-consumer/run.sh`](https://github.com/jamescarr/ankusa/blob/main/examples/oban-consumer/run.sh)
is the only test in this repo that proves zero loss on a real, multi-node
Kubernetes deployment rather than in-process. It stands up a `kind` cluster
(a 3-pod `ankusa` StatefulSet, each pod a self-contained all-role node with
its own store on a persistent volume, and a 2-replica `consumer` running Oban),
then drives [`tools/loadgen`](https://github.com/jamescarr/ankusa/blob/main/tools/loadgen)
through three phases against it:

1. **steady**: a paced `RATE` req/s for `DURATION` seconds.
2. **chaos**: the same load, with `kubectl delete pod` against `ankusa-0`,
   `ankusa-1`, and one `consumer` pod at +10s/+20s/+30s.
3. **burst**: closed-loop at `CONCURRENCY` workers, no rate cap, for
   `BURST_SECONDS`; the drain time it reports is how long the pipeline needed
   after ingest stopped, so it is the dispatch throughput ceiling referenced in
   [`deployment.md`](deployment.md#dispatch-throughput).

`mix loadgen.verify` polls `processed_webhooks` (the consumer's ground truth,
written by the idempotent `WebhookWorker.perform/1` upsert) until every acked
id shows up or its timeout expires, and fails the run on any `missing > 0` or
`sha_mismatches > 0`.

Run it yourself: `mise run e2e` (needs `docker`; `kind`, `kubectl`, and
Elixir come from `.mise.toml`). `RATE` defaults to 300; `KEEP=1` keeps the
cluster.

Results from `RATE=60` and `RATE=300` verification runs, `kind` cluster
otherwise idle (`RATE=60` is the same load the pre-fix numbers below used, so
the two tables are directly comparable):

| Machine | `RATE` | Phase | accepted/s | p50 | p95 | p99 | shed | errors | missing | extra deliveries | `drain_s` |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Apple M4 Pro, macOS, OrbStack | 60 | steady | 56.0 | 5.3 | 9.6 | 13.5 | 0 | 0 | **0** | 0 | 0.04 |
| Apple M4 Pro, macOS, OrbStack | 60 | chaos | 56.1 | 5.3 | 9.5 | 14.3 | 0 | 0 | **0** | 0 | 0.04 |
| Apple M4 Pro, macOS, OrbStack | 60 | burst (64 workers) | 1496.5 | 39.0 | 69.7 | 89.7 | 0 | 0 | **0** | 0 | 0.05 |
| Apple M4 Pro, macOS, OrbStack | 300 | steady | 283.5 | 3.8 | 9.2 | 34.5 | 0 | 0 | **0** | 0 | 0.08 |
| Apple M4 Pro, macOS, OrbStack | 300 | chaos | 283.5 | 4.1 | 48.0 | 165.0 | 0 | 0 | **0** | 0 | 0.09 |
| Apple M4 Pro, macOS, OrbStack | 300 | burst (64 workers) | 1236.5 | 43.0 | 88.7 | 282.8 | 0 | 0 | **0** | 0 | 0.09 |

(Latencies in ms. `drain_s` is the time from the end of ingest to the last ack
being visible in the consumer, measured on the verify poll; the burst generator
is closed-loop, so its `accepted/s` is what 64 workers and a 40 ms round trip
sustain, not a dispatch ceiling.) Every phase, including chaos, reports
`missing: 0` and `sha_mismatches: 0`. This is the same machine every other
number in this doc was measured on, not a production-scale claim.

Re-verified after rebasing onto `main` (NATS JetStream adapter, HMAC verifier
engine): `RATE=300` again reported `missing: 0` and `sha_mismatches: 0` in all
three phases: steady 284.0/s, chaos 284.0/s, burst 1360.2/s, `shed: 0`, and
`drain_s` 0.05–0.07 s.

`drain_s` in the tenths of a second with `shed: 0` at both rates is what makes
`RATE=300` the default: dispatch keeps up with ingest in real time, so the
paced phases never build a backlog for the burst phase to inherit.

### The chaos-phase loss: root cause and fix

> **Historical note.** This bug lived in the WAL.Postgres adapter, which has
> since been removed, along with the `ankusa_postgres` package; Ankusa's queue
> now commits to the RocksDB store, not to a log adapter. The write-up is kept
> because the failure mode (a reader
> losing a commit that landed out of `seq` order) is a real hazard for any log
> adapter, and because the chaos phase below is still the proof that an
> all-store node loses nothing under pod kills.

Killing an `ankusa-worker` pod used to drop ~0.5–1.5% of acked hooks
permanently. The mechanism was not in the dispatch pipeline: WAL.Postgres
allocated `seq` at INSERT time (`BIGSERIAL`) but a row only became visible at
COMMIT, so two writers could allocate 100 and 101 and commit in the opposite
order. A reader following the log with `seq > cursor` read 101, advanced its
cursor past it, and never saw 100 when it landed, and the compactor, whose
truncation is bounded by that same cursor, then deleted the row. The hook was
in `ankusa_wal` no longer, in no `oban_jobs` row, no `processed_webhooks` row,
no DLQ, exactly the observed signature, with the cursor already advanced. It
took killing the *worker* to reproduce because that bursts the catch-up load
onto the shared Postgres and widens the allocation→commit window.

WAL.Postgres.append/2 took a per-instance advisory lock
(`pg_advisory_xact_lock`) before allocating seqs and held it until COMMIT, so
seq order *was* commit order; fleet-wide serialized commits per instance was the
accepted cost. The regression test (`ankusa_postgres`) widened the window with a
sleeping statement trigger and, on the pre-fix code, failed with ~34 seqs a
cursor-following reader never saw. The chaos phase reported `missing: 0`.

Pre-fix code, same machine, same harness: `RATE=60`, `POOL_SIZE=30`, and the
fixed loadgen, so this isolates the core change:

| Phase | accepted/s | p50 | p95 | p99 | missing | `drain_s` |
| --- | --- | --- | --- | --- | --- | --- |
| steady | 56.3 | 11.1 | 13.5 | 15.6 | 0 | 0.04 |
| chaos | 56.3 | 11.2 | 15.4 | 18.7 | 0 | 0.04 |
| burst (64 workers) | 3168.1 | 18.0 | 31.7 | 40.7 | 0 | **320.6** |

The paced phases were fine: 60/s is far below the ~150/s the old
one-envelope-at-a-time dispatch could sustain. The burst phase is where the
ceiling shows: 47,632 accepted envelopes took **320.6 seconds** to drain after
ingest stopped (≈149/s), against 0.05–0.09 s now.

That run's chaos phase came back clean, which is honest but not reassuring:
the old loss was a race, and it needs the allocation→commit window to be wide
enough at the exact moment a cursor reader passes the tail. The earlier
documented run of the same harness did lose 12 of 1581 acked hooks in chaos
(with a 301.7 s verify timeout), and the `ankusa_postgres` regression test
reproduced the mechanism deterministically rather than by luck.
