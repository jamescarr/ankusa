# Changelog

All notable changes to `ankusa_ra` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `Ankusa.WAL.Ra`: a shared, multi-node `Ankusa.WAL` backed by a Ra (Raft) log.
  A majority-acked append, one writer (the Raft leader) so commit order is seq
  order without a fleet-wide lock, and replicated cursors, dedup ledger and
  leases with fencing tokens, so `:dispatch` and `:storage` can each run as an
  active/standby pair.
- `Ankusa.WAL.Ra.Machine`: the pure state machine. It stores *where* each live
  record is in the Raft log, never the payload, so a snapshot's size does not
  track payload bytes; `live_indexes/1` keeps the entries still holding live
  records and Ra reclaims the rest.
- `mix ankusa.wal.members` for membership changes and leadership transfer.
- `Ankusa.DedupStore.Ra`: the idempotent receiver's ledger kept by the WAL
  cluster's replicated state, so two dispatchers that fail over to each other
  share what they have seen. One consensus round trip per record, which is why
  `Ankusa.DedupStore.ETS` is still the default; a decision the cluster cannot
  answer is reported as `{:error, reason}` — dispatch leaves that record
  undecided and retries from its seq — because neither guessing would do:
  delivering it unrecorded would forfeit the guarantee for that event, and
  dropping it could lose a first delivery.
- `Ankusa.WAL.Ra.Machine`'s `{:dedup_record, …}` command. The ledger is swept in
  the same units as the in-process store — entries the rule would already
  ignore — because replicated state, unlike a process, does not go away on its
  own.
- `mix ankusa.wal.migrate` for an offline cutover from a Postgres WAL.

### Changed

- The chaos harness runs the replicated ledger (`ANKUSA_DISPATCH_DEDUP_STORE: ra`
  on its worker services, `--dedup-store ra` at verification, and the variable
  read by `examples/oban-consumer/ingest_app`'s `application.ex`, which is the
  config loader this stack actually runs). It kills dispatchers, so with the
  in-process default the next copy of an already delivered event is delivered
  again — a run that lost nothing failing its own dedup check, with a missing
  ack count of zero to match. The gate asserts the strong guarantee because it
  now deploys the thing that provides it, and that is the point of having the
  store.
- `mix ankusa.wal.migrate` no longer copies the old WAL's dedup ledger. The
  commands are absolute, so a re-run is still safe on a cluster that has taken
  no traffic. The ledger is not migrated because nothing reads it any more, and
  importing it would hand the receiver a stale reason to drop a record: it
  recorded what the old WAL had *accepted*, not what had been *delivered*, and
  those are not the same set.

### Fixed

- **Every node hosted a Raft member, including the ones that should not.**
  `hosts_member?/2` decided membership by comparing a node name against the
  running node's own name — and the name it was given was built from `node()`,
  so it was true for every node that asked. Every edge and every dispatcher
  therefore started a node-local Ra *system* and a member for a cluster whose
  members list did not contain it, kept a log on its own disk that nothing read,
  and — because that log is opened at boot — a node whose volume was full or
  read-only failed to start at all: an edge, whose whole job is to call the
  cluster, was down over a file nobody needed. Membership is now asked of the
  members list, as the moduledoc always said: `:wal` in the node's roles, or the
  node naming itself. Verified against the failure it caused, on a container
  whose Ra log directory cannot be written: the old build exits with
  `failed to start child: {Ankusa.WAL.Ra, :default}`, the fixed one boots,
  answers `/health` and never touches the directory.
- **The chaos gate's I8 check never ran.** `mix ankusa.chaos.verify` read the
  outage windows out of `<scenario>-faults.json` with string keys while
  `Ankusa.WAL.Checker` wants `{from_ms, to_ms}` tuples, so every window arrived
  as `{nil, nil}` and every event fell outside it. A malformed window is now an
  error rather than a silent skip, and the checker ignores only the first second
  of a window — the nemesis stamps the time before it applies the fault.
- **Nothing produced the event history the checker was given.**
  `run.sh` passed `<scenario>-events.jsonl`, which no script wrote; the observer
  wrote a separate file whose `read` events carried empty results, so I3 was
  vacuously clean. The observer now reports `{:observe, [seqs]}` (a read *plan*
  has no bodies in it) and `mix loadgen.run --events` writes a per-request
  `{:edge, status, id}` stream with epoch-millisecond timings; `run.sh` merges
  the two in time order.
- `Ankusa.WAL.Checker`'s report now carries `evaluated`: how many pieces of
  evidence each invariant inspected. `mix ankusa.chaos.verify` prints the
  invariants with none as `not exercised`, so a gate that checks less than it
  looks like it does says so instead of reporting a pass.
- `scenarios/final-scan.sh` called `Ankusa.WAL.Chaos.Scan.dump/0`; the module is
  `Ankusa.WAL.Chaos`, so the final scan — the evidence I1 and I2 rest on — came
  back empty.
- **Every `rpc` call in the harness was wrong.** The release's `rpc` takes one
  expression and prints *only what that expression prints*; the scripts called
  `rpc "1" '<expr>'`, which evaluated the literal `1` and printed nothing. So
  the observer, the final scan, the stats sampler, the leader lookup and
  `wait_for_stack`'s readiness probe all produced empty strings — the gate could
  not have got past its own readiness check against *any* cluster, working
  distribution or not. Each expression is now printed explicitly, with
  `IO.puts` where the caller expects JSON (`Ankusa.WAL.Chaos.dump/1`) and
  `IO.inspect` where it is parsed as an Elixir term.
- `wal_has_leader/0` called `:ra.leader`, which does not exist, so
  `wait_for_wal_leader` could only ever time out. The leader now comes from
  `:ra.members`'s third element.
- `wal_leader_container/0` grepped the member list for `wal-[0-9]` and took the
  **last** match, so `kill-leader` killed whichever member happened to be listed
  last rather than the leader. It now asks the cluster who leads and derives the
  container from that node name.
- The load generator's `--out`/`--report`/`--events`, `loadgen.verify`'s
  `--acked`/`--report`, and `ankusa.chaos.verify`'s `--events`/`--acked`/
  `--final`/`--fault`/`--report` were written or read through one relative path
  and one `$ROOT/ankusa_ra/$OUT` path — neither of which is the directory the
  nemesis writes to over its `/out` mount, and the last of which is not the
  directory the verify task runs from. They now share one absolute path.
- **I8 judged an ack by when its request started.** A real power-loss run
  reported 35 `acked_without_quorum` violations that were all appends invoked as
  the member was killed and answered once it was back: the adapter retries for
  `append_timeout_ms`, so the commit — and the ack — happened after quorum
  returned. An ack is the *response*, so I8 now requires both the invocation and
  the completion to fall inside the outage, per window — with the same
  one-second grace at each edge that the window's own stamps already carry (it
  opens before the fault is applied and closes after a once-a-second poll
  notices a leader again). A violation in the first or last second of an outage
  is therefore unreported; that is the right way for a nightly gate to be wrong.
- **The readiness probe could wait three minutes against a healthy stack.**
  `wait_for_stack` POSTed the same `{"id":"probe"}` until it got a `2xx`. While
  the edge deduplicated, the first probe was acked and every later one came back
  `200`, so a leader check that raced on that first iteration waited out its
  three minutes against a stack that was serving correctly. Each attempt now
  uses a fresh id — which the edge no longer needs, now that it acks every copy,
  and which is one less thing for the probe to depend on.
- **I8 demanded a 503 that the invariant does not require.** The bound only says
  anything when the outage *outlasts* it: quorum returning before the deadline is
  I8's own second sentence ("after quorum returns, `2xx` resumes within 2 × the
  election timeout plus the client retry backoff"), so a request held and then
  committed is correct. A 5.5 s power-loss outage at 30 req/s produced a
  violation from the load generator's own 10 s receive timeout firing while the
  edge was still retrying — and not one `503` or in-window `2xx` in the whole
  run. The shed and latency requirements now apply only to requests whose
  deadline falls inside the outage.
- **Earlier runs' evidence leaked into later ones.** The observer and the stats
  sampler append to their files and the harness only ever added to them, so a
  rerun inherited a previous run's observations — whose cursor starts at seq 1
  again — and the checker reported the observer's cursor going backwards, which
  reads as a reader gap. `run.sh` clears a scenario's evidence before it runs and
  the observer truncates its own file.
- Three chaos scenarios did not do what they said. `clock-skew` ran `faketime`
  against a `sleep`, which cannot reach a running BEAM: it now recreates a member
  and the active worker with the machine's own `time_offset_ms` set
  (`WAL_RA_TIME_OFFSET_MS`), which is the knob the code actually reads.
  `replace-member` called `docker compose up` where the nemesis had no compose
  file (and removed a volume a stopped container still held), so the member was
  never replaced: the compose file is now mounted read-only and the container and
  its volume are removed before the service is recreated. `rolling-upgrade`
  logged an image it never used — it now rolls the cluster onto a second image
  tag, asserting a leader after every step; the harness says plainly which parts
  of a version bump it cannot show (`docker tag` of the same code cannot make two
  members disagree about a machine version).
- `Ankusa.WAL.Chaos.scan/1` reported an empty log when a read failed. A read has
  no error channel — `do_read/3` maps every failure to `[]`, because a dispatch
  reader simply tries again on its next tick — so one transient failure during a
  post-restart window made the final scan answer "no records", which reads as
  total data loss. The scan now compares what it read against the live-record
  count in `stats/1`, taking that count before and after: a mismatch on a log
  that did not move underneath the scan means reads were lost, and the scan
  raises rather than under-report; a log that *did* move supports no completeness
  claim either way, so it stays quiet. `final-scan.sh` retries, prints the reason
  and exits non-zero instead of writing `[]`.
- `scenarios/disk-full.sh` ended a command list on `[ -n "$second" ] && …`,
  which aborts the scenario under `set -e` when there is no third member.

- The chaos stack holds the compactor off (`ANKUSA_STORAGE_INTERVAL_MS`), so a
  run's records are still in the log when its final scan is taken. With the
  normal one-second tick, dispatch keeping up in real time leaves the final scan
  empty — the log is not an archive — and I1 has nothing to check.

### Added (harness)

- `Ankusa.WAL.Checker.check/4` accepts the acked set as `id => digest` as well as
  a set of ids. The load generator's CSV carries both columns, so I2 (are the
  bodies byte-exact?) and I10 (was anything surfaced that nobody acked?) have
  something to check on a run that drives the system over HTTP and therefore
  never emits an `append` event. An id whose digest is unknown is still an ack —
  it just cannot be checked against a body.
- `SINGLE=1` runs the whole gate against one WAL node — the documented laptop
  shape, a one-member Ra cluster that elects itself. It needs no
  container-to-container distribution, so the gate can run in a sandbox or on a
  laptop; it exercises the load generator, the edge's 503-without-quorum path,
  the observer, the final scan and both verifiers, but **not** replication. The
  scenarios are narrowed to `power-loss` and `rolling-restart`, the two that a
  single member can recover from. `chaos.yml` takes the same switch as a
  dispatch input.
- `Ankusa.WAL.ChaosTest`: `scan/1` and `dump/1` against a real one-member
  cluster — what is live, what truncation removes, that a healthy log does not
  trip the completeness check, and that `dump/1` is JSON.
- `Ankusa.WAL.CheckerTest`: the checker is pure, so its own logic is now tested
  without a cluster and everywhere, not only inside the `:dist` suites. Includes
  the case that found the I8 hole: a malformed window must fail loudly.
