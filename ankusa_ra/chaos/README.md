# Chaos harness

The Level-4 gate: a real fleet in containers, real faults, and an invariant
checker that decides whether the run passed.

```sh
./run.sh kill-leader     # one scenario
./run.sh mixed           # the nightly schedule
./run.sh all             # every scenario, one after another
```

Each scenario writes `out/<scenario>-report.json` and exits non-zero when any
invariant is violated or a `2xx`-acked record is not readable.

## What runs

| Service | What it is |
| --- | --- |
| `wal-0..2` | three `ANKUSA_ROLES=wal` nodes, one volume each — a real Ra cluster |
| `edge` ×3 | three edge replicas behind `edge-lb` (nginx) |
| `worker-0..1` | two `dispatch,storage` nodes: one holds each lease, the other is a hot standby |
| `floci` | S3-compatible segment store |
| `sink` | an HTTP consumer that records `(id, sha256, seq, deliveries)` per delivery |
| `nemesis` | `iproute2`/`iptables`/`tc`/`docker` with `NET_ADMIN` and the Docker socket: it injects the faults |

`run.sh` brings the stack up, starts a cursor-following observer and a 250 ms
`stats/1` sampler, runs the load generator at `RATE` for `DURATION` seconds with
a 70/20/10 mix (keyed / `nil`-key / resend), injects the fault *concurrently*
with the load, then checks the evidence:

1. `mix loadgen.verify` — every acked id reached the sink, with the body it was
   sent (the consumer's own view).
2. `mix ankusa.chaos.verify` — `Ankusa.WAL.Checker` over the event history, the
   acked set and the final scan.

## Scenarios

| Scenario | Fault | Invariants it stresses |
| --- | --- | --- |
| `kill-leader` | `kill -9` the Raft leader every 20 s | I1, I4 |
| `kill-random` | `kill -9` a random member every 15 s | I1, I4 |
| `pause-leader` | `SIGSTOP` the leader for 2× the election timeout | I1, I4, I9 |
| `partition-halves` | iptables split, leader in the minority | I1, I8 |
| `partition-leader-bridge` | cut only leader↔one-follower | I1 |
| `partition-clients` | edges isolated from the WAL | I8 |
| `netem` | 100 ms ± 50 ms, 5% loss | I1, I8 |
| `disk-full` | fill one member's volume, then two | I1, I8 |
| `clock-skew` | a member's clock 30 s fast and the active worker's 30 s slow, through the machine's own `time_offset_ms` (`faketime` cannot reach a running BEAM) | I1, leadership; token fencing (I9) is the Level-2 drill |
| `kill-dispatch-active` | kill the lease holder every 25 s | I1, I9, I10 |
| `kill-storage-active` | kill it mid-segment | I1, I7 |
| `zombie-dispatch` | `SIGSTOP` past the TTL, then `SIGCONT` | I6, I9 |
| `rolling-restart` | restart every member and worker, one at a time | I1, I4 |
| `rolling-upgrade` | roll every node onto a second image tag, one at a time, asserting a leader after each step | I1, I4 (the machine-*version* semantics are the Level-2 drill) |
| `power-loss` | `kill -9` all members at once, then restart | I1, I2 |
| `replace-member` | remove a member, delete its container and volume, add it back | I1 (it can only catch up by snapshot) |
| `big-bodies` | 1–8 MB bodies during `partition-halves` | I1, I8 |
| `mixed` | the nightly schedule: several faults back to back | all |

## What the checker is given

| File | What it holds |
| --- | --- |
| `out/<scenario>-events.jsonl` | the event history: every request the load generator made, with its status and the epoch-millisecond timings (`invoked_at`/`completed_at`), merged in time order with the cursor observer's `observe` events |
| `out/<scenario>.csv` | the load generator's acked ids and the digest each one carried — both are evidence: the id for I1/I10, the digest for I2 |
| `out/<scenario>-final.json` | the final scan: every record the WAL will still read, from a member (`Ankusa.WAL.Chaos.dump/1`) |
| `out/<scenario>-faults.json` | the quorum-outage windows, when the scenario takes the whole cluster out of service (`[{"from": ms, "to": ms}, …]`) |
| `out/<scenario>-stats.jsonl` | the 250 ms `stats/1` series, for eyeballing rather than gating |

The invariant table it enforces is in `Ankusa.WAL.Checker`'s moduledoc. Two
details of how the evidence is shaped are worth knowing:

* **`observe`, not `read`.** The observer follows the log with a cursor, and a
  read *plan* carries seqs but no bodies — reading the bytes is a second step.
  So it reports `{:observe, [seq, …]}`, which feeds I3 alone. A body-bearing
  `{:read, after, limit}` comes from a client that actually fetched records, and
  feeds I2 and I3.
* **Outage windows are marked, not inferred.** I8 asks whether an edge acked
  while no quorum existed, and only the scenario knows when that was. The
  scenarios that take the *whole* cluster — or every edge's path to it — out of
  service (`power-loss`, `partition-clients`, `disk-full`) mark a window; the
  others deliberately do not, because a partition that leaves a majority on one
  side still has quorum and still acks honestly. Flagging those would turn a
  correct ack into a violation. The minority-leader case is covered by the
  Level-2 drill, which can see the exact moment the leader was fenced.

**The compactor is held off for the duration of a run**
(`ANKUSA_STORAGE_INTERVAL_MS=3600000` in the stack). A compactor ticking at its
normal one-second interval reclaims every segment dispatch has already consumed,
and with dispatch keeping up in real time that is *all* of them — within seconds
of being written. That is the log behaving correctly (it is not an archive), but
it leaves nothing for the final scan to check the acks against. Holding the tick
off makes the run's records still present at the end, so "every ack is readable"
is a claim the evidence can support; the sink-side check
(`mix loadgen.verify`) is unaffected and remains the end-to-end proof that every
acked body reached the consumer byte-exact. `rolling-upgrade`,
`kill-storage-active` and the other storage-role scenarios still boot, lease and
kill the storage pipeline — only the periodic compaction pass is held back.

The window opens *before* the fault is applied (the nemesis has to kill the
container after stamping the time), so the checker ignores the first second of
each window rather than failing on its own timing. A violation in that first
second is unreported; for a nightly gate that is the right way to be wrong.

## What this harness cannot show

Two properties need something a container cannot fake, and are checked where they
can be observed exactly — the `:dist` fault drills in
[`test/ankusa/wal_ra_faults_test.exs`](../test/ankusa/wal_ra_faults_test.exs):

* **Token fencing (I9).** The scenario machinery drives the system over HTTP, so
  the history has no lease or cursor writes to check. `clock-skew` therefore
  shows that a wrong clock does not cost a member its place in the cluster; the
  fencing itself is drill 7.
* **Machine-version semantics.** A member must refuse a snapshot from an
  incompatible machine version until every member advertises the new one. A
  `docker tag` of the same code cannot produce that difference, so
  `rolling-upgrade` proves the *mechanics* of rolling a mixed cluster (recreate,
  rejoin, catch up, never without a leader) and drill 9 proves the version rule.

## Which invariants this harness actually exercises

The event stream here is what the *edge* did (`{:edge, status, id}`) and what a
cursor-following reader saw (`{:observe, seqs}`) — there are no `append`, lease
or `truncate` events, because the load generator drives the system over HTTP
rather than through the WAL API. So:

| Invariant | In the chaos harness | Where else it is checked |
| --- | --- | --- |
| I1 (an ack is readable) | the acked ids (`.csv`, first column) against the final scan | `wal_ra_faults_test.exs` |
| I2 (bodies are byte-exact) | the acked digests (`.csv`, second column) against the stored rows, and end to end at the **sink** — `mix loadgen.verify` compares every delivery against what the edge acked | `wal_ra_faults_test.exs` (via `read` events) |
| I3 (a follower sees no gap) | the observer's `observe` events | `wal_ra_faults_test.exs` |
| I8 (no ack without quorum) | the outage windows, above | `wal_ra_faults_test.exs` |
| I10 (nothing un-acked is surfaced) | the readable rows that no client acked | `wal_ra_faults_test.exs` |
| I4, I5, I6, I7, I9 | not exercised — no append/lease/truncate events exist (the load generator drives the edge over HTTP) | `wal_ra_faults_test.exs`, the Level-3 property suite |

`mix ankusa.chaos.verify` prints a `not exercised` line and writes the same list
into `out/<scenario>-report.json`, so a scenario that checks less than it looks
like it does says so out loud. An invariant with no evidence is unchecked, not
satisfied.

## Running it without three nodes

`SINGLE=1` runs the whole gate against **one** WAL node — the
[laptop shape](../lib/ankusa/wal/ra.ex) the adapter documents, where a
one-member Ra cluster elects itself and commits immediately. It needs no Erlang
distribution between containers, so the gate can run on a laptop or in a
sandbox where three-node distribution does not work:

```sh
SINGLE=1 RATE=40 DURATION=60 FAULT_WINDOW_S=30 ./run.sh power-loss
SINGLE=1 ./run.sh all        # power-loss, then rolling-restart
```

Everything the gate is made of runs for real: the load generator's `202`s and
`503`s, the observer, the final scan, `Ankusa.WAL.Checker`, and both verifiers.
The scenarios are limited to the two that make sense with one member
(`power-loss`, `rolling-restart`) — a drill that kills a member and leaves it
down, or that needs a majority to elect a new leader, cannot recover on one
node. **Replication is not exercised at all**: a single member surviving the
loss of another is the thing `SINGLE=1` cannot show you, and the three-node
runs are still the gate for it.

## Running it

Needs `docker` with compose v2, `jq` (to merge the event streams), and
enough room for the images (the ingest
release is built from this repo). `KEEP=1 ./run.sh kill-leader` leaves the stack
up for inspection; `RATE`, `DURATION` and `FAULT_WINDOW_S` tune the load and the
fault window.
