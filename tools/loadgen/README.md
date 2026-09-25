# loadgen

Unpublished internal Mix project. Two tasks, used together by
`examples/oban-consumer/run.sh` to prove Ankusa delivers every webhook it
accepts to the Oban consumer fleet with zero loss under concurrent load.

Not part of the Ankusa package set; never referenced from `mix.exs` of any
published package.

## `mix loadgen.run`

Fires concurrent HTTP POSTs at a webhook ingest endpoint for a fixed wall
clock duration, then writes a CSV of every accepted (`202`) delivery plus a
JSON report of the run.

```
cd tools/loadgen
mix loadgen.run --url http://localhost:4000/webhooks/some-source \
  --concurrency 64 --duration 60 --rate 500 \
  --dup-ratio 0.05 --body-bytes 512 \
  --out acked.csv --report loadgen-report.json
```

### Flags

| Flag              | Type    | Default                 | Meaning                                                                                   |
|-------------------|---------|--------------------------|---------------------------------------------------------------------------------------------|
| `--url`           | string  | *(required)*             | Full URL the load generator POSTs JSON bodies to.                                           |
| `--concurrency`   | integer | `64`                     | Number of concurrent worker processes.                                                      |
| `--duration`      | integer | `60`                     | Wall clock seconds each worker runs for.                                                    |
| `--rate`          | integer | *(absent = closed loop)* | Target aggregate requests/second. Pacing is **open-loop**: request number `n` is *scheduled* at `n/rate` seconds after start, so a generator that falls behind does not lower the offered rate — it shows up as latency. Omit for closed-loop firing (each worker sends its next request immediately after the previous one completes). |
| `--dup-ratio`     | float   | `0.05`                   | Probability that a given request resends a body this worker previously got a `202` for. The resend is committed too — the log has no uniqueness constraint — and dispatch is what keeps it from being delivered twice. |
| `--body-bytes`    | integer | `512`                    | Size in bytes of the `"pad"` field in each freshly generated JSON body.                     |
| `--out`           | string  | `acked.csv`              | Path to write the "acked" CSV (`id,sha256hex` per accepted delivery, no header).             |
| `--report`        | string  | `loadgen-report.json`    | Path to write the JSON run report.                                                          |
| `--events`        | string  | *(absent = not written)* | Path to write a JSONL event per request — `{"client":"loadgen","op":{"tag":"edge","0":<status>,"1":<id>},"invoked_at":<epoch ms>,"completed_at":<epoch ms>}` — in the shape `Ankusa.WAL.Checker` reads. The aggregate report cannot answer *when* a request was shed; only a per-request stream can. |

### Response classification

- `202` → accepted; the response body's `"id"` and the SHA-256 of the sent body
  are recorded to `--out` and kept as one of this worker's own dedup-source
  bodies. The CSV also carries the provider event id the body declared (`--out`
  is `id,sha256,event_key`), which is what lets `mix loadgen.verify` recognise a
  copy that dispatch deduplicated.
- `202` with JSON `"status": "quarantined"` → accepted, but held back from
  delivery by the source's `on_verify_failure`: counted separately, and *not*
  written to `--out`, because no consumer will ever see it.
- `503` → counted as shed (backpressure).
- Anything else, including transport errors/timeouts → counted as an error.

### Report

`--report` is JSON with `sent`, `accepted`, `quarantined`, `shed`, `errors`,
`duration_s`, `sent_per_s`, `accepted_per_s`, and `latency_ms: {p50, p95, p99,
max}` (all percentiles over every attempted request, in milliseconds). The same
numbers are also printed to stdout as a table.

`sent_per_s` is the achieved send rate and is the honest check on `--rate`:
when `--rate` is set and `sent_per_s < 0.95 * rate`, the generator itself was
the bottleneck and loadgen prints a warning to stderr (the exit code is
unchanged). Latency under `--rate` is measured from each request's *scheduled*
send time, not the moment the worker actually got to it, so generator
slowdowns are visible instead of being hidden (coordinated omission).

### Exit code

- `0` — the run completed and at least one request was accepted. This does
  **not** mean every request succeeded; check `errors`/`shed` in the report
  for that.
- non-zero (`Mix.raise`, surfaced by `mix` as a failing exit code) — `--url`
  or another required flag was missing, or `accepted == 0` (nothing to
  verify downstream, treated as a hard failure).

## `mix loadgen.verify`

Reads the CSV written by `mix loadgen.run --out ...` and polls the
consumer's `processed_webhooks` table until every acked id has been
processed (or a timeout elapses), then reports on loss/duplication.

```
cd tools/loadgen
mix loadgen.verify --acked acked.csv \
  --database-url postgres://ankusa:ankusa@localhost:5432/consumer \
  --timeout 300 --report verify-report.json
```

### Flags

| Flag               | Type    | Default               | Meaning                                                                 |
|--------------------|---------|------------------------|--------------------------------------------------------------------------|
| `--acked`          | string  | *(required)*           | Path to the CSV written by `mix loadgen.run --out ...`.                  |
| `--database-url`   | string  | *(required)*           | `postgres://user:pass@host:port/dbname` connection string for the consumer's Postgres database (the one holding `processed_webhooks`). |
| `--timeout`        | integer | `300`                  | Maximum seconds to poll for full drain before giving up.                 |
| `--report`         | string  | `verify-report.json`   | Path to write the JSON verify report.                                    |

Polling happens every 2 seconds, querying `processed_webhooks` in chunks of
5,000 ids per `SELECT ... WHERE ankusa_id = ANY($1)`, until every acked id
has appeared or the timeout elapses.

The verifier runs while the sink is still being written to, so it connects with a
four-connection pool and a generous queue timeout, and retries a transient
`DBConnection.ConnectionError` instead of failing: a pool that is momentarily
saturated is not evidence that a delivery was lost. The poll deadline — not a
single query — is what decides whether a record is missing.

### Report

`--report` is JSON with:

- `acked` — number of ids read from `--acked`.
- `processed` — number of those ids found in `processed_webhooks`.
- `missing` — up to 10 example acked ids that are not in the table *and* whose
  provider event id never reached it either (the console summary prints the
  *full* missing count; the JSON list is truncated to 10 examples).
- `deduplicated` — acked copies that are not in the table because dispatch
  dropped them as duplicates of an event it had already delivered. Expected
  whenever a run resends bodies (`--dup-ratio`), and never a failure: the edge
  commits every copy, and the idempotent receiver is what stops the second one
  reaching a sink.
- `duplicate_deliveries` — events delivered more than once, counted per provider
  event id. Must be 0: this is the dedup guarantee, checked end to end.
- `sha_mismatches` — count of found rows whose `body_sha256` doesn't match
  the SHA-256 recorded for that id in the acked CSV.
- `extra_deliveries` — sum of `deliveries - 1` over every found row: a *record*
  that reached a sink more than once. Informational, and never a failure:
  delivery is at-least-once, so a dispatch retry or a dead-letter replay can
  legitimately deliver the same record twice, and the sink dedupes it like any
  other repeat. `duplicate_deliveries` is the field that has to be 0 — two
  *different* records of the same event is the guarantee, not two deliveries of
  one record.
- `drain_s` — approximate wall-clock seconds from the start of polling until
  the missing set first became empty (each poll re-queries the full acked
  set, so this is the poll-iteration timestamp where the count first hit
  zero, not the exact write timestamp of any single row).
- `unacked_processed` — rows in `processed_webhooks` not accounted for by
  this run's acked set. Informational only; never affects the exit code.

### Exit code

- `0` — every acked id was found in `processed_webhooks`, or was deduplicated
  against one that was, before the timeout, with no SHA-256 mismatches and no
  event delivered twice. Zero loss, and no double delivery, proven.
- non-zero (`Mix.raise`) — `--acked`/`--database-url` missing, or after the
  timeout an acked id is still `missing`, or `sha_mismatches > 0`, or
  `duplicate_deliveries > 0`. Check the printed summary and `--report` for the
  offending ids.
