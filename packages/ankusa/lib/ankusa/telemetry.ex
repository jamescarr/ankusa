defmodule Ankusa.Telemetry do
  @moduledoc """
  The cross-component contract. Components emit `:telemetry` events; they never
  call each other's reporters.

  Event prefix is `[:ankusa, ...]`. Core emits two shapes, and the distinction
  is deliberate:

    * **span** (`span/3`) — emits `:start`, `:stop`, and `:exception`, with
      `:duration` in native units on `:stop`. Used where a crash is itself worth
      an event: the request-path stages and the store commit.
    * **single event** (`emit/3`) — for background ticks that time themselves.

  | Event | Measurements | Metadata |
  | --- | --- | --- |
  | `[:ankusa, :ingest]` (span) | `:duration` | `:instance`, `:source_id`, `:tenant_id`, `:route_id` (the route the guard matched, `nil` with routes off; never a metric label), `:size`, `:outcome`, `:dropped_headers` (request headers the stored-header policy removed from the hook; `0` when it was not stored) |
  | `[:ankusa, :ingest, :refused]` | — | `:instance`, `:reason` (`:unknown_source`, `:payload_too_large`, `:body_read_failed`, `:invalid_header`, `:store_unavailable`) |
  | `[:ankusa, :verify]` (span) | `:duration` | `:instance`, `:source_id`, `:tenant_id`, `:provider`, `:scheme`, `:status`, `:weak_hash` |
  | `[:ankusa, :commit]` (span) | `:duration`, `:batch_size`, `:bytes` | `:instance` (a batch spans tenants, so it names none). The span is the write of a group into the store (`sync: false`); the fsync that makes it durable is `[:ankusa, :wal_sync]` |
  | `[:ankusa, :wal_sync]` (span) | `:duration`, `:groups` (groups this one fsync covered) | `:instance`. One WAL fsync by `Ankusa.Queue.Syncer`; a failed one emits `:exception` and acks none of its groups |
  | `[:ankusa, :load_shed]` | `:queue`, `:bytes` | `:instance` |
  | `[:ankusa, :quarantine, :rate_limited]` | — | `:instance`, `:source_id`, `:tenant_id` |
  | `[:ankusa, :quarantine, :full]` | — | `:instance`, `:source_id`, `:tenant_id` |
  | `[:ankusa, :rate_limit, :rejected]` | — | `:instance`, `:tenant_id`, `:source_id` |
  | `[:ankusa, :fleet, :degraded]` | `:degraded` (`1` while the kind is answered from this node alone, else `0`; every second, per kind that is `:fleet`-scoped) | `:instance`, `:kind` (`:rate_limit`, `:dedupe`) |
  | `[:ankusa, :fleet, :breaker]` | — | `:instance`, `:state` (`:open`, `:closed`) |
  | `[:ankusa, :fleet, :fallback]` | — | `:instance`, `:kind`, `:reason` (`:breaker_open`, `:error`). One per hook a fleet call did not answer |
  | `[:ankusa, :fleet, :settle_dropped]` | `:count` | `:instance`, `:reason` (`:queue_full`, `:error`). Dedupe claim outcomes that were not delivered to the fleet store |
  | `[:ankusa, :dispatch, :stop]` | — | `:instance`, `:result`, `:attempts`, `:sink`, `:source_id`, `:tenant_id` |
  | `[:ankusa, :dispatch, :dlq]` | — | `:instance`, `:source_id`, `:tenant_id`, `:sink` |
  | `[:ankusa, :dispatch, :breaker]` | — | `:instance`, `:sink`, `:source_id`, `:tenant_id`, `:state` (`:open`, `:closed`) |
  | `[:ankusa, :dispatch, :state]` | `:running`, `:claimed`, `:claimed_bytes`, `:runnable`, `:breakers_open`, `:active_tenants` (the most tenants one scan visited since the last report) | `:instance` |
  | `[:ankusa, :store, :state]` | `:hooks`, `:deliveries`, `:disk_bytes`, `:next_seq` | `:instance` |
  | `[:ankusa, :store, :write_failed]` | `:count` | `:instance`, `:class` (`:no_space`, `:io_error`, `:read_only`, `:corruption`, `:busy`, `:other`; `Ankusa.Store.failure_class/1`), `:reason` (the raw term; never a label). One per failed store write a process reported, and per failed readiness probe |
  | `[:ankusa, :store, :reopened]` | `:duration` (the close plus the open: the WAL replay), `:unavailable_ms` (how long the store has been closed) | `:instance`, `:result` (`:ok`, `:error`, `:deferred`: put off because the disk still refuses a synced write, nothing closed, both measurements `0`), `:class` (why the open failed or was put off, `nil` on `:ok`). One per reopen attempt, whoever asked for it, including the store's own retries (5 s, doubling to 60 s, jittered) |
  | `[:ankusa, :store, :unavailable]` | `:seconds` (how long the store has been closed; `0` at boot and once open again) | `:instance`, `:class` (why the open failed, `:none` once open) |
  | `[:ankusa, :queue, :state]` | `:pending` (first attempts waiting), `:scheduled` (retries waiting), `:inflight`, `:dead`, `:archive_pending`, `:claim_leases` (claim packs leased to live or just-reclaimed hooks), `:tenants`, `:oldest_due_age_ms` | `:instance` |
  | `[:ankusa, :tenant, :state]` | `:pending` (one event per tenant of the top 100 by pending) | `:instance`, `:tenant` |
  | `[:ankusa, :quarantine, :state]` | `:bytes`, `:entries` | `:instance` |
  | `[:ankusa, :disk, :state]` | `:free_bytes`, `:total_bytes` | `:instance` |
  | `[:ankusa, :sink_http, :state]` | `:origins` (receivers with an open connection pool in `Ankusa.Sink.Http.Pool`) | `:instance` |
  | `[:ankusa, :compact, :stop]` | `:records`, `:bytes`, `:duration` | `:instance` |
  | `[:ankusa, :backup, :stop]` | `:duration`, `:files`, `:bytes` | `:instance`, `:result` (`:ok`, `:error`), `:reason` (on `:error`) |
  | `[:ankusa, :backup, :state]` | `:age_seconds` (since the last successful backup, or since the uploader started) | `:instance` |
  | `[:ankusa, :claim_check, :check_in]` | `:duration`, `:size`, `:claims` | `:instance`, `:tenant_id`, `:pack_id` (the id the pack was written under, after any collision), `:result`, `:store` (index into `claim_check.blob_stores` the pack was written to, `nil` on failure) |
  | `[:ankusa, :claim_check, :pack_collision]` | — | `:instance`, `:tenant_id`, `:pack_id` (an id a claim store already held an object under; `check_in/4` mints a fresh one, up to 3 attempts) |
  | `[:ankusa, :claim_check, :redeem]` | `:duration`, `:size` | `:instance`, `:tenant_id`, `:claim_id`, `:result` |
  | `[:ankusa, :claim_check, :sweep]` | `:deleted` (partition directories removed), `:scanned` (partitions), `:packs` (pack objects deleted), `:kept` (packs a lease held back), `:leases_pruned` (lease rows deleted: grace over, or their hook no longer names the pack), `:backlog` (names the tick did not examine in the partition it stopped in), `:partitions_left` (expired partitions it did not reach), `:duration` | `:instance`, `:deleted` (`true` when the tick removed a partition, a pack or a lease row) |
  | `[:ankusa, :claim_check, :store_breaker]` | — | `:instance`, `:store` (index into `claim_check.blob_stores`), `:state` (`:open`, `:closed`), `:reason` (`nil` on `:closed`) |
  | `[:ankusa, :claim_check, :gateway]` | `:duration` | `:instance`, `:method`, `:status` (one per claim request the gateway answered; `502` is a claim, or its pack's manifest, that does not match the manifest's sha256) |
  | `[:ankusa, :claim_check, :read]` | `:duration` | `:instance`, `:kind` (`:index`, `:manifest`, `:bytes`, `:range`), `:source` (`:cache`, `:store`, `:coalesced`, `:shed`, `:direct`), `:result` (`:ok`, `:not_found`, `:forbidden`, `:integrity_mismatch`, `:throttled`, `:unavailable`, `:overloaded`, `:deadline`), `:store`. One per caller: `:store` is the caller whose fetch went to the store, `:coalesced` one that joined it, `:shed` one answered without a fetch's result (the queue was full, or its deadline passed — a fetch it started still finishes and is counted as `:store`) |
  | `[:ankusa, :archive, :failure]` | — | `:instance`. One per compactor tick that could not write a segment (or read the hooks it was to archive) |
  | `[:ankusa, :archive, :state]` | `:oldest_unarchived_age_ms` (how long the oldest hook still owed an archive segment has waited, by its `received_at`; `0` with none owed) | `:instance`. Every compactor tick that wrote or tried to write a segment, and every one that found nothing owed (a partial segment still waiting out `storage.roll_ms` reports nothing) |
  | `[:ankusa, :claim_check, :read_limit]` | `:limit` | `:instance`, `:store` (every change of a store's adaptive read limit) |
  | `[:ankusa, :claim_check, :read_state]` | `:limit`, `:inflight`, `:queued` | `:instance`, `:store` (every second, per store the gateway's reader has read from) |
  | `[:ankusa, :blob_store, :retry]` | `:delay_ms` (full jitter, or the throttle's `Retry-After` when longer) | `:instance`, `:attempt`, `:status` (an HTTP status or `:transport`) |
  | `[:ankusa, :blob_store, :breaker]` | — | `:instance`, `:endpoint`, `:bucket`, `:state` (`:open`, `:closed`), `:reason` (`{:failures, n}` or `{:slow, p99_ms}` on `:open`, `nil` on `:closed`) |
  | `[:ankusa, :lifecycle, :delivered]` | — | `:instance`, `:type`, `:sink` |
  | `[:ankusa, :lifecycle, :dropped]` | — | `:instance`, `:type`, `:sink`, `:reason` (`:gave_up`, `:unreadable`) |
  | `[:ankusa, :instance, :subtree_down]` | `:delay_ms` | `:instance`, `:domain`, `:reason` |
  | `[:ankusa, :instance, :subtree_up]` | — | `:instance`, `:domain` |
  | `[:ankusa, :replay, :moved]` | `:count` | `:instance`, `:replay_id`, `:kind` |
  | `[:ankusa, :replay, :throttled]` | — | `:instance`, `:replay_id`, `:reason` (`:unavailable`, `:lag`, `:window_full`) |
  | `[:ankusa, :replay, :state]` | — | `:instance`, `:replay_id`, `:kind`, `:state` |
  | `[:ankusa, :routes, :match]` | — | `:instance`, `:route_id`, `:cached`, `:cacheable` |
  | `[:ankusa, :routes, :reject]` | — | `:instance`, `:reason`, `:method`, `:path` |
  | `[:ankusa, :routes, :changed]` | — | `:instance`, `:action`, `:route_id`, `:version` |
  | `[:ankusa, :audit]` | `:count` (1) | `:instance`, `:seq`, `:action`, `:actor`, `:tenant`, `:resource`, `:result` (`"ok"`, `"failed"`, `"interrupted"`). One per row committed to the audit trail |
  | `[:ankusa, :audit, :pruned]` | `:count` | `:instance`. Rows the retention sweep (`audit.retention_days`) deleted |
  | `[:ankusa, :dlq, :evicted]` | `:count`, `:bytes` | `:instance`, `:tenant_id`, `:reason` (`"retention"`, `"max_rows"`, `"max_bytes"`, `"purge"`), `:ids`, `:seqs`. One per eviction batch of one tenant |
  | `[:ankusa, :quarantine, :pruned]` | `:count`, `:bytes` | `:instance`. Entries the retention sweep deleted |
  | `[:ankusa, :archive, :pruned]` | `:segments`, `:objects`, `:legacy` | `:instance`. Catalogue rows (and segment objects) the retention sweep deleted |
  | `[:ankusa, :audit, :state]` | `:rows`, `:bytes`, `:outbox_backlog` | `:instance`. The trail's size and the lifecycle outbox's lag |

  `:outcome` on `:ingest` is one of `:committed`, `:duplicate`, `:quarantined`,
  `:rejected`, `:rate_limited`, `:tenant_overload`, `:quarantine_rate_limited`,
  `:dedupe_pending`, `:quarantine_full`, `:overload` or `:store_unavailable`. `:source_id` on
  `[:ankusa, :ingest]` is
  always a configured source's id: a request for a source that does not exist
  is a refusal, emitted as `[:ankusa, :ingest, :refused]`, never on the span.
  `[:ankusa, :rate_limit, :rejected]` is a tenant over its quota: hooks or
  bytes per second (`:rate_limited`) or its share of the batchers
  (`:tenant_overload`).
  `:status` on `:verify` is `:ok` or `:failed`, independent of what the
  source's `on_verify_failure` policy then decides. `:weak_hash` is `true` when
  the source verifies with SHA-1 (`ankusa_verifier_weak_hash_total`).

  `Ankusa.Metrics` is the built-in Prometheus mapping of these events, served by
  the admin API's `GET /metrics`.
  """

  @doc "Emit a telemetry event."
  @spec emit([atom()], map(), map()) :: :ok
  def emit(event, measurements \\ %{}, meta \\ %{}) do
    :telemetry.execute([:ankusa | event], measurements, meta)
  end

  @doc """
  Run `fun` as a telemetry span, emitting `:start`/`:stop`/`:exception` events
  under `[:ankusa | event]`. Returns whatever `fun` returns.

  `fun` returns `{result, extra_meta}`, or `{result, measurements, extra_meta}`
  when the span has measurements of its own to report on `:stop` — what
  `Ankusa.Queue.Writer` does with a commit's `batch_size` and `bytes`. Either
  return shape merges the start metadata into the stop event, so a handler can
  read `:stop` alone.
  """
  @spec span([atom()], map(), (-> {result, map()} | {result, map(), map()})) :: result
        when result: term()
  def span(event, meta, fun) do
    :telemetry.span([:ankusa | event], meta, fn -> stop_event(meta, fun.()) end)
  end

  defp stop_event(meta, {result, measurements, extra_meta}),
    do: {result, measurements, Map.merge(meta, extra_meta)}

  defp stop_event(meta, {result, extra_meta}), do: {result, Map.merge(meta, extra_meta)}
end
