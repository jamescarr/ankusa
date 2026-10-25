defmodule Ankusa.Metrics do
  @moduledoc """
  The built-in Prometheus mapping of `Ankusa.Telemetry`'s events, and the
  reporter that serves it.

  Every deployment wants metrics, so this ships in core rather than in an
  adapter package: one `TelemetryMetricsPrometheus.Core` reporter per instance,
  named `:"ankusa_metrics_\#{instance}"` (bounded — one atom per configured
  instance) and scraped by the admin API's `GET /metrics`, or by the metrics
  listener's (`Ankusa.Metrics.Router`, `metrics.enabled`), which serves nothing
  else.

  `:telemetry` handlers are global to the VM, so a reporter sees only its own
  instance: every definition keeps just the events whose `:instance` metadata
  is the reporter's, and two instances in one VM never count each other's
  traffic. A reporter also covers only the events its own node emits, so an
  edge node's scrape has ingest series and a dispatch node's has delivery
  series. There is no cross-node aggregation here; point Prometheus at every
  node, or at every node's admin port through a proxy.

  ## Unit conversion

  `core`'s Prometheus reporter ignores `:unit`, so durations are converted to
  seconds by the measurement function itself (`duration_seconds/1`) rather than
  exported in `System.monotonic_time/0`'s native units. The `:unit` options
  below therefore describe the value that actually reaches Prometheus.

  ## Cardinality policy

  A scrape must not grow with the number of tenants or sources: ten thousand
  tenants each sending one hook must not be
  ten thousand series. `metrics.detail` decides which identity labels a
  per-hook metric carries, and `metrics.max_series_per_metric` caps what is kept
  of them.

    * `:tenant` (the default): `tenant_id` on the ingest, verify-failure,
      quarantine, rate-limit and delivery counters, and the two per-tenant
      gauges. Duration histograms carry no identity.
    * `:source`: `tenant_id` and `source_id` on all of those, and on the ingest
      duration histogram.
    * `:none`: neither label, and no per-tenant gauges: the series set is the
      same for one tenant and for a million.

  Under `:tenant` and `:source`, a metric keeps its first
  `max_series_per_metric - 1` label sets that carry an identity; every later one
  is reported with `tenant_id`/`source_id`/`tenant` set to `"other"` (see
  `Ankusa.Metrics.Cardinality`). An identity is never an unauthenticated
  value: `tenant_id` and `source_id` are only set after the source has been
  looked up, and a request for an unknown one is a refusal
  (`ankusa.ingest.refused.total`) that carries neither.

  The gauges that sample state (`ankusa.tenant.pending`,
  `ankusa.dispatch.tenant.inflight`) report the top 100 tenants and, past the
  ceiling, drop the tenants that were not admitted rather than overwrite a
  shared `"other"` gauge with one tenant's value.

  ## Bounded labels

  Every other label value comes from telemetry metadata the framework controls,
  but failure reasons are terms (`{:unavailable, {:status, 503, body}}`) and
  some tags are modules. Both would be an unbounded or unusable label, so every
  tag value goes through `normalize/1`: tuples collapse to their leading atom
  (`:unavailable`), modules become strings (`"Ankusa.Sink.Http"`), integers are
  rendered as numbers (only ever a store index or an HTTP status, both
  bounded), and anything else becomes `:other`. `:outcome` on ingest is already
  one of a fixed set (`:committed`, `:duplicate`, `:quarantined`, `:rejected`,
  `:rate_limited`, `:tenant_overload`, `:quarantine_rate_limited`,
  `:dedupe_pending`, `:quarantine_full`, `:overload`, `:tenant_limit`,
  `:dedupe_quota`, `:tenant_erasing`, `:store_unavailable`, and under `wal: :dual`
  `:intake_unavailable` or `:intake_pending`) because `Ankusa.Edge.Ingest` tags it before emitting.
  A request for a source that does not exist is counted on
  `ankusa.ingest.refused.total`, whose `:reason` is one of `:unknown_source`,
  `:payload_too_large`, `:body_read_failed`, `:invalid_header`,
  `:store_unavailable`, `:ip_rate_limited`, `:node_rate_limited` or
  `:source_rate_limited`, never on a series that carries the id from the URL.
  """

  import Telemetry.Metrics

  alias Ankusa.Metrics.Cardinality

  # 1 ms … 2.5 s, which covers a local WAL commit through a slow upstream sink.
  @seconds_buckets [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5]
  # The claim gateway answers at up to `claim_check.read_deadline_ms` (5 s by
  # default): the tail this histogram exists for sits past 2.5 s.
  @gateway_seconds_buckets @seconds_buckets ++ [5, 10]
  # A reopen replays the WAL: 10 ms for an idle store, up to a minute for one
  # that crashed with 512 MiB of log to apply.
  @replay_seconds_buckets [0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60]

  @doc """
  Child spec for this instance's metrics: a supervisor over the cardinality
  policy's table (`Ankusa.Metrics.Cardinality`) and the reporter.

  `start_async: false` registers the metrics before the reporter's
  `start_link/1` returns, and `Ankusa.Instance` starts this child before any
  other, so no sibling's events are missed. The two children restart together:
  the reporter keeps every series it has seen, and the policy must keep the
  record of which it admitted.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    instance = Keyword.fetch!(opts, :instance)
    %{id: {__MODULE__, instance}, start: {__MODULE__, :start_link, [instance]}, type: :supervisor}
  end

  @doc false
  @spec start_link(atom()) :: Supervisor.on_start()
  def start_link(instance) do
    Supervisor.start_link(
      [
        {Cardinality, instance: instance},
        {TelemetryMetricsPrometheus.Core,
         name: reporter_name(instance), metrics: metrics(instance), start_async: false}
      ],
      strategy: :one_for_all
    )
  end

  @doc "Registered name of this instance's reporter."
  @spec reporter_name(atom()) :: atom()
  # The instance name is operator config: one atom per instance, never request input.
  # sobelow_skip ["DOS.BinToAtom"]
  def reporter_name(instance), do: :"ankusa_metrics_#{instance}"

  @doc "Render the current metrics in Prometheus text exposition format."
  @spec scrape(atom()) :: String.t()
  def scrape(instance), do: TelemetryMetricsPrometheus.Core.scrape(reporter_name(instance))

  @doc """
  The metric definitions for `instance`'s reporter. Each keeps only events whose
  `:instance` metadata is `instance`.
  """
  @spec metrics(atom()) :: [Telemetry.Metrics.t()]
  def metrics(instance) do
    own = &(&1[:instance] == instance)
    policy = Ankusa.config(instance).metrics
    ids = identity_tags(policy.detail)
    # A histogram is a dozen series per label set, so it carries an identity
    # only when the operator asked for the most detail.
    histogram_ids = if policy.detail == :source, do: ids, else: []

    definitions = [
      counter(
        "ankusa.ingest.requests.total",
        scoped(own,
          event_name: [:ankusa, :ingest, :stop],
          tags: [:instance] ++ ids ++ [:outcome]
        )
      ),
      distribution(
        "ankusa.ingest.duration.seconds",
        scoped(own,
          event_name: [:ankusa, :ingest, :stop],
          measurement: &duration_seconds/1,
          unit: :second,
          tags: [:instance] ++ histogram_ids,
          reporter_options: [buckets: @seconds_buckets]
        )
      ),
      counter(
        "ankusa.ingest.refused.total",
        scoped(own, event_name: [:ankusa, :ingest, :refused], tags: [:instance, :reason])
      ),
      counter(
        "ankusa.verify.failures.total",
        scoped(own,
          event_name: [:ankusa, :verify, :stop],
          keep: &(&1.status == :failed),
          tags: [:instance] ++ ids ++ [:provider, :scheme]
        )
      ),
      counter(
        "ankusa.verifier.weak_hash.total",
        scoped(own,
          event_name: [:ankusa, :verify, :stop],
          keep: &(&1[:weak_hash] == true),
          tags: [:instance] ++ ids ++ [:scheme]
        )
      ),
      distribution(
        "ankusa.wal.commit.duration.seconds",
        scoped(own,
          event_name: [:ankusa, :commit, :stop],
          measurement: &duration_seconds/1,
          unit: :second,
          tags: [:instance],
          reporter_options: [buckets: @seconds_buckets]
        )
      ),
      sum(
        "ankusa.wal.commit.batch.size",
        scoped(own,
          event_name: [:ankusa, :commit, :stop],
          measurement: :batch_size,
          tags: [:instance]
        )
      ),
      distribution(
        "ankusa.wal.sync.duration.seconds",
        scoped(own,
          event_name: [:ankusa, :wal_sync, :stop],
          measurement: &duration_seconds/1,
          unit: :second,
          tags: [:instance],
          reporter_options: [buckets: @seconds_buckets]
        )
      ),
      counter(
        "ankusa.load_shed.total",
        scoped(own,
          event_name: [:ankusa, :load_shed],
          tags: [:instance]
        )
      ),
      counter(
        "ankusa.quarantine.rate_limited.total",
        scoped(own,
          event_name: [:ankusa, :quarantine, :rate_limited],
          tags: [:instance | ids]
        )
      ),
      counter(
        "ankusa.quarantine.full.total",
        scoped(own,
          event_name: [:ankusa, :quarantine, :full],
          tags: [:instance | ids]
        )
      ),
      counter(
        "ankusa.rate_limit.rejected.total",
        scoped(own,
          event_name: [:ankusa, :rate_limit, :rejected],
          tags: [:instance | ids]
        )
      ),
      last_value(
        "ankusa.fleet.limiter.degraded",
        scoped(own,
          event_name: [:ankusa, :fleet, :degraded],
          measurement: :degraded,
          unit: :unit,
          tags: [:instance, :kind]
        )
      ),
      counter(
        "ankusa.fleet.breaker.transitions.total",
        scoped(own, event_name: [:ankusa, :fleet, :breaker], tags: [:instance, :state])
      ),
      counter(
        "ankusa.fleet.fallback.total",
        scoped(own, event_name: [:ankusa, :fleet, :fallback], tags: [:instance, :kind, :reason])
      ),
      sum(
        "ankusa.fleet.settle.dropped.total",
        scoped(own,
          event_name: [:ankusa, :fleet, :settle_dropped],
          measurement: :count,
          tags: [:instance, :reason]
        )
      ),
      sum(
        "ankusa.dedupe.pruned.total",
        scoped(own,
          event_name: [:ankusa, :dedupe, :pruned],
          measurement: :count,
          tags: [:instance]
        )
      ),
      sum(
        "ankusa.dlq.evicted.total",
        scoped(own,
          event_name: [:ankusa, :dlq, :evicted],
          measurement: :count,
          tags: [:instance] ++ ids ++ [:reason]
        )
      ),
      sum(
        "ankusa.quarantine.pruned.total",
        scoped(own,
          event_name: [:ankusa, :quarantine, :pruned],
          measurement: :count,
          tags: [:instance]
        )
      ),
      sum(
        "ankusa.archive.catalogue_pruned.total",
        scoped(own,
          event_name: [:ankusa, :archive, :pruned],
          measurement: :segments,
          tags: [:instance]
        )
      ),
      sum(
        "ankusa.audit.pruned.total",
        scoped(own,
          event_name: [:ankusa, :audit, :pruned],
          measurement: :count,
          tags: [:instance]
        )
      ),
      sum(
        "ankusa.claim_check.pruned.total",
        scoped(own,
          event_name: [:ankusa, :claim_check, :sweep],
          measurement: :packs,
          tags: [:instance]
        )
      ),
      sum(
        "ankusa.claim_check.leases_pruned.total",
        scoped(own,
          event_name: [:ankusa, :claim_check, :sweep],
          measurement: :leases_pruned,
          tags: [:instance]
        )
      ),
      counter(
        "ankusa.dispatch.deliveries.total",
        scoped(own,
          event_name: [:ankusa, :dispatch, :stop],
          tags: [:instance] ++ ids ++ [:result, :sink]
        )
      ),
      sum(
        "ankusa.sink.headers_dropped.total",
        scoped(own,
          event_name: [:ankusa, :sink, :headers_dropped],
          measurement: :count,
          tags: [:instance] ++ ids ++ [:reason]
        )
      ),
      counter(
        "ankusa.dispatch.breaker.transitions.total",
        scoped(own,
          event_name: [:ankusa, :dispatch, :breaker],
          tags: [:instance, :sink, :state]
        )
      ),
      sum(
        "ankusa.http_pool.closed.total",
        scoped(own,
          event_name: [:ankusa, :http_pool, :closed],
          measurement: :count,
          tags: [:instance, :reason]
        )
      ),
      sum(
        "ankusa.http_pool.refused.total",
        scoped(own,
          event_name: [:ankusa, :http_pool, :refused],
          measurement: :count,
          tags: [:instance, :scope]
        )
      ),
      counter(
        "ankusa.dispatch.lane_key_missing.total",
        scoped(own,
          event_name: [:ankusa, :dispatch, :lane_key_missing],
          tags: [:instance, :sink]
        )
      ),
      counter(
        "ankusa.dispatch.dead_lettered.total",
        scoped(own,
          event_name: [:ankusa, :dispatch, :dlq],
          tags: [:instance] ++ ids ++ [:sink]
        )
      ),
      counter(
        "ankusa.dispatch.config_errors.total",
        scoped(own,
          event_name: [:ankusa, :dispatch, :config_error],
          tags: [:instance, :sink]
        )
      ),
      sum(
        "ankusa.replay.moved.total",
        scoped(own,
          event_name: [:ankusa, :replay, :moved],
          measurement: :count,
          tags: [:instance, :kind]
        )
      ),
      counter(
        "ankusa.replay.throttled.total",
        scoped(own,
          event_name: [:ankusa, :replay, :throttled],
          tags: [:instance, :reason]
        )
      ),
      sum(
        "ankusa.compact.records.total",
        scoped(own,
          event_name: [:ankusa, :compact, :stop],
          measurement: :records,
          tags: [:instance]
        )
      ),
      sum(
        "ankusa.compact.bytes.total",
        scoped(own,
          event_name: [:ankusa, :compact, :stop],
          measurement: :bytes,
          unit: :byte,
          tags: [:instance]
        )
      ),
      counter(
        "ankusa.claim_check.operations.total",
        scoped(own,
          event_name: [:ankusa, :claim_check, :check_in],
          tags: [:instance, :result]
        )
      ),
      counter(
        "ankusa.claim_check.redeems.total",
        scoped(own,
          event_name: [:ankusa, :claim_check, :redeem],
          tags: [:instance, :result]
        )
      ),
      counter(
        "ankusa.claim_check.store_breaker.transitions.total",
        scoped(own,
          event_name: [:ankusa, :claim_check, :store_breaker],
          tags: [:instance, :store, :state]
        )
      ),
      # The gateway's answers by status only: a status is one of a fixed set,
      # and nothing the caller sends (tenant, claim, method) is a label.
      counter(
        "ankusa.claim.gateway.requests.total",
        scoped(own,
          event_name: [:ankusa, :claim_check, :gateway],
          tags: [:instance, :status]
        )
      ),
      distribution(
        "ankusa.claim.gateway.duration.seconds",
        scoped(own,
          event_name: [:ankusa, :claim_check, :gateway],
          measurement: &duration_seconds/1,
          unit: :second,
          tags: [:instance],
          reporter_options: [buckets: @gateway_seconds_buckets]
        )
      ),
      counter(
        "ankusa.claim.sweeps.total",
        scoped(own,
          event_name: [:ankusa, :claim_check, :sweep],
          tags: [:instance, :deleted]
        )
      ),
      counter(
        "ankusa.archive.failures.total",
        scoped(own, event_name: [:ankusa, :archive, :failure], tags: [:instance])
      ),
      counter(
        "ankusa.claim_check.reads.total",
        scoped(own,
          event_name: [:ankusa, :claim_check, :read],
          tags: [:instance, :kind, :source, :result]
        )
      ),
      counter(
        "ankusa.blob_store.retries.total",
        scoped(own,
          event_name: [:ankusa, :blob_store, :retry],
          tags: [:instance, :status]
        )
      ),
      counter(
        "ankusa.lifecycle.delivered.total",
        scoped(own,
          event_name: [:ankusa, :lifecycle, :delivered],
          tags: [:instance, :type, :sink]
        )
      ),
      counter(
        "ankusa.lifecycle.dropped.total",
        scoped(own,
          event_name: [:ankusa, :lifecycle, :dropped],
          tags: [:instance, :type, :reason]
        )
      ),
      counter(
        "ankusa.backup.runs.total",
        scoped(own, event_name: [:ankusa, :backup, :stop], tags: [:instance, :result])
      ),
      counter(
        "ankusa.backup.verify.failed.total",
        scoped(own,
          event_name: [:ankusa, :backup, :verify],
          keep: &(&1.result == :error),
          tags: [:instance, :target]
        )
      ),
      counter(
        "ankusa.store.write_failures.total",
        scoped(own, event_name: [:ankusa, :store, :write_failed], tags: [:instance, :class])
      ),
      counter(
        "ankusa.store.reopen.total",
        scoped(own, event_name: [:ankusa, :store, :reopened], tags: [:instance, :result])
      ),
      last_value(
        "ankusa.store.unavailable.seconds",
        scoped(own,
          event_name: [:ankusa, :store, :unavailable],
          measurement: :seconds,
          unit: :second,
          tags: [:instance]
        )
      ),
      distribution(
        "ankusa.store.wal_replay.seconds",
        scoped(own,
          event_name: [:ankusa, :store, :reopened],
          keep: &(&1.result == :ok),
          measurement: &duration_seconds/1,
          unit: :second,
          tags: [:instance],
          reporter_options: [buckets: @replay_seconds_buckets]
        )
      )
    ]

    (definitions ++
       gauges(own) ++
       intake_metrics(own) ++ read_gauges(own) ++ tenant_gauges(own, policy.detail))
    |> Enum.map(&police(&1, instance, policy))
  end

  # `wal: :dual` (ADR 0002): publish latency by outcome, failures by class, what
  # the reconciler completed and refused, replay, and the state gauges
  # `Ankusa.Metrics.Gauges` samples. Every label is a fixed set; no series is
  # per tenant. No event, no series: the set is empty under `disk` and `none`.
  defp intake_metrics(own) do
    [
      distribution(
        "ankusa.intake.publish.duration.seconds",
        scoped(own,
          event_name: [:ankusa, :intake, :publish],
          measurement: &duration_seconds/1,
          unit: :second,
          tags: [:instance, :outcome],
          reporter_options: [buckets: @seconds_buckets]
        )
      ),
      counter(
        "ankusa.intake.failures.total",
        scoped(own, event_name: [:ankusa, :intake, :failure], tags: [:instance, :class])
      ),
      sum(
        "ankusa.intake.reconciled.total",
        scoped(own,
          event_name: [:ankusa, :intake, :reconciled],
          measurement: :count,
          tags: [:instance, :half]
        )
      ),
      sum(
        "ankusa.intake.refused.total",
        scoped(own,
          event_name: [:ankusa, :intake, :refused],
          measurement: :count,
          tags: [:instance, :reason]
        )
      ),
      sum(
        "ankusa.intake.replay.total",
        scoped(own,
          event_name: [:ankusa, :intake, :replay],
          measurement: :count,
          tags: [:instance, :result]
        )
      ),
      distribution(
        "ankusa.intake.replay.duration.seconds",
        scoped(own,
          event_name: [:ankusa, :intake, :replay_done],
          measurement: &duration_seconds/1,
          unit: :second,
          tags: [:instance],
          reporter_options: [buckets: @seconds_buckets]
        )
      )
    ] ++
      for {name, measurement, unit} <- [
            {"ankusa.intake.pending.records", :pending_records, :unit},
            {"ankusa.intake.pending.bytes", :pending_bytes, :byte},
            {"ankusa.intake.reconcile.oldest.age.seconds", :reconcile_oldest_age_seconds,
             :second},
            {"ankusa.intake.reconcile.stuck", :reconcile_stuck, :unit},
            {"ankusa.intake.offset.lag.seconds", :offset_lag_seconds, :second},
            {"ankusa.intake.ready", :ready, :unit}
          ] do
        last_value(
          name,
          scoped(own,
            event_name: [:ankusa, :intake, :state],
            measurement: measurement,
            unit: unit,
            tags: [:instance]
          )
        )
      end
  end

  # State, not events: `Ankusa.Metrics.Gauges` samples the store, the queue
  # index, the pen and the disk every `admin.gauge_interval_ms`, and dispatch
  # reports its scheduler on every housekeeping tick. A gauge whose probe fails
  # keeps its last value (the probe logs at `:debug`).
  defp gauges(own) do
    for {name, event, measurement, unit} <- [
          {"ankusa.store.hooks", [:ankusa, :store, :state], :hooks, :unit},
          {"ankusa.store.deliveries", [:ankusa, :store, :state], :deliveries, :unit},
          {"ankusa.store.disk.bytes", [:ankusa, :store, :state], :disk_bytes, :byte},
          {"ankusa.queue.pending", [:ankusa, :queue, :state], :pending, :unit},
          {"ankusa.queue.scheduled", [:ankusa, :queue, :state], :scheduled, :unit},
          {"ankusa.queue.inflight", [:ankusa, :queue, :state], :inflight, :unit},
          {"ankusa.queue.dead", [:ankusa, :queue, :state], :dead, :unit},
          {"ankusa.queue.archive_pending", [:ankusa, :queue, :state], :archive_pending, :unit},
          {"ankusa.archive.oldest_unarchived_age.seconds", [:ankusa, :archive, :state],
           &oldest_unarchived_age_seconds/1, :second},
          {"ankusa.dedupe.keys", [:ankusa, :queue, :state], :dedupe, :unit},
          {"ankusa.dlq.rows", [:ankusa, :queue, :state], :dead, :unit},
          {"ankusa.dlq.bytes", [:ankusa, :queue, :state], :dead_bytes, :byte},
          {"ankusa.dlq.full", [:ankusa, :queue, :state], :dlq_full, :unit},
          {"ankusa.claim.leases", [:ankusa, :queue, :state], :claim_leases, :unit},
          {"ankusa.dispatch.lanes", [:ankusa, :queue, :state], :lanes, :unit},
          {"ankusa.dispatch.lanes_blocked", [:ankusa, :queue, :state], :lanes_blocked, :unit},
          {"ankusa.claim_check.sweep.backlog", [:ankusa, :claim_check, :sweep], :backlog, :unit},
          {"ankusa.claim_check.sweep.partitions_left", [:ankusa, :claim_check, :sweep],
           :partitions_left, :unit},
          {"ankusa.claim_check.sweep.tenants_left", [:ankusa, :claim_check, :sweep],
           :tenants_left, :unit},
          {"ankusa.audit.rows", [:ankusa, :audit, :state], :rows, :unit},
          {"ankusa.audit.bytes", [:ankusa, :audit, :state], :bytes, :byte},
          {"ankusa.audit.outbox_backlog", [:ankusa, :audit, :state], :outbox_backlog, :unit},
          {"ankusa.audit.retention.blocked_by_cursor", [:ankusa, :audit, :retention],
           :blocked_by_cursor, :unit},
          {"ankusa.tenants", [:ankusa, :queue, :state], :tenants, :unit},
          {"ankusa.queue.oldest_due_age.seconds", [:ankusa, :queue, :state],
           &oldest_due_age_seconds/1, :second},
          {"ankusa.quarantine.bytes", [:ankusa, :quarantine, :state], :bytes, :byte},
          {"ankusa.quarantine.entries", [:ankusa, :quarantine, :state], :entries, :unit},
          {"ankusa.quarantine.pending", [:ankusa, :quarantine, :state], :pending, :unit},
          {"ankusa.disk.free.bytes", [:ankusa, :disk, :state], :free_bytes, :byte},
          {"ankusa.disk.total.bytes", [:ankusa, :disk, :state], :total_bytes, :byte},
          {"ankusa.dispatch.running", [:ankusa, :dispatch, :state], :running, :unit},
          {"ankusa.dispatch.claimed", [:ankusa, :dispatch, :state], :claimed, :unit},
          {"ankusa.dispatch.claimed.bytes", [:ankusa, :dispatch, :state], :claimed_bytes, :byte},
          {"ankusa.dispatch.runnable", [:ankusa, :dispatch, :state], :runnable, :unit},
          {"ankusa.dispatch.breakers_open", [:ankusa, :dispatch, :state], :breakers_open, :unit},
          {"ankusa.http.pools", [:ankusa, :http, :state], :pools, :unit},
          {"ankusa.dispatch.active_tenants", [:ankusa, :dispatch, :state], :active_tenants,
           :unit},
          {"ankusa.backup.age.seconds", [:ankusa, :backup, :state], :age_seconds, :second}
        ] do
      last_value(
        name,
        scoped(own, event_name: event, measurement: measurement, unit: unit, tags: [:instance])
      )
    end
  end

  # The tenants with the most first-attempt rows waiting (`Ankusa.Metrics.Gauges`
  # reports the top 100 from the queue counters), and the 100 holding the most
  # claimed, unfinished deliveries (`Ankusa.Dispatch.Pipeline` reports them from
  # its tenant windows, once per housekeeping tick). A tenant that leaves a top
  # is reported once as 0. Every tenant that ever entered one keeps a series, up
  # to the cardinality policy's ceiling (`metrics.max_series_per_metric`); with
  # `metrics.detail: :none` there are no per-tenant gauges.
  defp tenant_gauges(_own, :none), do: []

  defp tenant_gauges(own, _detail) do
    [
      last_value(
        "ankusa.tenant.pending",
        scoped(own,
          event_name: [:ankusa, :tenant, :state],
          measurement: :pending,
          unit: :unit,
          tags: [:instance, :tenant]
        )
      ),
      last_value(
        "ankusa.dispatch.tenant.inflight",
        scoped(own,
          event_name: [:ankusa, :dispatch, :tenant_state],
          measurement: :inflight,
          unit: :unit,
          tags: [:instance, :tenant]
        )
      )
    ]
  end

  # The gateway reader's per-store state, every second. Tagged by store too,
  # which `gauges/1` (tags `[:instance]` only) can't do.
  defp read_gauges(own) do
    for measurement <- [:limit, :inflight, :queued] do
      last_value(
        "ankusa.claim_check.read.#{measurement}",
        scoped(own,
          event_name: [:ankusa, :claim_check, :read_state],
          measurement: measurement,
          unit: :unit,
          tags: [:instance, :store]
        )
      )
    end
  end

  # What every definition shares: only this instance's events (ANDed with a
  # metric's own `:keep`), and bounded label values.
  defp scoped(own, opts) do
    keep =
      case opts[:keep] do
        nil -> own
        keep -> &(own.(&1) and keep.(&1))
      end

    Keyword.merge(opts, keep: keep, tag_values: &normalize/1)
  end

  # The cardinality policy for one definition. A metric with no identity label
  # is returned as it is. One with an identity folds the label sets past the
  # ceiling into `"other"` (`Cardinality.fold/5`), except a gauge, which drops
  # them: `"other"` would hold whichever tenant reported last.
  defp police(metric, instance, policy) do
    %{name: name, tags: tags} = metric
    table = Cardinality.table(instance)
    cap = policy.max_series_per_metric

    cond do
      not Cardinality.policed?(tags) ->
        metric

      is_struct(metric, Telemetry.Metrics.LastValue) ->
        keep = metric.keep
        admitted? = &(Cardinality.admit(table, name, tags, cap, normalize(&1)) == :admitted)
        %{metric | keep: &(keep.(&1) and admitted?.(&1))}

      true ->
        %{metric | tag_values: &Cardinality.fold(table, name, tags, cap, normalize(&1))}
    end
  end

  # The identity labels a per-hook metric carries at `detail`.
  defp identity_tags(:none), do: []
  defp identity_tags(:tenant), do: [:tenant_id]
  defp identity_tags(:source), do: [:tenant_id, :source_id]

  # `Telemetry.Metrics` calls this with the whole metadata map.
  defp normalize(meta), do: Map.new(meta, fn {k, v} -> {k, normalize_value(v)} end)

  defp normalize_value(v) when is_binary(v), do: v

  defp normalize_value(v) when is_atom(v) do
    case Atom.to_string(v) do
      "Elixir." <> _ -> inspect(v)
      _ -> v
    end
  end

  defp normalize_value({tag, _rest}), do: tag
  defp normalize_value(v) when is_integer(v), do: Integer.to_string(v)
  defp normalize_value(_other), do: :other

  defp oldest_due_age_seconds(%{oldest_due_age_ms: ms}), do: ms / 1000
  defp oldest_unarchived_age_seconds(%{oldest_unarchived_age_ms: ms}), do: ms / 1000

  defp duration_seconds(%{duration: native}) when is_integer(native) do
    System.convert_time_unit(native, :native, :microsecond) / 1_000_000
  end
end
