defmodule Ankusa.Metrics do
  @moduledoc """
  The built-in Prometheus mapping of `Ankusa.Telemetry`'s events, and the
  reporter that serves it.

  Every deployment wants metrics, so this ships in core rather than in an
  adapter package: one `TelemetryMetricsPrometheus.Core` reporter per instance,
  named `:"ankusa_metrics_\#{instance}"` (bounded — one atom per configured
  instance) and scraped by the admin API's `GET /metrics`.

  A reporter only covers the events its own node emits, so an edge node's
  scrape has ingest series and a dispatch node's has delivery series. There is
  no cross-node aggregation here; point Prometheus at every node, or at every
  node's admin port through a proxy.

  ## Unit conversion

  `core`'s Prometheus reporter ignores `:unit`, so durations are converted to
  seconds by the measurement function itself (`duration_seconds/1`) rather than
  exported in `System.monotonic_time/0`'s native units. The `:unit` options
  below therefore describe the value that actually reaches Prometheus.

  ## Bounded labels

  Label values come from telemetry metadata the framework controls, but failure
  reasons are terms (`{:unavailable, {:status, 503, body}}`) and adapters are
  modules. Both would be an unbounded or unusable label, so every tag value goes
  through `normalize/1`: tuples collapse to their leading atom (`:unavailable`),
  modules become strings (`"Ankusa.ClaimCheck.Direct"`), and anything else
  becomes `:other`. `:outcome` on ingest is already one of a fixed set —
  `:committed | :duplicate | :quarantined | :rejected`, or an `{:error, reason}`
  tag — because `Ankusa.Edge.Ingest` tags it before emitting.
  """

  import Telemetry.Metrics

  # 1 ms … 2.5 s, which covers a local WAL commit through a slow upstream sink.
  @seconds_buckets [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5]

  @doc """
  Child spec for this instance's metrics reporter.

  `start_async: false` matters: metrics are registered synchronously so the
  reporter can't miss the events a sibling child emits while starting.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    instance = Keyword.fetch!(opts, :instance)

    Supervisor.child_spec(
      {TelemetryMetricsPrometheus.Core,
       name: reporter_name(instance), metrics: metrics(), start_async: false},
      id: {__MODULE__, instance}
    )
  end

  @doc "Registered name of this instance's reporter."
  @spec reporter_name(atom()) :: atom()
  def reporter_name(instance), do: :"ankusa_metrics_#{instance}"

  @doc "Render the current metrics in Prometheus text exposition format."
  @spec scrape(atom()) :: String.t()
  def scrape(instance), do: TelemetryMetricsPrometheus.Core.scrape(reporter_name(instance))

  @doc "The metric definitions registered for every instance."
  @spec metrics() :: [Telemetry.Metrics.t()]
  def metrics do
    [
      counter("ankusa.ingest.requests.total",
        event_name: [:ankusa, :ingest, :stop],
        tags: [:instance, :source_id, :outcome],
        tag_values: &normalize/1
      ),
      distribution("ankusa.ingest.duration.seconds",
        event_name: [:ankusa, :ingest, :stop],
        measurement: &duration_seconds/1,
        unit: :second,
        tags: [:instance, :source_id],
        tag_values: &normalize/1,
        reporter_options: [buckets: @seconds_buckets]
      ),
      counter("ankusa.verify.failures.total",
        event_name: [:ankusa, :verify, :stop],
        tags: [:instance, :source_id, :provider],
        keep: &(&1.status == :failed),
        tag_values: &normalize/1
      ),
      distribution("ankusa.wal.commit.duration.seconds",
        event_name: [:ankusa, :commit, :stop],
        measurement: &duration_seconds/1,
        unit: :second,
        tags: [:instance],
        tag_values: &normalize/1,
        reporter_options: [buckets: @seconds_buckets]
      ),
      sum("ankusa.wal.commit.batch.size",
        event_name: [:ankusa, :commit, :stop],
        measurement: :batch_size,
        tags: [:instance],
        tag_values: &normalize/1
      ),
      counter("ankusa.dedup.hits.total",
        event_name: [:ankusa, :dedup, :hit],
        tags: [:instance, :source_id],
        tag_values: &normalize/1
      ),
      counter("ankusa.load_shed.total",
        event_name: [:ankusa, :load_shed],
        tags: [:instance],
        tag_values: &normalize/1
      ),
      counter("ankusa.quarantine.rate_limited.total",
        event_name: [:ankusa, :quarantine, :rate_limited],
        tags: [:instance, :source_id],
        tag_values: &normalize/1
      ),
      counter("ankusa.dispatch.deliveries.total",
        event_name: [:ankusa, :dispatch, :stop],
        tags: [:result],
        tag_values: &normalize/1
      ),
      counter("ankusa.dispatch.dead_lettered.total",
        event_name: [:ankusa, :dispatch, :dlq],
        tags: [:source_id, :sink],
        tag_values: &normalize/1
      ),
      sum("ankusa.compact.records.total",
        event_name: [:ankusa, :compact, :stop],
        measurement: :records,
        tags: [:instance],
        tag_values: &normalize/1
      ),
      sum("ankusa.compact.bytes.total",
        event_name: [:ankusa, :compact, :stop],
        measurement: :bytes,
        unit: :byte,
        tags: [:instance],
        tag_values: &normalize/1
      ),
      counter("ankusa.claim_check.operations.total",
        event_name: [:ankusa, :claim_check, :check_in],
        tags: [:adapter, :result],
        tag_values: &normalize/1
      ),
      counter("ankusa.claim_check.redeems.total",
        event_name: [:ankusa, :claim_check, :redeem],
        tags: [:adapter, :result],
        tag_values: &normalize/1
      )
    ]
  end

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
  defp normalize_value(_other), do: :other

  defp duration_seconds(%{duration: native}) when is_integer(native) do
    System.convert_time_unit(native, :native, :microsecond) / 1_000_000
  end
end
