defmodule Ankusa.Telemetry do
  @moduledoc """
  The cross-component contract. Components emit `:telemetry` events; they never
  call each other's reporters.

  Event prefix is `[:ankusa, ...]`. Core emits two shapes, and the distinction
  is deliberate:

    * **span** (`span/3`) — emits `:start`, `:stop`, and `:exception`, with
      `:duration` in native units on `:stop`. Used where a crash is itself worth
      an event: the request-path stages and the WAL commit.
    * **single event** (`emit/3`) — for background ticks that time themselves.

  | Event | Measurements | Metadata |
  | --- | --- | --- |
  | `[:ankusa, :ingest]` (span) | `:duration` | `:instance`, `:source_id`, `:size`, `:outcome` |
  | `[:ankusa, :verify]` (span) | `:duration` | `:instance`, `:source_id`, `:provider`, `:status` |
  | `[:ankusa, :commit]` (span) | `:duration`, `:batch_size`, `:bytes` | `:instance` |
  | `[:ankusa, :dedup, :hit]` | — | `:instance`, `:source_id` |
  | `[:ankusa, :load_shed]` | `:queue` | `:instance` |
  | `[:ankusa, :quarantine, :rate_limited]` | — | `:instance`, `:source_id` |
  | `[:ankusa, :dispatch, :stop]` | — | `:result`, `:attempts` |
  | `[:ankusa, :dispatch, :dlq]` | — | `:source_id`, `:sink` |
  | `[:ankusa, :compact, :stop]` | `:records`, `:bytes`, `:duration` | `:instance` |
  | `[:ankusa, :claim_check, :check_in]` | `:duration`, `:size` | `:tenant_id`, `:adapter`, `:result` |
  | `[:ankusa, :claim_check, :redeem]` | `:duration`, `:size` | `:tenant_id`, `:id`, `:adapter`, `:result` |
  | `[:ankusa, :claim_check, :sweep]` | `:deleted`, `:scanned`, `:duration` | `:instance` |

  `:outcome` on `:ingest` is `:committed | :duplicate | :quarantined | :rejected`,
  or the `{:error, reason}` tag. `:status` on `:verify` is `:ok` or `:failed`,
  independent of what the source's `on_verify_failure` policy then decides.

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
  `Ankusa.WAL.DiskLog` does with a commit's `batch_size` and `bytes`. Either
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
