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
  | `[:ankusa, :commit]` (span) | `:duration` | `:instance`, `:batch_size`, `:bytes` |
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
  """

  @doc "Emit a telemetry event."
  @spec emit([atom()], map(), map()) :: :ok
  def emit(event, measurements \\ %{}, meta \\ %{}) do
    :telemetry.execute([:ankusa | event], measurements, meta)
  end

  @doc """
  Run `fun` as a telemetry span, emitting `:start`/`:stop`/`:exception` events
  under `[:ankusa | event]`. Returns whatever `fun` returns.
  """
  @spec span([atom()], map(), (-> {result, map()})) :: result when result: term()
  def span(event, meta, fun) do
    :telemetry.span([:ankusa | event], meta, fn ->
      {result, extra} = fun.()
      {result, Map.merge(meta, extra)}
    end)
  end
end
