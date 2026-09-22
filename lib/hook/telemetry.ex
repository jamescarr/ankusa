defmodule Hook.Telemetry do
  @moduledoc """
  The cross-component contract. Components emit `:telemetry` events; they never
  call each other's reporters.

  Event prefix is `[:hook, ...]`. Notable events:

    * `[:hook, :ingest, :stop]`   — measurements: `%{duration, size}`
    * `[:hook, :commit, :stop]`   — measurements: `%{duration, batch_size, bytes}`
    * `[:hook, :verify, :stop]`   — meta: `%{status, provider, source_id}`
    * `[:hook, :dedup, :hit]`     — a duplicate was absorbed
    * `[:hook, :load_shed]`       — the batcher queue was full; 503 returned
    * `[:hook, :dispatch, :stop]` — meta: `%{result, attempts}`
    * `[:hook, :dispatch, :dlq]`  — a hook was dead-lettered
    * `[:hook, :compact, :stop]`  — measurements: `%{records, bytes, duration}`
  """

  @doc "Emit a telemetry event."
  @spec emit([atom()], map(), map()) :: :ok
  def emit(event, measurements \\ %{}, meta \\ %{}) do
    :telemetry.execute([:hook | event], measurements, meta)
  end

  @doc """
  Run `fun` as a telemetry span, emitting `:start`/`:stop`/`:exception` events
  under `[:hook | event]`. Returns whatever `fun` returns.
  """
  @spec span([atom()], map(), (-> {result, map()})) :: result when result: term()
  def span(event, meta, fun) do
    :telemetry.span([:hook | event], meta, fn ->
      {result, extra} = fun.()
      {result, Map.merge(meta, extra)}
    end)
  end
end
