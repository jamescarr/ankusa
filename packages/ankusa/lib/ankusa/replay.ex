defmodule Ankusa.Replay do
  @moduledoc """
  Operator API for replay jobs (`Ankusa.Dispatch.Replayer`).

  A replay re-sends dead delivery rows (`kind: :dlq`) or archived hooks over a
  time window (`kind: :archive`) through the normal dispatch pipeline, keeping
  each hook's original `id` and `dedupe_key` and stamping the delivery with the
  job's `replay_id`. It only ever uses dispatch capacity live traffic leaves
  free: rows are dripped in at `rate` items per second, and only while the
  Pipeline's oldest-due lag is at most `max_lag_ms` and its in-flight window is
  not full.

  Jobs are durable and resumable: every job is a store record, its cursor is
  committed in the same batch as the rows it moved, and a restart resumes from
  the last committed page. A job that keeps dead-lettering its deliveries
  pauses itself (auto-pause) instead of feeding a sink that is still down.

  ## Runbook for a Salesforce-sized incident

    1. One job per node — a fleet replay is one `POST /v1/replays` per node.
    2. Start low: `rate: 100` or so, and watch the node's metrics.
    3. PATCH `rate` upward while live latency stays flat. The replay takes only
       spare capacity, so it is always safe to leave running; the only knob is
       how long it takes.
    4. A `kind: :dlq` job only touches rows dead-lettered at or before its own
       creation, so rows that die again during the replay are never picked up a
       second time. To re-send those, start another job.
    5. A `kind: :archive` job covers only what the compactor has already
       archived: `to` must be at least `storage.roll_ms + storage.interval_ms`
       in the past.
  """

  @default_rate 1_000
  @default_max_lag_ms 2_000
  @min_rate 1
  @max_rate 100_000
  @min_max_lag_ms 100
  @max_max_lag_ms 600_000

  @dlq_keys [:kind, :source_id, :id, :since, :until, :rate, :max_lag_ms]
  @archive_keys [:kind, :from, :to, :source_id, :sinks, :rate, :max_lag_ms]
  @patch_keys [:state, :rate, :max_lag_ms]

  @doc """
  Create a replay job. `spec` (map or keyword) takes the admin API's keys as
  atoms: for `kind: :dlq` the optional `:source_id`, `:id`, `:since`, `:until`;
  for `kind: :archive` the required `:from`, `:to`, and the optional
  `:source_id` and `:sinks`; plus `:rate` and `:max_lag_ms`.

  Returns `{:ok, :created, job}`, or `{:ok, :existing, job}` when a `running`
  or `paused` job with the same kind and filter already exists (a proxy retry
  is idempotent). `{:error, {:invalid, field}}` names a bad field,
  `{:error, :too_many_replays}` the 16-job cap, and
  `{:error, {:role_not_enabled, :edge}}` an archive job on a node without a
  queue writer.
  """
  @spec start(atom(), map() | keyword()) ::
          {:ok, :created | :existing, map()}
          | {:error,
             {:invalid, String.t()}
             | :too_many_replays
             | {:role_not_enabled, atom()}
             | :store_unavailable}
  def start(instance, spec) do
    with {:ok, spec} <- validate_spec(instance, spec) do
      call(instance, {:start, spec})
    end
  end

  @doc "Every job, newest first."
  @spec list(atom()) :: {:ok, [map()]} | {:error, :store_unavailable}
  def list(instance), do: call(instance, :list)

  @doc "One job by id."
  @spec get(atom(), String.t()) :: {:ok, map()} | {:error, :not_found | :store_unavailable}
  def get(instance, id), do: call(instance, {:get, id})

  @doc """
  Patch a job: `:state` (`:running | :paused | :cancelled`), `:rate` and/or
  `:max_lag_ms`. `{:error, :finished}` for a `done`, `cancelled` or `failed`
  job. Resuming clears `error` and resets the auto-pause window.
  """
  @spec update(atom(), String.t(), map() | keyword()) ::
          {:ok, map()}
          | {:error, :not_found | :finished | {:invalid, String.t()} | :store_unavailable}
  def update(instance, id, patch) do
    with {:ok, patch} <- validate_patch(patch) do
      call(instance, {:update, id, patch})
    end
  end

  @doc "The admin API's Replay object for a job map (atoms to strings)."
  @spec to_json(map()) :: map()
  def to_json(job) do
    %{
      "id" => job.id,
      "kind" => to_string(job.kind),
      "state" => to_string(job.state),
      "filter" => Map.new(job.filter, fn {k, v} -> {to_string(k), v} end),
      "rate" => job.rate,
      "max_lag_ms" => job.max_lag_ms,
      "created_at" => job.created_at,
      "updated_at" => job.updated_at,
      "finished_at" => job.finished_at,
      "moved" => job.moved,
      "scanned" => job.scanned,
      "skipped" => job.skipped,
      "delivered" => job.delivered,
      "dead" => job.dead,
      "error" => job.error
    }
  end

  defp call(instance, message) do
    GenServer.call(Ankusa.via(instance, :replayer), message, 5_000)
  catch
    :exit, _ -> {:error, :store_unavailable}
  end

  # ── validation ────────────────────────────────────────────────────────────

  defp validate_spec(instance, spec) do
    spec = Map.new(spec)

    with :ok <- kind_ok(Map.get(spec, :kind)),
         :ok <- rate_ok(Map.get(spec, :rate, @default_rate)),
         :ok <- max_lag_ok(Map.get(spec, :max_lag_ms, @default_max_lag_ms)) do
      kind = Map.fetch!(spec, :kind)

      # Keys are validated per kind: a `from`/`to` on a dlq spec (or a
      # `since`/`until` on an archive spec) is a typo, not a silently dropped
      # bound — the former would otherwise replay the whole DLQ.
      keys = if kind == :dlq, do: @dlq_keys, else: @archive_keys

      with :ok <- reject_unknown(spec, keys) do
        case kind do
          :dlq ->
            with :ok <- optional_field(spec, :source_id, :string),
                 :ok <- optional_field(spec, :id, :string),
                 :ok <- optional_field(spec, :since, :integer),
                 :ok <- optional_field(spec, :until, :integer),
                 :ok <- until_ok(spec) do
              {:ok,
               %{
                 kind: :dlq,
                 filter: Map.take(spec, [:source_id, :id, :since, :until]),
                 rate: Map.get(spec, :rate, @default_rate),
                 max_lag_ms: Map.get(spec, :max_lag_ms, @default_max_lag_ms)
               }}
            end

          :archive ->
            with :ok <- from_to_ok(instance, spec),
                 :ok <- optional_field(spec, :source_id, :string),
                 :ok <- sinks_ok(Map.get(spec, :sinks)) do
              {:ok,
               %{
                 kind: :archive,
                 filter: Map.take(spec, [:from, :to, :source_id, :sinks]),
                 rate: Map.get(spec, :rate, @default_rate),
                 max_lag_ms: Map.get(spec, :max_lag_ms, @default_max_lag_ms)
               }}
            end
        end
      end
    end
  end

  defp validate_patch(patch) do
    patch = Map.new(patch)

    with :ok <- reject_unknown(patch, @patch_keys),
         :ok <- state_ok(Map.get(patch, :state)),
         :ok <- rate_ok(Map.get(patch, :rate)),
         :ok <- max_lag_ok(Map.get(patch, :max_lag_ms)) do
      {:ok, patch}
    end
  end

  # Sorted so the key named in the error does not depend on map internals.
  defp reject_unknown(map, allowed) do
    case map |> Map.keys() |> Enum.sort() |> Enum.find(&(&1 not in allowed)) do
      nil -> :ok
      key -> {:error, {:invalid, to_string(key)}}
    end
  end

  defp kind_ok(:dlq), do: :ok
  defp kind_ok(:archive), do: :ok
  defp kind_ok(_), do: {:error, {:invalid, "kind"}}

  defp state_ok(nil), do: :ok
  defp state_ok(s) when s in [:running, :paused, :cancelled], do: :ok
  defp state_ok(_), do: {:error, {:invalid, "state"}}

  defp rate_ok(nil), do: :ok

  defp rate_ok(rate) when is_integer(rate) and rate >= @min_rate and rate <= @max_rate,
    do: :ok

  defp rate_ok(_), do: {:error, {:invalid, "rate"}}

  defp max_lag_ok(nil), do: :ok

  defp max_lag_ok(lag)
       when is_integer(lag) and lag >= @min_max_lag_ms and lag <= @max_max_lag_ms,
       do: :ok

  defp max_lag_ok(_), do: {:error, {:invalid, "max_lag_ms"}}

  # An optional key: absent or nil is fine, anything else must be `kind`.
  defp optional_field(spec, key, kind) do
    case Map.get(spec, key) do
      nil -> :ok
      value -> if match_type?(value, kind), do: :ok, else: {:error, {:invalid, to_string(key)}}
    end
  end

  defp match_type?(value, :string), do: is_binary(value)
  defp match_type?(value, :integer), do: is_integer(value)

  defp until_ok(spec) do
    case {Map.get(spec, :since), Map.get(spec, :until)} do
      {since, until} when is_integer(since) and is_integer(until) and until < since ->
        {:error, {:invalid, "until"}}

      _ ->
        :ok
    end
  end

  # `from`/`to` are inclusive bounds on `received_at`. `to` must be old enough
  # that the compactor has already archived the whole window.
  defp from_to_ok(instance, spec) do
    from = Map.get(spec, :from)
    to = Map.get(spec, :to)

    cond do
      not is_integer(from) ->
        {:error, {:invalid, "from"}}

      not is_integer(to) ->
        {:error, {:invalid, "to"}}

      from < 0 ->
        {:error, {:invalid, "from"}}

      to <= from ->
        {:error, {:invalid, "to"}}

      true ->
        config = Ankusa.config(instance)
        roll_ms = config.storage.roll_ms
        interval_ms = config.storage.interval_ms

        if to <= System.system_time(:millisecond) - (roll_ms + interval_ms) do
          :ok
        else
          {:error, {:invalid, "to"}}
        end
    end
  end

  defp sinks_ok(nil), do: :ok

  defp sinks_ok(sinks) do
    if is_list(sinks) and length(sinks) >= 1 and length(sinks) <= 64 and
         Enum.all?(sinks, &(is_integer(&1) and &1 >= 0)) do
      :ok
    else
      {:error, {:invalid, "sinks"}}
    end
  end
end
