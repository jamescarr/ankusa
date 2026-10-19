defmodule Ankusa.Intake do
  @moduledoc """
  The intake log behind `wal: :dual` (ADR 0002): a replicated log an acked hook
  is also confirmed in, so a lost host does not lose it.

  An adapter implements this behaviour (`Ankusa.Intake.Memory` is the test
  double; `Ankusa.Intake.Kafka` ships in `ankusa_kafka`). Everything else is
  core: the writer's dual commit (`Ankusa.Queue.Writer`), the pending set and
  breaker (`Ankusa.Intake.Pending`, `Ankusa.Intake.Gate`), the live reconciler
  that carries a half-persisted hook to completion
  (`Ankusa.Intake.Reconciler`), and boot recovery (`Ankusa.Intake.Recovery`).

  A record is `{seq, bin, meta}`: the hook's `hooks` value, written once under
  the writer's pre-assigned seq. "Confirmed" is the adapter's promise that the
  log's replicas hold it (Kafka: `acks=all` with `min.insync.replicas`); the
  adapter makes one attempt per `c:publish_batch/3` call and the reconciler owns
  every retry. Nothing here assumes exactly-once publishing: every reader
  applies a record through the seq-and-id rule of `Ankusa.Intake.Recovery`, so a
  duplicate or reordered record changes nothing.

  An adapter's failure is one of the four classes of ground rule 2
  (`:not_found | :unavailable | :throttled | :permanent`), with an optional
  `retry_after` in milliseconds: `{:throttled, 500}`.
  """

  require Logger

  alias Ankusa.Config

  @type meta :: %{
          id: String.t(),
          tenant: String.t(),
          source: String.t(),
          incarnation: String.t()
        }
  @type log_record :: {seq :: non_neg_integer(), bin :: binary(), meta()}
  @type class :: :not_found | :unavailable | :throttled | :permanent
  @type failure :: class() | {class(), pos_integer()}
  @type replayed :: %{
          position: non_neg_integer(),
          node: String.t(),
          seq: non_neg_integer(),
          id: String.t(),
          kind: String.t(),
          version: String.t(),
          bin: binary()
        }

  @doc "The adapter's own processes (its client), started before the store."
  @callback child_spec(keyword()) :: Supervisor.child_spec()

  @doc """
  Blocks until the batch is confirmed or `opts[:timeout]` runs out. `position`
  is the log position of the batch's last record. `opts` also carries `:node`,
  `:topic` and `:partition` (the node's stamped partition, or `nil`).
  """
  @callback publish_batch(instance :: atom(), [log_record()], opts :: keyword()) ::
              {:ok, %{position: non_neg_integer()}} | {:error, failure()}

  @doc """
  Calls `fun` for every record of this node (`opts[:node]`) from position `from`,
  in log order. The adapter skips records of other nodes and of a kind or
  version it does not know before calling `fun`, and reports each skipped
  unknown one with `skipped/1`. Returns the position of the last record passed.
  """
  @callback replay(
              instance :: atom(),
              from :: non_neg_integer() | :earliest,
              (replayed() -> :ok | {:error, term()}),
              opts :: keyword()
            ) :: {:ok, non_neg_integer() | nil} | {:error, failure()}

  @doc "A cheap local answer (client up, leader known); never a produce."
  @callback ready?(instance :: atom()) :: boolean()

  @doc "Boot check of the log's topology, retention and record size."
  @callback verify(instance :: atom()) :: :ok | {:error, term()}

  @doc "The settings the startup sentence quotes, e.g. `acks=all, min.insync.replicas=2, replicas=3`."
  @callback describe(instance :: atom()) :: String.t()

  @doc "The earliest position the log still holds, `nil` for an empty log."
  @callback earliest(instance :: atom()) ::
              {:ok, non_neg_integer() | nil} | {:error, failure()}

  @doc "The partition this node writes, chosen once at its first dual boot."
  @callback assign_partition(instance :: atom(), node_id :: String.t()) ::
              {:ok, non_neg_integer()} | :none

  @optional_callbacks assign_partition: 2

  @classes [:not_found, :unavailable, :throttled, :permanent]

  @doc "Whether the config runs the dual commit."
  @spec enabled?(Config.t()) :: boolean()
  def enabled?(%Config{wal: :dual}), do: true
  def enabled?(%Config{}), do: false

  @doc "The adapter module `intake.type` names."
  @spec module(Config.t()) :: module()
  def module(%Config{intake: %{type: :memory}}), do: Ankusa.Intake.Memory
  def module(%Config{intake: %{type: :kafka}}), do: Ankusa.Intake.Kafka
  def module(%Config{intake: %{type: :jetstream}}), do: Ankusa.Intake.JetStream

  @doc "The log's name: `intake.topic`, else `ankusa.intake.<fleet>`."
  @spec topic(Config.t()) :: String.t()
  def topic(%Config{intake: %{topic: nil, fleet: fleet}}), do: "ankusa.intake." <> fleet
  def topic(%Config{intake: %{topic: topic}}), do: topic

  @doc "Boot check: the adapter's package is on the code path."
  @spec validate_config!(Config.t()) :: :ok
  def validate_config!(%Config{wal: :dual} = config) do
    mod = module(config)

    unless Code.ensure_loaded?(mod) do
      raise ArgumentError,
            "intake.type: #{inspect(config.intake.type)} needs #{inspect(mod)}, which is not " <>
              "loaded (add its adapter package, see docs/packaging.md)"
    end

    :ok
  end

  def validate_config!(%Config{}), do: :ok

  @doc """
  The startup sentence of `wal: :dual` (ADR 0002, section 1). `describe/1` of the
  adapter supplies the verified settings; with `intake.verify_topic: false` the
  sentence says they are unverified instead.
  """
  @spec durability(Config.t()) :: String.t()
  def durability(%Config{} = config) do
    settings =
      if config.intake.verify_topic,
        do: describe(config.instance),
        else: "intake topic settings UNVERIFIED"

    topic = topic(config)

    "Durable to power loss on this host AND confirmed in the intake log #{topic} " <>
      "(#{settings}) before every 2xx: an acked hook is lost only if this host and every " <>
      "in-sync replica of the log are lost together; backed up every " <>
      "#{config.backup.interval_ms} ms for faster recovery."
  end

  @doc """
  One publish attempt with the configured deadline, measured. Never raises:
  `{:ok, position}` or `{:error, class, retry_after_ms | nil}`.
  """
  @spec publish_batch(atom(), [log_record()]) ::
          {:ok, non_neg_integer()} | {:error, class(), pos_integer() | nil}
  def publish_batch(instance, records) do
    config = Ankusa.config(instance)
    mod = module(config)

    opts = [
      timeout: config.intake.publish_timeout_ms,
      node: config.intake.node_id,
      topic: topic(config),
      partition: Ankusa.Intake.Pending.partition(instance)
    ]

    started = System.monotonic_time()
    result = safe(fn -> apply(mod, :publish_batch, [instance, records, opts]) end)
    duration = System.monotonic_time() - started

    outcome = normalize(result)

    Ankusa.Telemetry.emit(
      [:intake, :publish],
      %{duration: duration, records: length(records)},
      %{instance: instance, outcome: outcome_label(outcome)}
    )

    with {:error, class, _retry_after} <- outcome do
      Ankusa.Telemetry.emit([:intake, :failure], %{count: 1}, %{instance: instance, class: class})
    end

    outcome
  end

  @doc "Replays this node's records from `from` (see `c:replay/4`)."
  @spec replay(atom(), non_neg_integer() | :earliest, (replayed() -> :ok | {:error, term()})) ::
          {:ok, non_neg_integer() | nil} | {:error, class()}
  def replay(instance, from, fun) do
    config = Ankusa.config(instance)

    opts = [
      node: config.intake.node_id,
      topic: topic(config),
      partition: Ankusa.Intake.Pending.partition(instance)
    ]

    case safe(fn -> apply(module(config), :replay, [instance, from, fun, opts]) end) do
      {:ok, last} -> {:ok, last}
      other -> with {:error, class, _} <- normalize(other), do: {:error, class}
    end
  end

  @doc "The adapter's cheap readiness answer; `false` if it cannot answer."
  @spec ready?(atom()) :: boolean()
  def ready?(instance) do
    mod = module(Ankusa.config(instance))
    safe(fn -> apply(mod, :ready?, [instance]) end) == true
  end

  @doc "The adapter's boot verification."
  @spec verify(atom()) :: :ok | {:error, term()}
  def verify(instance) do
    mod = module(Ankusa.config(instance))

    case safe(fn -> apply(mod, :verify, [instance]) end) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
      other -> {:error, other}
    end
  end

  @doc "The settings the adapter quotes in the startup sentence."
  @spec describe(atom()) :: String.t()
  def describe(instance) do
    mod = module(Ankusa.config(instance))

    case safe(fn -> apply(mod, :describe, [instance]) end) do
      text when is_binary(text) -> text
      _ -> "intake topic settings unavailable"
    end
  end

  @doc "The earliest position the log holds, `nil` when empty."
  @spec earliest(atom()) :: {:ok, non_neg_integer() | nil} | {:error, class()}
  def earliest(instance) do
    mod = module(Ankusa.config(instance))

    case safe(fn -> apply(mod, :earliest, [instance]) end) do
      {:ok, position} -> {:ok, position}
      other -> with {:error, class, _} <- normalize(other), do: {:error, class}
    end
  end

  @doc "Asks the adapter for this node's partition; `:none` when it has no such notion."
  @spec assign_partition(atom()) :: {:ok, non_neg_integer()} | :none
  def assign_partition(instance) do
    config = Ankusa.config(instance)
    mod = module(config)

    if function_exported?(mod, :assign_partition, 2) do
      case safe(fn -> apply(mod, :assign_partition, [instance, config.intake.node_id]) end) do
        {:ok, partition} when is_integer(partition) -> {:ok, partition}
        _ -> :none
      end
    else
      :none
    end
  end

  @doc "A record of a kind or version this release does not know (adapters call it per skip)."
  @spec skipped(atom()) :: :ok
  def skipped(instance) do
    Ankusa.Telemetry.emit([:intake, :replay], %{count: 1}, %{
      instance: instance,
      result: :skipped_unknown
    })
  end

  @doc "Whether an adapter should pass a record of this kind and version to the replay function."
  @spec known?(String.t(), String.t()) :: boolean()
  def known?(kind, version), do: kind == "hook" and version == "1"

  @doc """
  `:recovering` while boot replay runs, `:down` while the breaker is open and the
  adapter still says it is not ready (the readiness probe drains the node), `:up`
  otherwise: with the breaker open but the adapter ready again, the node is
  ready for the one batch that probes it.
  """
  @spec state(atom()) :: :up | :down | :recovering
  def state(instance) do
    t = Ankusa.Intake.Pending.table(instance)

    cond do
      :ets.whereis(t) == :undefined -> :down
      Ankusa.Intake.Pending.recovering?(t) -> :recovering
      Ankusa.Intake.Gate.state(t) == :open and not ready?(instance) -> :down
      true -> :up
    end
  end

  @doc "The `intake` object of `GET /v1/wal`."
  @spec info(atom()) :: map()
  def info(instance) do
    config = Ankusa.config(instance)
    t = Ankusa.Intake.Pending.table(instance)
    stats = Ankusa.Intake.Pending.stats(t, System.monotonic_time(:millisecond))

    read = fn key ->
      case Ankusa.Store.get(instance, :default, key) do
        {:ok, <<n::64>>} -> n
        _ -> nil
      end
    end

    %{
      node: config.intake.node_id,
      partition: Ankusa.Intake.Pending.partition(instance),
      offset: read.(Ankusa.Store.Keys.meta("intake_offset")),
      watermark: read.(Ankusa.Store.Keys.meta("intake_seq")),
      pending: Map.take(stats, [:records, :bytes, :carried, :stuck]),
      state: Atom.to_string(state(instance))
    }
  end

  # ── internals ─────────────────────────────────────────────────────────────

  defp safe(fun) do
    fun.()
  rescue
    exception ->
      Logger.error("[ankusa] intake adapter raised: #{Exception.message(exception)}")
      {:error, :unavailable}
  catch
    kind, reason ->
      Logger.error("[ankusa] intake adapter #{kind}: #{inspect(reason)}")
      {:error, :unavailable}
  end

  defp normalize({:ok, %{position: position}}) when is_integer(position), do: {:ok, position}

  defp normalize({:error, {class, retry_after}})
       when class in @classes and is_integer(retry_after) and retry_after > 0,
       do: {:error, class, retry_after}

  defp normalize({:error, class}) when class in @classes, do: {:error, class, nil}
  defp normalize(_other), do: {:error, :unavailable, nil}

  defp outcome_label({:ok, _}), do: :ok
  defp outcome_label({:error, class, _}), do: class
end
