defmodule Ankusa.Metrics.Cardinality do
  @moduledoc """
  The series ceiling of `Ankusa.Metrics`: how many distinct label sets one
  metric may keep before the rest are folded into `"other"`.

  Only the *identity* tags, `:tenant_id`, `:source_id` and `:tenant`, are
  policed. Every other label comes from a fixed set (an outcome, a sink module,
  an HTTP status), and `metrics.detail` decides which identity tags a metric
  carries at all. A metric's first `metrics.max_series_per_metric - 1` label
  sets that include an identity are admitted and remembered; one slot is left
  for the `"other"` series that every later label set is rewritten into
  (`:tenant_id`, `:source_id` and `:tenant` each become `"other"`, the other
  labels are kept, so a metric holds one `"other"` series per combination of
  its fixed labels). A real tenant named `other` shares that series.

  The admitted sets live in one public ETS table per instance, owned by this
  process (a restart empties it, and `Ankusa.Metrics` restarts the reporter
  with it, so the two never disagree). Admission runs in the process that
  emitted the telemetry event, once per event and metric:

    * a known label set is a single `:ets.member/2`;
    * an unknown one reads the metric's counter, and once it is full returns
      without writing anything, so a flood of new tenants does not contend on
      the counter;
    * otherwise it reserves a slot (`:ets.update_counter/4`), claims the label
      set (`:ets.insert_new/2`) and gives the slot back when another process
      claimed the same set first. A reserved slot is only ever released, so the
      admitted sets never exceed the ceiling.

  A series is never forgotten: `TelemetryMetricsPrometheus.Core` keeps one for
  as long as the reporter runs, and so does this table.
  """

  use GenServer

  @identity [:tenant_id, :source_id, :tenant]

  @doc "The label names this policy bounds."
  @spec identity_tags() :: [atom()]
  def identity_tags, do: @identity

  @doc "Whether `tags` carry an identity label, i.e. whether a metric with them is policed."
  @spec policed?([atom()]) :: boolean()
  def policed?(tags), do: Enum.any?(tags, &(&1 in @identity))

  @doc "Name of `instance`'s admitted-series table."
  @spec table(atom()) :: atom()
  # The instance name is operator config: one atom per instance, never request input.
  # sobelow_skip ["DOS.BinToAtom"]
  def table(instance), do: :"ankusa_metrics_series_#{instance}"

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    instance = Keyword.fetch!(opts, :instance)
    %{id: {__MODULE__, instance}, start: {__MODULE__, :start_link, [instance]}}
  end

  @spec start_link(atom()) :: GenServer.on_start()
  def start_link(instance), do: GenServer.start_link(__MODULE__, instance)

  @impl true
  def init(instance) do
    :ets.new(table(instance), [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    {:ok, instance}
  end

  @doc """
  The label values to report for one event: `values` itself when its label set
  is (or can become) one of `name`'s admitted series, else `values` with every
  identity tag in `tags` replaced by `"other"`.
  """
  @spec fold(atom(), term(), [atom()], pos_integer(), map()) :: map()
  def fold(table, name, tags, cap, values) do
    case admit(table, name, tags, cap, values) do
      :admitted -> values
      :overflow -> Enum.reduce(tags, values, &other/2)
    end
  end

  @doc """
  Admit `values`' label set into `name`'s series, or refuse it when `cap - 1`
  sets are already held. `tags` are the metric's labels.
  """
  @spec admit(atom(), term(), [atom()], pos_integer(), map()) :: :admitted | :overflow
  def admit(table, name, tags, cap, values) do
    key = {name, Map.take(values, tags)}

    if :ets.member(table, key), do: :admitted, else: claim(table, name, key, cap - 1)
  end

  # `limit` slots, counted in `{:count, name}`. The cheap read keeps a full
  # metric from writing at all; the increment is the reservation, and it is
  # given back when it overshot or when another process claimed the same set.
  defp claim(table, name, key, limit) do
    counter = {:count, name}

    cond do
      held(table, counter) >= limit ->
        :overflow

      :ets.update_counter(table, counter, 1, {counter, 0}) > limit ->
        :ets.update_counter(table, counter, -1)
        :overflow

      :ets.insert_new(table, {key}) ->
        :admitted

      true ->
        :ets.update_counter(table, counter, -1)
        :admitted
    end
  end

  defp held(table, counter) do
    case :ets.lookup(table, counter) do
      [{^counter, held}] -> held
      [] -> 0
    end
  end

  defp other(tag, values) when tag in @identity, do: Map.put(values, tag, "other")
  defp other(_tag, values), do: values
end
