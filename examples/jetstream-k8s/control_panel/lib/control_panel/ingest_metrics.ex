defmodule ControlPanel.IngestMetrics do
  @moduledoc """
  Per-pod ingest and dispatch numbers, scraped once a second from each ready
  ankusa pod's admin port (`:4002/metrics`, Prometheus text), direct and
  durable alike.

  * `ankusa_ingest_requests_total{instance,source_id,outcome}` is a counter per
    outcome: `committed` is a hook the pod acked with `201`, `duplicate` a
    repeated dedupe key it acked as one (durable pods only), every other
    outcome (`rejected`, `rate_limited`, `overload`, `store_unavailable`, ...)
    a hook it answered with something else. In direct mode a sink or claim
    failure is `store_unavailable`, which is what a denied NATS connection
    shows up as.
  * `ankusa_dispatch_deliveries_total{result="ok"}` counts rows a durable pod
    delivered to JetStream.
  * The queue and store gauges (`ankusa_queue_pending`, `_scheduled`,
    `_inflight`, `_dead`, `ankusa_dispatch_breakers_open`,
    `ankusa_store_disk_bytes`) only mean something on a durable pod (a direct
    pod reports them as 0); one the body does not carry yet is `nil`.

  Publishes `{:ingest, %{pod_name => entry}}` on the `"ingest"` topic. `rate`
  and `delivered_rate` are per second since the pod's previous sample (zero on
  its first sample and after a restart, when its counters start over).
  """

  use GenServer

  alias ControlPanel.Cluster

  @scrape_ms 1_000
  @groups ["ankusa", "ankusa-durable"]

  defstruct snapshot: %{}, previous: %{}

  @type totals :: %{
          committed: number(),
          failed: number(),
          delivered: number(),
          pending: number() | nil,
          scheduled: number() | nil,
          inflight: number() | nil,
          dead: number() | nil,
          breakers_open: number() | nil,
          disk_bytes: number() | nil
        }

  ## API

  def start_link(_opts), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @spec snapshot() :: map()
  def snapshot, do: GenServer.call(__MODULE__, :snapshot)

  @doc "The numbers the panel uses from one pod's `/metrics` body."
  @spec parse(binary()) :: totals()
  def parse(body) when is_binary(body) do
    requests = sum_metric(body, "ankusa_ingest_requests_total") || 0
    committed = sum_metric(body, "ankusa_ingest_requests_total", ~s(outcome="committed")) || 0
    duplicate = sum_metric(body, "ankusa_ingest_requests_total", ~s(outcome="duplicate")) || 0

    %{
      committed: committed,
      failed: requests - committed - duplicate,
      delivered: sum_metric(body, "ankusa_dispatch_deliveries_total", ~s(result="ok")) || 0,
      pending: sum_metric(body, "ankusa_queue_pending"),
      scheduled: sum_metric(body, "ankusa_queue_scheduled"),
      inflight: sum_metric(body, "ankusa_queue_inflight"),
      dead: sum_metric(body, "ankusa_queue_dead"),
      breakers_open: sum_metric(body, "ankusa_dispatch_breakers_open"),
      disk_bytes: sum_metric(body, "ankusa_store_disk_bytes")
    }
  end

  # The sum of every sample of `name` (whatever its labels), or only of those
  # whose label set contains `label_filter`; `nil` when there is none.
  defp sum_metric(body, name, label_filter \\ nil) do
    ~r/^#{Regex.escape(name)}(?:\{(?<labels>[^}]*)\})?\s+(?<value>[0-9.eE+-]+)/m
    |> Regex.scan(body, capture: :all_names)
    |> Enum.filter(fn [labels, _value] -> is_nil(label_filter) or labels =~ label_filter end)
    |> case do
      [] -> nil
      samples -> samples |> Enum.map(fn [_labels, value] -> number(value) end) |> Enum.sum()
    end
  end

  defp number(text) do
    case Float.parse(text) do
      {value, _} -> value
      :error -> 0
    end
  end

  ## Server

  @impl true
  def init(_) do
    Process.send_after(self(), :scrape, @scrape_ms)
    {:ok, %__MODULE__{}}
  end

  @impl true
  def handle_call(:snapshot, _from, state), do: {:reply, state.snapshot, state}

  @impl true
  def handle_info(:scrape, state) do
    now = System.monotonic_time(:millisecond)

    samples =
      Cluster.snapshot().pods
      |> Enum.filter(&(&1.group in @groups and &1.ready? and is_binary(&1.ip)))
      |> Task.async_stream(&scrape/1,
        max_concurrency: 16,
        timeout: 1_500,
        on_timeout: :kill_task,
        ordered: false
      )
      |> Enum.flat_map(fn
        {:ok, {pod, {:ok, totals}}} -> [{pod, totals}]
        _failed_or_killed -> []
      end)

    snapshot =
      Map.new(samples, fn {pod, totals} ->
        {pod.name, entry(pod.group, totals, state.previous[pod.name], now)}
      end)

    previous =
      Map.new(samples, fn {pod, totals} ->
        {pod.name, %{committed: totals.committed, delivered: totals.delivered, at: now}}
      end)

    Phoenix.PubSub.broadcast(ControlPanel.PubSub, "ingest", {:ingest, snapshot})
    Process.send_after(self(), :scrape, @scrape_ms)
    {:noreply, %{state | snapshot: snapshot, previous: previous}}
  end

  defp scrape(%{ip: ip} = pod) do
    result =
      case Req.get("http://#{ip}:4002/metrics",
             receive_timeout: 800,
             retry: false,
             decode_body: false
           ) do
        {:ok, %Req.Response{status: 200, body: body}} -> {:ok, parse(body)}
        _ -> :error
      end

    {pod, result}
  end

  defp entry(group, totals, previous, now) do
    %{
      group: group,
      committed: round(totals.committed),
      failed: round(totals.failed),
      delivered: round(totals.delivered),
      rate: rate(totals.committed, previous[:committed], previous[:at], now),
      delivered_rate: rate(totals.delivered, previous[:delivered], previous[:at], now),
      pending: gauge(totals.pending),
      scheduled: gauge(totals.scheduled),
      inflight: gauge(totals.inflight),
      dead: gauge(totals.dead),
      breakers_open: gauge(totals.breakers_open),
      disk_bytes: gauge(totals.disk_bytes)
    }
  end

  defp rate(value, before, at, now) when is_number(before) and value >= before and now > at,
    do: Float.round((value - before) / ((now - at) / 1_000), 1)

  # First sample, or the counter went down (the pod restarted).
  defp rate(_value, _before, _at, _now), do: 0.0

  defp gauge(nil), do: nil
  defp gauge(value), do: round(value)
end
