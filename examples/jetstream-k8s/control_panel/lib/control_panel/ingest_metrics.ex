defmodule ControlPanel.IngestMetrics do
  @moduledoc """
  Per-pod ingest counters, scraped once a second from each ready ankusa pod's
  admin port (`:4002/metrics`, Prometheus text).

  `ankusa_ingest_requests_total{instance,source_id,outcome}` is a counter per
  outcome: `committed` is a hook the pod acked with `201`, every other outcome
  (`rejected`, `rate_limited`, `overload`, `store_unavailable`, ...) is a hook
  it answered with something else. In direct mode a sink or claim failure is
  `store_unavailable`, which is what a denied NATS connection shows up as.

  Publishes `{:ingest, %{pod_name => %{committed, failed, rate}}}` on the
  `"ingest"` topic. `rate` is committed hooks per second since the pod's
  previous sample (zero on its first sample and after a restart, when its
  counters start over).
  """

  use GenServer

  alias ControlPanel.Cluster

  @scrape_ms 1_000

  defstruct snapshot: %{}, previous: %{}

  ## API

  def start_link(_opts), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @spec snapshot() :: map()
  def snapshot, do: GenServer.call(__MODULE__, :snapshot)

  @doc """
  Sums `ankusa_ingest_requests_total` over the label sets in a `/metrics` body:
  `%{committed: n, failed: n}`.
  """
  @spec parse(binary()) :: %{committed: number(), failed: number()}
  def parse(body) when is_binary(body) do
    ~r/^ankusa_ingest_requests_total\{(?<labels>[^}]*)\}\s+(?<value>[0-9.eE+-]+)/m
    |> Regex.scan(body, capture: :all_names)
    |> Enum.reduce(%{committed: 0, failed: 0}, fn [labels, value], acc ->
      key =
        case Regex.named_captures(~r/outcome="(?<outcome>[^"]*)"/, labels) do
          %{"outcome" => "committed"} -> :committed
          _ -> :failed
        end

      Map.update!(acc, key, &(&1 + number(value)))
    end)
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
      |> Enum.filter(&(&1.group == "ankusa" and &1.ready? and is_binary(&1.ip)))
      |> Task.async_stream(&scrape/1,
        max_concurrency: 16,
        timeout: 1_500,
        on_timeout: :kill_task,
        ordered: false
      )
      |> Enum.flat_map(fn
        {:ok, {name, {:ok, totals}}} -> [{name, totals}]
        _failed_or_killed -> []
      end)

    snapshot =
      Map.new(samples, fn {name, totals} -> {name, entry(totals, state.previous[name], now)} end)

    previous = Map.new(samples, fn {name, totals} -> {name, {totals.committed, now}} end)

    Phoenix.PubSub.broadcast(ControlPanel.PubSub, "ingest", {:ingest, snapshot})
    Process.send_after(self(), :scrape, @scrape_ms)
    {:noreply, %{state | snapshot: snapshot, previous: previous}}
  end

  defp scrape(%{name: name, ip: ip}) do
    result =
      case Req.get("http://#{ip}:4002/metrics",
             receive_timeout: 800,
             retry: false,
             decode_body: false
           ) do
        {:ok, %Req.Response{status: 200, body: body}} -> {:ok, parse(body)}
        _ -> :error
      end

    {name, result}
  end

  defp entry(%{committed: committed, failed: failed}, {previous_committed, at}, now)
       when committed >= previous_committed and now > at do
    %{
      committed: round(committed),
      failed: round(failed),
      rate: Float.round((committed - previous_committed) / ((now - at) / 1_000), 1)
    }
  end

  # First sample, or the counter went down (the pod restarted).
  defp entry(%{committed: committed, failed: failed}, _previous, _now),
    do: %{committed: round(committed), failed: round(failed), rate: 0.0}
end
