defmodule ControlPanel.Cluster do
  @moduledoc """
  What the Kubernetes API says about the demo, polled every two seconds:
  the pods of every component, the two HPAs, which chaos `AuthorizationPolicy`
  levers are applied, and the fault on the gateway's `VirtualService`.

  The polling runs in a supervised task so `snapshot/0` always answers at once.
  A failed call keeps the last good value for that piece and sets `error`;
  the same error is logged at most once a minute.

  Publishes `{:cluster, snapshot}` on the `"cluster"` topic after every poll.
  """

  use GenServer

  require Logger

  alias ControlPanel.K8s

  @poll_ms 2_000
  @log_every_ms 60_000
  @groups ~w(ankusa ankusa-durable consumer claim-check nats floci)
  @lever_prefix "chaos-deny-"

  defstruct pods: [],
            hpa: %{ankusa: nil, consumer: nil},
            denies: MapSet.new(),
            fault: nil,
            error: nil,
            task: nil,
            logged_at: nil

  ## API

  def start_link(_opts), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @spec snapshot() :: map()
  def snapshot, do: GenServer.call(__MODULE__, :snapshot)

  ## Server

  @impl true
  def init(_) do
    send(self(), :poll)
    {:ok, %__MODULE__{}}
  end

  @impl true
  def handle_call(:snapshot, _from, state), do: {:reply, snapshot_of(state), state}

  @impl true
  def handle_info(:poll, %{task: nil} = state) do
    task = Task.Supervisor.async_nolink(ControlPanel.TaskSupervisor, &fetch/0)
    {:noreply, %{state | task: task}}
  end

  def handle_info(:poll, state), do: {:noreply, state}

  def handle_info({ref, result}, %{task: %Task{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    state = state |> merge(result) |> log_error() |> Map.put(:task, nil)
    Phoenix.PubSub.broadcast(ControlPanel.PubSub, "cluster", {:cluster, snapshot_of(state)})
    Process.send_after(self(), :poll, @poll_ms)
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{task: %Task{ref: ref}} = state) do
    state = log_error(%{state | error: "poll crashed: #{inspect(reason)}", task: nil})
    Process.send_after(self(), :poll, @poll_ms)
    {:noreply, state}
  end

  def handle_info(_stale, state), do: {:noreply, state}

  ## Merging a poll into the state

  # `result` maps each piece to `{:ok, value} | {:error, reason}`; a failed piece
  # keeps whatever the state already had.
  defp merge(state, result) do
    errors = for {_piece, {:error, reason}} <- result, do: K8s.describe(reason)

    %{
      state
      | pods: pick(result.pods, state.pods),
        hpa: %{
          ankusa: pick(result.hpa_ankusa, state.hpa.ankusa),
          consumer: pick(result.hpa_consumer, state.hpa.consumer)
        },
        denies: pick(result.denies, state.denies),
        fault: pick(result.fault, state.fault),
        error: List.first(errors)
    }
  end

  defp pick({:ok, value}, _previous), do: value
  defp pick({:error, _reason}, previous), do: previous

  defp log_error(%{error: nil} = state), do: state

  defp log_error(state) do
    now = System.monotonic_time(:millisecond)

    if is_nil(state.logged_at) or now - state.logged_at >= @log_every_ms do
      Logger.warning("cluster poll: #{state.error}")
      %{state | logged_at: now}
    else
      state
    end
  end

  defp snapshot_of(state) do
    %{
      pods: state.pods,
      hpa: state.hpa,
      denies: state.denies,
      fault: state.fault,
      error: state.error
    }
  end

  ## The poll (runs in a task)

  defp fetch do
    ns = K8s.namespace()

    %{
      pods: pods(ns),
      hpa_ankusa: ns |> hpa("ankusa") |> parse_hpa(:ankusa),
      hpa_consumer: ns |> hpa("keda-hpa-consumer") |> parse_hpa(:consumer),
      denies: denies(ns),
      fault: fault(ns)
    }
  end

  defp pods(ns) do
    selector = "app.kubernetes.io/name in (#{Enum.join(@groups, ",")})"

    with {:ok, %{"items" => items}} <-
           K8s.get("/api/v1/namespaces/#{ns}/pods", labelSelector: selector) do
      pods =
        items
        |> Enum.map(&parse_pod/1)
        |> Enum.reject(&(&1.phase in ["Succeeded", "Failed"]))
        |> Enum.sort_by(& &1.name)

      {:ok, pods}
    end
  end

  defp parse_pod(pod) do
    meta = pod["metadata"] || %{}
    status = pod["status"] || %{}

    %{
      name: meta["name"],
      group: get_in(meta, ["labels", "app.kubernetes.io/name"]),
      ready?:
        Enum.any?(status["conditions"] || [], &(&1["type"] == "Ready" and &1["status"] == "True")),
      phase: status["phase"],
      terminating?: meta["deletionTimestamp"] != nil,
      ip: status["podIP"],
      started_at: status["startTime"]
    }
  end

  defp hpa(ns, name),
    do: K8s.get("/apis/autoscaling/v2/namespaces/#{ns}/horizontalpodautoscalers/#{name}")

  # A missing HPA (KEDA has not created it yet) is "no autoscaler", not an error.
  defp parse_hpa({:error, {404, _}}, _which), do: {:ok, nil}
  defp parse_hpa({:error, _} = error, _which), do: error

  defp parse_hpa({:ok, %{"status" => status}}, which) do
    current = status["currentReplicas"] || 0
    metrics = status["currentMetrics"] || []

    hpa = %{current: current, desired: status["desiredReplicas"] || 0}

    case which do
      :ankusa ->
        metric = Enum.find(metrics, &(&1["type"] == "ContainerResource"))

        {:ok,
         Map.put(
           hpa,
           :cpu_pct,
           get_in(metric || %{}, ["containerResource", "current", "averageUtilization"])
         )}

      :consumer ->
        # KEDA's metric is an AverageValue: the HPA reports the total divided by
        # the current replicas, so the lag is that times the replicas.
        metric = Enum.find(metrics, &(&1["type"] == "External"))
        average = quantity(get_in(metric || %{}, ["external", "current", "averageValue"]))
        {:ok, Map.put(hpa, :lag, if(average, do: round(average * max(current, 1)), else: 0))}
    end
  end

  defp parse_hpa({:ok, _no_status}, _which), do: {:ok, nil}

  # A Kubernetes quantity: "50", "12500m", "1k".
  defp quantity(nil), do: nil

  defp quantity(value) when is_binary(value) do
    {number, suffix} = Float.parse(value)

    case suffix do
      "" -> number
      "m" -> number / 1_000
      "k" -> number * 1_000
      "M" -> number * 1_000_000
      _ -> nil
    end
  rescue
    _ -> nil
  end

  defp quantity(value) when is_number(value), do: value

  defp denies(ns) do
    with {:ok, %{"items" => items}} <-
           K8s.get("/apis/security.istio.io/v1/namespaces/#{ns}/authorizationpolicies") do
      levers =
        for %{"metadata" => %{"name" => @lever_prefix <> lever}} <- items, do: lever

      {:ok, MapSet.new(levers)}
    end
  end

  defp fault(ns) do
    with {:ok, vs} <-
           K8s.get("/apis/networking.istio.io/v1/namespaces/#{ns}/virtualservices/demo-routes") do
      {:ok, vs |> get_in(["spec", "http"]) |> List.wrap() |> List.first() |> parse_fault()}
    end
  end

  defp parse_fault(%{"fault" => %{"abort" => abort}}) do
    %{mode: :abort, pct: pct(abort), status: abort["httpStatus"]}
  end

  defp parse_fault(%{"fault" => %{"delay" => delay}}) do
    %{mode: :delay, pct: pct(delay), ms: delay_ms(delay["fixedDelay"])}
  end

  defp parse_fault(_http_route), do: nil

  defp pct(fault), do: get_in(fault, ["percentage", "value"]) || 100

  defp delay_ms(value) when is_binary(value) do
    case Integer.parse(value) do
      {n, "ms"} -> n
      {n, "s"} -> n * 1_000
      _ -> 0
    end
  end

  defp delay_ms(_), do: 0
end
