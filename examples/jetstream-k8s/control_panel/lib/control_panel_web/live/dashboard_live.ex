defmodule ControlPanelWeb.DashboardLive do
  @moduledoc """
  The control panel: the load generator's sliders, the live flow diagram, the
  chaos levers, and the tail of consumed hooks.

  Every producer (`ControlPanel.Generator`, `Cluster`, `IngestMetrics`,
  `Stream`) broadcasts a full snapshot on its own throttled topic, at most four
  updates a second, and this LiveView only assigns it: nothing here is driven
  per message. Chaos actions run in a supervised task so a slow Kubernetes API
  never blocks the page; a failure comes back as a flash.
  """

  use ControlPanelWeb, :live_view

  alias ControlPanel.{Chaos, Cluster, Generator, IngestMetrics, K8s, Stream}
  alias ControlPanelWeb.DashboardLive.{Flow, Format}

  @topics ~w(sender cluster ingest consumed jetstream)
  @tail_size 30
  @kill_groups [
    {"ankusa", "ankusa"},
    {"consumer", "consumer"},
    {"nats", "NATS"},
    {"claim-check", "claim-check"}
  ]
  @levers [
    {"ankusa-nats", "ankusa → NATS"},
    {"consumer-nats", "consumer → NATS"},
    {"consumer-claim-check", "consumer → claim-check"}
  ]

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Enum.each(@topics, &Phoenix.PubSub.subscribe(ControlPanel.PubSub, &1))
    end

    {:ok,
     assign(socket,
       page_title: "Ankusa · JetStream on Kubernetes",
       sender: Generator.snapshot(),
       cluster: Cluster.snapshot(),
       ingest: IngestMetrics.snapshot(),
       consumed: Stream.snapshot() |> cap_tail(),
       jetstream: Stream.jetstream(),
       fault_params: %{"mode" => "abort", "pct" => "50", "delay_ms" => "2000"},
       kill_groups: @kill_groups,
       levers: @levers
     )}
  end

  ## Snapshots from the producers

  @impl true
  def handle_info({:sender, snapshot}, socket), do: {:noreply, assign(socket, sender: snapshot)}
  def handle_info({:cluster, snapshot}, socket), do: {:noreply, assign(socket, cluster: snapshot)}
  def handle_info({:ingest, snapshot}, socket), do: {:noreply, assign(socket, ingest: snapshot)}

  def handle_info({:consumed, snapshot}, socket),
    do: {:noreply, assign(socket, consumed: cap_tail(snapshot))}

  def handle_info({:jetstream, snapshot}, socket),
    do: {:noreply, assign(socket, jetstream: snapshot)}

  def handle_info({:chaos_result, _label, :ok}, socket), do: {:noreply, socket}

  def handle_info({:chaos_result, label, {:error, reason}}, socket),
    do: {:noreply, put_flash(socket, :error, "#{label}: #{describe(reason)}")}

  defp cap_tail(consumed), do: Map.update!(consumed, :tail, &Enum.take(&1, @tail_size))

  defp describe(:none), do: "no running pod in that group"
  defp describe(reason), do: K8s.describe(reason)

  ## Events

  @impl true
  def handle_event("settings", params, socket) do
    {:noreply, assign(socket, sender: params |> settings() |> Generator.set())}
  end

  def handle_event("toggle", _params, socket) do
    sender = if socket.assigns.sender.running, do: Generator.stop(), else: Generator.start()
    {:noreply, assign(socket, sender: sender)}
  end

  def handle_event("burst", _params, socket),
    do: {:noreply, assign(socket, sender: Generator.fire_burst())}

  def handle_event("kill", %{"pod" => pod}, socket),
    do: chaos(socket, "kill #{pod}", fn -> Chaos.kill_pod(pod) end)

  def handle_event("kill_random", %{"group" => group}, socket),
    do: chaos(socket, "kill a #{group} pod", fn -> Chaos.kill_random(group) end)

  def handle_event("deny", %{"lever" => lever}, socket),
    do: chaos(socket, "deny #{lever}", fn -> Chaos.deny(lever) end)

  def handle_event("allow", %{"lever" => lever}, socket),
    do: chaos(socket, "allow #{lever}", fn -> Chaos.allow(lever) end)

  def handle_event("fault_change", params, socket),
    do:
      {:noreply,
       assign(socket,
         fault_params:
           Map.merge(socket.assigns.fault_params, Map.take(params, ~w(mode pct delay_ms)))
       )}

  def handle_event("fault", params, socket) do
    socket =
      assign(socket,
        fault_params:
          Map.merge(socket.assigns.fault_params, Map.take(params, ~w(mode pct delay_ms)))
      )

    fault = fault(params)
    chaos(socket, "gateway fault", fn -> Chaos.fault(fault) end)
  end

  defp chaos(socket, label, fun) do
    lv = self()

    Task.Supervisor.start_child(ControlPanel.TaskSupervisor, fn ->
      send(lv, {:chaos_result, label, fun.()})
    end)

    {:noreply, socket}
  end

  # Only the slider that moved is sent, so moving size_min past size_max drags
  # size_max along instead of snapping size_min back.
  defp settings(%{"_target" => [field]} = params),
    do: params |> Map.take([field]) |> settings_map()

  defp settings(params), do: settings_map(params)

  defp settings_map(params) do
    for {key, value} <- params,
        {name, convert} <- setting(key),
        {int, _} <- [Integer.parse(value)],
        into: %{} do
      {name, convert.(int)}
    end
  end

  defp setting("rate"), do: [{:rate, & &1}]
  defp setting("burst"), do: [{:burst, & &1}]
  defp setting("size_min_pos"), do: [{:size_min, &Format.pos_to_bytes/1}]
  defp setting("size_max_pos"), do: [{:size_max, &Format.pos_to_bytes/1}]
  defp setting(_other), do: []

  defp fault(%{"mode" => "abort"} = params), do: {:abort, int(params["pct"], 50)}

  defp fault(%{"mode" => "delay"} = params),
    do: {:delay, int(params["pct"], 50), int(params["delay_ms"], 2_000)}

  defp fault(_none), do: :none

  defp int(value, default) do
    case Integer.parse(to_string(value)) do
      {n, _} -> n
      :error -> default
    end
  end

  ## Render

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash}>
      <header class="flex flex-wrap items-center gap-x-6 gap-y-3">
        <div class="mr-auto">
          <h1 class="text-xl font-semibold leading-tight">Ankusa on Kubernetes</h1>
          <p class="text-sm text-base-content/60">kind · Istio · NATS JetStream · HPA + KEDA</p>
        </div>
        <div class="flex flex-wrap items-end gap-x-5 gap-y-2" id="totals">
          <.metric label="sent" value={Format.int(@sender.totals.sent)} />
          <.metric label="accepted" value={Format.int(@sender.totals.accepted)} />
          <.metric label="consumed (unique)" value={Format.int(@consumed.unique_total)} />
          <.metric label="duplicates" value={Format.int(@consumed.duplicate_total)} />
          <.metric
            label="failed"
            value={Format.int(@sender.totals.failed)}
            tone={@sender.totals.failed > 0 && "text-error"}
          />
          <.metric label="retried" value={Format.int(@sender.totals.retried)} />
          <.metric label="in flight" value={Format.int(@sender.inflight)} />
          <.metric label="ingest latency" value={"#{Format.int(@sender.mean_latency_ms)} ms"} />
          <.metric
            :if={is_map(@jetstream) and @jetstream.dlq > 0}
            label="dead-lettered"
            value={Format.int(@jetstream.dlq)}
            tone="text-error"
          />
          <.zero_loss sender={@sender} consumed={@consumed} jetstream={@jetstream} />
        </div>
        <button
          id="toggle"
          phx-click="toggle"
          class={["btn", (@sender.running && "btn-error") || "btn-primary"]}
        >
          {if @sender.running, do: "Stop", else: "Start"}
        </button>
        <Layouts.theme_toggle />
      </header>

      <div class="grid gap-4 lg:grid-cols-[22rem_minmax(0,1fr)]">
        <section id="controls" class="card bg-base-100 border border-base-300 lg:col-start-1">
          <div class="card-body gap-3 p-4">
            <h2 class="card-title text-base">Sender</h2>
            <.form for={%{}} id="settings" phx-change="settings" class="space-y-3">
              <.slider
                label="rate"
                name="rate"
                value={@sender.rate}
                min={0}
                max={500}
                step={5}
                shown={"#{@sender.rate} req/s"}
              />
              <.slider
                label="size min"
                name="size_min_pos"
                value={Format.bytes_to_pos(@sender.size_min)}
                min={0}
                max={100}
                step={1}
                shown={Format.bytes(@sender.size_min)}
              />
              <.slider
                label="size max"
                name="size_max_pos"
                value={Format.bytes_to_pos(@sender.size_max)}
                min={0}
                max={100}
                step={1}
                shown={Format.bytes(@sender.size_max)}
              />
              <.slider
                label="burst volume"
                name="burst"
                value={@sender.burst}
                min={10}
                max={5000}
                step={10}
                shown={"#{Format.int(@sender.burst)} hooks"}
              />
            </.form>
            <button id="burst" phx-click="burst" class="btn btn-outline btn-sm">
              Fire burst ({Format.int(@sender.burst)})
            </button>
            <p class="text-xs text-base-content/60">
              Bodies over 64 KiB are checked in to the claim store; the hook carries a claim ref the consumer
              redeems. The store (floci) holds them in memory, so the sender spends at most {Format.bytes(
                Application.get_env(:control_panel, :claim_bytes_per_sec)
              )}/s on them.
            </p>
            <p :if={@sender.throttled} id="throttled" class="text-xs text-warning">
              Claim bandwidth cap reached: the sender is holding back large hooks.
            </p>
          </div>
        </section>

        <section
          id="flow"
          class="card bg-base-100 border border-base-300 lg:col-start-2 lg:row-start-1 lg:row-span-2"
        >
          <div class="card-body p-4">
            <Flow.flow
              sender={@sender}
              cluster={@cluster}
              ingest={@ingest}
              consumed={@consumed}
              jetstream={@jetstream}
            />
            <p :if={@cluster.error} id="cluster-error" class="text-xs text-warning">
              Kubernetes API: {@cluster.error}
            </p>
          </div>
        </section>

        <section id="chaos" class="card bg-base-100 border border-base-300 lg:col-start-1">
          <div class="card-body gap-3 p-4">
            <h2 class="card-title text-base">Chaos</h2>

            <div>
              <h3 class="mb-1 text-xs font-semibold uppercase tracking-wide text-base-content/60">
                Kill a random pod (SIGKILL)
              </h3>
              <div class="flex flex-wrap gap-2">
                <button
                  :for={{group, label} <- @kill_groups}
                  id={"kill-random-#{group}"}
                  phx-click="kill_random"
                  phx-value-group={group}
                  class="btn btn-outline btn-error btn-sm"
                >
                  {label}
                </button>
              </div>
            </div>

            <div>
              <h3 class="mb-1 text-xs font-semibold uppercase tracking-wide text-base-content/60">
                Cut a connection (Istio DENY)
              </h3>
              <ul class="space-y-1">
                <li :for={{lever, label} <- @levers} class="flex items-center justify-between gap-2">
                  <span class="text-sm">{label}</span>
                  <.lever lever={lever} denied={lever in @cluster.denies} />
                </li>
              </ul>
              <p class="mt-1 text-xs text-base-content/60">
                Takes up to ~10 s: Envoy drains existing connections.
              </p>
            </div>

            <div>
              <h3 class="mb-1 text-xs font-semibold uppercase tracking-wide text-base-content/60">
                Gateway fault
              </h3>
              <.form
                for={%{}}
                id="fault-form"
                phx-change="fault_change"
                phx-submit="fault"
                class="space-y-2"
              >
                <select name="mode" class="select select-sm w-full" id="fault-mode">
                  <option value="none" selected={@fault_params["mode"] == "none"}>
                    none (clear)
                  </option>
                  <option value="abort" selected={@fault_params["mode"] == "abort"}>
                    abort: 503
                  </option>
                  <option value="delay" selected={@fault_params["mode"] == "delay"}>delay</option>
                </select>
                <.slider
                  label="affected"
                  name="pct"
                  value={@fault_params["pct"]}
                  min={0}
                  max={100}
                  step={5}
                  shown={"#{@fault_params["pct"]}%"}
                />
                <.slider
                  :if={@fault_params["mode"] == "delay"}
                  label="delay"
                  name="delay_ms"
                  value={@fault_params["delay_ms"]}
                  min={100}
                  max={5000}
                  step={100}
                  shown={"#{@fault_params["delay_ms"]} ms"}
                />
                <button id="fault-apply" type="submit" class="btn btn-outline btn-sm">Apply</button>
              </.form>
            </div>
          </div>
        </section>

        <section id="tail" class="card bg-base-100 border border-base-300 lg:col-span-2">
          <div class="card-body gap-2 p-4">
            <h2 class="card-title text-base">
              Consumed
              <span class="text-xs font-normal text-base-content/60">last {length(@consumed.tail)} (UTC)</span>
            </h2>
            <div class="overflow-x-auto">
              <table class="table table-xs font-mono">
                <thead>
                  <tr>
                    <th>time</th>
                    <th>hook</th>
                    <th>consumer</th>
                    <th class="text-right">size</th>
                    <th>via</th>
                    <th class="text-right">latency</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  <tr :for={entry <- @consumed.tail}>
                    <td>{Format.clock(entry["consumed_at"])}</td>
                    <td>{Format.tail_id(entry["hook_id"], 8)}</td>
                    <td>{Format.short_name(entry["consumer"], "consumer")}</td>
                    <td class="text-right">{Format.bytes(entry["size"])}</td>
                    <td>
                      <span class={[
                        "badge badge-sm",
                        (entry["via"] == "claim" && "badge-warning") || "badge-ghost"
                      ]}>
                        {entry["via"]}
                      </span>
                    </td>
                    <td class="text-right">{Format.int(entry["latency_ms"])} ms</td>
                    <td>
                      <span :if={entry["duplicate"]} class="badge badge-sm badge-info">dup</span>
                    </td>
                  </tr>
                  <tr :if={@consumed.tail == []}>
                    <td colspan="7" class="text-base-content/60">
                      Nothing consumed yet. Press Start.
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </section>
      </div>
    </Layouts.app>
    """
  end

  ## Parts

  attr :label, :string, required: true
  attr :value, :string, required: true
  attr :tone, :any, default: nil

  defp metric(assigns) do
    ~H"""
    <div class="flex flex-col items-start gap-0.5">
      <span class="text-[10px] uppercase tracking-wide text-base-content/60">{@label}</span>
      <span class={["font-mono text-lg leading-none", @tone]}>{@value}</span>
    </div>
    """
  end

  attr :label, :string, required: true
  attr :name, :string, required: true
  attr :value, :any, required: true
  attr :min, :integer, required: true
  attr :max, :integer, required: true
  attr :step, :integer, required: true
  attr :shown, :string, required: true

  defp slider(assigns) do
    ~H"""
    <label class="block">
      <span class="flex items-baseline justify-between text-sm">
        <span>{@label}</span>
        <span class="font-mono text-xs text-base-content/70">{@shown}</span>
      </span>
      <input
        type="range"
        name={@name}
        value={@value}
        min={@min}
        max={@max}
        step={@step}
        phx-debounce="100"
        class="range range-primary range-xs w-full"
      />
    </label>
    """
  end

  attr :lever, :string, required: true
  attr :denied, :boolean, required: true

  defp lever(assigns) do
    ~H"""
    <div class="flex items-center gap-2">
      <span
        id={"lever-state-#{@lever}"}
        class={["badge badge-sm", (@denied && "badge-error") || "badge-success badge-outline"]}
      >
        {if @denied, do: "DENIED", else: "ALLOWED"}
      </span>
      <button
        id={"lever-#{@lever}"}
        phx-click={if @denied, do: "allow", else: "deny"}
        phx-value-lever={@lever}
        class={["btn btn-xs", (@denied && "btn-success") || "btn-outline btn-error"]}
      >
        {if @denied, do: "Allow", else: "Deny"}
      </button>
    </div>
    """
  end

  attr :sender, :map, required: true
  attr :consumed, :map, required: true
  attr :jetstream, :any, required: true

  # The zero-loss badge: every hook the ingest acked (`accepted`) must end up
  # consumed exactly once (`unique`); green only once the sender has stopped
  # and everything in transit has landed. The sender's snapshot trails the
  # consumers' by up to half a second, so while it runs `consumed` can briefly
  # lead `accepted`: that is in transit too, not a surplus.
  defp zero_loss(assigns) do
    missing = assigns.sender.totals.accepted - assigns.consumed.unique_total

    settled? =
      not assigns.sender.running and assigns.sender.inflight == 0 and drained?(assigns.jetstream)

    {tone, text} =
      cond do
        missing == 0 and settled? -> {"badge-success", "0 lost"}
        missing > 0 and settled? -> {"badge-warning", "#{Format.int(missing)} not consumed"}
        missing < 0 and settled? -> {"badge-warning", "#{Format.int(-missing)} extra"}
        missing > 0 -> {"badge-ghost", "#{Format.int(missing)} in transit"}
        true -> {"badge-ghost", "0 in transit"}
      end

    assigns = assign(assigns, tone: tone, text: text)

    ~H"""
    <div class="flex flex-col items-start gap-0.5">
      <span class="text-[10px] uppercase tracking-wide text-base-content/60">zero loss</span>
      <span id="zero-loss" class={["badge badge-lg font-mono", @tone]}>{@text}</span>
    </div>
    """
  end

  # Nothing left in the stream or in flight to a consumer. If JetStream cannot
  # be asked, the badge goes by the counts alone.
  defp drained?(%{messages: messages}), do: messages == 0
  defp drained?(_unavailable), do: true
end
