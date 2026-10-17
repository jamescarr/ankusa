defmodule ControlPanelWeb.DashboardLive.Flow do
  @moduledoc """
  The live flow diagram: sender -> Istio gateway -> ankusa pods -> JetStream ->
  consumer pods, with the claim-check side path (floci, claim-check gateway)
  underneath. Pure HEEx/SVG, no JavaScript: every number comes from the
  snapshots the dashboard LiveView already holds.

  Geometry is fixed (`viewBox` 1000x560, columns at x = 20 / 200 / 380 / 580 /
  780); a column of pods is centred on the flow's axis, at most eight boxes
  tall, with a `+N more` line for the rest.
  """

  use Phoenix.Component

  alias ControlPanelWeb.DashboardLive.Format

  @axis 232
  @pod_w 160
  @pod_h 36
  @pod_gap 8
  @max_pods 8

  @x_sender 20
  @x_gateway 200
  @x_ankusa 380
  @x_jetstream 580
  @x_consumer 780

  @nats_pods ~w(nats-0 nats-1 nats-2)

  attr :sender, :map, required: true
  attr :cluster, :map, required: true
  attr :ingest, :map, required: true
  attr :consumed, :map, required: true
  attr :jetstream, :any, required: true

  def flow(assigns) do
    ankusa = layout(pods(assigns.cluster, "ankusa"))
    consumers = layout(pods(assigns.cluster, "consumer"))

    ingest_rate =
      ankusa.pods
      |> Enum.map(fn {pod, _y} -> pod_rate(assigns.ingest, pod.name) end)
      |> Enum.sum()

    consumed_rate =
      assigns.consumed.per_consumer |> Map.values() |> Enum.map(& &1.rate) |> Enum.sum()

    tail = assigns.consumed.tail

    claim_share =
      if tail == [], do: 0, else: Enum.count(tail, &(&1["via"] == "claim")) / length(tail)

    assigns =
      assign(assigns,
        ankusa: ankusa,
        consumers: consumers,
        ingest_rate: ingest_rate,
        consumed_rate: consumed_rate,
        claim_rate: consumed_rate * claim_share,
        denies: assigns.cluster.denies,
        floci: first_pod(assigns.cluster, "floci"),
        claim_check: first_pod(assigns.cluster, "claim-check"),
        nats: Map.new(pods(assigns.cluster, "nats"), &{&1.name, &1})
      )

    ~H"""
    <svg
      viewBox="0 0 1000 560"
      class="w-full h-auto"
      role="img"
      aria-label="Webhook flow through the cluster"
    >
      <g class="fill-base-content/60 text-[11px] uppercase tracking-wide">
        <text x={x_sender()} y="34">sender</text>
        <text x={x_gateway()} y="34">istio gateway</text>
        <text x={x_ankusa()} y="34">ankusa · direct</text>
        <text x={x_jetstream()} y="34">jetstream · R3</text>
        <text x={x_consumer()} y="34">consumers · keda</text>
      </g>

      <%!-- edges first, so the boxes sit on top of their ends --%>
      <.edge
        x1={x_sender() + 130}
        y1={axis()}
        x2={x_gateway()}
        y2={axis()}
        rate={@sender.per_sec.sent}
      />
      <text
        x={x_sender() + 165}
        y={axis() - 8}
        text-anchor="middle"
        class="fill-base-content/70 text-[9px]"
      >
        {Format.rate(@sender.per_sec.sent)}/s
      </text>

      <%= for {pod, y} <- @ankusa.pods do %>
        <.edge
          x1={x_gateway() + 130}
          y1={axis()}
          x2={x_ankusa()}
          y2={y + pod_h() / 2}
          rate={pod_rate(@ingest, pod.name)}
        />
        <.edge
          x1={x_ankusa() + pod_w()}
          y1={y + pod_h() / 2}
          x2={x_jetstream()}
          y2={axis()}
          rate={pod_rate(@ingest, pod.name)}
          denied={"ankusa-nats" in @denies}
        />
      <% end %>
      <%= for {pod, y} <- @consumers.pods do %>
        <.edge
          x1={x_jetstream() + pod_w()}
          y1={axis()}
          x2={x_consumer()}
          y2={y + pod_h() / 2}
          rate={consumer_rate(@consumed, pod.name)}
          denied={"consumer-nats" in @denies}
        />
      <% end %>

      <.edge x1={x_ankusa() + 80} y1="444" x2={x_ankusa() + 80} y2="480" rate={@claim_rate} side />
      <.edge
        x1={x_consumer() + 80}
        y1="444"
        x2={x_jetstream() + pod_w()}
        y2="498"
        rate={@claim_rate}
        denied={"consumer-claim-check" in @denies}
        side
      />
      <.edge x1={x_jetstream()} y1="498" x2={x_ankusa() + pod_w()} y2="498" rate={@claim_rate} side />
      <g class="fill-base-content/60 text-[9px]">
        <text x={x_ankusa() + 86} y="466">check-in</text>
        <text x={x_consumer() - 6} y="478" text-anchor="end">redeem</text>
      </g>

      <%!-- the denied levers, labelled once each --%>
      <g class="fill-error text-[10px] font-semibold" text-anchor="middle">
        <text :if={"ankusa-nats" in @denies} x={x_ankusa() + pod_w() + 20} y="50">⛔ denied</text>
        <text :if={"consumer-nats" in @denies} x={x_jetstream() + pod_w() + 20} y="50">⛔ denied</text>
        <text :if={"consumer-claim-check" in @denies} x={x_consumer() + 10} y="512">⛔ denied</text>
      </g>

      <%!-- sender --%>
      <g transform={"translate(#{x_sender()},#{axis() - 28})"}>
        <rect
          width="130"
          height="56"
          rx="8"
          class={["fill-base-200", (@sender.running && "stroke-success") || "stroke-base-content/40"]}
          stroke-width="2"
        />
        <text x="10" y="20" class="fill-base-content text-[12px] font-semibold">load generator</text>
        <text x="10" y="36" class="fill-base-content/70 text-[10px]">
          {if @sender.running, do: "#{@sender.rate}/s target", else: "stopped"}
        </text>
        <text x="10" y="49" class="fill-base-content/70 text-[10px]">
          {@sender.inflight} in flight
        </text>
      </g>

      <%!-- gateway --%>
      <g transform={"translate(#{x_gateway()},#{axis() - 28})"}>
        <rect
          width="130"
          height="56"
          rx="8"
          class={["fill-base-200", (@cluster.fault && "stroke-warning") || "stroke-base-content/40"]}
          stroke-width="2"
        />
        <text x="10" y="20" class="fill-base-content text-[12px] font-semibold">Envoy ingress</text>
        <text x="10" y="36" class="fill-base-content/70 text-[10px]">POST /webhooks/demo</text>
        <text
          x="10"
          y="49"
          class={[(@cluster.fault && "fill-warning") || "fill-base-content/70", "text-[10px]"]}
        >
          fault: {fault_label(@cluster.fault)}
        </text>
      </g>

      <%!-- ankusa pods --%>
      <.pod_box
        :for={{pod, y} <- @ankusa.pods}
        pod={pod}
        x={x_ankusa()}
        y={y}
        sub={ankusa_sub(@ingest, pod)}
        killable
      />
      <text :if={@ankusa.more > 0} x={x_ankusa()} y="414" class="fill-base-content/60 text-[10px]">
        +{@ankusa.more} more
      </text>
      <.chip x={x_ankusa()} y={424} text={ankusa_hpa(@cluster.hpa.ankusa)} />

      <%!-- jetstream --%>
      <g transform={"translate(#{x_jetstream()},#{axis() - 60})"}>
        <rect
          width="160"
          height="120"
          rx="8"
          class={[
            "fill-base-200",
            (@jetstream == :unavailable && "stroke-warning") || "stroke-primary"
          ]}
          stroke-width="2"
        />
        <text x="80" y="22" text-anchor="middle" class="fill-base-content text-[12px] font-semibold">
          JetStream · ANKUSA
        </text>
        <g :if={@jetstream == :unavailable}>
          <text x="80" y="64" text-anchor="middle" class="fill-warning text-[11px]">unavailable</text>
        </g>
        <g :if={@jetstream != :unavailable} class="fill-base-content/80 text-[10.5px]">
          <text x="10" y="44">
            {Format.int(@jetstream.messages)} msgs · {Format.bytes(@jetstream.bytes)}
          </text>
          <text x="10" y="60">pending {Format.int(@jetstream.pending)}</text>
          <text x="10" y="76">ack-pending {Format.int(@jetstream.ack_pending)}</text>
          <text x="10" y="92">redelivered {Format.int(@jetstream.redelivered)}</text>
          <text x="10" y="108">leader {String.slice(@jetstream.leader || "–", 0, 18)}</text>
        </g>
      </g>
      <g
        :for={{name, i} <- Enum.with_index(nats_pods())}
        transform={"translate(#{x_jetstream() + i * 55},#{axis() + 68})"}
      >
        <.nats_chip pod={@nats[name]} name={name} />
      </g>

      <%!-- consumer pods --%>
      <.pod_box
        :for={{pod, y} <- @consumers.pods}
        pod={pod}
        x={x_consumer()}
        y={y}
        sub={consumer_sub(@consumed, pod)}
        killable
      />
      <text
        :if={@consumers.more > 0}
        x={x_consumer()}
        y="414"
        class="fill-base-content/60 text-[10px]"
      >
        +{@consumers.more} more
      </text>
      <.chip x={x_consumer()} y={424} text={consumer_keda(@cluster.hpa.consumer)} />

      <%!-- the claim-check side path --%>
      <.pod_box :if={@floci} pod={@floci} x={x_ankusa()} y={480} sub="S3 emulator · claim store" />
      <.pod_box
        :if={@claim_check}
        pod={@claim_check}
        x={x_jetstream()}
        y={480}
        sub="gateway · redeems claims"
        killable
      />
    </svg>
    """
  end

  ## Parts

  attr :x1, :any, required: true
  attr :y1, :any, required: true
  attr :x2, :any, required: true
  attr :y2, :any, required: true
  attr :rate, :any, default: 0
  attr :denied, :boolean, default: false
  attr :side, :boolean, default: false

  # A flowing edge dashes in the direction of travel, faster and thicker with
  # the rate; an idle one is a faint static line; a denied one is red and still.
  defp edge(assigns) do
    assigns = assign(assigns, state: edge_state(assigns.denied, assigns.rate))

    ~H"""
    <line
      x1={@x1}
      y1={@y1}
      x2={@x2}
      y2={@y2}
      stroke-width={edge_width(@rate, @state)}
      class={[
        "fill-none",
        edge_class(@state),
        @side && @state == :flow && "opacity-60"
      ]}
      style={@state == :flow && "animation-duration: #{edge_duration(@rate)}s"}
    />
    """
  end

  attr :pod, :map, required: true
  attr :x, :integer, required: true
  attr :y, :any, required: true
  attr :sub, :string, required: true
  attr :killable, :boolean, default: false

  defp pod_box(assigns) do
    ~H"""
    <g transform={"translate(#{@x},#{@y})"}>
      <title>{@pod.name} · {@pod.phase}{(@pod.ip && " · " <> @pod.ip) || ""}</title>
      <rect
        width={pod_w()}
        height={pod_h()}
        rx="6"
        class={["fill-base-200", pod_stroke(@pod)]}
        stroke-width="2"
        stroke-dasharray={@pod.terminating? && "4 3"}
      />
      <text x="10" y="15" class="fill-base-content text-[11px] font-semibold">
        {Format.short_name(@pod.name, @pod.group)}
      </text>
      <text x="10" y="29" class="fill-base-content/70 text-[10px]">{pod_sub(@pod, @sub)}</text>
      <g
        :if={@killable and not @pod.terminating?}
        phx-click="kill"
        phx-value-pod={@pod.name}
        class="cursor-pointer"
      >
        <title>kill {@pod.name} (SIGKILL)</title>
        <rect x="138" y="4" width="18" height="18" rx="4" class="fill-base-300 hover:fill-error" />
        <text
          x="147"
          y="17"
          text-anchor="middle"
          class="fill-base-content text-[11px] pointer-events-none"
        >
          ✕
        </text>
      </g>
    </g>
    """
  end

  attr :pod, :any, default: nil
  attr :name, :string, required: true

  defp nats_chip(assigns) do
    ~H"""
    <rect
      width="50"
      height="20"
      rx="5"
      class={["fill-base-200", (@pod && pod_stroke(@pod)) || "stroke-base-content/30"]}
      stroke-width="2"
      stroke-dasharray={@pod && @pod.terminating? && "4 3"}
    />
    <text x="6" y="14" class="fill-base-content text-[9.5px]">{@name}</text>
    <g
      :if={@pod && not @pod.terminating?}
      phx-click="kill"
      phx-value-pod={@pod.name}
      class="cursor-pointer"
    >
      <title>kill {@pod.name} (SIGKILL)</title>
      <rect x="37" y="3" width="11" height="14" rx="3" class="fill-base-300 hover:fill-error" />
      <text
        x="42.5"
        y="13.5"
        text-anchor="middle"
        class="fill-base-content text-[9px] pointer-events-none"
      >
        ✕
      </text>
    </g>
    """
  end

  attr :x, :integer, required: true
  attr :y, :integer, required: true
  attr :text, :string, required: true

  defp chip(assigns) do
    ~H"""
    <g transform={"translate(#{@x},#{@y})"}>
      <rect width="160" height="20" rx="10" class="fill-base-300" />
      <text x="80" y="14" text-anchor="middle" class="fill-base-content text-[10px]">{@text}</text>
    </g>
    """
  end

  ## Layout

  defp pods(cluster, group) do
    cluster.pods
    |> Enum.filter(&(&1.group == group))
    |> Enum.sort_by(&{&1.started_at || "", &1.name})
  end

  defp first_pod(cluster, group), do: cluster |> pods(group) |> List.first()

  # The visible pods with their y, centred on the flow's axis, and how many
  # did not fit.
  defp layout(pods) do
    {shown, rest} = Enum.split(pods, @max_pods)
    count = length(shown)
    top = @axis - (count * (@pod_h + @pod_gap) - @pod_gap) / 2
    ys = for i <- 0..(count - 1)//1, do: top + i * (@pod_h + @pod_gap)
    %{pods: Enum.zip(shown, ys), more: length(rest)}
  end

  defp axis, do: @axis
  defp pod_w, do: @pod_w
  defp pod_h, do: @pod_h
  defp x_sender, do: @x_sender
  defp x_gateway, do: @x_gateway
  defp x_ankusa, do: @x_ankusa
  defp x_jetstream, do: @x_jetstream
  defp x_consumer, do: @x_consumer
  defp nats_pods, do: @nats_pods

  ## Rates and labels

  defp pod_rate(ingest, name), do: get_in(ingest, [name, :rate]) || 0.0
  defp consumer_rate(consumed, name), do: get_in(consumed.per_consumer, [name, :rate]) || 0

  defp ankusa_sub(ingest, pod) do
    case ingest[pod.name] do
      nil -> "– /s"
      %{rate: rate, failed: 0} -> "#{Format.rate(rate)}/s"
      %{rate: rate, failed: failed} -> "#{Format.rate(rate)}/s · #{Format.int(failed)} err"
    end
  end

  defp consumer_sub(consumed, pod) do
    case consumed.per_consumer[pod.name] do
      nil -> "0/s"
      %{rate: rate, total: total} -> "#{Format.rate(rate)}/s · #{Format.int(total)} done"
    end
  end

  defp pod_sub(%{terminating?: true}, _sub), do: "terminating…"
  defp pod_sub(%{ready?: false}, _sub), do: "not ready"
  defp pod_sub(_pod, sub), do: sub

  defp ankusa_hpa(nil), do: "HPA –"
  defp ankusa_hpa(%{current: cur, desired: des, cpu_pct: nil}), do: "HPA #{cur}/#{des}"

  defp ankusa_hpa(%{current: cur, desired: des, cpu_pct: cpu}),
    do: "HPA #{cur}/#{des} · cpu #{cpu}%"

  defp consumer_keda(nil), do: "KEDA –"

  defp consumer_keda(%{current: cur, desired: des, lag: lag}),
    do: "KEDA #{cur}/#{des} · lag #{Format.int(lag)}"

  defp fault_label(nil), do: "none"
  defp fault_label(%{mode: :abort, pct: pct, status: status}), do: "#{status} abort #{pct}%"
  defp fault_label(%{mode: :delay, pct: pct, ms: ms}), do: "delay #{ms}ms #{pct}%"

  ## Styling

  defp pod_stroke(%{terminating?: true}), do: "stroke-error"
  defp pod_stroke(%{ready?: true}), do: "stroke-success"
  defp pod_stroke(_not_ready), do: "stroke-warning"

  defp edge_state(true, _rate), do: :denied
  defp edge_state(false, rate) when rate > 0, do: :flow
  defp edge_state(false, _rate), do: :idle

  defp edge_class(:flow), do: ["flow", "stroke-info"]
  defp edge_class(:denied), do: ["flow-denied", "stroke-error"]
  defp edge_class(:idle), do: ["flow-idle", "stroke-base-content"]

  defp edge_width(rate, :flow), do: 1 + min(6, rate / 25)
  defp edge_width(_rate, _state), do: 1.5

  defp edge_duration(rate), do: Float.round(max(0.3, 3 / (1 + rate / 50)), 2)
end
