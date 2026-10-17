defmodule ControlPanel.Generator do
  @moduledoc """
  The synthetic webhook sender: `POST /webhooks/demo` on the ingress gateway at
  a steady rate, plus on-demand bursts, with a random body size per request.

  A tick every 100 ms starts `rate / 10` requests (the remainder is carried to
  the next tick) and drains the burst backlog. At most `@max_inflight` requests
  run at once; what the cap holds back is deferred, never dropped. Each request
  is a supervised task that retries `503`/`429`/transport errors up to three
  times, one second apart, with the *same* event id: that is the provider
  behaviour the ingest's `dedupe` and the consumer's idempotency key absorb.
  Anything else (a `4xx`, an unexpected `5xx`) fails at once.

  Publishes `{:sender, snapshot}` on the `"sender"` topic every 500 ms.
  """

  use GenServer

  @tick_ms 100
  @broadcast_ms 500
  @max_inflight 256
  # What the steady stream may queue while the in-flight cap holds it back.
  @max_deferred 2_000
  @max_retries 3
  @retry_after_ms 1_000
  @latency_windows 5

  @rate_range 0..500
  @burst_range 10..5_000
  @min_size 64
  @max_size 4 * 1024 * 1024

  # One shared 4 MiB pad: a request's body references a prefix of it instead of
  # allocating its own (the body is built as iodata).
  @pad String.duplicate("x", @max_size)

  defstruct running: false,
            rate: 20,
            burst: 500,
            size_min: 512,
            size_max: 4_096,
            carry: 0.0,
            deferred: 0,
            burst_backlog: 0,
            claim_tokens: 0.0,
            throttled: false,
            throttled_last: false,
            seq: 0,
            tasks: %{},
            totals: %{sent: 0, accepted: 0, retried: 0, failed: 0, duplicates: 0},
            window: %{sent: 0, accepted: 0, failed: 0},
            per_sec: %{sent: 0, accepted: 0, failed: 0},
            ticks: 0,
            latency: [{0, 0}]

  ## API

  def start_link(_opts), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @doc "Update any of `:rate`, `:burst`, `:size_min`, `:size_max` (clamped; `size_min <= size_max` is kept)."
  @spec set(map()) :: map()
  def set(settings), do: GenServer.call(__MODULE__, {:set, settings})

  @spec start() :: map()
  def start, do: GenServer.call(__MODULE__, {:running, true})

  @spec stop() :: map()
  def stop, do: GenServer.call(__MODULE__, {:running, false})

  @doc "Queue `burst` more requests; works whether or not the steady stream is running."
  @spec fire_burst() :: map()
  def fire_burst, do: GenServer.call(__MODULE__, :fire_burst)

  @spec snapshot() :: map()
  def snapshot, do: GenServer.call(__MODULE__, :snapshot)

  @doc "The size bounds the sliders map onto."
  def size_bounds, do: {@min_size, @max_size}

  ## Server

  @impl true
  def init(_) do
    :timer.send_interval(@tick_ms, :tick)
    :timer.send_interval(@broadcast_ms, :broadcast)
    {:ok, %__MODULE__{claim_tokens: claim_cap() * 1.0}}
  end

  @impl true
  def handle_call({:set, settings}, _from, state) do
    state = apply_settings(state, settings)
    {:reply, snapshot_of(state), state}
  end

  def handle_call({:running, true}, _from, state) do
    state = %{state | running: true}
    {:reply, snapshot_of(state), state}
  end

  def handle_call({:running, false}, _from, state) do
    # Stopping stops everything that has not been sent yet, a burst included.
    state = %{state | running: false, carry: 0.0, deferred: 0, burst_backlog: 0}
    {:reply, snapshot_of(state), state}
  end

  def handle_call(:fire_burst, _from, state) do
    state = %{state | burst_backlog: state.burst_backlog + state.burst}
    {:reply, snapshot_of(state), state}
  end

  def handle_call(:snapshot, _from, state), do: {:reply, snapshot_of(state), state}

  @impl true
  def handle_cast(:retried, state), do: {:noreply, bump(state, :retried)}

  @impl true
  def handle_info(:tick, state) do
    state = state |> schedule_requests() |> roll_window()
    {:noreply, state}
  end

  def handle_info(:broadcast, state) do
    Phoenix.PubSub.broadcast(ControlPanel.PubSub, "sender", {:sender, snapshot_of(state)})
    {:noreply, state}
  end

  # A request task finished: {outcome, latency_ms, duplicate?}.
  def handle_info({ref, {outcome, latency_ms, duplicate?}}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    {:noreply,
     state |> Map.update!(:tasks, &Map.delete(&1, ref)) |> finish(outcome, latency_ms, duplicate?)}
  end

  # A request task crashed: count it as failed so the in-flight number heals.
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    {:noreply, state |> Map.update!(:tasks, &Map.delete(&1, ref)) |> finish(:failed, nil, false)}
  end

  ## Settings

  defp apply_settings(state, settings) do
    state
    |> put_clamped(:rate, settings[:rate], @rate_range)
    |> put_clamped(:burst, settings[:burst], @burst_range)
    |> put_sizes(settings[:size_min], settings[:size_max])
  end

  defp put_clamped(state, _key, nil, _range), do: state

  defp put_clamped(state, key, value, first..last//_) when is_number(value),
    do: Map.put(state, key, value |> round() |> max(first) |> min(last))

  defp put_clamped(state, _key, _value, _range), do: state

  # Moving one bound keeps the pair ordered: the bound the caller did not touch
  # follows the one it did.
  defp put_sizes(state, nil, nil), do: state

  defp put_sizes(state, min, max) do
    min_new = clamp_size(min || state.size_min)
    max_new = clamp_size(max || state.size_max)

    {min_new, max_new} =
      cond do
        min_new <= max_new -> {min_new, max_new}
        is_nil(max) -> {min_new, min_new}
        true -> {max_new, max_new}
      end

    %{state | size_min: min_new, size_max: max_new}
  end

  defp clamp_size(value) when is_number(value),
    do: value |> round() |> max(@min_size) |> min(@max_size)

  defp clamp_size(_), do: @min_size

  ## Scheduling

  defp schedule_requests(state) do
    {n_rate, carry} =
      if state.running do
        wanted = state.rate / (1_000 / @tick_ms) + state.carry
        whole = trunc(wanted)
        {whole, wanted - whole}
      else
        {0, 0.0}
      end

    state = %{
      state
      | carry: carry,
        deferred: min(state.deferred + n_rate, @max_deferred),
        claim_tokens: min(claim_cap(), state.claim_tokens + claim_cap() / (1_000 / @tick_ms))
    }

    drain(state, max(0, @max_inflight - map_size(state.tasks)))
  end

  # Starts requests while there is room: the steady stream first, then the burst
  # backlog. A body over the ingest's inline limit is checked in to floci, which
  # holds it in memory, so those also have to fit the claim bandwidth budget; a
  # request that does not fit stops the tick (it stays queued) and the sender
  # reports `throttled`.
  defp drain(state, 0), do: state
  defp drain(%{deferred: 0, burst_backlog: 0} = state, _free), do: state

  defp drain(state, free) do
    source = if state.deferred > 0, do: :deferred, else: :burst_backlog
    size = pick_size(state)

    case take_claim_budget(state, size) do
      {:ok, state} ->
        state |> Map.update!(source, &(&1 - 1)) |> start_request(size) |> drain(free - 1)

      :starved ->
        %{state | throttled: true}
    end
  end

  defp pick_size(%{size_min: size_min, size_max: size_max}),
    do: size_min + :rand.uniform(size_max - size_min + 1) - 1

  defp take_claim_budget(state, size) do
    cond do
      size <= claim_threshold() -> {:ok, state}
      state.claim_tokens >= size -> {:ok, %{state | claim_tokens: state.claim_tokens - size}}
      true -> :starved
    end
  end

  defp claim_cap, do: Application.fetch_env!(:control_panel, :claim_bytes_per_sec)
  defp claim_threshold, do: Application.fetch_env!(:control_panel, :claim_threshold_bytes)

  defp start_request(state, size) do
    seq = state.seq + 1
    url = Application.fetch_env!(:control_panel, :gateway_url) <> "/webhooks/demo"
    generator = self()

    task =
      Task.Supervisor.async_nolink(ControlPanel.TaskSupervisor, fn ->
        run_request(generator, url, event_id(), seq, size)
      end)

    %{state | seq: seq, tasks: Map.put(state.tasks, task.ref, true)} |> bump(:sent)
  end

  ## One request (runs in a task)

  defp run_request(generator, url, event_id, seq, size) do
    attempt(generator, url, event_id, build_body(event_id, seq, size), 0)
  end

  defp attempt(generator, url, event_id, body, retries) do
    started = System.monotonic_time(:millisecond)

    result =
      Req.post(url,
        body: body,
        headers: [{"content-type", "application/json"}, {"x-demo-event-id", event_id}],
        retry: false,
        receive_timeout: 15_000,
        decode_body: false
      )

    latency = System.monotonic_time(:millisecond) - started

    case result do
      {:ok, %Req.Response{status: 201, body: response}} ->
        {:accepted, latency, duplicate?(response)}

      {:ok, %Req.Response{status: status}} when status in [503, 429] ->
        retry_or_fail(generator, url, event_id, body, retries)

      {:ok, _other} ->
        {:failed, latency, false}

      {:error, _exception} ->
        retry_or_fail(generator, url, event_id, body, retries)
    end
  end

  defp retry_or_fail(_generator, _url, _event_id, _body, retries) when retries >= @max_retries,
    do: {:failed, nil, false}

  defp retry_or_fail(generator, url, event_id, body, retries) do
    GenServer.cast(generator, :retried)
    Process.sleep(@retry_after_ms)
    attempt(generator, url, event_id, body, retries + 1)
  end

  defp duplicate?(response) when is_binary(response) do
    case JSON.decode(response) do
      {:ok, %{"duplicate" => true}} -> true
      _ -> false
    end
  end

  defp duplicate?(_), do: false

  # Exactly `size` bytes (a body can't be smaller than its own envelope).
  defp build_body(event_id, seq, size) do
    head =
      ~s({"event_id":"#{event_id}","seq":#{seq},"sent_at":#{System.system_time(:millisecond)},"pad":")

    tail = ~s("})
    pad = max(0, size - byte_size(head) - byte_size(tail))
    [head, binary_part(@pad, 0, pad), tail]
  end

  defp event_id do
    <<a::32, b::16, _::4, c::12, _::2, d::14, e::48>> = :crypto.strong_rand_bytes(16)

    hex = fn value, digits ->
      value |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(digits, "0")
    end

    "#{hex.(a, 8)}-#{hex.(b, 4)}-4#{hex.(c, 3)}-#{hex.(0x8000 + d, 4)}-#{hex.(e, 12)}"
  end

  ## Accounting

  defp finish(state, :accepted, latency_ms, duplicate?) do
    state
    |> bump(:accepted)
    |> bump(:duplicates, if(duplicate?, do: 1, else: 0))
    |> add_latency(latency_ms)
  end

  defp finish(state, :failed, _latency_ms, _duplicate?), do: bump(state, :failed)

  defp bump(state, key, by \\ 1) do
    totals = Map.update!(state.totals, key, &(&1 + by))

    window =
      if Map.has_key?(state.window, key),
        do: Map.update!(state.window, key, &(&1 + by)),
        else: state.window

    %{state | totals: totals, window: window}
  end

  defp add_latency(%{latency: [{sum, count} | rest]} = state, ms),
    do: %{state | latency: [{sum + ms, count + 1} | rest]}

  # Every 10 ticks (one second) the window becomes `per_sec` and a new one opens.
  defp roll_window(%{ticks: ticks} = state) when ticks + 1 < div(1_000, @tick_ms),
    do: %{state | ticks: ticks + 1}

  defp roll_window(state) do
    %{
      state
      | ticks: 0,
        per_sec: state.window,
        window: %{sent: 0, accepted: 0, failed: 0},
        latency: Enum.take([{0, 0} | state.latency], @latency_windows),
        throttled_last: state.throttled,
        throttled: false
    }
  end

  defp snapshot_of(state) do
    {sum, count} =
      Enum.reduce(state.latency, {0, 0}, fn {s, c}, {sum, count} -> {sum + s, count + c} end)

    %{
      running: state.running,
      rate: state.rate,
      burst: state.burst,
      size_min: state.size_min,
      size_max: state.size_max,
      inflight: map_size(state.tasks),
      totals: state.totals,
      per_sec: state.per_sec,
      mean_latency_ms: if(count == 0, do: 0, else: round(sum / count)),
      throttled: state.throttled or state.throttled_last
    }
  end
end
