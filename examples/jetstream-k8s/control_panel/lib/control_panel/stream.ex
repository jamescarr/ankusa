defmodule ControlPanel.Stream do
  @moduledoc """
  The consumed side of the demo, read straight from NATS.

  * Subscribes to the core subject `demo.consumed`, where every consumer pod
    announces each hook it processed, and keeps the last 30 announcements, a
    per-pod total and rate, and the unique/duplicate counts.
  * Asks JetStream for the `ANKUSA` stream and the `worker` consumer once a
    second (`$JS.API.*.INFO`) for the stream's depth, the consumer's pending /
    ack-pending / redelivered counts, and which node leads the stream.

  "Unique" is counted here, by idempotency key, not by the consumers' own
  `duplicate` flag: each consumer pod only remembers its own hooks, but a hook
  redelivered to a different pod is still the same hook. That is what makes
  `accepted - unique` the zero-loss figure.

  Publishes `{:consumed, ...}` every 250 ms on `"consumed"` and
  `{:jetstream, ...}` every second on `"jetstream"`.
  """

  use GenServer

  @subject "demo.consumed"
  @tail_size 30
  @broadcast_ms 250
  @jetstream_ms 1_000
  @resubscribe_ms 1_000
  @seen_cap 100_000

  defstruct conn: nil,
            tail: [],
            totals: %{},
            window: %{},
            rates: %{},
            ticks: 0,
            unique_total: 0,
            duplicate_total: 0,
            seen: MapSet.new(),
            seen_order: :queue.new(),
            jetstream: :unavailable,
            jetstream_task: nil

  ## API

  def start_link(_opts), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @doc "The `\"consumed\"` payload."
  @spec snapshot() :: map()
  def snapshot, do: GenServer.call(__MODULE__, :snapshot)

  @doc "The `\"jetstream\"` payload: a map, or `:unavailable`."
  @spec jetstream() :: map() | :unavailable
  def jetstream, do: GenServer.call(__MODULE__, :jetstream)

  ## Server

  @impl true
  def init(_) do
    send(self(), :subscribe)
    :timer.send_interval(@broadcast_ms, :broadcast)
    :timer.send_interval(@jetstream_ms, :jetstream)
    {:ok, %__MODULE__{}}
  end

  @impl true
  def handle_call(:snapshot, _from, state), do: {:reply, consumed(state), state}
  def handle_call(:jetstream, _from, state), do: {:reply, state.jetstream, state}

  @impl true
  def handle_info(:subscribe, %{conn: nil} = state) do
    # The connection supervisor registers the connection as :nats, and starts a
    # new one (under the same name) whenever the socket closes.
    with pid when is_pid(pid) <- Process.whereis(:nats),
         {:ok, _sid} <- subscribe(pid) do
      Process.monitor(pid)
      {:noreply, %{state | conn: pid}}
    else
      _not_yet ->
        Process.send_after(self(), :subscribe, @resubscribe_ms)
        {:noreply, state}
    end
  end

  def handle_info(:subscribe, state), do: {:noreply, state}

  def handle_info({:DOWN, _ref, :process, pid, _reason}, %{conn: pid} = state) do
    Process.send_after(self(), :subscribe, @resubscribe_ms)
    {:noreply, %{state | conn: nil}}
  end

  def handle_info({:msg, %{topic: @subject, body: body} = message}, state) do
    case JSON.decode(body) do
      {:ok, %{"consumer" => _, "idempotency_key" => _} = entry} ->
        state = record(state, entry)
        confirm(state.conn, message[:reply_to])
        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info(:broadcast, state) do
    state = roll_window(state)
    Phoenix.PubSub.broadcast(ControlPanel.PubSub, "consumed", {:consumed, consumed(state)})
    {:noreply, state}
  end

  def handle_info(:jetstream, %{jetstream_task: nil} = state) do
    # Capture the connection, not the state: the state holds the seen-key set.
    conn = state.conn

    task =
      Task.Supervisor.async_nolink(ControlPanel.TaskSupervisor, fn -> fetch_jetstream(conn) end)

    {:noreply, %{state | jetstream_task: task}}
  end

  def handle_info(:jetstream, state), do: {:noreply, state}

  def handle_info({ref, info}, %{jetstream_task: %Task{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    Phoenix.PubSub.broadcast(ControlPanel.PubSub, "jetstream", {:jetstream, info})
    {:noreply, %{state | jetstream: info, jetstream_task: nil}}
  end

  def handle_info(
        {:DOWN, ref, :process, _pid, _reason},
        %{jetstream_task: %Task{ref: ref}} = state
      ) do
    {:noreply, %{state | jetstream: :unavailable, jetstream_task: nil}}
  end

  def handle_info(_other, state), do: {:noreply, state}

  defp subscribe(pid) do
    Gnat.sub(pid, self(), @subject)
  catch
    # The connection process died between the lookup and the call.
    :exit, _reason -> :error
  end

  # The consumers announce a hook with a request and ack it only on this reply,
  # so a hook this ledger never recorded is redelivered instead of lost.
  defp confirm(conn, reply) when is_pid(conn) and is_binary(reply) and reply != "" do
    Gnat.pub(conn, reply, "ok")
  catch
    :exit, _reason -> :ok
  end

  defp confirm(_conn, _reply), do: :ok

  ## Recording consumed hooks

  defp record(state, %{"consumer" => consumer} = entry) do
    state = %{
      state
      | tail: Enum.take([entry | state.tail], @tail_size),
        totals: Map.update(state.totals, consumer, 1, &(&1 + 1)),
        window: Map.update(state.window, consumer, 1, &(&1 + 1))
    }

    if panel_event?(entry), do: count(state, entry), else: state
  end

  # The unique/duplicate counts back the zero-loss badge, so they only count
  # what the load generator sent: an event id that is a UUIDv4. A hand-made
  # `curl` (or run.sh's smoke hook) is still consumed, listed and rated, but it
  # was never `accepted` by the sender, so it must not show up as "extra".
  defp panel_event?(%{"dedupe_key" => key}) when is_binary(key),
    do:
      Regex.match?(
        ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/,
        key
      )

  defp panel_event?(_entry), do: false

  # A hook is a duplicate if the panel has seen its key before, whatever pod
  # announced it: a pod only remembers its own hooks.
  defp count(state, %{"idempotency_key" => key}) do
    {state, unique?} = remember(state, key)
    duplicate? = not unique?

    %{
      state
      | unique_total: state.unique_total + if(duplicate?, do: 0, else: 1),
        duplicate_total: state.duplicate_total + if(duplicate?, do: 1, else: 0)
    }
  end

  # Idempotency keys seen so far, oldest evicted first.
  defp remember(state, key) do
    if MapSet.member?(state.seen, key) do
      {state, false}
    else
      seen = MapSet.put(state.seen, key)
      order = :queue.in(key, state.seen_order)

      {seen, order} =
        if MapSet.size(seen) > @seen_cap do
          {{:value, oldest}, order} = :queue.out(order)
          {MapSet.delete(seen, oldest), order}
        else
          {seen, order}
        end

      {%{state | seen: seen, seen_order: order}, true}
    end
  end

  # Every four broadcasts (one second) the counts of the last second become
  # the per-pod rates.
  defp roll_window(%{ticks: ticks} = state) when ticks + 1 < div(1_000, @broadcast_ms),
    do: %{state | ticks: ticks + 1}

  defp roll_window(state), do: %{state | ticks: 0, rates: state.window, window: %{}}

  defp consumed(state) do
    per_consumer =
      Map.new(state.totals, fn {pod, total} ->
        {pod, %{total: total, rate: Map.get(state.rates, pod, 0)}}
      end)

    %{
      tail: state.tail,
      per_consumer: per_consumer,
      unique_total: state.unique_total,
      duplicate_total: state.duplicate_total
    }
  end

  ## JetStream state (runs in a task)

  defp fetch_jetstream(nil), do: :unavailable

  defp fetch_jetstream(conn) do
    with {:ok, stream} <- api(conn, "$JS.API.STREAM.INFO.ANKUSA"),
         {:ok, consumer} <- api(conn, "$JS.API.CONSUMER.INFO.ANKUSA.worker") do
      %{
        messages: get_in(stream, ["state", "messages"]) || 0,
        bytes: get_in(stream, ["state", "bytes"]) || 0,
        pending: consumer["num_pending"] || 0,
        ack_pending: consumer["num_ack_pending"] || 0,
        redelivered: consumer["num_redelivered"] || 0,
        leader: get_in(stream, ["cluster", "leader"])
      }
    else
      _unavailable -> :unavailable
    end
  end

  # A JetStream API reply is JSON; a missing stream or consumer is a JSON
  # `{"error": ...}`, and an unreachable server a timeout or an exit.
  defp api(conn, subject) do
    with {:ok, %{body: body}} <- Gnat.request(conn, subject, "", receive_timeout: 800),
         {:ok, %{} = decoded} <- JSON.decode(body),
         false <- Map.has_key?(decoded, "error") do
      {:ok, decoded}
    else
      _ -> :error
    end
  catch
    :exit, _reason -> :error
  end
end
