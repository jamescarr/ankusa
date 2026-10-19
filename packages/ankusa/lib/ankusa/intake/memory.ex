defmodule Ankusa.Intake.Memory do
  @moduledoc """
  An in-memory `Ankusa.Intake` for tests, benches and the fleet drill's fake. Not
  a production mode: its "replicas" are one process.

  Options (`intake.memory`): `path` (a file every record is appended to and the
  log is reloaded from at start, so a replacement node started on an empty data
  directory finds the log of the node it replaces) and `latency_ms` (every
  confirm is delayed by it).

  Faults are injected per call, by instance, from a test:

    * `fail(instance, class, count)` — the next `count` publishes answer
      `{:error, class}` and store **nothing**
    * `lost_ack(instance, class, count)` — the next `count` publishes **store**
      the batch and answer `{:error, class}` (a timeout the broker completed)
    * `hold(instance)` / `release(instance)` — publishes wait, unanswered
    * `hold_replay(instance)` / `release_replay(instance)` — replay waits
    * `set_latency/2`, `set_ready/2`, `set_verify/2`, `set_earliest/2`
    * `put_raw(instance, record)` — a record as it would sit in the log

  `records/1` reads the log back.
  """

  @behaviour Ankusa.Intake

  use GenServer

  alias Ankusa.Intake

  @call_timeout 5_000

  # ── adapter ───────────────────────────────────────────────────────────────

  @impl Ankusa.Intake
  def child_spec(opts) do
    instance = Keyword.fetch!(opts, :instance)
    %{id: {__MODULE__, instance}, start: {__MODULE__, :start_link, [opts]}}
  end

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :intake_memory))
  end

  @impl Ankusa.Intake
  def publish_batch(instance, records, opts) do
    timeout = Keyword.get(opts, :timeout, 2_000)
    node = Keyword.get(opts, :node)

    try do
      GenServer.call(server(instance), {:publish, node, records}, timeout)
    catch
      :exit, _ -> {:error, :unavailable}
    end
  end

  @impl Ankusa.Intake
  def replay(instance, from, fun, opts) do
    node = Keyword.get(opts, :node)

    case call(instance, {:replay_records, from}, :infinity) do
      {:error, class} ->
        {:error, class}

      {:ok, records} ->
        records
        |> Enum.filter(&(&1.node == node))
        |> Enum.reduce_while({:ok, nil}, fn record, {:ok, last} ->
          if Intake.known?(record.kind, record.version) do
            case fun.(record) do
              :ok -> {:cont, {:ok, record.position}}
              {:error, _reason} -> {:halt, {:error, :unavailable}}
            end
          else
            Intake.skipped(instance)
            {:cont, {:ok, last}}
          end
        end)
    end
  end

  @impl Ankusa.Intake
  def ready?(instance), do: call(instance, :ready?, @call_timeout) == true

  @impl Ankusa.Intake
  def verify(instance), do: call(instance, :verify, @call_timeout)

  @impl Ankusa.Intake
  def describe(_instance), do: "in-memory test log, acks=all simulated"

  @impl Ankusa.Intake
  def earliest(instance), do: call(instance, :earliest, @call_timeout)

  # ── test controls ─────────────────────────────────────────────────────────

  @doc "The next `count` publishes (default: all) fail with `class` and store nothing."
  def fail(instance, class, count \\ :infinity),
    do: GenServer.call(server(instance), {:fault, :fail, class, count})

  @doc "The next `count` publishes store the batch and answer `class`: a lost ack."
  def lost_ack(instance, class, count \\ 1),
    do: GenServer.call(server(instance), {:fault, :lost_ack, class, count})

  @doc "Clears every injected publish fault."
  def heal(instance), do: GenServer.call(server(instance), :heal)

  def hold(instance), do: GenServer.call(server(instance), {:hold, true})
  def release(instance), do: GenServer.call(server(instance), {:hold, false})
  def hold_replay(instance), do: GenServer.call(server(instance), {:hold_replay, true})
  def release_replay(instance), do: GenServer.call(server(instance), {:hold_replay, false})
  def set_latency(instance, ms), do: GenServer.call(server(instance), {:latency, ms})
  def set_ready(instance, ready?), do: GenServer.call(server(instance), {:ready, ready?})
  def set_verify(instance, result), do: GenServer.call(server(instance), {:verify, result})

  def set_earliest(instance, position),
    do: GenServer.call(server(instance), {:earliest, position})

  @doc "A record exactly as it sits in the log (a test of unknown kinds and versions)."
  def put_raw(instance, record), do: GenServer.call(server(instance), {:put_raw, record})

  @doc "Every record of the log, in position order."
  def records(instance), do: GenServer.call(server(instance), :records)

  @doc "How many publish calls the log has seen (confirmed or not)."
  def publish_calls(instance), do: GenServer.call(server(instance), :publish_calls)

  # ── server ────────────────────────────────────────────────────────────────

  @impl GenServer
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    options = Ankusa.config(instance).intake.memory
    path = Keyword.get(options, :path)

    state = %{
      path: path,
      records: %{},
      next: 0,
      latency: Keyword.get(options, :latency_ms, 0),
      faults: [],
      held: false,
      waiting: [],
      replay_held: false,
      replay_waiting: [],
      ready: true,
      verify: :ok,
      earliest: :auto,
      calls: 0
    }

    {:ok, load(state)}
  end

  @impl GenServer
  def handle_call({:publish, node, records}, from, state) do
    state = %{state | calls: state.calls + 1}

    if state.held do
      {:noreply, %{state | waiting: state.waiting ++ [{from, node, records}]}}
    else
      {:noreply, publish(state, from, node, records)}
    end
  end

  def handle_call({:replay_records, _from_position} = request, from, state) do
    if state.replay_held,
      do: {:noreply, %{state | replay_waiting: [{from, request} | state.replay_waiting]}},
      else: {:reply, replay_reply(state, request), state}
  end

  def handle_call({:fault, kind, class, count}, _from, state),
    do: {:reply, :ok, %{state | faults: state.faults ++ [{kind, class, count}]}}

  def handle_call(:heal, _from, state), do: {:reply, :ok, %{state | faults: []}}

  def handle_call({:hold, true}, _from, state), do: {:reply, :ok, %{state | held: true}}

  def handle_call({:hold, false}, _from, state) do
    state =
      Enum.reduce(state.waiting, %{state | held: false, waiting: []}, fn {from, node, records},
                                                                         acc ->
        publish(acc, from, node, records)
      end)

    {:reply, :ok, state}
  end

  def handle_call({:hold_replay, true}, _from, state),
    do: {:reply, :ok, %{state | replay_held: true}}

  def handle_call({:hold_replay, false}, _from, state) do
    Enum.each(Enum.reverse(state.replay_waiting), fn {from, request} ->
      GenServer.reply(from, replay_reply(state, request))
    end)

    {:reply, :ok, %{state | replay_held: false, replay_waiting: []}}
  end

  def handle_call({:latency, ms}, _from, state), do: {:reply, :ok, %{state | latency: ms}}
  def handle_call({:ready, ready?}, _from, state), do: {:reply, :ok, %{state | ready: ready?}}
  def handle_call({:verify, result}, _from, state), do: {:reply, :ok, %{state | verify: result}}

  def handle_call({:earliest, position}, _from, state),
    do: {:reply, :ok, %{state | earliest: position}}

  def handle_call(:ready?, _from, state), do: {:reply, state.ready, state}
  def handle_call(:verify, _from, state), do: {:reply, state.verify, state}
  def handle_call(:publish_calls, _from, state), do: {:reply, state.calls, state}

  def handle_call(:earliest, _from, %{earliest: :auto} = state) do
    first = state.records |> Map.keys() |> Enum.min(fn -> nil end)
    {:reply, {:ok, first}, state}
  end

  def handle_call(:earliest, _from, state), do: {:reply, {:ok, state.earliest}, state}

  def handle_call({:put_raw, record}, _from, state) do
    position = state.next
    {:reply, {:ok, position}, store(state, [Map.put(record, :position, position)])}
  end

  def handle_call(:records, _from, state), do: {:reply, sorted(state), state}

  @impl GenServer
  def handle_info({:reply, from, reply}, state) do
    GenServer.reply(from, reply)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  # ── internals ─────────────────────────────────────────────────────────────

  defp server(instance), do: Ankusa.via(instance, :intake_memory)

  defp call(instance, request, timeout) do
    GenServer.call(server(instance), request, timeout)
  catch
    :exit, _ -> {:error, :unavailable}
  end

  defp publish(state, from, node, records) do
    {fault, state} = take_fault(state)

    case fault do
      {:fail, class} ->
        reply(state, from, {:error, class})
        state

      {:lost_ack, class} ->
        {_position, state} = append(state, node, records)
        reply(state, from, {:error, class})
        state

      nil ->
        {position, state} = append(state, node, records)
        reply(state, from, {:ok, %{position: position}})
        state
    end
  end

  defp reply(%{latency: latency}, from, reply) when latency > 0,
    do: Process.send_after(self(), {:reply, from, reply}, latency)

  defp reply(_state, from, reply), do: GenServer.reply(from, reply)

  defp take_fault(%{faults: []} = state), do: {nil, state}

  defp take_fault(%{faults: [{kind, class, count} | rest]} = state) do
    faults =
      case count do
        :infinity -> [{kind, class, count} | rest]
        1 -> rest
        n -> [{kind, class, n - 1} | rest]
      end

    {{kind, class}, %{state | faults: faults}}
  end

  defp append(state, node, records) do
    stamped =
      records
      |> Enum.with_index(state.next)
      |> Enum.map(fn {{seq, bin, meta}, position} ->
        %{
          position: position,
          node: node,
          seq: seq,
          id: meta.id,
          kind: "hook",
          version: "1",
          bin: bin,
          tenant: meta.tenant,
          source: meta.source,
          incarnation: meta.incarnation
        }
      end)

    {position, state} =
      case stamped do
        [] -> {state.next - 1, state}
        _ -> {List.last(stamped).position, store(state, stamped)}
      end

    {position, state}
  end

  # The log file is the test's own (`intake.memory.path`), never request input.
  # sobelow_skip ["Traversal.FileModule"]
  defp store(state, stamped) do
    records = Enum.reduce(stamped, state.records, &Map.put(&2, &1.position, &1))

    if state.path do
      data = for record <- stamped, into: <<>>, do: frame(record)
      File.write!(state.path, data, [:append, :binary])
    end

    %{state | records: records, next: Enum.max(Enum.map(stamped, & &1.position)) + 1}
  end

  defp frame(record) do
    bin = :erlang.term_to_binary(record)
    <<byte_size(bin)::32, bin::binary>>
  end

  defp load(%{path: nil} = state), do: state

  # sobelow_skip ["Traversal.FileModule"]
  defp load(%{path: path} = state) do
    case File.read(path) do
      {:ok, data} ->
        records = unframe(data, [])
        next = if records == [], do: 0, else: Enum.max(Enum.map(records, & &1.position)) + 1
        %{state | records: Map.new(records, &{&1.position, &1}), next: next}

      {:error, _} ->
        File.mkdir_p!(Path.dirname(path))
        state
    end
  end

  # The file is the log this process wrote itself.
  # sobelow_skip ["Misc.BinToTerm"]
  defp unframe(<<size::32, bin::binary-size(size), rest::binary>>, acc),
    do: unframe(rest, [:erlang.binary_to_term(bin, [:safe]) | acc])

  defp unframe(_partial, acc), do: Enum.reverse(acc)

  defp sorted(state), do: state.records |> Map.values() |> Enum.sort_by(& &1.position)

  defp replay_reply(state, {:replay_records, from}) do
    floor = if from == :earliest, do: 0, else: from
    {:ok, Enum.filter(sorted(state), &(&1.position >= floor))}
  end
end
