defmodule Ankusa.Dispatch.Pipeline do
  @moduledoc """
  Async dispatch pipeline: reads committed envelopes from the WAL in ascending
  `seq` order and delivers each to every sink configured on its source, with
  per-source retry/backoff and dead-lettering. At-least-once — the durable
  dispatch cursor only advances once an envelope has been fully handled.

  `start_link/1` opts: `:instance`, `:config`, and optional `:max_sleep_ms`
  which clamps every backoff sleep (so deterministic tests don't hang).
  """

  use GenServer

  alias Ankusa.{SourceStore, Telemetry, WAL}
  alias Ankusa.Dispatch.DLQ

  # ── public API ────────────────────────────────────────────────────────────

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :dispatch))
  end

  def child_spec(opts) do
    instance = Keyword.fetch!(opts, :instance)

    %{
      id: {__MODULE__, instance},
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  @doc "Run one synchronous drain pass; returns the number of envelopes handled."
  @spec tick(atom()) :: {:ok, non_neg_integer()}
  def tick(instance) do
    GenServer.call(Ankusa.via(instance, :dispatch), :tick)
  end

  # ── GenServer ─────────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    config = Keyword.fetch!(opts, :config)
    max_sleep = Keyword.get(opts, :max_sleep_ms, nil)

    state = %{
      instance: instance,
      config: config,
      cursor: WAL.get_cursor(instance, :dispatch),
      max_sleep: max_sleep
    }

    schedule(config)
    {:ok, state}
  end

  @impl true
  def handle_call(:tick, _from, state) do
    {count, state} = drain(state)
    {:reply, {:ok, count}, state}
  end

  @impl true
  def handle_info(:poll, state) do
    {_count, state} = drain(state)
    schedule(state.config)
    {:noreply, state}
  end

  # ── internals ─────────────────────────────────────────────────────────────

  defp schedule(config) do
    Process.send_after(self(), :poll, config.dispatch.poll_ms)
  end

  defp drain(state) do
    envelopes = WAL.read(state.instance, state.cursor, state.config.dispatch.batch)

    Enum.reduce(envelopes, {0, state}, fn env, {count, st} ->
      handle_envelope(env, st)
      WAL.put_cursor(st.instance, :dispatch, env.seq)
      {count + 1, %{st | cursor: env.seq}}
    end)
  end

  defp handle_envelope(env, state) do
    sinks =
      case SourceStore.fetch(state.instance, env.source_id) do
        {:ok, source} -> source.sinks
        :error -> []
      end

    Enum.each(sinks, fn sink -> deliver_with_retry(env, sink, state, 1) end)
  end

  defp deliver_with_retry(env, {mod, opts} = sink, state, attempt) do
    ctx = %{
      instance: state.instance,
      source_id: env.source_id,
      tenant_id: env.tenant_id,
      attempt: attempt
    }

    case mod.deliver(env, ctx, opts) do
      :ok ->
        Telemetry.emit([:dispatch, :stop], %{}, %{result: :ok, attempts: attempt})
        :ok

      {:error, reason} ->
        {rmod, ropts} = state.config.dispatch.retry

        case rmod.backoff(attempt, ropts) do
          {:retry, delay} ->
            sleep(delay, state.max_sleep)
            deliver_with_retry(env, sink, state, attempt + 1)

          :give_up ->
            DLQ.write(state.config, env, {:sink, mod, reason})
            Telemetry.emit([:dispatch, :dlq], %{}, %{source_id: env.source_id, sink: mod})
            Telemetry.emit([:dispatch, :stop], %{}, %{result: :dlq, attempts: attempt})
            :ok
        end
    end
  end

  defp sleep(delay, nil), do: Process.sleep(delay)
  defp sleep(delay, cap), do: Process.sleep(min(delay, cap))
end
