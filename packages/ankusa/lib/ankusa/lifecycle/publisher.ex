defmodule Ankusa.Lifecycle.Publisher do
  @max_pending 10_000
  @max_concurrency 8

  @moduledoc """
  Delivers lifecycle events (`Ankusa.Lifecycle`) to `config.lifecycle.sinks`
  from memory, bypassing the store.

  One supervised process per instance. `publish/4` is a cast: the admin call
  that made the change returns without waiting on a broker. Each event becomes
  one job per sink, so a sink that is down never holds back another. A failed
  job is retried after the delay `config.dispatch.retry` gives (an
  `Ankusa.RetryPolicy`); when the policy gives up the job is dropped.
  An attempt that has not returned after `config.dispatch.attempt_timeout_ms` is
  killed and counts as a failed attempt.

  Everything is best effort and in memory:

    * the queue is bounded at #{@max_pending} pending sink deliveries (a job that is
      waiting, running, or sleeping before a retry); an event that does not fit
      is dropped;
    * pending jobs are lost when the node stops;
    * jobs run concurrently (at most #{@max_concurrency} at a time), so there is no ordering.

  Every drop is logged and emitted as `[:ankusa, :lifecycle, :dropped]` with a
  `:reason` of `:not_running`, `:queue_full`, or `:gave_up`; a confirmed
  delivery is `[:ankusa, :lifecycle, :delivered]`.

  Sinks are called without a `ctx.claim`, so a sink with an inline threshold
  checks an oversized body in itself (`Ankusa.Sink.Message.encode/3`).
  """

  use GenServer

  require Logger

  alias Ankusa.{Envelope, Sink, Telemetry}

  @type job :: %{
          env: Envelope.t(),
          type: String.t(),
          subject: String.t(),
          sink: {module(), keyword()},
          attempt: pos_integer()
        }

  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :instance)},
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  @doc """
  Start the publisher for `opts[:instance]`. `opts[:max_pending]` overrides the
  queue bound.
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts,
      name: Ankusa.via(Keyword.fetch!(opts, :instance), :lifecycle)
    )
  end

  @doc """
  Queue `env` for delivery to every lifecycle sink. Always `:ok`: the caller's
  change has already happened, and a lifecycle failure never fails it.
  """
  @spec publish(atom(), Envelope.t(), String.t(), String.t()) :: :ok
  def publish(instance, %Envelope{} = env, type, subject) do
    case Ankusa.whereis(instance, :lifecycle) do
      nil ->
        Logger.error(
          "[ankusa] lifecycle event #{type} for #{subject} dropped: publisher not running"
        )

        for {mod, _opts} <- Ankusa.config(instance).lifecycle.sinks do
          dropped(instance, type, mod, :not_running)
        end

        :ok

      pid ->
        GenServer.cast(pid, {:publish, env, type, subject})
    end
  end

  @impl true
  def init(opts) do
    instance = Keyword.fetch!(opts, :instance)
    config = Ankusa.config(instance)
    {:ok, sup} = Task.Supervisor.start_link()

    {:ok,
     %{
       instance: instance,
       sinks: config.lifecycle.sinks,
       retry: config.dispatch.retry,
       attempt_timeout_ms: config.dispatch.attempt_timeout_ms,
       max_pending: Keyword.get(opts, :max_pending, @max_pending),
       sup: sup,
       ready: :queue.new(),
       running: %{},
       pending: 0
     }}
  end

  # Crash reports print the state: the sinks carry credentials and the queued
  # jobs carry the lifecycle envelopes.
  @impl true
  def format_status(%{state: %{sinks: sinks} = state} = status) do
    %{
      status
      | state: %{
          state
          | sinks: Enum.map(sinks, &elem(&1, 0)),
            ready: :queue.len(state.ready),
            running: map_size(state.running)
        }
    }
  end

  def format_status(status), do: status

  @impl true
  def handle_cast({:publish, env, type, subject}, state) do
    count = length(state.sinks)

    if state.pending + count > state.max_pending do
      Logger.error(
        "[ankusa] lifecycle event #{type} for #{subject} dropped: publisher queue full"
      )

      for {mod, _opts} <- state.sinks, do: dropped(state.instance, type, mod, :queue_full)
      {:noreply, state}
    else
      ready =
        Enum.reduce(state.sinks, state.ready, fn sink, ready ->
          :queue.in(%{env: env, type: type, subject: subject, sink: sink, attempt: 1}, ready)
        end)

      {:noreply, start_jobs(%{state | ready: ready, pending: state.pending + count})}
    end
  end

  @impl true
  def handle_info({ref, result}, %{running: running} = state) when is_map_key(running, ref) do
    Process.demonitor(ref, [:flush])
    {%{job: job, timer: timer}, running} = Map.pop!(running, ref)
    Process.cancel_timer(timer)

    {:noreply, %{state | running: running} |> finished(job, result) |> start_jobs()}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{running: running} = state)
      when is_map_key(running, ref) do
    {%{job: job, timer: timer}, running} = Map.pop!(running, ref)
    Process.cancel_timer(timer)

    {:noreply, %{state | running: running} |> retry(job, {:exit, reason}) |> start_jobs()}
  end

  # The attempt outlasted `dispatch.attempt_timeout_ms`: kill it and count a
  # failed attempt. A reply that landed in the mailbox meanwhile still counts.
  def handle_info({:attempt_timeout, ref}, %{running: running} = state)
      when is_map_key(running, ref) do
    {%{job: job, task: task}, running} = Map.pop!(running, ref)
    state = %{state | running: running}

    state =
      case Task.shutdown(task, :brutal_kill) do
        {:ok, result} ->
          finished(state, job, result)

        {:exit, reason} ->
          retry(state, job, {:exit, reason})

        nil ->
          {mod, _opts} = job.sink

          Logger.warning(
            "[ankusa] lifecycle event #{job.type} for #{job.subject}: #{inspect(mod)} did not " <>
              "finish within #{state.attempt_timeout_ms}ms; attempt #{job.attempt} failed"
          )

          retry(state, job, {:attempt_timeout, state.attempt_timeout_ms})
      end

    {:noreply, start_jobs(state)}
  end

  def handle_info({:retry, job}, state) do
    {:noreply, start_jobs(%{state | ready: :queue.in(job, state.ready)})}
  end

  def handle_info(_other, state), do: {:noreply, state}

  defp start_jobs(%{running: running} = state) when map_size(running) >= @max_concurrency,
    do: state

  defp start_jobs(state) do
    case :queue.out(state.ready) do
      {:empty, _ready} ->
        state

      {{:value, job}, ready} ->
        {mod, opts} = job.sink

        ctx = %{
          instance: state.instance,
          source_id: job.env.source_id,
          tenant_id: job.env.tenant_id,
          attempt: job.attempt
        }

        task =
          Task.Supervisor.async_nolink(state.sup, fn ->
            Sink.safe_deliver(mod, job.env, ctx, opts)
          end)

        timer = Process.send_after(self(), {:attempt_timeout, task.ref}, state.attempt_timeout_ms)
        entry = %{job: job, task: task, timer: timer}

        start_jobs(%{state | ready: ready, running: Map.put(state.running, task.ref, entry)})
    end
  end

  defp finished(state, job, :ok) do
    {mod, _opts} = job.sink

    Telemetry.emit([:lifecycle, :delivered], %{}, %{
      instance: state.instance,
      type: job.type,
      sink: inspect(mod)
    })

    %{state | pending: state.pending - 1}
  end

  defp finished(state, job, {:error, reason}), do: retry(state, job, reason)

  defp retry(state, job, reason) do
    {retry_mod, retry_opts} = state.retry
    {mod, _opts} = job.sink

    case retry_mod.backoff(job.attempt, retry_opts) do
      {:retry, delay} ->
        Process.send_after(self(), {:retry, %{job | attempt: job.attempt + 1}}, delay)
        state

      :give_up ->
        Logger.error(
          "[ankusa] lifecycle event #{job.type} for #{job.subject} to #{inspect(mod)} " <>
            "dropped after #{job.attempt} attempts: #{inspect(reason)}"
        )

        dropped(state.instance, job.type, mod, :gave_up)
        %{state | pending: state.pending - 1}
    end
  end

  defp dropped(instance, type, mod, reason) do
    Telemetry.emit([:lifecycle, :dropped], %{}, %{
      instance: instance,
      type: type,
      sink: inspect(mod),
      reason: reason
    })
  end
end
