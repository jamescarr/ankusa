# The harness behind every adapter's `*_faults_test.exs` (DEL-7): a client for
# toxiproxy's HTTP API, and what the suites share around it. Each adapter's
# `test_helper.exs` loads it by path (the way core's own harnesses are loaded);
# core's suite never does, so core needs no toxiproxy.
#
# toxiproxy sits between the sink under test and its broker (each adapter's
# `docker-compose.yml` runs it). A toxic on the proxy delays, drops or cuts the
# bytes in either direction; killing the broker itself is a `docker compose
# kill` of its service.

defmodule Ankusa.Toxiproxy do
  @moduledoc false

  @doc "Creates (or keeps) every proxy in `proxies`: `[%{name:, listen:, upstream:}]`."
  @spec populate!(String.t(), [map()]) :: :ok
  def populate!(api, proxies) do
    request!(api, "/populate", Enum.map(proxies, &Map.put(&1, :enabled, true)))
  end

  @doc "Adds a toxic of `type` to one direction (`upstream` or `downstream`) of `proxy`."
  @spec toxic!(String.t(), String.t(), String.t(), String.t(), map()) :: :ok
  def toxic!(api, proxy, type, stream, attributes) do
    request!(api, "/proxies/#{proxy}/toxics", %{
      name: "#{type}_#{stream}",
      type: type,
      stream: stream,
      toxicity: 1.0,
      attributes: attributes
    })
  end

  @doc "Removes every toxic and re-enables every proxy."
  @spec reset!(String.t()) :: :ok
  def reset!(api), do: request!(api, "/reset", nil)

  defp request!(api, path, body) do
    payload = if body == nil, do: "", else: JSON.encode!(body)

    case Req.post(api <> path,
           body: payload,
           headers: [{"content-type", "application/json"}],
           retry: false,
           receive_timeout: 10_000
         ) do
      {:ok, %Req.Response{status: status}} when status in [200, 201, 204] ->
        :ok

      other ->
        raise "toxiproxy #{path}: #{inspect(other)}"
    end
  end
end

defmodule Ankusa.BrokerFaults do
  @moduledoc false

  import ExUnit.Assertions
  import ExUnit.Callbacks

  alias Ankusa.{Envelope, UUIDv7}

  @source_id "faults"

  @doc "`docker compose` in the test's working directory (the package), which must exit 0."
  @spec compose!([String.t()]) :: :ok
  def compose!(args) do
    {out, status} =
      System.cmd("docker", ["compose", "-f", "docker-compose.yml" | args],
        cd: File.cwd!(),
        stderr_to_stdout: true
      )

    assert status == 0, "docker compose #{Enum.join(args, " ")}: #{out}"
    :ok
  end

  @doc "SIGKILLs the broker's container and starts it again, returning once it is healthy."
  @spec kill_and_restart!(String.t()) :: :ok
  def kill_and_restart!(service) do
    compose!(["kill", service])
    compose!(["up", "-d", "--wait", service])
  end

  @doc "Polls `fun` every 50 ms until it returns a truthy value, or fails the test."
  @spec eventually((-> term()), pos_integer(), String.t()) :: term()
  def eventually(fun, timeout_ms, what) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    poll(fun, deadline, what)
  end

  defp poll(fun, deadline, what) do
    case fun.() do
      falsy when falsy in [nil, false] ->
        if System.monotonic_time(:millisecond) > deadline do
          flunk("timed out waiting for #{what}")
        else
          Process.sleep(50)
          poll(fun, deadline, what)
        end

      value ->
        value
    end
  end

  @doc """
  Calls `deliver` until it answers `:ok`, the way the retry policy would, for at
  most `timeout_ms`. Every other answer must be a transient error: a sink that
  answers a permanent one for a broker that merely went away is wrong.
  """
  @spec deliver_until_ok!((-> :ok | {:error, term()}), pos_integer()) :: :ok
  def deliver_until_ok!(deliver, timeout_ms \\ 60_000) do
    eventually(
      fn ->
        case deliver.() do
          :ok ->
            :ok

          {:error, reason} ->
            assert transient?(reason), "not retryable: #{inspect(reason)}"
            Process.sleep(200)
            nil
        end
      end,
      timeout_ms,
      "the sink to deliver again"
    )
  end

  @doc "Is `reason` one dispatch retries (not `{:permanent, _}`)?"
  @spec transient?(term()) :: boolean()
  def transient?(reason), do: match?({:transient, _}, Ankusa.Sink.classify(reason))

  @doc "A hook of the source the pipeline tests use."
  @spec envelope(map()) :: Envelope.t()
  def envelope(overrides \\ %{}) do
    struct(
      %Envelope{
        id: UUIDv7.generate(),
        source_id: @source_id,
        tenant_id: "t1",
        received_at: 1_737_500_000_000,
        method: "POST",
        path: "/hooks/faults",
        headers: [],
        content_type: "application/json",
        body: ~s({"hello":"world"}),
        size: 17
      },
      overrides
    )
  end

  @doc "The delivery context of `instance`, as dispatch hands it to a sink."
  @spec ctx(atom()) :: Ankusa.Sink.ctx()
  def ctx(instance),
    do: %{instance: instance, source_id: @source_id, tenant_id: "t1", attempt: 1}

  @doc """
  Starts an instance whose dispatch pipeline delivers the source's hooks to
  `sink` (`{module, opts}`), retrying a failure after 200 ms, with every attempt
  killed at `attempt_timeout_ms`. The breaker is off: a run of failures must
  not park the row the test is waiting on.
  """
  @spec start_dispatch!({module(), keyword()}, pos_integer()) :: atom()
  def start_dispatch!(sink, attempt_timeout_ms) do
    instance = :"faults_#{System.unique_integer([:positive])}"

    dir =
      Path.join(
        System.tmp_dir!(),
        "ankusa_#{instance}_#{System.unique_integer([:positive])}_#{System.os_time(:microsecond)}"
      )

    on_exit(fn -> File.rm_rf(dir) end)

    config =
      Ankusa.Config.new(
        instance: instance,
        data_dir: dir,
        port: 0,
        roles: [:edge, :dispatch],
        source_store: {Ankusa.SourceStore.Static, sources: %{@source_id => %{sinks: [sink]}}},
        dispatch: %{
          attempt_timeout_ms: attempt_timeout_ms,
          breaker_failures: 0,
          retry:
            {Ankusa.RetryPolicy.Exponential,
             base_ms: 200, max_ms: 1_000, max_attempts: 100, jitter: false}
        }
      )

    start_supervised!({Ankusa.Instance, config})
    instance
  end

  @doc "Commits `env` to `instance`'s queue, with the sinks its source has."
  @spec enqueue!(atom(), Envelope.t()) :: Envelope.t()
  def enqueue!(instance, %Envelope{} = env) do
    {:ok, source} = Ankusa.SourceStore.fetch(instance, env.source_id)

    {:ok, [{:committed, committed}]} =
      Ankusa.Queue.enqueue(instance, [%{envelope: env, sinks: source.sinks}])

    committed
  end

  @doc """
  Waits for a delivery row of the source that dispatch has attempted and whose
  last attempt was killed at its deadline, and returns it. The row must still be
  alive: not dead-lettered.
  """
  @spec await_attempt_timeout!(atom(), pos_integer()) :: map()
  def await_attempt_timeout!(instance, timeout_ms) do
    row =
      eventually(
        fn ->
          Enum.find(rows(instance), fn %{row: row} ->
            row.attempts >= 1 and is_binary(row.error) and row.error =~ "attempt_timeout"
          end)
        end,
        timeout_ms,
        "an attempt killed at dispatch.attempt_timeout_ms"
      )

    assert {:ok, %{total: 0}} = Ankusa.Queue.dead(instance, limit: 10)
    row
  end

  defp rows(instance) do
    for family <- [:due, :inflight],
        {:ok, rows} =
          Ankusa.Queue.Deliveries.fold_source(
            instance,
            family,
            @source_id,
            [],
            &{:cont, &1 ++ &2}
          ),
        row <- rows,
        do: row
  end

  @doc "Waits until nothing of the source is pending or claimed; none of it may be dead."
  @spec await_delivered!(atom(), pos_integer()) :: :ok
  def await_delivered!(instance, timeout_ms) do
    eventually(
      fn ->
        match?(
          {:ok, %{pending: 0, inflight: 0}},
          Ankusa.Queue.pending_for_source(instance, @source_id)
        )
      end,
      timeout_ms,
      "dispatch to deliver every hook of the source"
    )

    assert {:ok, %{total: 0}} = Ankusa.Queue.dead(instance, limit: 10)
    :ok
  end
end
