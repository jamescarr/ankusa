defmodule Ankusa.DrainTest do
  @moduledoc """
  What a stop does to a node under load (OPS-4).

  The drill is OS-level, like `loss_test.exs`: a child BEAM
  (`test/support/drain_harness.exs`) runs the real `:ankusa` application with one
  instance whose queue writer the test stalls, so 200 POSTs sit in the batchers.
  The test then sends SIGTERM and asserts what the clients and the store saw: `/ready` answers
  `503 draining` within 100 ms of the signal, every request is answered `201`
  or `503` (zero connection resets), and every `201` is in the store after a
  restart. The `in process` tests cover `Ankusa.Instance.Drain` and
  `Ankusa.Edge.Batcher.queued/2` without a second VM.
  """
  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Edge.Batcher
  alias Ankusa.Edge.Router
  alias Ankusa.Health
  alias Ankusa.Instance.Drain

  @moduletag capture_log: true

  @requests 200
  @stall_ms 3_000
  @harness Path.expand("../support/drain_harness.exs", __DIR__)

  describe "SIGTERM" do
    @describetag timeout: 180_000

    test "with 200 requests in flight: ready flips, zero resets, every 201 survives a restart" do
      data_dir = unique_data_dir(:drain_harness)
      on_exit(fn -> File.rm_rf(data_dir) end)
      http_port = free_port()

      port = start_child(data_dir, http_port)
      on_exit(fn -> kill_if_alive(port) end)
      {:os_pid, os_pid} = Port.info(port, :os_pid)

      await_line(port, "READY", 60_000)
      Port.command(port, "STALL #{@stall_ms}\n")
      await_line(port, "STALLED", 10_000)

      tasks = for n <- 1..@requests, do: Task.async(fn -> post(http_port, n) end)

      # The writer is stalled: nothing can be answered yet.
      assert Enum.all?(Task.yield_many(tasks, 500), fn {_task, result} -> result == nil end),
             "a request was answered while the writer was stalled"

      sent = System.monotonic_time(:millisecond)
      {_, 0} = System.cmd("kill", ["-TERM", Integer.to_string(os_pid)])
      flipped = await_draining(http_port, sent)
      flip_ms = flipped - sent

      assert flip_ms <= 100, "/ready took #{flip_ms} ms to flip to draining"

      results = Task.await_many(tasks, 60_000)
      resets = for {:reset, why} <- results, do: why
      answered = for {:answered, status, id} <- results, do: {status, id}

      assert resets == [],
             "#{length(resets)} request(s) were reset: #{inspect(Enum.take(resets, 3))}"

      assert length(answered) == @requests
      assert Enum.all?(answered, fn {status, _id} -> status in [201, 503] end)

      acked = for {201, id} <- answered, do: id
      assert acked != [], "no request was acked"

      assert_receive {^port, {:exit_status, 0}}, 60_000

      config = Ankusa.DrainHarness.config(data_dir)
      put_config(config)
      start_supervised!({Ankusa.Store, instance: config.instance})
      {:ok, hooks} = Ankusa.Queue.hooks(config.instance, 0, 1_000_000)
      missing = MapSet.difference(MapSet.new(acked), MapSet.new(hooks, & &1.id))

      assert MapSet.size(missing) == 0,
             "lost #{MapSet.size(missing)} acked hooks: #{inspect(Enum.take(missing, 5))}"

      IO.puts(
        "drain: ready flips in #{flip_ms} ms; zero resets; #{length(acked)} x 201, " <>
          "#{length(answered) - length(acked)} x 503; every 201 present after restart"
      )
    end
  end

  describe "in process" do
    defp start_edge(overrides \\ []) do
      config =
        test_config(
          Keyword.merge(
            [
              roles: [:edge],
              listener: %{drain_delay_ms: 0},
              source_store:
                {Ankusa.SourceStore.Static,
                 sources: %{"demo" => [verifier: {Ankusa.Verifier.None, []}]}}
            ],
            overrides
          )
        )

      start_supervised!({Ankusa.Instance, config})
      config
    end

    defp ingest(config) do
      Router.call(
        Plug.Test.conn(:post, "/webhooks/demo", ~s({"hello":"world"})),
        Router.init(instance: config.instance)
      )
    end

    defp held(instance) do
      Enum.sum(for p <- 0..3, do: Batcher.queued(instance, p))
    end

    defp eventually(fun, tries \\ 100) do
      cond do
        fun.() -> :ok
        tries == 0 -> flunk("condition never held")
        true -> Process.sleep(20) && eventually(fun, tries - 1)
      end
    end

    test "run/1 flips /ready to draining, closes the ingress listener, and a restart takes hooks again" do
      config = start_edge()
      inst = config.instance
      name = to_string(inst)

      assert {:ok, %{status: "ready"} = ready} = Health.ready(inst)
      refute Map.has_key?(ready, :drain)
      assert ingress_pid(inst)

      assert :ok = Drain.run(inst)

      assert {:error,
              %{
                status: "draining",
                instance: ^name,
                drain: "draining",
                reasons: %{drain: "shutting_down"}
              }} = Health.ready(inst)

      assert get_ready(config).status == 503
      assert Health.draining?(inst)
      refute ingress_pid(inst), "the ingress listener is still running"

      # The instance stops with its tree, and starts clean.
      stop_supervised!({Ankusa.Instance, inst})
      start_supervised!({Ankusa.Instance, config})

      refute Health.draining?(inst)
      assert {:ok, %{status: "ready"} = again} = Health.ready(inst)
      refute Map.has_key?(again, :drain)
    end

    test "run/1 waits for the requests a batcher holds" do
      config = start_edge()
      inst = config.instance
      writer = Ankusa.whereis(inst, :queue_writer)
      :ok = :sys.suspend(writer)

      try do
        request = Task.async(fn -> ingest(config) end)
        eventually(fn -> held(inst) == 1 end)

        drain = Task.async(fn -> Drain.run(inst) end)
        # The listener is closed and the batcher still holds the record: not done.
        eventually(fn -> ingress_pid(inst) == nil end)
        assert Task.yield(drain, 200) == nil

        :ok = :sys.resume(writer)
        assert Task.await(request).status == 201
        assert Task.await(drain) == :ok
        assert held(inst) == 0
      after
        # A no-op once resumed; never leave the writer suspended past the test.
        if Process.alive?(writer), do: :sys.resume(writer)
      end
    end

    test "run/1 on an instance that never started is a no-op" do
      assert Drain.run(:"no_such_instance_#{System.unique_integer([:positive])}") == :ok
    end

    test "Batcher.queued/2 counts the records a partition holds, buffered or in flight" do
      config = start_edge()
      inst = config.instance
      assert held(inst) == 0

      writer = Ankusa.whereis(inst, :queue_writer)
      :ok = :sys.suspend(writer)

      try do
        request = Task.async(fn -> ingest(config) end)
        eventually(fn -> held(inst) == 1 end)
        :ok = :sys.resume(writer)
        assert Task.await(request).status == 201
        eventually(fn -> held(inst) == 0 end)
      after
        if Process.alive?(writer), do: :sys.resume(writer)
      end
    end
  end

  # ── child BEAM ────────────────────────────────────────────────────────────

  defp start_child(data_dir, http_port) do
    Port.open({:spawn_executable, System.find_executable("elixir")}, [
      :binary,
      :exit_status,
      {:line, 4096},
      args: [@harness, "serve"],
      env: [
        {~c"ERL_LIBS", String.to_charlist(Mix.Project.build_path() <> "/lib")},
        {~c"LOG_LEVEL", ~c"none"},
        {~c"HTTP_PORT", String.to_charlist(Integer.to_string(http_port))},
        {~c"DATA_DIR", String.to_charlist(data_dir)}
      ]
    ])
  end

  defp kill_if_alive(port) do
    case Port.info(port, :os_pid) do
      {:os_pid, os_pid} ->
        System.cmd("kill", ["-9", Integer.to_string(os_pid)], stderr_to_stdout: true)
        :ok

      nil ->
        :ok
    end
  end

  defp await_line(port, line, timeout) do
    receive do
      {^port, {:data, {:eol, ^line}}} ->
        :ok

      {^port, {:data, _other}} ->
        await_line(port, line, timeout)

      {^port, {:exit_status, status}} ->
        flunk("the drain harness exited with status #{status} before printing #{line}")
    after
      timeout -> flunk("the drain harness never printed #{line}")
    end
  end

  defp ingress_pid(instance) do
    instance
    |> Ankusa.via(:edge)
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {:ingress, pid, _type, _modules} when is_pid(pid) -> pid
      _other -> nil
    end)
  end

  defp get_ready(config) do
    Router.call(Plug.Test.conn(:get, "/ready"), Router.init(instance: config.instance))
  end

  # ── a raw HTTP client: a connection reset must be visible ────────────────

  # One POST on its own connection, `connection: close`, read to the end.
  # `{:answered, status, hook id}` when a whole response came back, else
  # `{:reset, why}`.
  defp post(port, n) do
    body = ~s({"n":#{n}})

    request =
      "POST /webhooks/drain HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\n" <>
        "content-length: #{byte_size(body)}\r\nconnection: close\r\n\r\n" <> body

    case exchange(port, request) do
      {:answered, status, response_body} ->
        id =
          case JSON.decode(response_body) do
            {:ok, %{"id" => id}} -> id
            _ -> nil
          end

        {:answered, status, id}

      reset ->
        reset
    end
  end

  # `{:answered, status, body}` once the server closed a connection that carried
  # a whole response, else `{:reset, why}`: it never answered, or the connection
  # broke first.
  defp exchange(port, request) do
    case :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 10_000) do
      {:ok, socket} ->
        try do
          with :ok <- :gen_tcp.send(socket, request),
               {:ok, data} <- recv_all(socket, "") do
            parse(data)
          else
            {:error, reason} -> {:reset, reason}
          end
        after
          :gen_tcp.close(socket)
        end

      {:error, reason} ->
        {:reset, {:connect, reason}}
    end
  end

  defp recv_all(socket, acc) do
    case :gen_tcp.recv(socket, 0, 60_000) do
      {:ok, data} -> recv_all(socket, acc <> data)
      {:error, :closed} -> {:ok, acc}
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse("HTTP/1.1 " <> <<status::binary-size(3), _rest::binary>> = data) do
    case String.split(data, "\r\n\r\n", parts: 2) do
      [_head, body] -> {:answered, String.to_integer(status), body}
      _ -> {:reset, {:truncated, data}}
    end
  end

  defp parse(data), do: {:reset, {:no_response, data}}

  # Poll `/ready` until it answers `503` with `"status":"draining"`; the
  # monotonic millisecond it first did. `unavailable` is some other check, and
  # not it.
  defp await_draining(port, since) do
    request = "GET /ready HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n"

    case exchange(port, request) do
      {:answered, 503, body} = answer ->
        if body =~ "\"status\":\"draining\"",
          do: System.monotonic_time(:millisecond),
          else: retry_draining(port, since, answer)

      other ->
        retry_draining(port, since, other)
    end
  end

  defp retry_draining(port, since, last) do
    if System.monotonic_time(:millisecond) - since > 5_000,
      do: flunk("/ready never answered 503 draining; the last answer was #{inspect(last)}"),
      else: await_draining(port, since)
  end
end
