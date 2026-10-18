defmodule Ankusa.Routes.Store.RedisLifecycleTest do
  @moduledoc """
  The lifecycle outbox on the dual writes: a route in the Redis route store and a
  source in the Redis source store are changed in Redis, outside the node's own
  store, so a node killed in the middle leaves a *pending* audit row and nobody
  knows whether the change happened.

  `test/support/lifecycle_harness.exs` runs a child BEAM that leaves one pending
  row for each place a kill can land and is `kill -9`ed; the reopened node must
  settle each from what Redis holds, announcing the changes that happened (and the
  state somebody else left) and none that did not.

  Needs the package's compose Redis (`docker compose up -d --wait`); `REDIS_URL`
  overrides the default `redis://localhost:6399`.
  """

  use ExUnit.Case, async: false

  alias Ankusa.{Audit, RedisLifecycleHarness}

  @harness Path.expand("../../../support/lifecycle_harness.exs", __DIR__)

  defmodule CaptureSink do
    @moduledoc false
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, _ctx, opts) do
      send(Keyword.fetch!(opts, :to), {:lifecycle, env.id, JSON.decode!(env.body)})
      :ok
    end
  end

  # Only the harness's own keys are deleted, never the database.
  defp clean_redis do
    {:ok, conn} = Redix.start_link(RedisLifecycleHarness.url())

    {:ok, _deleted} =
      Redix.command(conn, [
        "DEL" | RedisLifecycleHarness.keys() ++ RedisLifecycleHarness.source_keys()
      ])

    Redix.stop(conn)
  end

  setup do
    clean_redis()
    on_exit(&clean_redis/0)
    :ok
  end

  test "pending rows left by kill -9 are settled from what Redis holds, and only the real changes are announced" do
    data_dir =
      Path.join(System.tmp_dir!(), "ankusa_redis_lifecycle_#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf(data_dir) end)

    port =
      Port.open({:spawn_executable, System.find_executable("elixir")}, [
        :binary,
        :exit_status,
        {:line, 4096},
        args: [@harness, "serve"],
        env: [
          {~c"ERL_LIBS", String.to_charlist(Mix.Project.build_path() <> "/lib")},
          {~c"DATA_DIR", String.to_charlist(data_dir)}
        ]
      ])

    on_exit(fn -> kill_if_alive(port) end)
    {:os_pid, os_pid} = Port.info(port, :os_pid)
    await_ready(port)

    {_, 0} = System.cmd("kill", ["-9", Integer.to_string(os_pid)])
    assert_receive {^port, {:exit_status, 137}}, 10_000

    config = RedisLifecycleHarness.config(data_dir, [{CaptureSink, to: self()}])
    start_supervised!({Ankusa.Instance, config})

    # The finished change first (it was in the outbox all along); the pending
    # rows are settled by a tick once their store has loaded, the route store's
    # and the source store's not necessarily in the same one, so their order
    # among themselves is not asserted.
    [first | settled] = for(_ <- 1..4, do: next_event())
    assert first == {"io.ankusa.route.created", "r-diverged", 1}

    assert Enum.sort(settled) ==
             Enum.sort([
               {"io.ankusa.route.created", "r-applied", 1},
               {"io.ankusa.route.updated", "r-diverged", 1},
               {"io.ankusa.source.created", "acme.s-applied", nil}
             ])

    refute_receive {:lifecycle, _, _}, 500

    rows =
      config.instance
      |> trail()
      |> Enum.map(fn row ->
        {row.action, row.resource, row.result, row.detail && row.detail["reconciled"],
         row.publish}
      end)

    assert Enum.sort(rows) ==
             Enum.sort([
               {"route.create", "r-diverged", "ok", nil, true},
               {"route.create", "r-applied", "ok", "applied", true},
               {"route.create", "r-ghost", "failed", "not_applied", false},
               {"route.update", "r-diverged", "interrupted", "diverged", true},
               {"source.create", "acme.s-applied", "ok", "applied", true},
               {"source.create", "acme.s-ghost", "failed", "not_applied", false}
             ])
  end

  defp next_event do
    receive do
      {:lifecycle, _id, event} ->
        {event["type"], event["subject"], event["revision"]}
    after
      30_000 -> flunk("no lifecycle event within 30s")
    end
  end

  # Every settled row: the boot's reconciliation runs on a tick, so wait for all six.
  defp trail(instance) do
    deadline = System.monotonic_time(:millisecond) + 30_000
    await_trail(instance, deadline)
  end

  defp await_trail(instance, deadline) do
    {:ok, entries} = Audit.page(instance, 0, 100)
    rows = for {_seq, {:ok, row}} <- entries, do: row

    cond do
      length(rows) >= 6 ->
        rows

      System.monotonic_time(:millisecond) > deadline ->
        flunk("the trail holds #{length(rows)} rows, not 6")

      true ->
        Process.sleep(50)
        await_trail(instance, deadline)
    end
  end

  defp await_ready(port) do
    receive do
      {^port, {:data, {:eol, "READY"}}} -> :ok
      {^port, {:data, {:eol, _other}}} -> await_ready(port)
      {^port, {:exit_status, status}} -> flunk("the harness exited with #{status}")
    after
      90_000 -> flunk("the harness was not ready")
    end
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
end
