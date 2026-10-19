defmodule Ankusa.LossTest do
  @moduledoc """
  The loss checker: the project's credibility. Every acked id must be readable
  after a crash. Zero tolerance.

  This is an OS-level crash. A separate BEAM running `test/support/crash_harness.exs`
  (`Ankusa.CrashHarness`) owns the same store and ingests concurrently; the
  parent waits for 500 acks and then `kill -9`s it, so the child gets no chance
  to flush anything. `Ankusa.Queue.Writer` commits with `sync: true` *before*
  the ingest acks, so every printed id must read back from the reopened store.
  The container drills in `scripts/drills/` run the same harness under ENOSPC,
  EIO and a power-loss cut.
  """
  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  @count 500
  @ack_deadline_ms 30_000
  @harness Path.expand("../support/crash_harness.exs", __DIR__)

  test "every acked hook survives a hard crash of the whole instance" do
    data_dir = unique_data_dir(:crash_harness)
    on_exit(fn -> File.rm_rf(data_dir) end)
    config = Ankusa.CrashHarness.config(data_dir)

    port = start_child(data_dir)
    on_exit(fn -> kill_if_alive(port) end)
    {:os_pid, os_pid} = Port.info(port, :os_pid)

    acked = collect_acks(port, @count)

    assert MapSet.size(acked) == @count,
           "child acked only #{MapSet.size(acked)} of #{@count} hooks before the deadline"

    # SIGKILL: no clean shutdown, no flush beyond what `sync: true` already put
    # on disk. This is crash-after-commit.
    {_, 0} = System.cmd("kill", ["-9", Integer.to_string(os_pid)])
    assert_receive {^port, {:exit_status, status}}, 10_000
    assert status == 137, "expected SIGKILL (137), got #{status}"

    # Reopen only the store over the same data dir and read every hook back.
    put_config(config)
    start_supervised!({Ankusa.Store, instance: config.instance})
    {:ok, hooks} = Ankusa.Queue.hooks(config.instance, 0, 1_000_000)
    stored = MapSet.new(hooks, & &1.id)

    missing = MapSet.difference(acked, stored)

    assert MapSet.size(missing) == 0,
           "lost #{MapSet.size(missing)} acked hooks: #{inspect(Enum.take(missing, 5))}"
  end

  # The index and the counters are written in the same synced batch as the hook,
  # so a SIGKILL leaves them agreeing: every hook has one due row under its
  # tenant's ordinal, and the counters are what a fold of the index says.
  test "counters equal the index after kill -9" do
    tenants = 4
    data_dir = unique_data_dir(:crash_harness)
    on_exit(fn -> File.rm_rf(data_dir) end)
    config = Ankusa.CrashHarness.config(data_dir, tenants: tenants)

    port = start_child(data_dir, [{~c"TENANTS", ~c"#{tenants}"}])
    on_exit(fn -> kill_if_alive(port) end)
    {:os_pid, os_pid} = Port.info(port, :os_pid)

    acked = collect_acks(port, @count)
    assert MapSet.size(acked) == @count

    {_, 0} = System.cmd("kill", ["-9", Integer.to_string(os_pid)])
    assert_receive {^port, {:exit_status, 137}}, 10_000

    put_config(config)
    start_supervised!({Ankusa.Store, instance: config.instance})
    {:ok, hooks} = Ankusa.Queue.hooks(config.instance, 0, 1_000_000)
    assert MapSet.subset?(acked, MapSet.new(hooks, & &1.id))

    by_tenant = Enum.frequencies_by(hooks, & &1.tenant_id)
    assert by_tenant |> Map.keys() |> Enum.sort() == for(i <- 0..(tenants - 1), do: "t#{i}")

    %{lo: lo, hi: hi} = Ankusa.Store.Keys.family(:due)

    {:ok, due} =
      Ankusa.Store.fold(config.instance, :due, {lo, hi}, [], fn key, _value, acc ->
        {:cont, [Ankusa.Store.Keys.decode_due(key) | acc]}
      end)

    due_by_ord = Enum.frequencies_by(due, fn {ord, _at, _seq, _sink} -> ord end)

    for {tenant, count} <- by_tenant do
      {:ok, ord} = Ankusa.Queue.Tenants.ord(config.instance, tenant)
      assert due_by_ord[ord] == count, "tenant #{tenant}: #{due_by_ord[ord]} due rows, #{count} hooks"
      assert {:ok, %{pending: ^count, scheduled: 0, inflight: 0}} =
               Ankusa.Queue.pending_for_tenant(config.instance, tenant)
    end

    assert stored_counters(config.instance) == folded_counters(config.instance)
    assert stored_counters(config.instance)[0].pending == length(hooks)
  end

  # ── child BEAM ────────────────────────────────────────────────────────────

  # The child has no Mix and no `:ankusa` application: the harness starts the
  # registry and one `:edge` instance itself, from the build's `ERL_LIBS`.
  defp start_child(data_dir, extra_env \\ []) do
    Port.open({:spawn_executable, System.find_executable("elixir")}, [
      :binary,
      :exit_status,
      {:line, 4096},
      args: [@harness, "serve"],
      env: [
        {~c"ERL_LIBS", String.to_charlist(Mix.Project.build_path() <> "/lib")},
        {~c"DATA_DIR", String.to_charlist(data_dir)} | extra_env
      ]
    ])
  end

  # If the test fails before the explicit kill, do not leave the child BEAM
  # holding the store.
  defp kill_if_alive(port) do
    case Port.info(port, :os_pid) do
      {:os_pid, os_pid} ->
        System.cmd("kill", ["-9", Integer.to_string(os_pid)], stderr_to_stdout: true)
        :ok

      nil ->
        :ok
    end
  end

  defp collect_acks(port, count) do
    deadline = System.monotonic_time(:millisecond) + @ack_deadline_ms
    collect(port, MapSet.new(), count, deadline)
  end

  defp collect(port, acc, count, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    cond do
      MapSet.size(acc) >= count ->
        acc

      remaining <= 0 ->
        acc

      true ->
        receive do
          {^port, {:data, {:eol, line}}} ->
            case String.trim(line) do
              "ACK " <> id -> collect(port, MapSet.put(acc, id), count, deadline)
              _ -> collect(port, acc, count, deadline)
            end

          {^port, {:data, {:noeol, _partial}}} ->
            collect(port, acc, count, deadline)

          {^port, {:exit_status, status}} ->
            flunk("crash child exited early with status #{status} after #{MapSet.size(acc)} acks")
        after
          remaining -> acc
        end
    end
  end
end
