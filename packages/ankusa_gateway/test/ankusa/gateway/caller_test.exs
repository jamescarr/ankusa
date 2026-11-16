defmodule Ankusa.Gateway.CallerTest do
  @moduledoc """
  `Ankusa.Gateway.Caller` under the loads it exists for: coalescing, the limit
  and the queue, deadlines, batching, the breaker, and fairness between
  partitions.
  """

  use ExUnit.Case, async: true

  @moduletag :capture_log

  alias Ankusa.Gateway.Caller

  @far 60_000

  defp start(opts) do
    inst = :"caller#{System.unique_integer([:positive])}"
    Ankusa.put_config(Ankusa.Config.new(instance: inst, roles: []))

    defaults = [
      instance: inst,
      name: :c,
      label: "test",
      max_inflight: 4,
      queue_max: 100,
      partition_queue_max: 100,
      batch: nil,
      breaker: %{failures: 0, open_ms: 1_000}
    ]

    start_supervised!({Caller, Keyword.merge(defaults, opts)})
    inst
  end

  defp deadline(ms), do: System.monotonic_time(:millisecond) + ms

  defp call(inst, key, partition \\ :p, item \\ nil, ms \\ @far),
    do: Caller.call(inst, :c, key, partition, item || key, deadline(ms))

  # A run that reports itself and waits for `{:go, item}` (or `:go_all`).
  defp gated(test) do
    fn item ->
      send(test, {:running, item, self()})

      receive do
        {:go, result} -> result
      end
    end
  end

  test "100 concurrent calls of one key run once, and every caller gets its result" do
    test = self()
    counter = :counters.new(1, [])

    inst =
      start(
        run: fn item ->
          :counters.add(counter, 1, 1)
          send(test, :ran)
          Process.sleep(50)
          {:ok, item}
        end
      )

    results =
      1..100
      |> Enum.map(fn _ -> Task.async(fn -> call(inst, :k) end) end)
      |> Task.await_many(5_000)

    assert Enum.all?(results, &(&1 == {:ok, :k}))
    assert :counters.get(counter, 1) == 1
  end

  test "past the limit keys queue; past queue_max they are shed at once" do
    test = self()
    inst = start(max_inflight: 2, queue_max: 2, run: gated(test))

    tasks = for k <- [:a, :b, :c, :d], do: Task.async(fn -> call(inst, k) end)
    Process.sleep(50)
    assert_receive {:running, first, first_pid}, 1_000
    assert_receive {:running, second, second_pid}, 1_000
    started = [{first, first_pid}, {second, second_pid}]
    refute_receive {:running, _, _}, 50

    # :c and :d are queued, so a fifth key has no room.
    assert call(inst, :e) == {:error, {:unavailable, :overloaded}}

    for {item, pid} <- started, do: send(pid, {:go, {:ok, item}})

    for _ <- 1..2 do
      assert_receive {:running, item, pid}, 1_000
      send(pid, {:go, {:ok, item}})
    end

    assert Enum.sort(Task.await_many(tasks, 5_000)) == [
             {:ok, :a},
             {:ok, :b},
             {:ok, :c},
             {:ok, :d}
           ]
  end

  test "a partition holds at most partition_queue_max queued keys" do
    test = self()
    inst = start(max_inflight: 1, queue_max: 10, partition_queue_max: 2, run: gated(test))

    t0 = Task.async(fn -> call(inst, :busy, :heavy) end)
    assert_receive {:running, :busy, runner}, 1_000

    queued = for k <- [:h1, :h2], do: Task.async(fn -> call(inst, k, :heavy) end)
    Process.sleep(20)
    assert call(inst, :h3, :heavy) == {:error, {:unavailable, :overloaded}}

    light = Task.async(fn -> call(inst, :l1, :light) end)
    Process.sleep(20)
    send(runner, {:go, {:ok, :busy}})

    # Round robin: the light partition's key does not wait behind both heavy ones.
    assert_receive {:running, first, pid1}, 1_000
    send(pid1, {:go, {:ok, first}})
    assert_receive {:running, second, pid2}, 1_000
    send(pid2, {:go, {:ok, second}})
    assert :l1 in [first, second]
    assert_receive {:running, third, pid3}, 1_000
    send(pid3, {:go, {:ok, third}})

    assert Task.await(t0) == {:ok, :busy}
    assert Task.await(light) == {:ok, :l1}
    assert Enum.sort(Task.await_many(queued)) == [{:ok, :h1}, {:ok, :h2}]
  end

  test "a queued key whose deadline passed never runs" do
    test = self()
    inst = start(max_inflight: 1, run: gated(test))

    first = Task.async(fn -> call(inst, :first) end)
    assert_receive {:running, :first, runner}, 1_000

    assert call(inst, :late, :p, nil, 30) == {:error, {:unavailable, :deadline}}
    send(runner, {:go, {:ok, :first}})
    assert Task.await(first) == {:ok, :first}
    refute_receive {:running, :late, _}, 100
  end

  test "a caller that times out still gets the cache filled by the run" do
    test = self()
    table = :ets.new(:filled, [:public])

    inst =
      start(
        run: fn item ->
          Process.sleep(100)
          :ets.insert(table, {item, :done})
          send(test, :finished)
          {:ok, item}
        end
      )

    assert call(inst, :slow, :p, nil, 20) == {:error, {:unavailable, :deadline}}
    assert_receive :finished, 1_000
    assert :ets.lookup(table, :slow) == [{:slow, :done}]
  end

  test "a batch is flushed at batch.max" do
    test = self()

    inst =
      start(
        batch: %{max: 3, window_ms: 5_000},
        run: fn items ->
          send(test, {:batch, items})
          Enum.map(items, &{:ok, &1})
        end
      )

    tasks = for k <- [:a, :b, :c], do: Task.async(fn -> call(inst, k) end)
    assert_receive {:batch, items}, 1_000
    assert Enum.sort(items) == [:a, :b, :c]
    assert Enum.sort(Task.await_many(tasks)) == [{:ok, :a}, {:ok, :b}, {:ok, :c}]
  end

  test "a batch is flushed at the window, whatever its size" do
    test = self()

    inst =
      start(
        batch: %{max: 50, window_ms: 30},
        run: fn items ->
          send(test, {:batch, items})
          Enum.map(items, &{:ok, &1})
        end
      )

    {micros, result} = :timer.tc(fn -> call(inst, :alone) end)
    assert result == {:ok, :alone}
    assert_received {:batch, [:alone]}
    assert div(micros, 1000) >= 25
  end

  test "a batch answer of the wrong length is unavailable for every item" do
    inst = start(batch: %{max: 2, window_ms: 1_000}, run: fn _items -> [{:ok, :only_one}] end)

    tasks = for k <- [:a, :b], do: Task.async(fn -> call(inst, k) end)

    assert Task.await_many(tasks) == [
             {:error, {:unavailable, :bad_batch}},
             {:error, {:unavailable, :bad_batch}}
           ]
  end

  test "the breaker opens after N failures, lets one probe through, and closes" do
    test = self()
    mode = :atomics.new(1, [])

    inst =
      start(
        breaker: %{failures: 3, open_ms: 100},
        run: fn item ->
          send(test, {:ran, item})
          if :atomics.get(mode, 1) == 0, do: {:error, {:unavailable, :down}}, else: {:ok, item}
        end
      )

    for k <- [:a, :b, :c], do: assert(call(inst, k) == {:error, {:unavailable, :down}})
    assert call(inst, :d) == {:error, {:unavailable, :breaker_open}}
    refute_received {:ran, :d}

    Process.sleep(120)
    :atomics.put(mode, 1, 1)
    assert call(inst, :probe) == {:ok, :probe}
    assert call(inst, :after) == {:ok, :after}
  end

  test "a failed probe opens the breaker again" do
    inst =
      start(
        breaker: %{failures: 1, open_ms: 50},
        run: fn _item -> {:error, {:unavailable, :down}} end
      )

    assert call(inst, :a) == {:error, {:unavailable, :down}}
    assert call(inst, :b) == {:error, {:unavailable, :breaker_open}}
    Process.sleep(60)
    assert call(inst, :probe) == {:error, {:unavailable, :down}}
    assert call(inst, :c) == {:error, {:unavailable, :breaker_open}}
  end

  test "the limit halves on an unavailable result and grows back" do
    mode = :atomics.new(1, [])

    inst =
      start(
        max_inflight: 8,
        run: fn item ->
          if :atomics.get(mode, 1) == 0, do: {:error, {:unavailable, :slow}}, else: {:ok, item}
        end
      )

    pid = Ankusa.whereis(inst, {:gateway_caller, :c})
    assert :sys.get_state(pid).limit == 8
    call(inst, :x)
    assert :sys.get_state(pid).limit == 4

    :atomics.put(mode, 1, 1)
    for i <- 1..40, do: call(inst, i)
    assert :sys.get_state(pid).limit == 8
  end

  test "a run that raises is unavailable for its waiters" do
    inst = start(run: fn _item -> raise "boom" end)
    assert {:error, {:unavailable, {:exit, _reason}}} = call(inst, :a)
  end

  test "refresh runs in the background, and not for a key already in flight" do
    test = self()
    inst = start(run: gated(test))

    Caller.refresh(inst, :c, :r, :r)
    assert_receive {:running, :r, pid}, 1_000
    Caller.refresh(inst, :c, :r, :r)
    refute_receive {:running, :r, _}, 50
    send(pid, {:go, {:ok, :r}})
  end

  test "a light partition keeps its latency beside a 99:1 heavy one" do
    # A run takes 5 ms and 4 run at once. Behind the heavy partition's ~100
    # queued keys, a light key served in turn waits about one run; served in
    # arrival order it would wait for the whole queue (~125 ms). The floor
    # keeps scheduling noise on a loaded machine from failing a fair caller.
    inst =
      start(
        max_inflight: 4,
        queue_max: 400,
        partition_queue_max: 100,
        run: fn _item ->
          Process.sleep(5)
          {:ok, :done}
        end
      )

    solo = for i <- 1..20, do: timed(fn -> call(inst, {:solo, i}, :light) end)

    heavy =
      for i <- 1..99 do
        Task.async(fn -> call(inst, {:heavy, i}, :heavy) end)
      end

    Process.sleep(5)

    mixed =
      for i <- 1..20 do
        timed(fn -> call(inst, {:light, i}, :light) end)
      end

    Task.await_many(heavy, 10_000)
    assert p99(mixed) <= 2 * max(p99(solo), 25_000)
  end

  defp timed(fun) do
    {micros, {:ok, :done}} = :timer.tc(fun)
    micros
  end

  defp p99(samples), do: samples |> Enum.sort() |> Enum.at(round(length(samples) * 0.99) - 1)
end
