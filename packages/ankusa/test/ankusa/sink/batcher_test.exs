defmodule Ankusa.Sink.BatcherTest do
  @moduledoc """
  `Ankusa.Sink.Batcher` with a codec that reports every batch to the test: when a
  batch goes out, who is in it, what a caller is told, and what the queue
  refuses. The codec's batch blocks until the test releases it (`gate: true`),
  so a request can be held in flight.
  """

  use ExUnit.Case, async: true

  alias Ankusa.Sink.Batcher

  @moduletag capture_log: true

  defmodule Codec do
    @behaviour Ankusa.Sink.Batcher

    @impl true
    def sink, do: Ankusa.Sink.BatcherTest

    @impl true
    def batch_identity(opts), do: Map.new(Keyword.take(opts, [:name, :batch_max_entries]))

    @impl true
    def batch_config(opts, instance) do
      Batcher.config(opts, instance,
        entries: {10, 10},
        bytes: {100_000, 100_000},
        timeout_ms: Keyword.get(opts, :timeout_ms, 1_000)
      )
    end

    @impl true
    def send_batch(items, opts) do
      pid = Keyword.fetch!(opts, :pid)
      send(pid, {:batch, Enum.map(items, & &1.n), self()})

      if Keyword.get(opts, :gate, false) do
        receive do
          :go -> :ok
        after
          5_000 -> :ok
        end
      end

      case Keyword.get(opts, :mode, :ok) do
        :ok ->
          {Enum.map(items, &Map.get(&1, :result, :ok)), %{requests: 1, bisections: 0}}

        :raise ->
          raise "codec boom"

        :short ->
          {[:ok], %{requests: 1, bisections: 0}}
      end
    end
  end

  setup do
    instance = :"batcher_test_#{System.unique_integer([:positive])}"
    start_supervised!({Batcher.Workers, instance: instance})

    handler = "batcher-test-#{instance}"
    test = self()

    :telemetry.attach_many(
      handler,
      [[:ankusa, :sink_batch, :flush], [:ankusa, :sink_batch, :refused]],
      fn event, measurements, meta, _ ->
        if meta.instance == instance, do: send(test, {:telemetry, event, measurements, meta})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    %{instance: instance, name: "dest-#{instance}"}
  end

  defp opts(name, extra \\ []) do
    [pid: self(), name: name, batch_linger_ms: 30] ++ extra
  end

  defp entry(n, extra \\ %{}) do
    Map.merge(
      %{tenant: "acme", bytes: 10, group: nil, attempt: 1, item: %{n: n}},
      extra
    )
  end

  # Runs `Batcher.deliver/4` in its own process, so a test can have several
  # callers waiting at once.
  defp async(instance, opts, entry) do
    Task.async(fn -> Batcher.deliver(Codec, instance, opts, entry) end)
  end

  defp item(n, extra), do: Map.merge(%{n: n}, extra)

  # Lets every batch that is held, or arrives, finish.
  defp release_all do
    receive do
      {:batch, _ns, task} ->
        send(task, :go)
        release_all()
    after
      300 -> :ok
    end
  end

  test "deliveries within the linger go out as one batch, and each caller gets its own result",
       %{instance: instance, name: name} do
    o = opts(name)

    tasks = for n <- 1..3, do: async(instance, o, entry(n))

    assert_receive {:batch, ns, _}, 1_000
    assert Enum.sort(ns) == [1, 2, 3]
    assert Enum.map(tasks, &Task.await/1) == [:ok, :ok, :ok]
    refute_receive {:batch, _, _}, 100

    assert_receive {:telemetry, [:ankusa, :sink_batch, :flush], %{entries: 3, requests: 1}, _}
  end

  test "an entry's own result is its caller's, not its batch's", %{instance: instance, name: name} do
    o = opts(name)

    ok = async(instance, o, entry(1))
    bad = async(instance, o, entry(2, %{item: item(2, %{result: {:error, :nope}})}))

    assert Task.await(ok) == :ok
    assert Task.await(bad) == {:error, :nope}
  end

  test "a full batch goes out at once, not after the linger", %{instance: instance, name: name} do
    o = opts(name, batch_max_entries: 3, batch_linger_ms: 30_000)

    tasks = for n <- 1..3, do: async(instance, o, entry(n))

    assert_receive {:batch, ns, _}, 1_000
    assert Enum.sort(ns) == [1, 2, 3]
    Enum.each(tasks, &Task.await/1)
  end

  test "a batch is cut at max_bytes; a single larger entry still goes alone", %{
    instance: instance,
    name: name
  } do
    o = opts(name, batch_max_bytes: 100, batch_linger_ms: 30_000)

    tasks = [
      async(instance, o, entry(1, %{bytes: 60})),
      async(instance, o, entry(2, %{bytes: 60})),
      async(instance, o, entry(3, %{bytes: 500}))
    ]

    # 120 queued bytes pass the 100 cap: the first two cannot share a batch
    assert_receive {:batch, first, _}, 1_000
    assert length(first) == 1
    assert_receive {:batch, second, _}, 1_000
    assert length(second) == 1
    assert_receive {:batch, third, _}, 1_000
    assert length(third) == 1
    assert Enum.sort(first ++ second ++ third) == [1, 2, 3]
    Enum.each(tasks, &Task.await/1)
  end

  test "an ordered group has one request in flight, its entries keep their order", %{
    instance: instance,
    name: name
  } do
    o = opts(name, gate: true, batch_max_inflight: 4)

    a = async(instance, o, entry(1, %{group: "g"}))
    assert_receive {:batch, [1], first}, 1_000

    # `g` is in flight: its next entry waits, an entry of another group does not
    b = async(instance, o, entry(2, %{group: "g"}))
    c = async(instance, o, entry(3, %{group: "h"}))
    assert_receive {:batch, [3], third}, 1_000
    refute_receive {:batch, _, _}, 150

    send(third, :go)
    assert Task.await(c) == :ok
    refute_receive {:batch, _, _}, 100

    send(first, :go)
    assert Task.await(a) == :ok
    assert_receive {:batch, [2], second}, 1_000
    send(second, :go)
    assert Task.await(b) == :ok
  end

  test "entries of one group share a batch in the order they were admitted", %{
    instance: instance,
    name: name
  } do
    o = opts(name, batch_linger_ms: 100)

    tasks =
      for n <- 1..4 do
        task = async(instance, o, entry(n, %{group: "g"}))
        # admission order is the order the calls arrive
        Process.sleep(5)
        task
      end

    assert_receive {:batch, [1, 2, 3, 4], _}, 1_000
    Enum.each(tasks, &Task.await/1)
  end

  test "a batch takes entries round-robin across tenants", %{instance: instance, name: name} do
    o = opts(name, gate: true, batch_max_inflight: 1, batch_max_entries: 2, batch_linger_ms: 10)

    first = async(instance, o, entry(0, %{tenant: "heavy"}))
    assert_receive {:batch, [0], held}, 1_000

    # a 99:1 backlog queued behind the request in flight
    heavy = for n <- 1..6, do: async(instance, o, entry(n, %{tenant: "heavy"}))
    Process.sleep(20)
    light = async(instance, o, entry(100, %{tenant: "light"}))
    Process.sleep(20)

    send(held, :go)
    assert Task.await(first) == :ok

    assert_receive {:batch, next, next_task}, 1_000
    # the light tenant is in the very next batch, not behind six heavy entries
    assert 100 in next
    assert length(next) == 2
    send(next_task, :go)

    release_all()

    Enum.each([light | heavy], &Task.await/1)
  end

  describe "the queue is bounded (ground rule 3)" do
    test "a delivery past queue_max is refused with a jittered retry_after and counted", %{
      instance: instance,
      name: name
    } do
      o = opts(name, gate: true, batch_max_inflight: 1, batch_queue_max: 2, batch_linger_ms: 5)

      held = async(instance, o, entry(0))
      assert_receive {:batch, [0], task}, 1_000

      queued = [async(instance, o, entry(1)), async(instance, o, entry(2))]
      Process.sleep(30)

      assert {:error, {:retry_after, ms, {:batch_queue_full, :entries}}} =
               Batcher.deliver(Codec, instance, o, entry(3, %{attempt: 3}))

      assert ms in 1..30_000

      assert_receive {:telemetry, [:ankusa, :sink_batch, :refused], %{},
                      %{reason: :entries, sink: Ankusa.Sink.BatcherTest}}

      assert %{batchers: 1, queued: 2, queued_bytes: 20} = Batcher.stats(instance)

      send(task, :go)

      release_all()

      Enum.each([held | queued], &Task.await/1)
    end

    test "queue_max_bytes refuses by size, and one tenant cannot take another's share", %{
      instance: instance,
      name: name
    } do
      o =
        opts(name,
          gate: true,
          batch_max_inflight: 1,
          batch_queue_max_bytes: 25,
          batch_queue_tenant_max: 1,
          batch_linger_ms: 5
        )

      held = async(instance, o, entry(0, %{tenant: "a"}))
      assert_receive {:batch, [0], task}, 1_000

      one = async(instance, o, entry(1, %{tenant: "a"}))
      Process.sleep(20)

      # tenant `a` is at its share; `b` is not
      assert {:error, {:retry_after, _, {:batch_queue_full, :tenant}}} =
               Batcher.deliver(Codec, instance, o, entry(2, %{tenant: "a"}))

      two = async(instance, o, entry(3, %{tenant: "b"}))
      Process.sleep(20)

      # 20 bytes queued, 10 more would pass 25
      assert {:error, {:retry_after, _, {:batch_queue_full, :bytes}}} =
               Batcher.deliver(Codec, instance, o, entry(4, %{tenant: "c"}))

      send(task, :go)

      release_all()

      Enum.each([held, one, two], &Task.await/1)
    end
  end

  describe "failures lose nothing" do
    test "a codec that raises, or answers the wrong number of results, fails its entries",
         %{instance: instance, name: name} do
      raising = opts(name, mode: :raise)

      assert {:error, {:raised, %RuntimeError{message: "codec boom"}}} =
               Batcher.deliver(Codec, instance, raising, entry(1))

      short = opts(name <> "-short", mode: :short)

      tasks = for n <- 1..2, do: async(instance, short, entry(n))

      assert [{:error, {:bad_batch_results, _}}, {:error, {:bad_batch_results, _}}] =
               Enum.map(tasks, &Task.await/1)
    end

    test "a killed batcher fails the call, and the next delivery starts a new one", %{
      instance: instance,
      name: name
    } do
      o = opts(name, gate: true)
      waiting = async(instance, o, entry(1))
      assert_receive {:batch, [1], task}, 1_000

      [{pid, _}] =
        Registry.select(Ankusa.Registry, [
          {{{instance, {:sink_batcher, :_, :_}}, :"$1", :_}, [], [{{:"$1", 0}}]}
        ])

      Process.exit(pid, :kill)

      # the request in flight was not the batcher's: it still completes, but the
      # call is answered by the batcher's death, so the attempt fails and retries
      assert {:error, {:batcher_down, :killed}} = Task.await(waiting)
      send(task, :go)

      later = async(instance, Keyword.put(o, :gate, false), entry(2))
      assert_receive {:batch, [2], _}, 1_000
      assert Task.await(later) == :ok
    end

    test "an entry whose caller died before its batch left is not sent", %{
      instance: instance,
      name: name
    } do
      o = opts(name, gate: true, batch_max_inflight: 1, batch_linger_ms: 5)

      held = async(instance, o, entry(0))
      assert_receive {:batch, [0], task}, 1_000

      gone = spawn(fn -> Batcher.deliver(Codec, instance, o, entry(1)) end)
      alive = async(instance, o, entry(2))
      Process.sleep(30)
      Process.exit(gone, :kill)

      send(task, :go)
      assert_receive {:batch, [2], second}, 1_000
      send(second, :go)
      refute_receive {:batch, _, _}, 100

      assert Task.await(held) == :ok
      assert Task.await(alive) == :ok
    end

    test "a batch that never answers fails its callers at task_timeout_ms", %{
      instance: instance,
      name: name
    } do
      o = opts(name, gate: true, timeout_ms: 50)

      assert {:error, {:batch_timeout, 100}} = Batcher.deliver(Codec, instance, o, entry(1))
    end
  end

  test "batch_* options are range-checked", %{instance: instance} do
    assert_raise ArgumentError, ~r/:batch_max_entries must be an integer from 1 to 10/, fn ->
      Codec.batch_config([batch_max_entries: 11], instance)
    end

    assert_raise ArgumentError, ~r/:batch_linger_ms/, fn ->
      Codec.batch_config([batch_linger_ms: 0], instance)
    end
  end

  test "available?/1 is false outside an instance" do
    refute Batcher.available?(nil)
    refute Batcher.available?(:no_such_instance_for_batching)
  end
end
