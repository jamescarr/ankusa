defmodule Ankusa.QueueDualTest do
  @moduledoc """
  `wal: :dual` (ADR 0002): the ack path against `Ankusa.Intake.Memory`, one half
  at a time. Every case asserts on what a consumer would see (the `201`, the
  hooks in the store, the deliveries to the sink, the records in the log), not
  only on return values.
  """
  use ExUnit.Case, async: false

  @moduletag capture_log: true

  import Ankusa.Test.Dual
  import Ankusa.TestHelpers, only: [request: 2]

  alias Ankusa.Edge.{Ingest, Router}
  alias Ankusa.Intake
  alias Ankusa.Intake.{Memory, Pending}
  alias Ankusa.Queue

  alias Ankusa.Test.Dual.FleetStub

  describe "the ack waits for the intake log" do
    test "no ack without intake confirm" do
      config = start_dual()
      Memory.hold(config.instance)

      task = Task.async(fn -> ingest(config, "evt-1") end)

      # The local half is done (the hook is in the store) and still no `201`:
      # the reply is withheld until the log confirms.
      assert wait_until(fn -> length(hook_ids(config)) == 1 end)
      assert Task.yield(task, 200) == nil
      assert Memory.records(config.instance) == []

      Memory.release(config.instance)
      assert {:ok, env} = Task.await(task)
      assert [%{id: id}] = Memory.records(config.instance)
      assert id == env.id
      assert drained(config)
    end

    @doc false
    # Runs in the queue's syncer, before its fsync: tells the test and waits.
    def hold_sync(_event, _measurements, %{instance: inst}, {inst, test_pid}) do
      send(test_pid, {:syncing, self()})

      receive do
        :release -> :ok
      after
        10_000 -> :ok
      end
    end

    def hold_sync(_event, _measurements, _metadata, _config), do: :ok

    # DUR-4.b: the local half's confirm is the syncer's, joined with the publish's.
    test "no ack before the local fsync, even once the log has confirmed" do
      config = start_dual()
      handler = "dual-hold-sync-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:ankusa, :wal_sync, :start],
        &__MODULE__.hold_sync/4,
        {config.instance, self()}
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      task = Task.async(fn -> ingest(config, "evt-1") end)
      assert_receive {:syncing, syncer}, 5_000

      # applied locally and confirmed by the log, fsync still running: no `201`
      assert wait_until(fn -> length(Memory.records(config.instance)) == 1 end)
      assert Task.yield(task, 200) == nil

      send(syncer, :release)
      assert {:ok, env} = Task.await(task)
      assert [%{id: id}] = Memory.records(config.instance)
      assert id == env.id
      assert drained(config)
    end

    test "ack waits for both halves and costs the max, not the sum" do
      config = start_dual()
      Memory.set_latency(config.instance, 30)

      {micros, {:ok, _env}} = :timer.tc(fn -> ingest(config, "evt-1") end)

      assert micros >= 30_000
      # the local fsync rides inside the confirm's 30 ms; the sequential figure
      # would be 30 ms plus the fsync
      assert micros < 200_000
    end

    test "a confirmed hook leaves the pending set and the watermark follows" do
      config = start_dual()
      assert {:ok, env} = ingest(config, "evt-1")
      assert drained(config)

      t = Pending.table(config.instance)
      assert Pending.watermark(t, env.seq + 1) == env.seq
      assert is_integer(Pending.offset(t))
    end
  end

  describe "one half persists" do
    test "publish fails after local commit: 503 intake_unavailable, one hook id per dedupe key, one delivery" do
      config = start_dual(roles: [:edge, :dispatch])
      Memory.fail(config.instance, :unavailable)

      assert {:error, :intake_unavailable} = ingest(config, "evt-1")
      assert_receive {:delivered, first}

      # The retry meets an original the log has not confirmed: never a `201`.
      assert {:error, :intake_pending} = ingest(config, "evt-1")

      Memory.heal(config.instance)
      assert drained(config)

      # Now the original is in both places: the retry is a duplicate of it.
      assert {:duplicate, %{id: ^first}} = ingest(config, "evt-1")

      deliver_all(config)
      refute_receive {:delivered, _}, 300
      assert [%{id: ^first}] = Memory.records(config.instance)
    end

    test "local commit fails after publish: 503 store_unavailable, the reconciler writes the local half" do
      config = start_dual()
      assert {:ok, _} = ingest(config, "warm")
      assert drained(config)

      sup = Ankusa.via(config.instance, :instance)
      :ok = Supervisor.terminate_child(sup, {Ankusa.Store, config.instance})

      # No dedupe key: nothing reads the store before the commit, so the log
      # half confirms and the local write fails.
      assert {:error, :store_unavailable} =
               Ingest.ingest(config.instance, request("load", ~s({"k":"solo"})))

      assert Enum.count(Memory.records(config.instance)) == 2
      assert pending(config) == 1

      {:ok, _pid} = Supervisor.restart_child(sup, {Ankusa.Store, config.instance})
      assert drained(config)

      # the hook is in the store under the seq the log record carries
      [_warm, %{seq: seq, id: id}] = Memory.records(config.instance)
      assert {:ok, hooks} = Queue.hooks(config.instance, 0, 10)
      assert Enum.any?(hooks, &(&1.id == id and &1.seq == seq))
    end

    test "both halves fail: the record is carried and completes when either recovers" do
      config = start_dual()
      assert {:ok, _} = ingest(config, "warm")
      assert drained(config)

      sup = Ankusa.via(config.instance, :instance)
      :ok = Supervisor.terminate_child(sup, {Ankusa.Store, config.instance})
      Memory.fail(config.instance, :unavailable)

      assert {:error, :store_unavailable} =
               Ingest.ingest(config.instance, request("load", ~s({"k":"solo"})))

      assert pending(config) == 1

      {:ok, _pid} = Supervisor.restart_child(sup, {Ankusa.Store, config.instance})
      assert wait_until(fn -> length(hook_ids(config)) == 2 end)
      assert pending(config) == 1

      Memory.heal(config.instance)
      assert drained(config)
      assert length(Memory.records(config.instance)) == 2
    end

    test "publish timeout is a failure and a republish applies once" do
      config = start_dual()
      # the batch lands in the log, the ack is lost
      Memory.lost_ack(config.instance, :unavailable, 1)

      assert {:error, :intake_unavailable} = ingest(config, "evt-1")
      assert drained(config)

      # the log holds the record twice (the first attempt and the republish)
      assert [%{seq: seq, id: id}, %{seq: seq, id: id}] = Memory.records(config.instance)
      assert hook_ids(config) == [id]
    end
  end

  describe "refused before either half" do
    test "an open breaker writes nothing, and the claim is not held" do
      config = start_dual(intake: %{breaker_failures: 1})
      Memory.set_ready(config.instance, false)
      Memory.fail(config.instance, :unavailable)

      assert {:error, :intake_unavailable} = ingest(config, "evt-1")
      assert wait_until(fn -> Intake.state(config.instance) == :down end)

      before = hook_ids(config)
      assert {:error, :intake_unavailable} = ingest(config, "evt-2")
      assert hook_ids(config) == before
    end

    test "the pending ceiling refuses before a seq is assigned" do
      config = start_dual(intake: %{pending: %{max_records: 1}})
      Memory.hold(config.instance)

      first = Task.async(fn -> ingest(config, "evt-1") end)
      assert wait_until(fn -> pending(config) == 1 end)

      assert {:ok, [{:error, :intake_unavailable}]} = enqueue_direct(config)
      assert length(hook_ids(config)) == 1

      Memory.release(config.instance)
      assert {:ok, _} = Task.await(first)
    end

    test "the pending byte ceiling refuses too" do
      config = start_dual(intake: %{pending: %{max_bytes: 1}})
      assert {:error, :intake_unavailable} = ingest(config, "evt-1")
      assert hook_ids(config) == []
      assert Memory.publish_calls(config.instance) == 0
    end

    test "a record over intake.max_record_bytes is refused alone" do
      config = start_dual(max_body_bytes: 10, intake: %{max_record_bytes: 65_546})
      pad = String.duplicate("x", 70_000)

      assert {:error, :intake_unavailable} = ingest(config, "big", [{"x-pad", pad}])
      assert hook_ids(config) == []
      assert {:ok, _} = ingest(config, "small")
    end
  end

  describe "the breaker" do
    test "opens after breaker_failures, half-opens one batch, closes on confirm; never disk-only" do
      config = start_dual(intake: %{breaker_failures: 3, probe_ms: 150})
      Memory.set_ready(config.instance, false)
      Memory.fail(config.instance, :unavailable)

      for n <- 1..3, do: assert({:error, :intake_unavailable} = ingest(config, "evt-#{n}"))

      # open: every request is refused and nothing new is stored
      stored = length(hook_ids(config))
      logged = length(Memory.records(config.instance))

      for n <- 4..8, do: assert({:error, :intake_unavailable} = ingest(config, "evt-#{n}"))
      assert length(hook_ids(config)) == stored
      assert length(Memory.records(config.instance)) == logged

      # the log is back: after probe_ms one batch is let through and its confirm closes it
      Memory.heal(config.instance)
      Memory.set_ready(config.instance, true)
      Process.sleep(200)
      assert {:ok, _} = ingest(config, "evt-9")
      assert {:ok, _} = ingest(config, "evt-10")
    end
  end

  describe "edge mapping" do
    test "intake_unavailable reaches the provider with Retry-After; a retry meeting the original is intake_pending" do
      config = start_dual(intake: %{retry_after_s: 7})
      Memory.fail(config.instance, :unavailable)

      post = fn key ->
        conn =
          :post
          |> Plug.Test.conn("/webhooks/load", "{}")
          |> Plug.Conn.put_req_header("x-event-id", key)

        Router.call(conn, Router.init(instance: config.instance))
      end

      # The first request is carried (its log half failed): a 503 for the provider
      first = post.("evt-1")
      assert first.status == 503
      assert JSON.decode!(first.resp_body) == %{"error" => "intake_unavailable"}

      retry = post.("evt-1")
      assert retry.status == 503
      assert JSON.decode!(retry.resp_body) == %{"error" => "intake_pending"}
      assert Plug.Conn.get_resp_header(retry, "retry-after") == ["1"]

      # with the breaker open (the test double is not ready) the answer carries
      # intake.retry_after_s
      Memory.set_ready(config.instance, false)
      for n <- 2..4, do: post.("evt-#{n}")
      refused = post.("evt-9")
      assert refused.status == 503
      assert Plug.Conn.get_resp_header(refused, "retry-after") == ["7"]
    end
  end

  describe "under dedupe.scope: fleet" do
    test "the claim stays pending while a half is missing, is promoted on completion, and is not released" do
      table = :ets.new(:fleet_stub, [:public, :set])

      config =
        start_dual(
          fleet: %{store: {FleetStub, table: table}, timeout_ms: 20},
          dedupe: %{scope: :fleet}
        )

      Memory.fail(config.instance, :unavailable)
      assert {:error, :intake_unavailable} = ingest(config, "evt-1")
      [id] = hook_ids(config)

      key = {"default", "load", "evt-1"}
      Process.sleep(100)
      assert [{^key, {:pending, ^id}}] = :ets.lookup(table, key)
      assert ops(table) == []

      Memory.heal(config.instance)
      assert wait_until(fn -> match?([{^key, {:committed, ^id}}], :ets.lookup(table, key)) end)
      assert [{:promote, %{key: "evt-1"}, ^id}] = ops(table)
    end
  end

  describe "startup line" do
    test "states the dual loss window" do
      config = start_dual()
      line = Intake.durability(config)

      assert line =~ "Durable to power loss on this host AND confirmed in the intake log"
      assert line =~ "ankusa.intake.default"
      assert line =~ "an acked hook is lost only if this host and every in-sync replica"
      assert line =~ "backed up every 3600000 ms"
    end

    test "says UNVERIFIED when verify_topic is false" do
      config = dual_config(intake: %{verify_topic: false})
      assert Intake.durability(config) =~ "intake topic settings UNVERIFIED"
    end
  end

  describe "metrics" do
    test "the intake series exist, and their number does not grow with tenants" do
      config = start_dual(metrics: %{enabled: true, port: 0})
      Memory.fail(config.instance, :unavailable, 1)

      assert {:error, :intake_unavailable} = ingest(config, "evt-1")
      assert {:ok, _} = ingest(config, "evt-2")
      assert drained(config)
      Ankusa.Metrics.Gauges.measure(config.instance)

      scrape = Ankusa.Metrics.scrape(config.instance)

      for series <- [
            ~s(ankusa_intake_publish_duration_seconds_count{instance="#{config.instance}",outcome="ok"}),
            ~s(ankusa_intake_publish_duration_seconds_count{instance="#{config.instance}",outcome="unavailable"}),
            ~s(ankusa_intake_failures_total{class="unavailable",instance="#{config.instance}"}),
            ~s(ankusa_intake_reconciled_total{half="log",instance="#{config.instance}"}),
            "ankusa_intake_pending_records",
            "ankusa_intake_pending_bytes",
            "ankusa_intake_reconcile_oldest_age_seconds",
            "ankusa_intake_reconcile_stuck",
            "ankusa_intake_offset_lag_seconds",
            "ankusa_intake_ready"
          ] do
        assert scrape =~ series, "missing #{series}"
      end

      # no intake series carries a tenant
      refute scrape =~ ~r/ankusa_intake_[a-z_]+\{[^}]*tenant/
    end
  end

  describe "group commit" do
    defp keyed_direct(config, key) do
      env = %Ankusa.Envelope{
        id: Ankusa.UUIDv7.generate(),
        source_id: "load",
        tenant_id: "default",
        received_at: System.system_time(:millisecond),
        method: "POST",
        path: "/webhooks/load",
        headers: [],
        body: "{}",
        size: 2,
        dedupe_key: key
      }

      Queue.enqueue(config.instance, [
        %{
          envelope: env,
          sinks: [{Ankusa.Test.Dual.CollectSink, [pid: self()]}],
          dedupe_ttl_ms: 60_000
        }
      ])
    end

    test "the same key from two partitions is one hook, answered only after the log confirms" do
      config = start_dual()
      inst = config.instance
      writer = Ankusa.whereis(inst, :queue_writer)
      Memory.hold(inst)
      :ok = :sys.suspend(writer)

      tasks = for _ <- 1..2, do: Task.async(fn -> keyed_direct(config, "same-key") end)

      assert wait_until(fn ->
               Process.info(writer, :message_queue_len) == {:message_queue_len, 2}
             end)

      :ok = :sys.resume(writer)

      # both prefetches read the key as absent; the group committed one hook, and
      # nothing is acked while the log has not confirmed it
      assert wait_until(fn -> length(hook_ids(config)) == 1 end)
      assert Enum.all?(tasks, &(Task.yield(&1, 100) == nil))
      assert Memory.records(inst) == []

      Memory.release(inst)
      replies = Enum.map(tasks, &Task.await(&1, 5_000))

      assert [{:ok, [{:committed, committed}]}] =
               Enum.filter(replies, &match?({:ok, [{:committed, _}]}, &1))

      assert [{:ok, [{:duplicate, duplicate}]}] =
               Enum.filter(replies, &match?({:ok, [{:duplicate, _}]}, &1))

      assert duplicate.id == committed.id

      assert [%{id: id}] = Memory.records(inst)
      assert id == committed.id
      assert drained(config)
    end
  end
end
