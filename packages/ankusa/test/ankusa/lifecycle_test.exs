defmodule Ankusa.LifecycleTest do
  @moduledoc """
  Lifecycle events: a source or route created, updated, or deleted becomes one
  CloudEvents 1.0 message on `config.lifecycle.sinks`, read by
  `Ankusa.Lifecycle.Publisher` from the audit row the change committed with:
  durable across a `kill -9`, in revision order, with an id derived from the row.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.{
    Audit,
    Config,
    Edge.Ingest,
    Envelope,
    Lifecycle,
    LifecycleHarness,
    Routes,
    SourceStore,
    Store
  }

  alias Ankusa.Audit.Context
  alias Ankusa.SourceStore.Persistent
  alias Ankusa.Store.Keys

  defmodule CaptureSink do
    @moduledoc "Reports every delivery as `{:lifecycle, ctx, env, decoded_body}`, then follows `:outcome`."

    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, ctx, opts) do
      send(Keyword.fetch!(opts, :to), {:lifecycle, ctx, env, JSON.decode!(env.body)})
      if Keyword.get(opts, :outcome, :ok) == :ok, do: :ok, else: {:error, :nope}
    end
  end

  defmodule FlakySink do
    @moduledoc "Refuses the first `:fail` attempts of each event, then confirms."

    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, ctx, opts) do
      send(Keyword.fetch!(opts, :to), {:attempt, ctx.attempt})

      if ctx.attempt <= Keyword.fetch!(opts, :fail) do
        {:error, :broker_down}
      else
        send(Keyword.fetch!(opts, :to), {:lifecycle, ctx, env, JSON.decode!(env.body)})
        :ok
      end
    end
  end

  defmodule BadReturnSink do
    @moduledoc "A sink that is broken in a quieter way: it returns something that is neither `:ok` nor `{:error, _}`."

    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, _opts), do: {:ok, :offset}
  end

  defmodule RaisingSink do
    @moduledoc "A sink that is broken: it raises."

    @behaviour Ankusa.Sink

    @impl true
    def deliver(_env, _ctx, _opts), do: raise("boom")
  end

  defmodule GateSink do
    @moduledoc "Holds each delivery until the test sends `:go` to the delivering process."

    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, _ctx, opts) do
      send(Keyword.fetch!(opts, :to), {:gated, self(), env.id})

      receive do
        :go -> :ok
      end
    end
  end

  defmodule ClaimSink do
    @moduledoc "A queue-style sink: claims any body over 200 bytes, as the shipped broker sinks do."

    @behaviour Ankusa.Sink

    @impl true
    def inline_max_bytes(_opts), do: 200

    @impl true
    def deliver(env, ctx, opts) do
      {:ok, message} = Ankusa.Sink.Message.encode(env, ctx, 200)
      send(Keyword.fetch!(opts, :to), {:claimed, env, JSON.decode!(message)})
      :ok
    end
  end

  defmodule StaggerSink do
    @moduledoc "Slows a delivery the lower its revision is (300 ms minus 100 ms per revision), so a publisher that did not wait would reorder."

    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, ctx, opts) do
      event = JSON.decode!(env.body)
      Process.sleep(max(0, 300 - 100 * Map.get(event, "revision", 3)))
      send(Keyword.fetch!(opts, :to), {:lifecycle, ctx, env, event})
      :ok
    end
  end

  defmodule RecordPlug do
    @moduledoc "A loopback listener that reports every request path to the test."

    @behaviour Plug
    import Plug.Conn

    @impl true
    def init(opts), do: opts

    @impl true
    def call(conn, opts) do
      send(opts[:test], {:request, conn.request_path})
      send_resp(conn, 200, "ok")
    end
  end

  @doc false
  def report(event, _measurements, meta, pid), do: send(pid, {:telemetry, event, meta})

  @spec_map %{
    "verify" => %{"type" => "hmac", "secret" => "s3cr3t", "signature_header" => "X-Sig"},
    "sinks" => [%{"type" => "log"}]
  }

  @route %{"id" => "stripe", "path" => "/webhooks/stripe", "methods" => ["POST"]}

  @fast_retry {Ankusa.RetryPolicy.Exponential,
               base_ms: 1, max_ms: 5, jitter: false, max_attempts: 3}

  # What the server's YAML decoder is to a real store: enough to accept a spec.
  @doc false
  def decoder(_source_id, _spec), do: [sinks: [{Ankusa.Sink.Log, []}]]

  # Under `wal: :none` a source write needs a durable sink.
  @doc false
  def durable_decoder(_source_id, _spec),
    do: [sinks: [{Ankusa.Sink.Http, [url: "http://sink.test"]}]]

  defp start_instance(overrides \\ []) do
    base = [
      source_store: {Persistent, decoder: &__MODULE__.decoder/2},
      routes: [enabled: true, admin: [port: 0]],
      lifecycle: %{sinks: [{CaptureSink, to: self()}]}
    ]

    config = test_config(Keyword.merge(base, overrides))
    start_supervised!({Ankusa.Instance, config})
    config
  end

  defp attach(events) do
    handler = "lifecycle-#{inspect(self())}-#{System.unique_integer([:positive])}"
    :telemetry.attach_many(handler, events, &__MODULE__.report/4, self())
    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp next_event(timeout \\ 5_000) do
    receive do
      {:lifecycle, ctx, env, event} -> {ctx, env, event}
    after
      timeout -> flunk("no lifecycle event within #{timeout}ms")
    end
  end

  defp rows(instance) do
    {:ok, entries} = Audit.page(instance, 0, 100)
    for {_seq, {:ok, row}} <- entries, do: row
  end

  defp eventually(fun, deadline_ms \\ 5_000) do
    do_eventually(fun, System.monotonic_time(:millisecond) + deadline_ms)
  end

  defp do_eventually(fun, deadline) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("the condition was still false after the deadline")

      true ->
        Process.sleep(25)
        do_eventually(fun, deadline)
    end
  end

  describe "with a lifecycle sink" do
    setup do
      %{config: start_instance()}
    end

    test "a source's life is three events carrying the redacted source", %{config: config} do
      instance = config.instance
      {:ok, %{next_seq: seq}} = Ankusa.Queue.stats(instance)

      {:ok, _} = SourceStore.put(instance, "acme", "billing", @spec_map, :create)

      assert_receive {:lifecycle, ctx, env, event}, 2_000
      assert ctx.source_id == "ankusa:lifecycle"
      assert ctx.tenant_id == "acme"
      assert env.content_type == "application/cloudevents+json"

      assert %{
               "specversion" => "1.0",
               "id" => id,
               "source" => source,
               "type" => "io.ankusa.source.created",
               "subject" => "acme.billing",
               "datacontenttype" => "application/json",
               "data" => data
             } = event

      assert id == env.id
      assert source == "urn:ankusa:instance:#{instance}"
      assert {:ok, _time, 0} = DateTime.from_iso8601(event["time"])

      # The secret never leaves in an event, any more than in the admin API.
      assert data["verify"]["secret"] == "[REDACTED]"
      assert data["verify"]["signature_header"] == "X-Sig"
      assert data["ingest_path"] == "/webhooks/acme.billing"

      # The event bypassed the store: no seq was assigned to it.
      assert {:ok, %{next_seq: ^seq}} = Ankusa.Queue.stats(instance)

      {:ok, _} = SourceStore.put(instance, "acme", "billing", @spec_map, :update)
      assert_receive {:lifecycle, _, _, %{"type" => "io.ankusa.source.updated"} = updated}, 2_000
      assert updated["subject"] == "acme.billing"

      :ok = SourceStore.delete(instance, "acme", "billing")
      assert_receive {:lifecycle, _, _, %{"type" => "io.ankusa.source.deleted"} = deleted}, 2_000
      assert deleted["subject"] == "acme.billing"
      assert deleted["data"]["source_id"] == "acme.billing"
      assert deleted["data"]["verify"]["secret"] == "[REDACTED]"
    end

    test "a refused change emits nothing", %{config: config} do
      instance = config.instance

      {:ok, _} = SourceStore.put(instance, "acme", "billing", @spec_map, :create)
      assert_receive {:lifecycle, _, _, %{"type" => "io.ankusa.source.created"}}, 2_000

      assert {:error, :exists} = SourceStore.put(instance, "acme", "billing", @spec_map, :create)
      assert {:error, :not_found} = SourceStore.delete(instance, "acme", "missing")

      assert {:error, :not_found} =
               SourceStore.put(instance, "acme", "missing", @spec_map, :update)

      refute_receive {:lifecycle, _, _, _}, 200
    end

    test "a route's life: created, replaced, patched, deleted", %{config: config} do
      instance = config.instance

      {:ok, _} = Routes.create(instance, @route)

      assert_receive {:lifecycle, ctx, _, %{"type" => "io.ankusa.route.created"} = created}, 2_000
      assert created["subject"] == "stripe"
      assert created["data"]["path"] == "/webhooks/stripe"
      # Routes have no tenant: the envelope carries the default one.
      assert ctx.tenant_id == "default"

      # PUT on an id that exists is an update; on one that does not, a creation.
      {:ok, _} = Routes.replace(instance, "stripe", Map.put(@route, "methods", ["POST", "PUT"]))
      assert_receive {:lifecycle, _, _, %{"type" => "io.ankusa.route.updated"} = replaced}, 2_000
      assert replaced["subject"] == "stripe"
      assert replaced["data"]["methods"] == ["POST", "PUT"]

      {:ok, _} =
        Routes.replace(instance, "github", %{"path" => "/webhooks/github", "methods" => ["POST"]})

      assert_receive {:lifecycle, _, _,
                      %{"type" => "io.ankusa.route.created", "subject" => "github"}},
                     2_000

      {:ok, _} = Routes.update(instance, "stripe", %{"enabled" => false})
      assert_receive {:lifecycle, _, _, %{"type" => "io.ankusa.route.updated"} = patched}, 2_000
      assert patched["data"]["enabled"] == false

      :ok = Routes.delete(instance, "stripe")
      assert_receive {:lifecycle, _, _, %{"type" => "io.ankusa.route.deleted"} = deleted}, 2_000
      assert deleted["subject"] == "stripe"
      assert deleted["data"]["id"] == "stripe"
      assert deleted["data"]["path"] == "/webhooks/stripe"
      # The deletion is the route's fourth change; `data` is the route as it was.
      assert deleted["revision"] == 4
      assert deleted["data"]["revision"] == 3
    end

    test "the reserved source is not reachable by ingest", %{config: config} do
      assert Ingest.ingest(config.instance, request("ankusa:lifecycle", "{}")) ==
               {:error, :unknown_source}
    end
  end

  test "with no lifecycle sinks there is no publisher and changes still succeed" do
    config = start_instance(lifecycle: %{sinks: []})

    assert Ankusa.whereis(config.instance, :lifecycle) == nil
    assert {:ok, _} = SourceStore.put(config.instance, "acme", "billing", @spec_map, :create)
    assert {:ok, _} = Routes.create(config.instance, @route)
    refute_receive {:lifecycle, _, _, _}, 100
  end

  test "an event is published under wal: :none too" do
    config =
      start_instance(
        roles: [:edge],
        wal: :none,
        source_store: {Persistent, decoder: &__MODULE__.durable_decoder/2}
      )

    {:ok, _} = SourceStore.put(config.instance, "acme", "billing", @spec_map, :create)
    assert_receive {:lifecycle, _, _, %{"type" => "io.ankusa.source.created"}}, 2_000
  end

  describe "retries and drops" do
    test "a refusing sink is retried until it confirms" do
      config =
        start_instance(
          dispatch: [retry: @fast_retry],
          lifecycle: %{sinks: [{FlakySink, to: self(), fail: 2}]}
        )

      {:ok, _} = SourceStore.put(config.instance, "acme", "billing", @spec_map, :create)

      assert_receive {:lifecycle, %{attempt: 3}, _, %{"type" => "io.ankusa.source.created"}},
                     2_000

      assert_received {:attempt, 1}
      assert_received {:attempt, 2}
      assert_received {:attempt, 3}
      refute_receive {:lifecycle, _, _, _}, 100
    end

    test "when retries run out the event is dropped and counted, and the change stands" do
      attach([[:ankusa, :lifecycle, :dropped]])

      config =
        start_instance(
          dispatch: [retry: @fast_retry],
          lifecycle: %{sinks: [{CaptureSink, to: self(), outcome: :error}]}
        )

      assert {:ok, %{source_id: "acme.billing"}} =
               SourceStore.put(config.instance, "acme", "billing", @spec_map, :create)

      assert_receive {:telemetry, [:ankusa, :lifecycle, :dropped],
                      %{reason: :gave_up, type: "io.ankusa.source.created", instance: instance}},
                     2_000

      assert instance == config.instance
      assert {:ok, _} = SourceStore.get(config.instance, "acme", "billing")
    end

    test "a broken sink does not hold back another" do
      attach([[:ankusa, :lifecycle, :dropped]])

      config =
        start_instance(
          dispatch: [retry: @fast_retry],
          lifecycle: %{sinks: [{RaisingSink, []}, {CaptureSink, to: self()}]}
        )

      {:ok, _} = SourceStore.put(config.instance, "acme", "billing", @spec_map, :create)

      assert_receive {:lifecycle, _, _, %{"type" => "io.ankusa.source.created"}}, 2_000

      assert_receive {:telemetry, [:ankusa, :lifecycle, :dropped],
                      %{reason: :gave_up, sink: sink}},
                     2_000

      assert sink == inspect(RaisingSink)
    end

    test "a sink returning neither :ok nor an error is a failed delivery, and the publisher survives" do
      attach([[:ankusa, :lifecycle, :dropped]])

      config =
        start_instance(
          dispatch: [retry: @fast_retry],
          lifecycle: %{sinks: [{BadReturnSink, []}]}
        )

      publisher = Ankusa.whereis(config.instance, :lifecycle)
      assert is_pid(publisher)

      {:ok, _} = SourceStore.put(config.instance, "acme", "billing", @spec_map, :create)

      assert_receive {:telemetry, [:ankusa, :lifecycle, :dropped],
                      %{reason: :gave_up, sink: sink, type: "io.ankusa.source.created"}},
                     2_000

      assert sink == inspect(BadReturnSink)
      assert Ankusa.whereis(config.instance, :lifecycle) == publisher
    end

    test "an attempt that outlasts dispatch.attempt_timeout_ms is killed and retried, then dropped" do
      attach([[:ankusa, :lifecycle, :dropped]])

      config =
        start_instance(
          dispatch: [retry: @fast_retry, attempt_timeout_ms: 100],
          lifecycle: %{sinks: [{GateSink, to: self()}]}
        )

      {:ok, _} = SourceStore.put(config.instance, "acme", "billing", @spec_map, :create)

      # This GateSink blocks until `:go`, which the test never sends: only the
      # deadline ends the attempt. `:noproc` only if the kill beat the monitor.
      assert_receive {:gated, pid1, _}, 2_000
      ref = Process.monitor(pid1)
      assert_receive {:DOWN, ^ref, :process, ^pid1, reason}, 1_000
      assert reason in [:killed, :noproc]

      # The retry gates again in a new process, and every attempt times out.
      assert_receive {:gated, pid2, _}, 2_000
      assert pid2 != pid1

      assert_receive {:telemetry, [:ankusa, :lifecycle, :dropped],
                      %{reason: :gave_up, type: "io.ankusa.source.created"}},
                     3_000
    end

    test "an HTTP lifecycle sink aimed at loopback is refused by the egress policy, never sent" do
      attach([[:ankusa, :lifecycle, :dropped], [:ankusa, :lifecycle, :delivered]])
      port = Ankusa.TestHelpers.free_port()

      start_supervised!(
        {Bandit, plug: {RecordPlug, test: self()}, scheme: :http, ip: {127, 0, 0, 1}, port: port}
      )

      sink =
        {Ankusa.Sink.Http,
         url: "http://127.0.0.1:#{port}/events", allow_unsigned: true, timeout_ms: 500}

      # Plain http is allowed here; the loopback address still is not.
      config =
        start_instance(
          dispatch: [retry: @fast_retry],
          egress: %{allow_insecure_http: true},
          lifecycle: %{sinks: [sink]}
        )

      {:ok, _} = SourceStore.put(config.instance, "acme", "billing", @spec_map, :create)

      assert_receive {:telemetry, [:ankusa, :lifecycle, :dropped], %{reason: :gave_up}}, 2_000
      refute_received {:request, _}
    end

    test "the same sink delivers once egress allows the address" do
      attach([[:ankusa, :lifecycle, :delivered]])
      port = Ankusa.TestHelpers.free_port()

      start_supervised!(
        {Bandit, plug: {RecordPlug, test: self()}, scheme: :http, ip: {127, 0, 0, 1}, port: port}
      )

      config =
        start_instance(
          dispatch: [retry: @fast_retry],
          egress: %{allow_insecure_http: true, allow_cidrs: ["127.0.0.0/8"]},
          lifecycle: %{
            sinks: [
              {Ankusa.Sink.Http, url: "http://127.0.0.1:#{port}/events", allow_unsigned: true}
            ]
          }
        )

      {:ok, _} = SourceStore.put(config.instance, "acme", "billing", @spec_map, :create)

      assert_receive {:request, "/events"}, 2_000

      assert_receive {:telemetry, [:ankusa, :lifecycle, :delivered], %{sink: "Ankusa.Sink.Http"}},
                     2_000
    end
  end

  test "an event over a sink's inline threshold is claim-checked under a valid tenant" do
    config = start_instance(lifecycle: %{sinks: [{ClaimSink, to: self()}]})

    # A route's metadata makes its event larger than the 200-byte threshold.
    route = Map.put(@route, "metadata", %{"note" => String.duplicate("x", 500)})
    {:ok, _} = Routes.create(config.instance, route)

    assert_receive {:claimed, env, %{"claim" => claim, "sha256" => sha256} = message}, 2_000
    assert env.source_id == "ankusa:lifecycle"
    # Routes carry no tenant; a claim needs a real one (`[A-Za-z0-9_-]{1,64}`).
    assert env.tenant_id == "default"
    assert message["tenant_id"] == "default"
    assert env.size > 200

    assert {:ok, body} = Ankusa.ClaimCheck.redeem(config.instance, claim, sha256)
    assert body == env.body
    assert %{"type" => "io.ankusa.route.created", "subject" => "stripe"} = JSON.decode!(body)
  end

  describe "the outbox" do
    test "ordered by revision: three rapid updates arrive in revision order, whatever each takes" do
      config = start_instance(lifecycle: %{sinks: [{StaggerSink, to: self()}]})

      {:ok, _} = SourceStore.put(config.instance, "acme", "billing", @spec_map, :create)
      {:ok, _} = SourceStore.put(config.instance, "acme", "billing", @spec_map, :update)
      {:ok, _} = SourceStore.put(config.instance, "acme", "billing", @spec_map, :update)

      events = for _ <- 1..3, do: elem(next_event(), 2)

      assert Enum.map(events, & &1["revision"]) == [1, 2, 3]

      assert Enum.map(events, & &1["type"]) ==
               [
                 "io.ankusa.source.created",
                 "io.ankusa.source.updated",
                 "io.ankusa.source.updated"
               ]

      sequences = Enum.map(events, &String.to_integer(&1["sequence"]))
      assert sequences == Enum.sort(sequences)
      assert events |> Enum.map(& &1["id"]) |> Enum.uniq() |> length() == 3
      refute_receive {:lifecycle, _, _, _}, 200
    end

    test "an event carries revision, actor, instance, sequence and the row's time" do
      config = start_instance()
      context = %Context{actor: "alice"}

      {:ok, _} =
        SourceStore.put(config.instance, "acme", "billing", @spec_map, :create, audit: context)

      {_ctx, env, event} = next_event()

      assert event["revision"] == 1
      assert event["data"]["revision"] == 1
      assert event["actor"] == "alice"
      assert event["instance"] == to_string(config.instance)

      assert event["id"] =~
               ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-8[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/

      assert env.dedupe_key == event["id"]

      [row] = rows(config.instance)
      assert String.to_integer(event["sequence"]) == row.seq
      assert {:ok, time, 0} = DateTime.from_iso8601(event["time"])
      assert DateTime.to_unix(time, :millisecond) == row.at

      :ok = SourceStore.delete(config.instance, "acme", "billing", audit: context)
      {_ctx, _env, deleted} = next_event()
      assert deleted["type"] == "io.ankusa.source.deleted"
      assert deleted["revision"] == 2
    end

    test "publishing the same row twice yields the same event id, body and idempotency key" do
      config = start_instance()
      {:ok, _} = SourceStore.put(config.instance, "acme", "billing", @spec_map, :create)
      {_ctx, live, _event} = next_event()

      {:ok, [{seq, {:ok, row}}]} = Audit.page(config.instance, 0, 10)
      {:ok, entity} = Audit.entity(config.instance, seq)

      {:ok, again} = Lifecycle.envelope(config.instance, row, entity)
      {:ok, replayed} = Lifecycle.envelope(config.instance, row, entity, "replay-1")

      assert again.id == live.id
      assert replayed.id == live.id
      assert again.body == live.body
      assert Envelope.idempotency_key(again) == Envelope.idempotency_key(live)

      # `replayid` rides only on the replay path.
      assert JSON.decode!(replayed.body)["replayid"] == "replay-1"
      refute Map.has_key?(JSON.decode!(live.body), "replayid")
    end

    test "a publisher that died before the cursor moved delivers the event again, same id" do
      config = start_instance()
      {:ok, _} = SourceStore.put(config.instance, "acme", "billing", @spec_map, :create)
      {_ctx, first, _event} = next_event()

      [row] = rows(config.instance)
      cursor = <<row.seq::64>>

      eventually(fn ->
        Store.get(config.instance, :default, Keys.lifecycle_cursor()) == {:ok, cursor}
      end)

      # As if the node had died after the sink confirmed and before the write.
      :ok =
        Store.write(config.instance, [{:put, :default, Keys.lifecycle_cursor(), <<0::64>>}],
          sync: true
        )

      Process.exit(Ankusa.whereis(config.instance, :lifecycle), :kill)

      {_ctx, second, _event} = next_event()
      assert second.id == first.id
    end

    test "the row is flagged and the entity sits beside it, redacted; the row holds none" do
      config = start_instance()
      {:ok, _} = SourceStore.put(config.instance, "acme", "billing", @spec_map, :create)
      next_event()

      [row] = rows(config.instance)
      assert row.publish
      refute Map.has_key?(row, :entity)

      {:ok, entity} = Audit.entity(config.instance, row.seq)
      assert entity["verify"]["secret"] == "[REDACTED]"

      {:ok, stored} = Store.get(config.instance, :audit, Keys.audit(row.seq, row.at))
      refute String.contains?(stored, "s3cr3t")
    end

    test "what changed while lifecycle events were off is history" do
      config = start_instance(lifecycle: %{sinks: []})
      {:ok, _} = SourceStore.put(config.instance, "acme", "old", @spec_map, :create)

      [row] = rows(config.instance)
      refute row.publish
      assert Audit.entity(config.instance, row.seq) == :not_found

      stop_supervised!({Ankusa.Instance, config.instance})
      config = %{config | lifecycle: %{sinks: [{CaptureSink, to: self()}]}}
      start_supervised!({Ankusa.Instance, config})

      {:ok, _} = SourceStore.put(config.instance, "acme", "new", @spec_map, :create)
      assert {_ctx, _env, %{"subject" => "acme.new"}} = next_event()
      refute_receive {:lifecycle, _, _, _}, 200
    end
  end

  describe "survives kill" do
    @harness Path.expand("../support/lifecycle_harness.exs", __DIR__)

    test "source.created is delivered after kill -9 right after SourceStore.put/5 returns" do
      data_dir = unique_data_dir(:lifecycle_harness)
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
      await_put_done(port)

      # The child's only sink never confirms, so the event cannot have been
      # delivered: it exists only as what the change committed with.
      {_, 0} = System.cmd("kill", ["-9", Integer.to_string(os_pid)])
      assert_receive {^port, {:exit_status, 137}}, 10_000

      config = LifecycleHarness.config(data_dir, [{CaptureSink, to: self()}])
      start_supervised!({Ankusa.Instance, config})

      assert {%{tenant_id: "acme"}, _env,
              %{
                "type" => "io.ankusa.source.created",
                "subject" => "acme.billing",
                "revision" => 1
              }} = next_event(30_000)

      refute_receive {:lifecycle, _, _, _}, 300
    end

    defp await_put_done(port) do
      receive do
        {^port, {:data, {:eol, "PUT_DONE"}}} -> :ok
        {^port, {:data, {:eol, _other}}} -> await_put_done(port)
        {^port, {:exit_status, status}} -> flunk("the harness exited with #{status}")
      after
        90_000 -> flunk("the harness never finished its change")
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

  describe "a change nobody saw finish" do
    setup do
      config = start_instance()
      {:ok, _} = SourceStore.put(config.instance, "acme", "billing", @spec_map, :create)
      next_event()
      {:ok, current} = SourceStore.get(config.instance, "acme", "billing")

      %{
        config: config,
        hash: Audit.digest(config.instance, current.spec),
        attrs: %{
          action: "source.update",
          tenant: "acme",
          resource: "acme.billing",
          revision: 5
        }
      }
    end

    # A caller that begins a change and dies before it finishes it.
    defp left_pending(instance, attrs) do
      parent = self()

      {pid, ref} =
        spawn_monitor(fn ->
          {:ok, _} = Audit.begin(instance, Context.system(), attrs)
          send(parent, :began)
        end)

      assert_receive :began
      assert_receive {:DOWN, ^ref, :process, ^pid, _reason}
    end

    defp settled_row(instance) do
      eventually(fn -> length(rows(instance)) == 2 end)
      List.last(rows(instance))
    end

    test "a state that is the one the change was heading to is applied, and announced", ctx do
      left_pending(
        ctx.config.instance,
        Map.merge(ctx.attrs, %{before_hash: "x", after_hash: ctx.hash})
      )

      assert {_ctx, _env, %{"type" => "io.ankusa.source.updated", "revision" => 5} = event} =
               next_event()

      assert event["data"]["source_id"] == "acme.billing"
      row = settled_row(ctx.config.instance)
      assert %{result: "ok", publish: true, detail: %{"reconciled" => "applied"}} = row
    end

    test "a state that is the one it started from was not applied, and is not announced", ctx do
      left_pending(
        ctx.config.instance,
        Map.merge(ctx.attrs, %{before_hash: ctx.hash, after_hash: "y"})
      )

      row = settled_row(ctx.config.instance)
      assert %{result: "failed", publish: false, detail: %{"reconciled" => "not_applied"}} = row
      refute_receive {:lifecycle, _, _, _}, 300
    end

    test "a state that is neither stays interrupted and announces what is there now", ctx do
      left_pending(
        ctx.config.instance,
        Map.merge(ctx.attrs, %{before_hash: "x", after_hash: "y"})
      )

      # The current revision, not the one the interrupted change was heading to.
      assert {_ctx, _env, %{"type" => "io.ankusa.source.updated", "revision" => 1} = event} =
               next_event()

      assert event["data"]["source_id"] == "acme.billing"
      row = settled_row(ctx.config.instance)
      assert %{result: "interrupted", publish: true, detail: %{"reconciled" => "diverged"}} = row
    end

    test "an absent subject that was not the aim is announced as deleted", ctx do
      :ok = SourceStore.delete(ctx.config.instance, "acme", "billing")
      next_event()

      attrs = Map.merge(ctx.attrs, %{action: "source.update", before_hash: "x", after_hash: "y"})
      left_pending(ctx.config.instance, attrs)

      assert {_ctx, _env, %{"type" => "io.ankusa.source.deleted", "data" => data}} = next_event()
      assert data == %{"tenant" => "acme", "name" => "billing"}
    end

    test "a deletion that was applied is announced with the entity the row recorded", ctx do
      :ok = SourceStore.delete(ctx.config.instance, "acme", "billing")
      next_event()

      attrs = %{
        action: "source.delete",
        tenant: "acme",
        resource: "acme.billing",
        before_hash: ctx.hash,
        entity: %{"tenant" => "acme", "name" => "billing", "last" => "view"}
      }

      left_pending(ctx.config.instance, attrs)

      assert {_ctx, _env, %{"type" => "io.ankusa.source.deleted", "data" => data}} = next_event()
      assert data["last"] == "view"
    end

    test "a row a killed node left pending is settled at the next boot", ctx do
      attrs = Map.merge(ctx.attrs, %{before_hash: "x", after_hash: ctx.hash})
      {:ok, _pending} = Audit.begin(ctx.config.instance, Context.system(), attrs)

      stop_supervised!({Ankusa.Instance, ctx.config.instance})
      start_supervised!({Ankusa.Instance, ctx.config})

      assert {_ctx, _env, %{"type" => "io.ankusa.source.updated", "revision" => 5}} =
               next_event(15_000)

      assert %{result: "ok"} = settled_row(ctx.config.instance)
    end
  end

  describe "validate_config!/1" do
    test "refuses a lifecycle that is not a list of sinks" do
      assert_raise ArgumentError,
                   ~r/lifecycle.sinks must be a list of \{module, opts\} sinks/,
                   fn ->
                     Ankusa.Lifecycle.validate_config!(Config.new(lifecycle: %{sinks: [:nope]}))
                   end
    end

    test "refuses a static source that squats on the reserved id" do
      config =
        Config.new(
          source_store: {Ankusa.SourceStore.Static, sources: %{"ankusa:lifecycle" => []}},
          lifecycle: %{sinks: [{CaptureSink, to: self()}]}
        )

      assert_raise ArgumentError, ~r/"ankusa:lifecycle" is reserved for lifecycle events/, fn ->
        Ankusa.Lifecycle.validate_config!(config)
      end
    end

    test "a log-only lifecycle is valid under wal: :none" do
      config =
        Config.new(roles: [:edge], wal: :none, lifecycle: %{sinks: [{Ankusa.Sink.Log, []}]})

      assert Ankusa.Lifecycle.validate_config!(config) == :ok
    end

    test "lifecycle off is always valid" do
      assert Ankusa.Lifecycle.validate_config!(Config.new(roles: [:edge], wal: :none)) == :ok
    end
  end
end
