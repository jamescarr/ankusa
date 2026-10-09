defmodule Ankusa.MetricsTest do
  @moduledoc """
  The built-in Prometheus mapping, proved end to end: a real instance emits real
  telemetry, and the scrape shows it. `Ankusa.Metrics` is only meaningful as a
  whole — a definition that compiles but drops every event (a tag missing from
  the metadata, a `:keep` that never matches) is indistinguishable from a
  working one until someone looks at a scrape.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.SourceStore.Static

  defp start_instance(extra \\ []) do
    config =
      test_config(
        Keyword.merge(
          [
            roles: [:edge],
            admin: %{enabled: true, port: 0},
            source_store:
              {Static,
               sources: %{
                 "demo" => [
                   verifier: {Ankusa.Verifier.None, []},
                   sinks: [{Ankusa.Sink.Log, []}]
                 ],
                 "strict" => [
                   verifier: {Ankusa.Verifier.Hmac, scheme: :stripe, secret: "whsec_x"},
                   on_verify_failure: :quarantine
                 ]
               }}
          ],
          extra
        )
      )

    put_config(config)
    start_supervised!({Ankusa.Instance, config})
    config
  end

  test "a scrape reports a committed ingest with bounded labels" do
    config = start_instance()

    assert {:ok, _env} =
             Ankusa.Edge.Ingest.ingest(config.instance, request("demo", ~s({"id":"evt_1"})))

    scrape = Ankusa.Metrics.scrape(config.instance)

    assert scrape =~ "ankusa_ingest_requests_total{"
    assert scrape =~ ~s(outcome="committed")
    assert scrape =~ ~s(source_id="demo")
    assert scrape =~ ~s(instance="#{config.instance}")
  end

  test "unknown sources never mint a source_id series" do
    config = start_instance()
    ids = for _ <- 1..50, do: Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)

    for id <- ids do
      assert route_through_edge(config, request(id, "{}")).status == 404
    end

    scrape = Ankusa.Metrics.scrape(config.instance)

    for id <- ids, do: refute(scrape =~ id)

    assert [refused] =
             scrape
             |> String.split("\n")
             |> Enum.filter(&String.starts_with?(&1, "ankusa_ingest_refused_total{"))

    assert refused =~ ~s(instance="#{config.instance}")
    assert refused =~ ~s(reason="unknown_source")
    assert String.ends_with?(refused, "} 50")
    refute scrape =~ ~s(outcome="unknown_source")
  end

  test "an oversize body is a refusal, not a request" do
    config = start_instance(max_body_bytes: 16)

    assert route_through_edge(config, request("demo", String.duplicate("x", 100))).status == 413

    scrape = Ankusa.Metrics.scrape(config.instance)

    assert scrape =~ ~r/^ankusa_ingest_refused_total\{[^}]*reason="payload_too_large"[^}]*\} 1$/m
    refute scrape =~ ~s(source_id="demo")
  end

  test "a header no sink can carry is a refusal" do
    config = start_instance()

    conn = route_through_edge(config, request("demo", "x", [{"x-note", "caf\xC3\xA9"}]))
    assert conn.status == 400

    scrape = Ankusa.Metrics.scrape(config.instance)

    assert scrape =~ ~r/^ankusa_ingest_refused_total\{[^}]*reason="invalid_header"[^}]*\} 1$/m
  end

  test "only failed verifications count as verify failures" do
    config = start_instance()

    assert {:quarantined, _reason} =
             Ankusa.Edge.Ingest.ingest(config.instance, request("strict", ~s({"id":"q1"})))

    assert {:ok, _env} =
             Ankusa.Edge.Ingest.ingest(config.instance, request("demo", ~s({"id":"evt_ok"})))

    scrape = Ankusa.Metrics.scrape(config.instance)

    assert scrape =~ "ankusa_verify_failures_total{"
    assert scrape =~ ~s(provider="Ankusa.Verifier.Hmac")
    assert scrape =~ ~s(scheme="stripe")
    # ...and the successful verification above left no series at all: only
    # `status: :failed` events are kept.
    refute scrape =~ ~s(provider="Ankusa.Verifier.None")
  end

  test "a denied hook is counted once, with its tenant, and tagged rate_limited" do
    config = start_instance(rate_limits: %{tenants: %{"acme" => %{rate: 0.001, burst: 1}}})

    req = Map.put(request("demo", "x"), :tenant_id, "acme")

    assert {:ok, _env} = Ankusa.Edge.Ingest.ingest(config.instance, req)
    assert {:error, {:rate_limited, _}} = Ankusa.Edge.Ingest.ingest(config.instance, req)

    scrape = Ankusa.Metrics.scrape(config.instance)

    assert scrape =~ "ankusa_rate_limit_rejected_total{"
    assert scrape =~ ~s(tenant_id="acme")
    assert scrape =~ ~s(outcome="rate_limited")
  end

  test "duration histograms are exported in seconds, not native units" do
    config = start_instance()

    assert {:ok, _env} =
             Ankusa.Edge.Ingest.ingest(config.instance, request("demo", ~s({"id":"evt_2"})))

    scrape = Ankusa.Metrics.scrape(config.instance)

    assert scrape =~ "ankusa_ingest_duration_seconds_bucket{"

    [_, sum] = Regex.run(~r/ankusa_ingest_duration_seconds_sum\{[^}]*\} ([\d.e-]+)/, scrape)
    seconds = String.to_float(sum)

    # A local commit is milliseconds; native (nanosecond) units would be ~1e6.
    assert seconds < 60
    assert seconds >= 0
  end

  test "two instances in one VM each scrape only their own series" do
    a = start_instance()
    b = start_instance()

    assert {:ok, _env} = Ankusa.Edge.Ingest.ingest(a.instance, request("demo", ~s({"id":"a1"})))
    assert {:ok, _env} = Ankusa.Edge.Ingest.ingest(b.instance, request("demo", ~s({"id":"b1"})))

    scrape_a = Ankusa.Metrics.scrape(a.instance)

    assert scrape_a =~ ~s(instance="#{a.instance}")
    refute scrape_a =~ ~s(instance="#{b.instance}")
    assert Ankusa.Metrics.scrape(b.instance) =~ ~s(instance="#{b.instance}")
  end

  defmodule OkSink do
    @behaviour Ankusa.Sink
    @impl true
    def deliver(_env, _ctx, _opts), do: :ok
  end

  test "deliveries are counted per sink under dispatch and in direct mode" do
    sources = %{"q" => [verifier: {Ankusa.Verifier.None, []}, sinks: [{OkSink, []}]]}

    queued = start_instance(roles: [:edge, :dispatch], source_store: {Static, sources: sources})
    assert {:ok, _env} = Ankusa.Edge.Ingest.ingest(queued.instance, request("q", ~s({"id":"1"})))
    assert {:ok, _} = Ankusa.Dispatch.Pipeline.tick(queued.instance)

    direct = start_instance(wal: :none, source_store: {Static, sources: sources})
    assert {:ok, _env} = Ankusa.Edge.Ingest.ingest(direct.instance, request("q", ~s({"id":"2"})))

    for config <- [queued, direct] do
      assert Ankusa.Metrics.scrape(config.instance) =~
               ~r/^ankusa_dispatch_deliveries_total\{[^}]*result="ok"[^}]*sink="Ankusa.MetricsTest.OkSink"[^}]*\} 1$/m
    end
  end

  describe "gauges" do
    defp gauge(scrape, name, instance) do
      case Regex.run(~r/^#{name}\{instance="#{instance}"\} (\S+)$/m, scrape) do
        [_, value] -> value |> Float.parse() |> elem(0)
        nil -> nil
      end
    end

    test "the store, queue and pen gauges report what is held" do
      config = start_instance()
      inst = config.instance

      for i <- 1..3 do
        assert {:ok, _} = Ankusa.Edge.Ingest.ingest(inst, request("demo", ~s({"id":"g#{i}"})))
      end

      # A forged hook for a quarantining source goes to the pen.
      assert {:quarantined, _} = Ankusa.Edge.Ingest.ingest(inst, request("strict", "{}"))

      :ok = Ankusa.Metrics.Gauges.measure(inst)
      scrape = Ankusa.Metrics.scrape(inst)

      # No dispatch role here: the three hooks' rows are due and stay due.
      assert gauge(scrape, "ankusa_queue_pending", inst) == 3.0
      assert gauge(scrape, "ankusa_queue_scheduled", inst) == 0.0
      assert gauge(scrape, "ankusa_queue_inflight", inst) == 0.0
      assert gauge(scrape, "ankusa_queue_dead", inst) == 0.0
      assert gauge(scrape, "ankusa_queue_oldest_due_age_seconds", inst) >= 0.0
      assert gauge(scrape, "ankusa_store_hooks", inst) != nil
      assert gauge(scrape, "ankusa_store_disk_bytes", inst) != nil
      assert gauge(scrape, "ankusa_quarantine_entries", inst) == 1.0
      assert gauge(scrape, "ankusa_quarantine_bytes", inst) > 0.0
    end

    test "a probe that fails emits nothing and the others still run" do
      config = start_instance()
      inst = config.instance
      pen = Ankusa.whereis(inst, :quarantine)
      :ok = :sys.suspend(pen)
      on_exit(fn -> if Process.alive?(pen), do: :sys.resume(pen) end)

      task = Task.async(fn -> Ankusa.Metrics.Gauges.measure(inst) end)
      assert Task.await(task, 10_000) == :ok
      :ok = :sys.resume(pen)

      scrape = Ankusa.Metrics.scrape(inst)
      assert gauge(scrape, "ankusa_quarantine_entries", inst) == nil
      assert gauge(scrape, "ankusa_queue_pending", inst) == 0.0
    end

    test "dispatch reports its scheduler on every housekeeping tick" do
      config = start_instance(roles: [:edge, :dispatch])
      Process.sleep(1_200)

      scrape = Ankusa.Metrics.scrape(config.instance)
      assert gauge(scrape, "ankusa_dispatch_running", config.instance) == 0.0
      assert gauge(scrape, "ankusa_dispatch_breakers_open", config.instance) == 0.0
      assert gauge(scrape, "ankusa_dispatch_runnable", config.instance) == 0.0
    end
  end
end
