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

  test "a scrape reports a committed ingest by tenant, with bounded labels" do
    config = start_instance()

    assert {:ok, _env} =
             Ankusa.Edge.Ingest.ingest(config.instance, request("demo", ~s({"id":"evt_1"})))

    scrape = Ankusa.Metrics.scrape(config.instance)

    assert scrape =~ "ankusa_ingest_requests_total{"
    assert scrape =~ ~s(outcome="committed")
    assert scrape =~ ~s(tenant_id="default")
    assert scrape =~ ~s(instance="#{config.instance}")
    # `metrics.detail` defaults to `:tenant`: a source is not a label.
    refute scrape =~ "source_id="
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

  test "a SHA-1 source counts every verification, ok or failed, and nothing else does" do
    scheme = %Ankusa.Verifier.Hmac.Scheme{signature_header: "x-sig", hash: :sha1}

    config =
      start_instance(
        source_store:
          {Static,
           sources: %{
             "legacy" => [
               verifier: {Ankusa.Verifier.Hmac, scheme: scheme, secret: "k"},
               on_verify_failure: :accept_flag,
               sinks: [{Ankusa.Sink.Log, []}]
             ],
             "modern" => [
               verifier: {Ankusa.Verifier.Hmac, scheme: :stripe, secret: "whsec_x"},
               on_verify_failure: :accept_flag,
               sinks: [{Ankusa.Sink.Log, []}]
             ]
           }},
        metrics: %{detail: :source}
      )

    good = Base.encode16(:crypto.mac(:hmac, :sha, "k", "{}"), case: :lower)

    assert {:ok, _} =
             Ankusa.Edge.Ingest.ingest(
               config.instance,
               request("legacy", "{}", [{"x-sig", good}])
             )

    assert {:ok, _} = Ankusa.Edge.Ingest.ingest(config.instance, request("legacy", "{}"))
    assert {:ok, _} = Ankusa.Edge.Ingest.ingest(config.instance, request("modern", "{}"))

    scrape = Ankusa.Metrics.scrape(config.instance)

    assert scrape =~
             ~r/^ankusa_verifier_weak_hash_total\{(?=[^}]*scheme="custom")(?=[^}]*source_id="legacy")[^}]*\} 2$/m

    refute scrape =~ ~r/^ankusa_verifier_weak_hash_total\{[^}]*source_id="modern"/m
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

  test "a failed backup verify is counted per target, a clean one is not" do
    config = start_instance()
    inst = config.instance

    verify = fn target, result ->
      Ankusa.Telemetry.emit([:backup, :verify], %{duration: 1, files: 3, bytes: 9}, %{
        instance: inst,
        target: target,
        result: result,
        reason: nil
      })
    end

    verify.(:primary, :ok)
    verify.(:primary, :none)
    verify.(:secondary, :error)
    verify.(:secondary, :error)

    scrape = Ankusa.Metrics.scrape(inst)
    assert scrape =~ ~r/ankusa_backup_verify_failed_total\{[^}]*target="secondary"[^}]*\} 2/
    refute scrape =~ ~r/ankusa_backup_verify_failed_total\{[^}]*target="primary"/
  end

  test "dropped forwarded headers are counted per reason, never per source" do
    inst = start_instance().instance

    for {reason, count} <- [bytes: 2, bytes: 1, count: 4] do
      Ankusa.Telemetry.emit([:sink, :headers_dropped], %{count: count}, %{
        instance: inst,
        source_id: "demo",
        tenant_id: "default",
        reason: reason
      })
    end

    scrape = Ankusa.Metrics.scrape(inst)

    assert scrape =~
             ~r/^ankusa_sink_headers_dropped_total\{[^}]*reason="bytes"[^}]*\} 3$/m

    assert scrape =~
             ~r/^ankusa_sink_headers_dropped_total\{[^}]*reason="count"[^}]*\} 4$/m

    refute scrape =~ ~r/^ankusa_sink_headers_dropped_total\{[^}]*source_id=/m
  end

  test "the store's failure series follow its write failures and reopens" do
    config = start_instance()
    inst = config.instance
    store = Ankusa.via(inst, :store)

    # Booted: the outage gauge is there to alert on, at 0.
    assert gauge(Ankusa.Metrics.scrape(inst), "ankusa_store_unavailable_seconds", inst) == 0.0

    # A failed write is counted by class, and the reopen it asks for is timed.
    Ankusa.Store.report_write_failure(inst, ~c"IO error: No space left on device: While append")

    scrape =
      scrape_until(inst, ~r/^ankusa_store_wal_replay_seconds_count\{instance="#{inst}"\} 1$/m)

    assert scrape =~
             ~r/^ankusa_store_write_failures_total\{class="no_space",instance="#{inst}"\} 1$/m

    assert scrape =~ ~r/^ankusa_store_reopen_total\{instance="#{inst}",result="ok"\} 1$/m
    assert gauge(scrape, "ankusa_store_unavailable_seconds", inst) == 0.0

    # A reopen that fails is counted, adds nothing to the replay histogram, and
    # the one that works again resets the gauge.
    pid = GenServer.whereis(store)
    good = :sys.get_state(pid).path
    blocker = Path.join(config.data_dir, "blocker")
    File.write!(blocker, "a file where a directory has to be")
    :sys.replace_state(pid, fn state -> %{state | path: Path.join(blocker, "store")} end)
    assert {:error, _reason} = Ankusa.Store.reopen(inst)

    scrape = Ankusa.Metrics.scrape(inst)
    assert scrape =~ ~r/^ankusa_store_reopen_total\{instance="#{inst}",result="error"\} 1$/m
    assert scrape =~ ~r/^ankusa_store_wal_replay_seconds_count\{instance="#{inst}"\} 1$/m
    assert gauge(scrape, "ankusa_store_unavailable_seconds", inst) >= 0.0

    :sys.replace_state(pid, fn state -> %{state | path: good} end)
    assert :ok = Ankusa.Store.reopen(inst)

    scrape = Ankusa.Metrics.scrape(inst)
    assert scrape =~ ~r/^ankusa_store_reopen_total\{instance="#{inst}",result="ok"\} 2$/m
    assert scrape =~ ~r/^ankusa_store_wal_replay_seconds_count\{instance="#{inst}"\} 2$/m
    assert gauge(scrape, "ankusa_store_unavailable_seconds", inst) == 0.0
  end

  # The scrape once `pattern` shows in it (a write failure's reopen runs in the
  # store process, after the report returns), or the last one after 2 s.
  defp scrape_until(inst, pattern, tries \\ 100) do
    scrape = Ankusa.Metrics.scrape(inst)

    if scrape =~ pattern or tries == 0 do
      scrape
    else
      Process.sleep(20)
      scrape_until(inst, pattern, tries - 1)
    end
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
      assert gauge(scrape, "ankusa_quarantine_pending", inst) == 0.0
      assert gauge(scrape, "ankusa_http_pools", inst) == 0.0
    end

    test "config errors are counted per sink, and carry no tenant or source label" do
      config = start_instance()
      inst = config.instance

      for _ <- 1..2 do
        :telemetry.execute([:ankusa, :dispatch, :config_error], %{}, %{
          instance: inst,
          sink: Ankusa.Sink.SQS,
          source_id: "acme.billing",
          tenant_id: "acme"
        })
      end

      scrape = Ankusa.Metrics.scrape(inst)

      assert scrape =~
               ~r/^ankusa_dispatch_config_errors_total\{[^}]*sink="Ankusa.Sink.SQS"[^}]*\} 2(\.0)?$/m

      refute scrape =~ ~r/ankusa_dispatch_config_errors_total\{[^}]*(tenant_id|source_id)/
    end

    test "the dedupe and audit-retention series follow their events" do
      config = start_instance()
      inst = config.instance

      :telemetry.execute([:ankusa, :audit, :retention], %{blocked_by_cursor: 7}, %{instance: inst})

      :telemetry.execute([:ankusa, :dedupe, :pruned], %{count: 5}, %{instance: inst})
      :ok = Ankusa.Metrics.Gauges.measure(inst)
      scrape = Ankusa.Metrics.scrape(inst)

      assert gauge(scrape, "ankusa_audit_retention_blocked_by_cursor", inst) == 7.0
      assert gauge(scrape, "ankusa_dedupe_keys", inst) == 0.0
      assert scrape =~ ~r/^ankusa_dedupe_pruned_total\{instance="#{inst}"\} 5(\.0)?$/m
    end

    test "the dlq, audit-size and pruned series follow the counters and events" do
      config = start_instance()
      inst = config.instance

      :telemetry.execute([:ankusa, :dlq, :evicted], %{count: 4, bytes: 40}, %{
        instance: inst,
        tenant_id: "acme",
        reason: "max_rows",
        ids: ["a"],
        seqs: [1]
      })

      :telemetry.execute([:ankusa, :quarantine, :pruned], %{count: 2, bytes: 9}, %{instance: inst})

      :telemetry.execute([:ankusa, :archive, :pruned], %{segments: 3, objects: 0, legacy: 1}, %{
        instance: inst
      })

      :ok = Ankusa.Metrics.Gauges.measure(inst)
      scrape = Ankusa.Metrics.scrape(inst)

      assert gauge(scrape, "ankusa_dlq_rows", inst) == 0.0
      assert gauge(scrape, "ankusa_dlq_bytes", inst) == 0.0
      assert gauge(scrape, "ankusa_dlq_full", inst) == 0.0
      assert gauge(scrape, "ankusa_audit_rows", inst) != nil
      assert gauge(scrape, "ankusa_audit_outbox_backlog", inst) == 0.0
      assert scrape =~ ~r/^ankusa_dlq_evicted_total\{.*reason="max_rows".*\} 4(\.0)?$/m
      assert scrape =~ ~r/^ankusa_quarantine_pruned_total\{instance="#{inst}"\} 2(\.0)?$/m
      assert scrape =~ ~r/^ankusa_archive_catalogue_pruned_total\{instance="#{inst}"\} 3(\.0)?$/m
    end

    test "the claim lease gauge follows the counter, the sweep series its event" do
      config = start_instance()
      inst = config.instance

      :telemetry.execute(
        [:ankusa, :claim_check, :sweep],
        %{
          deleted: 1,
          scanned: 3,
          packs: 4,
          kept: 2,
          leases_pruned: 6,
          backlog: 9,
          partitions_left: 2,
          tenants_left: 4,
          duration: 1
        },
        %{instance: inst, deleted: true}
      )

      :ok = Ankusa.Metrics.Gauges.measure(inst)
      scrape = Ankusa.Metrics.scrape(inst)

      assert gauge(scrape, "ankusa_claim_leases", inst) == 0.0
      assert gauge(scrape, "ankusa_claim_check_sweep_backlog", inst) == 9.0
      assert gauge(scrape, "ankusa_claim_check_sweep_partitions_left", inst) == 2.0
      assert gauge(scrape, "ankusa_claim_check_sweep_tenants_left", inst) == 4.0
      assert scrape =~ ~r/^ankusa_claim_check_pruned_total\{instance="#{inst}"\} 4(\.0)?$/m
      assert scrape =~ ~r/^ankusa_claim_check_leases_pruned_total\{instance="#{inst}"\} 6(\.0)?$/m
    end

    test "gateway metrics: requests by status alone, a duration histogram, sweeps by whether they deleted" do
      config = start_instance()
      inst = config.instance

      for {method, status} <- [{"GET", 200}, {"HEAD", 200}, {"GET", 502}] do
        :telemetry.execute(
          [:ankusa, :claim_check, :gateway],
          %{duration: 2_000_000},
          %{instance: inst, method: method, status: status}
        )
      end

      for deleted <- [true, false, false] do
        :telemetry.execute(
          [:ankusa, :claim_check, :sweep],
          %{
            deleted: 0,
            scanned: 0,
            packs: 0,
            kept: 0,
            leases_pruned: 0,
            backlog: 0,
            partitions_left: 0,
            tenants_left: 0,
            duration: 1
          },
          %{instance: inst, deleted: deleted}
        )
      end

      scrape = Ankusa.Metrics.scrape(inst)

      assert scrape =~
               ~r/^ankusa_claim_gateway_requests_total\{[^}]*status="200"[^}]*\} 2(\.0)?$/m

      assert scrape =~
               ~r/^ankusa_claim_gateway_requests_total\{[^}]*status="502"[^}]*\} 1(\.0)?$/m

      # Status is the only label a caller's request can shape; no method, no tenant, no claim.
      for line <- series(scrape, "ankusa_claim_gateway_requests_total") do
        refute line =~ ~r/method=|tenant|claim=/
      end

      assert scrape =~
               ~r/^ankusa_claim_gateway_duration_seconds_count\{instance="#{inst}"\} 3(\.0)?$/m

      assert scrape =~ "ankusa_claim_gateway_duration_seconds_bucket{"
      assert scrape =~ ~r/^ankusa_claim_sweeps_total\{[^}]*deleted="true"[^}]*\} 1(\.0)?$/m
      assert scrape =~ ~r/^ankusa_claim_sweeps_total\{[^}]*deleted="false"[^}]*\} 2(\.0)?$/m
    end

    test "archive metrics: failures count, and the oldest unarchived age is a gauge" do
      config = start_instance()
      inst = config.instance

      :telemetry.execute([:ankusa, :archive, :failure], %{}, %{instance: inst})
      :telemetry.execute([:ankusa, :archive, :failure], %{}, %{instance: inst})

      :telemetry.execute(
        [:ankusa, :archive, :state],
        %{oldest_unarchived_age_ms: 90_000},
        %{instance: inst}
      )

      scrape = Ankusa.Metrics.scrape(inst)
      assert scrape =~ ~r/^ankusa_archive_failures_total\{instance="#{inst}"\} 2(\.0)?$/m
      assert gauge(scrape, "ankusa_archive_oldest_unarchived_age_seconds", inst) == 90.0

      :telemetry.execute(
        [:ankusa, :archive, :state],
        %{oldest_unarchived_age_ms: 0},
        %{instance: inst}
      )

      assert gauge(
               Ankusa.Metrics.scrape(inst),
               "ankusa_archive_oldest_unarchived_age_seconds",
               inst
             ) ==
               0.0
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

    # The families `Ankusa.Store` is asked to fold (`fold/6`) and to seek in
    # (`first/4`) while `Gauges.measure/1` runs. The trace must have seen reads,
    # so a broken trace cannot pass.
    defp gauge_scans(inst) do
      {_result, calls} = store_calls(fn -> Ankusa.Metrics.Gauges.measure(inst) end)
      assert calls != []
      %{folds: for({:fold, f} <- calls, do: f), seeks: for({:first, f} <- calls, do: f)}
    end

    defp tenant_gauge(scrape, tenant) do
      case Regex.run(~r/^ankusa_tenant_pending\{[^}]*tenant="#{tenant}"[^}]*\} (\S+)$/m, scrape) do
        [_, value] -> value |> Float.parse() |> elem(0)
        nil -> nil
      end
    end

    test "queue gauges read counters" do
      config = start_instance()
      inst = config.instance

      for i <- 1..3 do
        assert {:ok, _} = Ankusa.Edge.Ingest.ingest(inst, request("demo", ~s({"id":"c#{i}"})))
      end

      scans = gauge_scans(inst)

      # Nothing folds a queue family: the gauges are counter reads and one seek
      # into the active index (plus the first due row it names).
      refute Enum.any?(
               scans.folds,
               &(&1 in [:due, :inflight, :dead, :dead_by_tenant, :archive_pending])
             )

      assert Enum.count(scans.seeks, &(&1 == :due)) <= 1

      scrape = Ankusa.Metrics.scrape(inst)
      assert gauge(scrape, "ankusa_queue_pending", inst) == 3.0
      assert gauge(scrape, "ankusa_queue_scheduled", inst) == 0.0
      assert gauge(scrape, "ankusa_tenants", inst) == 1.0
    end

    test "oldest due age comes from the active index" do
      config = start_instance()
      inst = config.instance
      now = System.system_time(:millisecond)

      :ok = Ankusa.Store.write(inst, due_index_ops(inst, 700, 0, now - 7_000, 10), sync: true)

      scans = gauge_scans(inst)

      refute Enum.any?(
               scans.folds,
               &(&1 in [:due, :inflight, :dead, :dead_by_tenant, :archive_pending])
             )

      assert :active in scans.seeks or :active in scans.folds

      scrape = Ankusa.Metrics.scrape(inst)
      age = gauge(scrape, "ankusa_queue_oldest_due_age_seconds", inst)
      assert age >= 7.0 and age < 30.0
    end

    test "top tenants by pending" do
      config = start_instance()
      inst = config.instance

      for {tenant, n} <- [{"acme", 3}, {"globex", 1}, {"initech", 2}], i <- 1..n do
        req = request("demo", ~s({"id":"#{tenant}#{i}"})) |> Map.put(:tenant_id, tenant)
        assert {:ok, _} = Ankusa.Edge.Ingest.ingest(inst, req)
      end

      :ok = Ankusa.Metrics.Gauges.measure(inst)
      scrape = Ankusa.Metrics.scrape(inst)
      assert tenant_gauge(scrape, "acme") == 3.0
      assert tenant_gauge(scrape, "initech") == 2.0
      assert tenant_gauge(scrape, "globex") == 1.0

      # A tenant that leaves the list is reported once as 0, not left at its last value.
      assert {:ok, %{pending: 3}} = Ankusa.Queue.purge_tenant(inst, "acme", [:pending])
      :ok = Ankusa.Metrics.Gauges.measure(inst)
      scrape = Ankusa.Metrics.scrape(inst)
      assert tenant_gauge(scrape, "acme") == 0.0
      assert tenant_gauge(scrape, "initech") == 2.0
    end
  end

  describe "cardinality policy" do
    defp series(scrape, name) do
      scrape |> String.split("\n") |> Enum.filter(&String.starts_with?(&1, name <> "{"))
    end

    defp label(line, key) do
      case Regex.run(~r/[{,]#{key}="([^"]*)"/, line) do
        [_, value] -> value
        nil -> nil
      end
    end

    defp sample(line) do
      line |> String.split(" ") |> List.last() |> Float.parse() |> elem(0)
    end

    # Every series of a scrape, without its value and its instance label: what two
    # instances are compared on.
    defp series_set(scrape, instance) do
      scrape
      |> String.split("\n", trim: true)
      |> Enum.reject(&String.starts_with?(&1, "#"))
      |> Enum.map(fn line ->
        line
        |> String.replace(~s(instance="#{instance}"), "instance=\"-\"")
        |> String.replace(~r/ \S+$/, "")
      end)
      |> MapSet.new()
    end

    defp ingest_as(inst, tenant, n) do
      req = request("demo", ~s({"id":"#{tenant}-#{n}"})) |> Map.put(:tenant_id, tenant)
      assert {:ok, _env} = Ankusa.Edge.Ingest.ingest(inst, req)
    end

    @tag timeout: 180_000
    test "bounded series under 10k sources" do
      sources =
        for i <- 1..10_000, into: %{} do
          {"s#{i}",
           [
             verifier: {Ankusa.Verifier.None, []},
             sinks: [{Ankusa.Sink.Log, []}],
             tenant_id: "t#{i}"
           ]}
        end

      config = start_instance(source_store: {Static, sources: sources})
      inst = config.instance

      1..10_000
      |> Task.async_stream(
        fn i -> Ankusa.Edge.Ingest.ingest(inst, request("s#{i}", ~s({"id":"#{i}"}))) end,
        max_concurrency: 64,
        timeout: 120_000
      )
      |> Enum.each(fn {:ok, result} -> assert {:ok, _env} = result end)

      cap = config.metrics.max_series_per_metric
      assert cap == 5_000

      micros =
        for _ <- 1..3 do
          {us, scrape} = :timer.tc(fn -> Ankusa.Metrics.scrape(inst) end)
          {us, scrape}
        end

      {fastest, scrape} = Enum.min_by(micros, &elem(&1, 0))
      assert fastest < 100_000, "a scrape took #{fastest} µs"

      lines = series(scrape, "ankusa_ingest_requests_total")
      others = Enum.filter(lines, &(label(&1, "tenant_id") == "other"))

      assert length(lines) <= cap
      assert length(others) == 1
      # Folded, not dropped: every request is counted once.
      assert lines |> Enum.map(&sample/1) |> Enum.sum() == 10_000.0
      assert sample(hd(others)) == 10_000.0 - (length(lines) - 1)

      # By default no series carries a source, and the whole scrape stays small.
      refute scrape =~ "source_id="
      assert length(String.split(scrape, "\n")) <= 10_000
    end

    test "past the ceiling a source is folded into other, with detail: :source" do
      sources =
        for i <- 1..20, into: %{} do
          {"s#{i}", [verifier: {Ankusa.Verifier.None, []}, sinks: [{Ankusa.Sink.Log, []}]]}
        end

      config =
        start_instance(
          source_store: {Static, sources: sources},
          metrics: %{detail: :source, max_series_per_metric: 6}
        )

      for i <- 1..20 do
        assert {:ok, _env} =
                 Ankusa.Edge.Ingest.ingest(config.instance, request("s#{i}", ~s({"id":"#{i}"})))
      end

      lines = series(Ankusa.Metrics.scrape(config.instance), "ankusa_ingest_requests_total")
      named = Enum.reject(lines, &(label(&1, "source_id") == "other"))

      # Five admitted, the sixth slot is `other`'s.
      assert length(named) == 5
      assert length(lines) == 6
      assert Enum.all?(lines, &(label(&1, "tenant_id") in ["default", "other"]))
      assert lines |> Enum.map(&sample/1) |> Enum.sum() == 20.0

      other = Enum.find(lines, &(label(&1, "source_id") == "other"))
      assert label(other, "tenant_id") == "other"
      assert sample(other) == 15.0
    end

    test "detail: :source carries tenant and source, and its duration histogram too" do
      config = start_instance(metrics: %{detail: :source})
      inst = config.instance

      assert {:ok, _env} = Ankusa.Edge.Ingest.ingest(inst, request("demo", ~s({"id":"d1"})))
      scrape = Ankusa.Metrics.scrape(inst)

      assert scrape =~
               ~r/^ankusa_ingest_requests_total\{(?=[^}]*source_id="demo")(?=[^}]*tenant_id="default")[^}]*\} 1$/m

      assert scrape =~
               ~r/^ankusa_ingest_duration_seconds_bucket\{(?=[^}]*source_id="demo")(?=[^}]*tenant_id="default")/m
    end

    test "detail: :tenant leaves the duration histogram without an identity" do
      config = start_instance()
      inst = config.instance

      ingest_as(inst, "acme", 1)
      scrape = Ankusa.Metrics.scrape(inst)

      assert scrape =~ ~r/^ankusa_ingest_requests_total\{[^}]*tenant_id="acme"/m
      assert scrape =~ "ankusa_ingest_duration_seconds_bucket{"
      refute scrape =~ ~r/^ankusa_ingest_duration_seconds_bucket\{[^}]*tenant_id/m
    end

    test "detail: :none has no identity label and no per-tenant gauge, at any tenant count" do
      one = start_instance(metrics: %{detail: :none})
      many = start_instance(metrics: %{detail: :none})

      ingest_as(one.instance, "t0", 0)
      for i <- 1..40, do: ingest_as(many.instance, "t#{i}", i)

      for config <- [one, many], do: :ok = Ankusa.Metrics.Gauges.measure(config.instance)

      scrape_one = Ankusa.Metrics.scrape(one.instance)
      scrape_many = Ankusa.Metrics.scrape(many.instance)

      for scrape <- [scrape_one, scrape_many] do
        refute scrape =~ "tenant_id="
        refute scrape =~ "source_id="
        refute scrape =~ "ankusa_tenant_pending"
        refute scrape =~ "ankusa_dispatch_tenant_inflight"
        assert scrape =~ ~r/^ankusa_ingest_requests_total\{[^}]*outcome="committed"/m
      end

      # The series set is the same for one tenant and for forty.
      assert series_set(scrape_one, one.instance) == series_set(scrape_many, many.instance)

      assert gauge(scrape_many, "ankusa_tenants", many.instance) == 40.0
      assert gauge(scrape_one, "ankusa_tenants", one.instance) == 1.0
    end

    test "the default scrape names tenants, never sources, and keeps the series families" do
      config = start_instance(rate_limits: %{tenants: %{"acme" => %{rate: 0.001, burst: 1}}})
      inst = config.instance

      ingest_as(inst, "acme", 1)
      req = Map.put(request("demo", "x"), :tenant_id, "acme")
      assert {:error, {:rate_limited, _}} = Ankusa.Edge.Ingest.ingest(inst, req)
      assert {:quarantined, _} = Ankusa.Edge.Ingest.ingest(inst, request("strict", "{}"))
      :ok = Ankusa.Metrics.Gauges.measure(inst)
      scrape = Ankusa.Metrics.scrape(inst)

      refute scrape =~ "source_id="

      families =
        scrape
        |> String.split("\n", trim: true)
        |> Enum.reject(&String.starts_with?(&1, "#"))
        |> Enum.map(&(&1 |> String.split("{") |> hd()))
        |> MapSet.new()

      for family <- ~w(ankusa_ingest_requests_total ankusa_ingest_duration_seconds_bucket
                       ankusa_ingest_duration_seconds_count ankusa_ingest_duration_seconds_sum
                       ankusa_verify_failures_total ankusa_rate_limit_rejected_total
                       ankusa_wal_commit_duration_seconds_bucket ankusa_tenant_pending
                       ankusa_queue_pending ankusa_tenants) do
        assert family in families, "#{family} missing from the scrape"
      end

      # Every identity-carrying series names the tenant.
      for name <-
            ~w(ankusa_ingest_requests_total ankusa_verify_failures_total ankusa_rate_limit_rejected_total) do
        assert [_ | _] = lines = series(scrape, name)
        assert Enum.all?(lines, &(label(&1, "tenant_id") != nil)), name
      end
    end

    test "a gauge past the ceiling drops the tenant instead of writing to other" do
      config = start_instance(metrics: %{max_series_per_metric: 2})
      inst = config.instance

      for {tenant, n} <- [{"acme", 3}, {"globex", 1}, {"initech", 2}], i <- 1..n do
        ingest_as(inst, tenant, i)
      end

      :ok = Ankusa.Metrics.Gauges.measure(inst)
      lines = series(Ankusa.Metrics.scrape(inst), "ankusa_tenant_pending")

      # One slot for the gauge, none for an "other" that would hold a stray value.
      assert [line] = lines
      refute label(line, "tenant") == "other"
    end

    test "the quarantine and dispatch counters carry the tenant" do
      sources = %{
        "strict" => [
          verifier: {Ankusa.Verifier.Hmac, scheme: :stripe, secret: "whsec_x"},
          on_verify_failure: :quarantine
        ],
        "q" => [verifier: {Ankusa.Verifier.None, []}, sinks: [{OkSink, []}]]
      }

      config =
        start_instance(
          roles: [:edge, :dispatch],
          source_store: {Static, sources: sources},
          quarantine: %{rate: 0.001, burst: 1}
        )

      inst = config.instance
      forged = Map.put(request("strict", "{}"), :tenant_id, "acme")

      assert {:quarantined, _} = Ankusa.Edge.Ingest.ingest(inst, forged)
      assert {:error, {:quarantine_rate_limited, _}} = Ankusa.Edge.Ingest.ingest(inst, forged)

      req = Map.put(request("q", ~s({"id":"1"})), :tenant_id, "globex")
      assert {:ok, _env} = Ankusa.Edge.Ingest.ingest(inst, req)
      assert {:ok, _} = Ankusa.Dispatch.Pipeline.tick(inst)

      scrape = Ankusa.Metrics.scrape(inst)

      assert scrape =~ ~r/^ankusa_quarantine_rate_limited_total\{[^}]*tenant_id="acme"[^}]*\} 1$/m

      assert scrape =~
               ~r/^ankusa_dispatch_deliveries_total\{[^}]*result="ok"[^}]*tenant_id="globex"[^}]*\} 1$/m

      refute scrape =~ "source_id="
    end

    test "admission never exceeds its ceiling and leaks no slot under concurrent first sightings" do
      inst = :"cardinality_#{System.unique_integer([:positive])}"
      start_supervised!({Ankusa.Metrics.Cardinality, instance: inst})
      table = Ankusa.Metrics.Cardinality.table(inst)

      outcomes =
        for key <- 1..500, _worker <- 1..8 do
          Task.async(fn ->
            Ankusa.Metrics.Cardinality.admit(table, [:m], [:tenant_id], 50, %{
              tenant_id: Integer.to_string(key)
            })
          end)
        end
        |> Task.await_many(30_000)

      admitted = for {{[:m], %{tenant_id: _}}} <- :ets.tab2list(table), do: 1

      assert length(outcomes) == 4_000
      assert Enum.all?(outcomes, &(&1 in [:admitted, :overflow]))
      # 50 slots, one of them `other`'s: never more than 49 label sets, and the
      # counter holds exactly the sets that were inserted (a slot reserved by a
      # process that lost the race for its set was given back).
      assert length(admitted) in 1..49
      assert :ets.lookup(table, {:count, [:m]}) == [{{:count, [:m]}, length(admitted)}]
    end
  end
end
