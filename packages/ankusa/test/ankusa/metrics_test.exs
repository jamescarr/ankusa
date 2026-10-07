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
end
