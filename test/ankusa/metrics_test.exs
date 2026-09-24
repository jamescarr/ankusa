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

  defp start_instance do
    config =
      test_config(
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
               verifier: {Ankusa.Verifier.Stripe, secret: "whsec_x"},
               on_verify_failure: :quarantine
             ]
           }}
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

  test "only failed verifications count as verify failures" do
    config = start_instance()

    assert {:quarantined, _reason} =
             Ankusa.Edge.Ingest.ingest(config.instance, request("strict", ~s({"id":"q1"})))

    assert {:ok, _env} =
             Ankusa.Edge.Ingest.ingest(config.instance, request("demo", ~s({"id":"evt_ok"})))

    scrape = Ankusa.Metrics.scrape(config.instance)

    assert scrape =~ "ankusa_verify_failures_total{"
    assert scrape =~ ~s(provider="Ankusa.Verifier.Stripe")
    # ...and the successful verification above left no series at all: only
    # `status: :failed` events are kept.
    refute scrape =~ ~s(provider="Ankusa.Verifier.None")
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
