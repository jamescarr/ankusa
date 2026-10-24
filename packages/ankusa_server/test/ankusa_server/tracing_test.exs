defmodule AnkusaServer.TracingTest do
  # Starts and stops the OpenTelemetry SDK, which is global to the VM.
  use ExUnit.Case, async: false

  alias AnkusaServer.Tracing

  @on %{enabled: true, otlp_endpoint: "http://127.0.0.1:1", sample_ratio: 0.5}

  test "tracing off starts nothing: core keeps the no-op API" do
    assert Tracing.start(%{@on | enabled: false}) == :ok

    refute Ankusa.Trace.active?()
    refute List.keymember?(Application.started_applications(), :opentelemetry, 0)
  end

  test "the SDK environment is a parent-based sampler over the ratio, an OTLP/HTTP exporter" do
    assert Tracing.sdk_env(@on)[:sampler] ==
             {:parent_based, %{root: {:trace_id_ratio_based, 0.5}}}

    assert Tracing.sdk_env(@on)[:traces_exporter] == {:opentelemetry_exporter, %{}}
    assert Tracing.sdk_env(@on)[:resource].service.name == "ankusa"

    assert Tracing.exporter_env(@on) ==
             [otlp_protocol: :http_protobuf, otlp_endpoint: "http://127.0.0.1:1"]
  end

  describe "tracing on" do
    setup do
      on_exit(fn ->
        Application.stop(:opentelemetry)
        Application.stop(:opentelemetry_exporter)

        # The SDK parks its tracers in `:persistent_term`; left behind they point
        # at a dead provider.
        for {key, _value} <- :persistent_term.get(),
            match?({:opentelemetry, _, :tracer, _}, key) or
              key == {:opentelemetry, :otel_module_to_application_key},
            do: :persistent_term.erase(key)

        for key <- [:sampler, :processors, :traces_exporter, :resource],
            do: Application.delete_env(:opentelemetry, key)

        for key <- [:otlp_protocol, :otlp_endpoint],
            do: Application.delete_env(:opentelemetry_exporter, key)
      end)
    end

    test "starts the SDK and the exporter, and core's spans are recorded" do
      assert Tracing.start(@on) == :ok

      assert Ankusa.Trace.active?()

      assert Application.fetch_env!(:opentelemetry_exporter, :otlp_endpoint) ==
               "http://127.0.0.1:1"

      assert Application.fetch_env!(:opentelemetry, :sampler) == Tracing.sdk_env(@on)[:sampler]

      # A span is real: it has valid ids, and a context to store on a hook.
      assert %{traceparent: "00-" <> _} =
               Ankusa.Trace.span("ankusa.test", [], fn -> Ankusa.Trace.current(nil) end)
    end
  end
end
