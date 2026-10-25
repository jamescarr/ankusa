defmodule Ankusa.TracingTest do
  @moduledoc """
  OPS-1: a hook is one trace from the provider's request to the consumer.

  Without an OpenTelemetry SDK (the default for core) the request's
  `traceparent` / `tracestate` ride the hook verbatim and no span exists. With
  one (started here with the in-memory `:otel_exporter_pid`, the exporter that
  sends every finished span to the test process) the edge, the store commit, and
  each dispatch attempt are spans of the provider's trace.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  require Record

  alias Ankusa.Dispatch.Pipeline
  alias Ankusa.Envelope
  alias Ankusa.Replay
  alias Ankusa.Sink.Message
  alias Ankusa.SourceStore
  alias Ankusa.Trace

  Record.defrecordp(
    :span,
    Record.extract(:span, from_lib: "opentelemetry/include/otel_span.hrl")
  )

  Record.defrecordp(
    :link,
    Record.extract(:link, from_lib: "opentelemetry/include/otel_span.hrl")
  )

  Record.defrecordp(
    :status,
    Record.extract(:status, from_lib: "opentelemetry_api/include/opentelemetry.hrl")
  )

  @moduletag capture_log: true

  @trace_id "4bf92f3577b34da6a3ce929d0e0e4736"
  @provider_span "00f067aa0ba902b7"
  @traceparent "00-#{@trace_id}-#{@provider_span}-01"
  @tracestate "vendor=value,other=thing"

  # Reports what a queue-style sink would put on the wire for the hook it got.
  defmodule CaptureSink do
    @moduledoc false
    @behaviour Ankusa.Sink

    @impl true
    def inline_max_bytes(opts), do: Keyword.get(opts, :inline_max_bytes, 65_536)

    @impl true
    def deliver(env, ctx, opts) do
      {:ok, json} = Message.encode(env, ctx, inline_max_bytes(opts))

      send(Keyword.fetch!(opts, :pid), {
        :delivered,
        env,
        JSON.decode!(json),
        Message.headers(env, ctx),
        Message.attributes(env, ctx)
      })

      if Keyword.get(opts, :outcome, :ok) == :ok, do: :ok, else: {:error, {:transient, :down}}
    end
  end

  defmodule LifecycleSink do
    @moduledoc false
    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, _ctx, opts) do
      send(Keyword.fetch!(opts, :pid), {:lifecycle, env, JSON.decode!(env.body)})
      :ok
    end
  end

  @doc false
  def decoder(_source_id, _spec), do: [sinks: [{Ankusa.Sink.Log, []}]]

  # ── harness ────────────────────────────────────────────────────────────────

  defp start_instance(sink_opts \\ [], overrides \\ []) do
    sources = %{"demo" => %{sinks: [{CaptureSink, [pid: self()] ++ sink_opts}]}}

    config =
      test_config(
        Keyword.merge(
          [
            roles: [:edge, :dispatch],
            source_store: {Ankusa.SourceStore.Static, sources: sources},
            dispatch: %{
              retry: {Ankusa.RetryPolicy.Exponential, base_ms: 0, max_attempts: 2, jitter: false}
            }
          ],
          overrides
        )
      )

    start_supervised!({Ankusa.Instance, config})
    config
  end

  defp post(config, headers, body \\ ~s({"id":"evt_1"})) do
    conn = route_through_edge(config, request("demo", body, headers))
    assert conn.status == 201
    JSON.decode!(conn.resp_body)["id"]
  end

  defp deliver_queued(config) do
    assert {:ok, _} = Pipeline.tick(config.instance)
    assert_receive {:delivered, env, message, headers, attributes}, 5_000
    {env, message, headers, attributes}
  end

  defp trace_headers, do: [{"traceparent", @traceparent}, {"tracestate", @tracestate}]

  # ── the SDK, for the tests that need spans ─────────────────────────────────

  defp start_sdk(extra_env \\ []) do
    # A simple processor exports each span as it ends; `:otel_exporter_pid` sends
    # it to this process as `{:span, record}`.
    env = [
      processors: [{:otel_simple_processor, %{}}],
      traces_exporter: {:otel_exporter_pid, self()}
    ]

    for {key, value} <- env ++ extra_env, do: Application.put_env(:opentelemetry, key, value)

    {:ok, _} = Application.ensure_all_started(:opentelemetry)
    on_exit(&stop_sdk/0)
  end

  # The SDK parks its tracers in `:persistent_term`; left behind they point at a
  # dead provider and every later test would try to record into it.
  defp stop_sdk do
    Application.stop(:opentelemetry)

    for {key, _value} <- :persistent_term.get(),
        match?({:opentelemetry, _, :tracer, _}, key) or
          key == {:opentelemetry, :otel_module_to_application_key},
        do: :persistent_term.erase(key)

    for key <- [:processors, :traces_exporter, :sampler],
        do: Application.delete_env(:opentelemetry, key)
  end

  # Finished spans by name, until every name in `names` has been seen.
  defp spans(names, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    collect(names, %{}, deadline)
  end

  defp collect(names, seen, deadline) do
    if Enum.all?(names, &Map.has_key?(seen, &1)) do
      seen
    else
      wait = max(deadline - System.monotonic_time(:millisecond), 0)

      receive do
        {:span, s} ->
          collect(names, Map.update(seen, span(s, :name), [s], &(&1 ++ [s])), deadline)
      after
        wait ->
          flunk(
            "spans #{inspect(names -- Map.keys(seen))} never finished; saw #{inspect(Map.keys(seen))}"
          )
      end
    end
  end

  defp one(spans, name), do: spans |> Map.fetch!(name) |> hd()

  defp hex(int, width),
    do: int |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(width, "0")

  defp attributes(s), do: s |> span(:attributes) |> :otel_attributes.map()

  defp links(s), do: s |> span(:links) |> :otel_links.list()

  # ── without an SDK ─────────────────────────────────────────────────────────

  describe "no SDK" do
    test "a traceparent sent to the edge reaches the consumer unchanged" do
      refute Trace.active?()
      config = start_instance()

      post(config, trace_headers())
      {env, message, headers, attributes} = deliver_queued(config)

      assert env.trace == %{traceparent: @traceparent, tracestate: @tracestate}

      # the queue message: the fields, and the broker-native pairs
      assert message["traceparent"] == @traceparent
      assert message["tracestate"] == @tracestate
      assert {"traceparent", @traceparent} in headers
      assert {"tracestate", @tracestate} in headers
      assert {"traceparent", @traceparent} in attributes
      assert {"tracestate", @tracestate} in attributes
    end

    test "a hook with no traceparent carries none" do
      config = start_instance()

      post(config, [{"x-other", "1"}])
      {env, message, headers, attributes} = deliver_queued(config)

      assert env.trace == nil
      assert Map.fetch!(message, "traceparent") == nil
      assert Map.fetch!(message, "tracestate") == nil
      refute List.keymember?(headers, "traceparent", 0)
      refute List.keymember?(attributes, "traceparent", 0)
    end

    test "a traceparent over the direct publish path (wal: none) reaches the sink too" do
      config = start_instance([], wal: :none, roles: [:edge])

      post(config, trace_headers())

      assert_receive {:delivered, env, message, _headers, _attributes}, 2_000
      assert env.trace.traceparent == @traceparent
      assert message["traceparent"] == @traceparent
    end

    test "a malformed, repeated or zeroed traceparent is no trace" do
      assert Trace.from_headers([{"traceparent", @traceparent}]) ==
               %{traceparent: @traceparent, tracestate: nil}

      for bad <- [
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
            "garbage",
            ""
          ] do
        assert Trace.from_headers([{"traceparent", bad}]) == nil, "accepted #{inspect(bad)}"
      end

      assert Trace.from_headers([{"traceparent", @traceparent}, {"traceparent", @traceparent}]) ==
               nil
    end

    test "names match without case, a future version is cut to v00, flags keep the sampled bit" do
      future = "01-#{@trace_id}-#{@provider_span}-03-and-more"

      assert Trace.from_headers([
               {"TraceParent", future},
               {"TraceState", "a=1"},
               {"tracestate", "b=2"}
             ]) ==
               %{traceparent: "00-#{@trace_id}-#{@provider_span}-01", tracestate: "a=1,b=2"}

      assert Trace.from_headers([{"traceparent", "00-#{@trace_id}-#{@provider_span}-00"}]).traceparent ==
               "00-#{@trace_id}-#{@provider_span}-00"
    end

    test "a tracestate over 512 bytes is dropped, the traceparent still travels" do
      big = "k=" <> String.duplicate("v", 600)

      assert Trace.from_headers([{"traceparent", @traceparent}, {"tracestate", big}]) ==
               %{traceparent: @traceparent, tracestate: nil}
    end

    test "Sink.Http sends the hook's context and replaces a forwarded provider header" do
      test = self()

      Req.Test.stub(__MODULE__, fn conn ->
        send(test, {:http_headers, conn.req_headers})
        Plug.Conn.send_resp(conn, 200, "ok")
      end)

      env = %Envelope{
        id: Ankusa.UUIDv7.generate(),
        source_id: "demo",
        tenant_id: "acme",
        received_at: 1,
        method: "POST",
        path: "/hooks",
        # the provider's own header is in the forwarded set, as `forward_headers: :default` leaves it
        headers: [{"traceparent", "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"}],
        content_type: "application/json",
        body: "{}",
        size: 2,
        trace: %{traceparent: @traceparent, tracestate: @tracestate}
      }

      opts = [
        url: "https://203.0.113.10/hooks",
        allow_unsigned: true,
        req_options: [plug: {Req.Test, __MODULE__}]
      ]

      assert :ok = Ankusa.Sink.Http.deliver(env, %{attempt: 1}, opts)
      assert_receive {:http_headers, sent}

      assert for({"traceparent", v} <- sent, do: v) == [@traceparent]
      assert for({"tracestate", v} <- sent, do: v) == [@tracestate]

      # with no trace the provider's header is forwarded as before
      assert :ok = Ankusa.Sink.Http.deliver(%{env | trace: nil}, %{attempt: 1}, opts)
      assert_receive {:http_headers, sent}
      assert for({"traceparent", v} <- sent, do: v) == [List.first(env.headers) |> elem(1)]
      assert for({"tracestate", v} <- sent, do: v) == []
    end
  end

  # ── with an SDK ────────────────────────────────────────────────────────────

  describe "with an SDK" do
    setup do
      start_sdk()
      :ok
    end

    test "one trace across stages" do
      assert Trace.active?()
      config = start_instance()

      post(config, trace_headers())
      {env, message, headers, _attributes} = deliver_queued(config)

      spans =
        spans(["ankusa.ingest", "ankusa.verify", "ankusa.commit", "ankusa.dispatch.attempt"])

      ingest = one(spans, "ankusa.ingest")
      verify = one(spans, "ankusa.verify")
      commit = one(spans, "ankusa.commit")
      attempt = one(spans, "ankusa.dispatch.attempt")

      trace_id = String.to_integer(@trace_id, 16)

      # every stage is the provider's trace
      for s <- [ingest, verify, commit, attempt], do: assert(span(s, :trace_id) == trace_id)

      # ingest is a child of the provider's span (a remote parent); verify and
      # commit are children of ingest
      assert hex(span(ingest, :parent_span_id), 16) == @provider_span
      assert span(ingest, :parent_span_is_remote) == true
      assert span(ingest, :kind) == :server
      assert span(verify, :parent_span_id) == span(ingest, :span_id)
      assert span(commit, :parent_span_id) == span(ingest, :span_id)

      # the attempt continues the stored context and links to the ingest span
      assert span(attempt, :parent_span_id) == span(ingest, :span_id)
      assert span(attempt, :kind) == :producer

      assert [linked_span] = links(attempt)
      assert link(linked_span, :trace_id) == trace_id
      linked = link(linked_span, :span_id)
      assert linked == span(ingest, :span_id)

      # the delivered context is the ingest span's: same trace, our span as parent,
      # the provider's tracestate carried through
      expected = "00-#{@trace_id}-#{hex(span(ingest, :span_id), 16)}-01"
      assert env.trace == %{traceparent: expected, tracestate: @tracestate}
      assert message["traceparent"] == expected
      assert message["tracestate"] == @tracestate
      assert {"traceparent", expected} in headers

      attrs = attributes(attempt)
      assert attrs["ankusa.sink"] == inspect(CaptureSink)
      assert attrs["ankusa.attempt"] == 1
      assert attrs["ankusa.hook.id"] == env.id
      assert attrs["ankusa.source_id"] == "demo"
      assert attributes(ingest)["ankusa.outcome"] == "committed"
      assert attributes(commit)["ankusa.wal"] == "disk"
    end

    test "a hook with no traceparent starts a trace of its own, and carries it" do
      config = start_instance()

      post(config, [])
      {env, message, _headers, _attributes} = deliver_queued(config)
      spans = spans(["ankusa.ingest", "ankusa.dispatch.attempt"])
      ingest = one(spans, "ankusa.ingest")

      assert span(ingest, :parent_span_id) == :undefined
      assert span(one(spans, "ankusa.dispatch.attempt"), :trace_id) == span(ingest, :trace_id)

      expected = "00-#{hex(span(ingest, :trace_id), 32)}-#{hex(span(ingest, :span_id), 16)}-01"
      assert env.trace.traceparent == expected
      assert message["traceparent"] == expected
    end

    test "a retried attempt is another span of the same trace, carrying the same context" do
      config = start_instance(outcome: :error)

      post(config, trace_headers())
      {first_env, _message, _headers, _attributes} = deliver_queued(config)
      assert_receive {:delivered, retry_env, _, _, _}, 5_000

      assert retry_env.trace == first_env.trace

      attempts = spans(["ankusa.dispatch.attempt"]) |> Map.fetch!("ankusa.dispatch.attempt")
      # the failed attempt is marked, with its class and never its reason
      assert Enum.any?(attempts, fn s ->
               status(span(s, :status), :code) == :error and
                 status(span(s, :status), :message) == "transient"
             end)

      assert Enum.all?(attempts, &(span(&1, :trace_id) == String.to_integer(@trace_id, 16)))
    end

    test "a body over the sink's threshold is packed in a claim_check span" do
      config = start_instance(inline_max_bytes: 10)

      post(config, trace_headers(), String.duplicate("x", 100))
      {_env, message, _headers, _attributes} = deliver_queued(config)
      assert is_binary(message["claim"])

      claim = spans(["ankusa.claim_check"]) |> one("ankusa.claim_check")
      attrs = attributes(claim)

      # dispatch packs a batch of hooks at once, so the pack is a span of its own
      assert attrs["ankusa.tenant_id"] == "default"
      assert attrs["ankusa.claims"] == 1
      assert attrs["ankusa.bytes"] == 100
    end

    test "on the direct publish path the claim check is inside the commit" do
      config = start_instance([inline_max_bytes: 10], wal: :none, roles: [:edge])

      post(config, trace_headers(), String.duplicate("x", 100))
      assert_receive {:delivered, _env, %{"claim" => claim}, _headers, _attributes}, 2_000
      assert is_binary(claim)

      spans = spans(["ankusa.claim_check", "ankusa.commit"])
      pack = one(spans, "ankusa.claim_check")

      assert span(pack, :parent_span_id) == span(one(spans, "ankusa.commit"), :span_id)
      assert span(pack, :trace_id) == String.to_integer(@trace_id, 16)
    end

    test "the direct publish path (wal: none) is traced too" do
      config = start_instance([], wal: :none, roles: [:edge])

      post(config, trace_headers())

      assert_receive {:delivered, env, message, _headers, _attributes}, 2_000
      spans = spans(["ankusa.ingest", "ankusa.commit", "ankusa.dispatch.attempt"])
      ingest = one(spans, "ankusa.ingest")

      assert span(one(spans, "ankusa.dispatch.attempt"), :trace_id) == span(ingest, :trace_id)
      assert attributes(one(spans, "ankusa.commit"))["ankusa.wal"] == "none"
      assert message["traceparent"] == env.trace.traceparent
      assert String.contains?(env.trace.traceparent, hex(span(ingest, :span_id), 16))
    end

    test "a request the sampler drops still carries a context, with the sampled flag off" do
      stop_sdk()
      start_sdk(sampler: :always_off)

      config = start_instance()
      post(config, [])
      {env, _message, _headers, _attributes} = deliver_queued(config)

      assert String.ends_with?(env.trace.traceparent, "-00")
      refute_receive {:span, _}, 200
    end

    test "a lifecycle event carries a traceparent, and a replay of it is a span" do
      config =
        test_config(
          source_store: {SourceStore.Persistent, decoder: &__MODULE__.decoder/2},
          lifecycle: %{sinks: [{LifecycleSink, pid: self()}]}
        )

      start_supervised!({Ankusa.Instance, config})

      spec = %{"sinks" => [%{"type" => "log"}]}
      {:ok, _} = SourceStore.put(config.instance, "acme", "billing", spec, :create)

      assert_receive {:lifecycle, env, event}, 5_000
      assert event["traceparent"] == env.trace.traceparent
      assert event["traceparent"] =~ ~r/^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/
      refute Map.has_key?(event, "tracestate")

      spans = spans(["ankusa.lifecycle.event", "ankusa.dispatch.attempt"])
      emit = one(spans, "ankusa.lifecycle.event")

      assert String.contains?(event["traceparent"], hex(span(emit, :span_id), 16))
      assert attributes(emit)["ankusa.event.type"] == "io.ankusa.source.created"
      assert span(one(spans, "ankusa.dispatch.attempt"), :trace_id) == span(emit, :trace_id)

      assert {:ok, :created, _job} = Replay.start(config.instance, kind: :lifecycle)
      assert_receive {:lifecycle, _env, %{"replayid" => _} = again}, 5_000
      assert again["id"] == event["id"]
      assert again["traceparent"] =~ ~r/^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/

      replay = spans(["ankusa.replay"]) |> one("ankusa.replay")
      assert attributes(replay)["ankusa.replay.kind"] == "lifecycle"
    end
  end
end
