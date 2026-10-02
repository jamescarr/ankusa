defmodule Ankusa.LifecycleTest do
  @moduledoc """
  Lifecycle events: a source or route created, updated, or deleted becomes one
  CloudEvents 1.0 message on `config.lifecycle.sinks`, published from memory by
  `Ankusa.Lifecycle.Publisher` and never written to the store.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.{Config, Edge.Ingest, Envelope, Lifecycle.Publisher, Routes, SourceStore}
  alias Ankusa.SourceStore.Persistent

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

  defp envelope(id, tenant \\ "acme") do
    body = JSON.encode!(%{"id" => id})

    %Envelope{
      id: id,
      source_id: "ankusa:lifecycle",
      tenant_id: tenant,
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/_ankusa/lifecycle",
      headers: [],
      content_type: "application/cloudevents+json",
      body: body,
      size: byte_size(body)
    }
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
      assert deleted["data"] == %{"id" => "stripe"}
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
    config = start_instance(roles: [:edge], wal: :none)

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

    test "a full queue drops the event and counts it" do
      attach([[:ankusa, :lifecycle, :dropped], [:ankusa, :lifecycle, :delivered]])

      config = test_config(lifecycle: %{sinks: [{GateSink, to: self()}]})
      put_config(config)
      start_supervised!({Publisher, instance: config.instance, config: config, max_pending: 1})

      :ok =
        Publisher.publish(config.instance, envelope("one"), "io.ankusa.source.created", "acme.a")

      assert_receive {:gated, delivering, "one"}, 2_000

      # The first is still in flight, so the bound of 1 is reached.
      :ok =
        Publisher.publish(config.instance, envelope("two"), "io.ankusa.source.created", "acme.b")

      assert_receive {:telemetry, [:ankusa, :lifecycle, :dropped],
                      %{reason: :queue_full, type: "io.ankusa.source.created"}}

      send(delivering, :go)
      assert_receive {:telemetry, [:ankusa, :lifecycle, :delivered], %{sink: sink}}, 2_000
      assert sink == inspect(GateSink)
      refute_receive {:gated, _, "two"}, 100
    end

    test "with the publisher down the event is dropped and counted, never raised" do
      attach([[:ankusa, :lifecycle, :dropped]])

      config = test_config(lifecycle: %{sinks: [{CaptureSink, to: self()}]})
      put_config(config)

      assert :ok =
               Publisher.publish(
                 config.instance,
                 envelope("one"),
                 "io.ankusa.source.created",
                 "acme.a"
               )

      assert_receive {:telemetry, [:ankusa, :lifecycle, :dropped], %{reason: :not_running}}
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
