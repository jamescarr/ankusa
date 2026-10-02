defmodule Ankusa.LifecycleTest do
  @moduledoc """
  Lifecycle events: a source or route created, updated, or deleted becomes one
  CloudEvents 1.0 message on `config.lifecycle.sinks`, through the same WAL and
  dispatch a hook uses (or in the call itself under `wal: :none`).
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.{Config, Edge.Ingest, Routes, SourceStore, WAL}
  alias Ankusa.SourceStore.Persistent

  defmodule CaptureSink do
    @moduledoc "Reports every delivery as `{:lifecycle, ctx, decoded_body}`, then follows `:outcome`."

    @behaviour Ankusa.Sink

    @impl true
    def deliver(env, ctx, opts) do
      send(Keyword.fetch!(opts, :to), {:lifecycle, ctx, env, JSON.decode!(env.body)})
      if Keyword.get(opts, :outcome, :ok) == :ok, do: :ok, else: {:error, :nope}
    end
  end

  defmodule ClaimSink do
    @moduledoc "A queue-style sink: claims any body over 200 bytes and reports the claim it was handed."

    @behaviour Ankusa.Sink

    @impl true
    def inline_max_bytes(_opts), do: 200

    @impl true
    def deliver(env, ctx, opts) do
      send(Keyword.fetch!(opts, :to), {:claimed, env, ctx[:claim]})
      :ok
    end
  end

  @doc false
  def report_dropped(_event, _measurements, meta, pid), do: send(pid, {:dropped, meta})

  @spec_map %{
    "verify" => %{"type" => "hmac", "secret" => "s3cr3t", "signature_header" => "X-Sig"},
    "sinks" => [%{"type" => "log"}]
  }

  @route %{"id" => "stripe", "path" => "/webhooks/stripe", "methods" => ["POST"]}

  # What the server's YAML decoder is to a real store: enough to accept a spec.
  @doc false
  def decoder(_source_id, _spec), do: [sinks: [{Ankusa.Sink.Log, []}]]

  defp start_instance(overrides \\ []) do
    base = [
      roles: [:edge, :dispatch, :storage],
      dispatch: [poll_ms: 10],
      source_store: {Persistent, decoder: &__MODULE__.decoder/2},
      routes: [enabled: true, admin: [port: 0]],
      lifecycle: %{sinks: [{CaptureSink, to: self()}]}
    ]

    config = test_config(Keyword.merge(base, overrides))
    start_supervised!({Ankusa.Instance, config})
    config
  end

  describe "under the WAL, with dispatch running" do
    setup do
      %{config: start_instance()}
    end

    test "a source's life is three events carrying the redacted source", %{config: config} do
      instance = config.instance

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

  test "with no lifecycle sinks nothing is written to the WAL" do
    config = start_instance(lifecycle: %{sinks: []})

    before = WAL.stats(config.instance).records
    {:ok, _} = SourceStore.put(config.instance, "acme", "billing", @spec_map, :create)
    {:ok, _} = Routes.create(config.instance, @route)

    assert WAL.stats(config.instance).records == before
    refute_receive {:lifecycle, _, _, _}, 100
  end

  test "an event over a sink's inline threshold is claim-checked under a valid tenant" do
    config = start_instance(lifecycle: %{sinks: [{ClaimSink, to: self()}]})

    # A route's metadata makes its event larger than the 200-byte threshold.
    route = Map.put(@route, "metadata", %{"note" => String.duplicate("x", 500)})
    {:ok, _} = Routes.create(config.instance, route)

    assert_receive {:claimed, env, %{ref: ref, sha256: sha256}}, 2_000
    assert env.source_id == "ankusa:lifecycle"
    # Routes carry no tenant; a claim needs a real one (`[A-Za-z0-9_-]{1,64}`).
    assert env.tenant_id == "default"
    assert env.size > 200

    assert {:ok, body} = Ankusa.ClaimCheck.redeem(config.instance, ref, sha256)
    assert body == env.body
    assert %{"type" => "io.ankusa.route.created", "subject" => "stripe"} = JSON.decode!(body)
  end

  describe "under wal: :none" do
    test "the event is delivered before the change returns" do
      config = start_instance(roles: [:edge], wal: :none)

      {:ok, _} = SourceStore.put(config.instance, "acme", "billing", @spec_map, :create)

      # `assert_received`: delivery had already happened when `put/5` returned.
      assert_received {:lifecycle, _, _, %{"type" => "io.ankusa.source.created"}}

      {:ok, _} = Routes.create(config.instance, @route)
      assert_received {:lifecycle, _, _, %{"type" => "io.ankusa.route.created"}}
    end

    test "a sink that refuses loses the event, not the change, and says so" do
      config =
        start_instance(
          roles: [:edge],
          wal: :none,
          lifecycle: %{sinks: [{CaptureSink, to: self(), outcome: :error}]}
        )

      handler = "lifecycle-dropped-#{inspect(self())}"

      :telemetry.attach(
        handler,
        [:ankusa, :lifecycle, :dropped],
        &__MODULE__.report_dropped/4,
        self()
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      assert {:ok, %{source_id: "acme.billing"}} =
               SourceStore.put(config.instance, "acme", "billing", @spec_map, :create)

      assert {:ok, _} = SourceStore.get(config.instance, "acme", "billing")

      assert_receive {:dropped, %{instance: instance, type: "io.ankusa.source.created"}}
      assert instance == config.instance
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

    test "under wal: :none refuses sinks none of which is durable, and accepts a durable one" do
      log_only =
        Config.new(roles: [:edge], wal: :none, lifecycle: %{sinks: [{Ankusa.Sink.Log, []}]})

      assert_raise ArgumentError, ~r/none of its sinks is durable/, fn ->
        Ankusa.Lifecycle.validate_config!(log_only)
      end

      durable =
        Config.new(
          roles: [:edge],
          wal: :none,
          lifecycle: %{sinks: [{Ankusa.Sink.Log, []}, {CaptureSink, to: self()}]}
        )

      assert Ankusa.Lifecycle.validate_config!(durable) == :ok
    end

    test "lifecycle off is always valid" do
      assert Ankusa.Lifecycle.validate_config!(Config.new(roles: [:edge], wal: :none)) == :ok
    end
  end
end
