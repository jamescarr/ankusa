defmodule Ankusa.EventTypeTest do
  use ExUnit.Case, async: false

  alias Ankusa.{Envelope, EventType, Routing, Source}
  alias Ankusa.Sink.Log

  defp env(headers \\ [], body \\ "{}") do
    %Envelope{
      id: Ankusa.UUIDv7.generate(),
      source_id: "s",
      tenant_id: "acme",
      received_at: 1,
      method: "POST",
      path: "/",
      headers: headers,
      body: body,
      size: byte_size(body)
    }
  end

  defp json(body), do: {:ok, JSON.decode!(body)}

  describe "event_type: the spec" do
    test "event_type: no spec, a preset atom, a preset map, a header and a json path" do
      assert EventType.new!(nil) == nil
      assert %EventType{from: {:header, "x-github-event"}} = EventType.new!(:github)
      assert %EventType{from: {:header, "x-shopify-topic"}} = EventType.new!(:shopify)
      assert %EventType{from: {:json, ["type"]}} = EventType.new!(%{preset: :stripe})
      assert %EventType{from: {:json, ["type"]}} = EventType.new!(:standard_webhooks)
      assert %EventType{from: {:json, ["type"]}} = EventType.new!(:svix)
      assert %EventType{from: {:json, ["event", "type"]}} = EventType.new!(:slack)

      assert %EventType{from: {:header, "x-custom-event"}} =
               EventType.new!(%{header: "X-Custom-Event"})

      assert %EventType{from: {:json, ["data", "kind"]}} = EventType.new!(%{json: "data.kind"})
      assert %EventType{from: {:header, "x-e"}} = EventType.new!(header: "X-E")

      assert Map.keys(EventType.presets()) |> Enum.sort() ==
               [:github, :shopify, :slack, :standard_webhooks, :stripe, :svix]
    end

    test "event_type: every rejected form raises, naming the source" do
      for {term, fragment} <- [
            {%{}, "set exactly one of preset, header, json (got none)"},
            {%{header: "x", json: "a"}, "set exactly one of preset, header, json"},
            {%{preset: :github, header: "x"}, "set exactly one of preset, header, json"},
            {:nope, "unknown preset :nope"},
            {%{header: ""}, "header must be a non-empty string"},
            {%{header: String.duplicate("h", 129)}, "at most 128 bytes"},
            {%{header: 1}, "header must be a non-empty string"},
            {%{json: "a..b"}, "json path must be 1 to 16 non-empty"},
            {%{json: ""}, "json path must be 1 to 16 non-empty"},
            {%{json: Enum.join(1..17, ".")}, "json path must be 1 to 16 non-empty"},
            {%{json: ["a"]}, "json must be a dot path string"},
            {"github", "expected a preset atom or a map"},
            {[1, 2], "expected a preset atom or a map"}
          ] do
        error = assert_raise ArgumentError, fn -> EventType.new!(term, "hooks") end
        assert error.message =~ ~s(source "hooks": event_type), inspect(term)
        assert error.message =~ fragment, inspect(term)
      end

      # 16 segments and 128 bytes are the ceilings, and they are allowed.
      assert %EventType{} = EventType.new!(%{json: Enum.join(1..16, ".")})
      assert %EventType{} = EventType.new!(%{header: String.duplicate("h", 128)})
    end

    test "event_type: Source.new/2 builds it and raises with the source id" do
      source = Source.new("billing", event_type: %{preset: :github})
      assert source.event_type.from == {:header, "x-github-event"}
      assert Source.new("billing", sinks: [{Log, []}]).event_type == nil

      error =
        assert_raise ArgumentError, fn -> Source.new("billing", event_type: %{json: "a..b"}) end

      assert error.message =~ ~s(source "billing": event_type)
    end
  end

  describe "event_type: extract" do
    test "event_type: a header, read case-insensitively; empty is no event type" do
      spec = EventType.new!(:github)
      assert EventType.extract(spec, env([{"X-GitHub-Event", "push"}]), :skip) == "push"
      assert EventType.extract(spec, env([{"x-github-event", "push"}]), :skip) == "push"
      assert EventType.extract(spec, env([{"x-github-event", ""}]), :skip) == nil
      assert EventType.extract(spec, env([]), :skip) == nil
      assert EventType.extract(nil, env([{"x-github-event", "push"}]), :skip) == nil
    end

    test "event_type: a json path yields a non-empty string and nothing else" do
      spec = EventType.new!(%{json: "data.kind"})

      assert EventType.extract(spec, env(), json(~s({"data":{"kind":"invoice.paid"}}))) ==
               "invoice.paid"

      for body <- [
            ~s({"data":{"kind":7}}),
            ~s({"data":{"kind":""}}),
            ~s({"data":{"kind":{"a":1}}}),
            ~s({"data":{"kind":["a"]}}),
            ~s({"data":{"kind":null}}),
            ~s({"data":{}}),
            ~s({"data":"x"}),
            ~s(["data"]),
            ~s("data")
          ] do
        assert EventType.extract(spec, env(), json(body)) == nil, body
      end
    end

    test "event_type: a body that did not decode, or was not decoded, is no event type" do
      spec = EventType.new!(:stripe)
      assert EventType.extract(spec, env(), {:error, :bad}) == nil
      assert EventType.extract(spec, env(), :skip) == nil
    end

    test "event_type: a value over 128 bytes is no event type, 128 is one" do
      spec = EventType.new!(:github)
      exactly = String.duplicate("e", 128)
      assert EventType.extract(spec, env([{"x-github-event", exactly}]), :skip) == exactly

      assert EventType.extract(spec, env([{"x-github-event", exactly <> "e"}]), :skip) == nil

      jspec = EventType.new!(:stripe)
      assert EventType.extract(jspec, env(), json(~s({"type":"#{exactly}"}))) == exactly
      assert EventType.extract(jspec, env(), json(~s({"type":"#{exactly}e"}))) == nil
    end

    test "event_type: needs_json? only for a json source" do
      refute EventType.needs_json?(nil)
      refute EventType.needs_json?(EventType.new!(:github))
      assert EventType.needs_json?(EventType.new!(:stripe))
    end
  end

  describe "event_type: Routing.resolve/4" do
    setup do
      handler = "event-type-#{System.unique_integer([:positive])}"
      test = self()

      :telemetry.attach(
        handler,
        [:ankusa, :ingest, :event_type_missing],
        fn _event, _measurements, metadata, _config -> send(test, {:missing, metadata}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)
    end

    test "event_type: resolves the dedupe key, the event type and the lanes together" do
      source =
        Source.new("s",
          sinks: [{Log, ordered: true, ordering_key: {:json, "customer"}}],
          dedupe: %{json: "id", ttl_ms: 1_000},
          event_type: %{json: "type"}
        )

      body = ~s({"id":"evt_1","type":"invoice.paid","customer":"cus_9"})
      r = Routing.resolve(:none, source, env([], body), dedupe: true)

      assert %Routing{dedupe_key: "evt_1", event_type: "invoice.paid", selected: :all} = r
      assert [{_lane, "cus_9"}] = r.lanes
      refute_receive {:missing, _}
    end

    test "event_type: dedupe: false extracts no provider event key but still the event type" do
      source =
        Source.new("s",
          dedupe: %{header: "x-id", ttl_ms: 1_000},
          event_type: %{header: "x-event"}
        )

      hook = env([{"x-id", "abc"}, {"x-event", "push"}])

      assert %Routing{dedupe_key: "abc", event_type: "push"} =
               Routing.resolve(:none, source, hook, dedupe: true)

      assert %Routing{dedupe_key: nil, event_type: "push", lanes: nil} =
               Routing.resolve(:none, source, hook, dedupe: false)
    end

    test "event_type: a source with no event_type resolves none and counts nothing" do
      source = Source.new("s", dedupe: %{header: "x-id", ttl_ms: 1_000})
      r = Routing.resolve(:none, source, env([{"x-id", "abc"}]), dedupe: true)
      assert %Routing{dedupe_key: "abc", event_type: nil} = r
      refute_receive {:missing, _}
    end

    test "event_type: a hook with none is counted, with the source and tenant" do
      source = Source.new("s", event_type: :github)

      assert %Routing{event_type: nil} = Routing.resolve(:inst, source, env(), dedupe: true)
      assert_receive {:missing, %{instance: :inst, source_id: "s", tenant_id: "acme"}}

      assert %Routing{event_type: "push"} =
               Routing.resolve(:inst, source, env([{"x-github-event", "push"}]), dedupe: true)

      refute_receive {:missing, _}
    end

    test "event_type: a json event type is counted missing for a body past the cap or not JSON" do
      source = Source.new("s", event_type: :stripe)

      assert %Routing{event_type: nil} =
               Routing.resolve(:inst, source, env([], "nope"), dedupe: true)

      assert_receive {:missing, %{source_id: "s"}}

      pad = String.duplicate("x", 1_048_576)
      big = ~s({"type":"a","pad":"#{pad}"})

      assert %Routing{event_type: nil} =
               Routing.resolve(:inst, source, env([], big), dedupe: true)

      assert_receive {:missing, %{source_id: "s"}}
    end

    test "event_type: decode/3 is skipped unless a setting reads the body, and past the cap" do
      env = env([], ~s({"type":"a"}))
      assert Routing.decode(Source.new("s", []), env, 1_000) == :skip
      assert Routing.decode(Source.new("s", event_type: :github), env, 1_000) == :skip

      assert {:ok, %{"type" => "a"}} =
               Routing.decode(Source.new("s", event_type: :stripe), env, 1_000)

      assert Routing.decode(Source.new("s", event_type: :stripe), env, 5) == :skip
    end
  end

  describe "event_type: the stored envelope" do
    test "event_type: round-trips, and a binary written before it decodes with nil" do
      hook = %{env([{"x-github-event", "push"}]) | event_type: "push"}

      assert {:ok, %Envelope{event_type: "push"}} =
               hook |> Envelope.to_binary() |> Envelope.from_binary()

      assert {:ok, %Envelope{event_type: "push"}} =
               hook |> Envelope.to_binary(version: 1) |> Envelope.from_binary()

      # What a node that predates the field wrote: the map without the key.
      old = hook |> Map.from_struct() |> Map.delete(:event_type) |> :erlang.term_to_binary()
      assert {:ok, %Envelope{event_type: nil, id: id}} = Envelope.from_binary(old)
      assert id == hook.id
    end
  end
end
