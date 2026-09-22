defmodule Hook.DedupKeyTest do
  use ExUnit.Case, async: true

  alias Hook.Envelope

  defp env(headers, body) do
    %Envelope{
      id: "e1",
      source_id: "src",
      received_at: 0,
      method: "POST",
      path: "/hook",
      headers: headers,
      body: body
    }
  end

  describe "Hook.DedupKey.Rules" do
    test "extracts from a configured header" do
      e = env([{"X-Request-Id", "abc123"}], "")
      assert {:ok, "abc123"} = Hook.DedupKey.Rules.extract(e, header: "x-request-id")
    end

    test "extracts from a top-level JSON key" do
      e = env([], ~s({"id":"evt_42","kind":"charge"}))
      assert {:ok, "evt_42"} = Hook.DedupKey.Rules.extract(e, json: ["id"])
    end

    test "extracts from a nested JSON path" do
      e = env([], ~s({"data":{"id":"nested_9"}}))
      assert {:ok, "nested_9"} = Hook.DedupKey.Rules.extract(e, json: ["data", "id"])
    end

    test "first matching rule wins" do
      e = env([{"webhook-id", "hdr_1"}], ~s({"id":"json_1"}))
      assert {:ok, "hdr_1"} = Hook.DedupKey.Rules.extract(e, header: "webhook-id", json: ["id"])
    end

    test "falls through to the next rule when the first is nil" do
      e = env([], ~s({"id":"json_1"}))
      assert {:ok, "json_1"} = Hook.DedupKey.Rules.extract(e, header: "absent", json: ["id"])
    end

    test "default rules try webhook-id header then JSON id" do
      hdr = env([{"webhook-id", "wid"}], ~s({"id":"jid"}))
      assert {:ok, "wid"} = Hook.DedupKey.Rules.extract(hdr, [])

      json = env([], ~s({"id":"jid"}))
      assert {:ok, "jid"} = Hook.DedupKey.Rules.extract(json, [])
    end

    test ":none when no rule yields a key" do
      assert :none = Hook.DedupKey.Rules.extract(env([], ~s({"kind":"x"})), json: ["id"])
      assert :none = Hook.DedupKey.Rules.extract(env([], "not json"), [])
      assert :none = Hook.DedupKey.Rules.extract(env([], ""), header: "absent")
    end

    test ":none when the JSON value is not a binary" do
      e = env([], ~s({"id":123}))
      assert :none = Hook.DedupKey.Rules.extract(e, json: ["id"])
    end
  end

  describe "Hook.DedupKey.Stripe" do
    test "extracts the event id from the body" do
      e = env([], ~s({"id":"evt_stripe_1","object":"event"}))
      assert {:ok, "evt_stripe_1"} = Hook.DedupKey.Stripe.extract(e, [])
    end

    test ":none without an id" do
      assert :none = Hook.DedupKey.Stripe.extract(env([], ~s({"object":"event"})), [])
    end
  end

  describe "Hook.DedupKey.GitHub" do
    test "extracts the delivery header" do
      e = env([{"X-GitHub-Delivery", "d-guid-1"}], "")
      assert {:ok, "d-guid-1"} = Hook.DedupKey.GitHub.extract(e, [])
    end

    test ":none without the delivery header" do
      assert :none = Hook.DedupKey.GitHub.extract(env([], ""), [])
    end
  end
end
