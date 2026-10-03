defmodule Ankusa.SDK.IdempotencyTest do
  use ExUnit.Case, async: true

  alias Ankusa.SDK.{Hook, Idempotency, Message, Webhook}

  defp message(overrides) do
    struct!(
      %Message{
        v: 1,
        id: "01a0",
        source_id: "stripe",
        received_at: 1_720_000_000_000,
        size: 5,
        body: "hello"
      },
      overrides
    )
  end

  defp headers(overrides) do
    struct!(
      %Webhook.Headers{id: "01a0", source: "stripe"},
      overrides
    )
  end

  test "with no dedupe key the key is the hook id" do
    assert Idempotency.key(message(%{})) == "01a0"
    assert Idempotency.key(headers(%{})) == "01a0"
  end

  test "a dedupe key is namespaced by the source" do
    assert Idempotency.key(message(%{dedupe_key: "evt_1"})) == "stripe:evt_1"
    assert Idempotency.key(headers(%{dedupe_key: "evt_1"})) == "stripe:evt_1"
  end

  test "an empty dedupe key falls back to the id" do
    assert Idempotency.key(message(%{dedupe_key: ""})) == "01a0"
  end

  test "a replay is ignored by default and appended with include_replay" do
    replayed = message(%{dedupe_key: "evt_1", replay_id: "rid-1"})

    assert Idempotency.key(replayed) == "stripe:evt_1"
    assert Idempotency.key(replayed, include_replay: true) == "stripe:evt_1#replay:rid-1"
  end

  test "include_replay with no dedupe key appends to the id" do
    replayed = message(%{replay_id: "rid-1"})

    assert Idempotency.key(replayed, include_replay: true) == "01a0#replay:rid-1"
  end

  test "a Hook works the same way" do
    hook = %Hook{id: "01a0", source_id: "stripe", dedupe_key: "evt_1", replay_id: "rid-1"}

    assert Idempotency.key(hook) == "stripe:evt_1"
    assert Idempotency.key(hook, include_replay: true) == "stripe:evt_1#replay:rid-1"
  end
end
