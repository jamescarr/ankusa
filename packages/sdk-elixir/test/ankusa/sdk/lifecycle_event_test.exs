defmodule Ankusa.SDK.LifecycleEventTest do
  use ExUnit.Case, async: true

  alias Ankusa.SDK.{InvalidMessageError, LifecycleEvent}

  defp event(overrides \\ %{}) do
    Map.merge(
      %{
        "specversion" => "1.0",
        "id" => "0193a2b4-7c2e-7000-8000-00000000a001",
        "source" => "urn:ankusa:instance:default",
        "type" => "io.ankusa.source.created",
        "subject" => "acme.stripe",
        "time" => "2026-10-25T09:30:00.123Z",
        "datacontenttype" => "application/json",
        "data" => %{"id" => "acme.stripe", "tenant_id" => "acme"},
        "revision" => 1,
        "actor" => "system",
        "instance" => "default",
        "sequence" => "42"
      },
      overrides
    )
  end

  defp decode(map), do: LifecycleEvent.decode(JSON.encode!(map))

  test "decodes every field the instance writes" do
    assert {:ok, %LifecycleEvent{} = decoded} = decode(event(%{"replayid" => "rpl_1"}))

    assert decoded.type == "io.ankusa.source.created"
    assert decoded.subject == "acme.stripe"
    assert decoded.data == %{"id" => "acme.stripe", "tenant_id" => "acme"}
    assert decoded.revision == 1
    assert decoded.sequence == "42"
    assert decoded.replayid == "rpl_1"
    assert decoded.traceparent == nil
  end

  test "a minimal event decodes, the optional fields nil" do
    minimal = Map.take(event(), ~w(specversion id source type subject time))

    assert {:ok, decoded} = decode(minimal)
    assert decoded.data == nil
    assert decoded.revision == nil
    assert decoded.sequence == nil
  end

  test "unknown keys and unknown types are ignored" do
    assert {:ok, decoded} = decode(event(%{"type" => "io.ankusa.source.archived", "x" => [1]}))
    assert decoded.type == "io.ankusa.source.archived"
  end

  test "the failures are InvalidMessageErrors, never retryable" do
    for {input, code, field} <- [
          {"not json", "invalid_json", nil},
          {"[1]", "not_an_object", nil},
          {JSON.encode!(event(%{"specversion" => "0.3"})), "unsupported_version", nil},
          {JSON.encode!(Map.delete(event(), "id")), "invalid_field", "id"},
          {JSON.encode!(event(%{"subject" => ""})), "invalid_field", "subject"},
          {JSON.encode!(event(%{"data" => [1]})), "invalid_field", "data"},
          {JSON.encode!(event(%{"revision" => "3"})), "invalid_field", "revision"},
          {JSON.encode!(event(%{"revision" => 0})), "invalid_field", "revision"},
          {JSON.encode!(event(%{"sequence" => 5})), "invalid_field", "sequence"},
          {JSON.encode!(event(%{"sequence" => "4a"})), "invalid_field", "sequence"}
        ] do
      assert {:error, %InvalidMessageError{code: ^code, field: ^field, retryable: false}} =
               LifecycleEvent.decode(input)
    end
  end

  test "the first failure wins, in the order the fields are documented" do
    assert {:error, %InvalidMessageError{field: "id"}} =
             decode(event(%{"id" => "", "revision" => "x"}))
  end
end
