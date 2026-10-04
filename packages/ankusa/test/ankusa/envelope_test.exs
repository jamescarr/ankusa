defmodule Ankusa.EnvelopeTest do
  use ExUnit.Case, async: true

  alias Ankusa.{Envelope, UUIDv7}

  defp envelope(overrides) do
    struct(
      %Envelope{
        id: UUIDv7.generate(),
        source_id: "stripe",
        tenant_id: "acme",
        received_at: 1_737_500_000_000,
        method: "POST",
        path: "/hooks/stripe",
        headers: [],
        content_type: "application/json",
        body: "{}",
        size: 2
      },
      overrides
    )
  end

  describe "idempotency_key/1" do
    test "a dedupe key is scoped by tenant and source" do
      assert Envelope.idempotency_key(envelope(%{dedupe_key: "evt_1"})) == "acme:stripe:evt_1"
    end

    test "two tenants sharing a provider event id get distinct keys" do
      acme = envelope(%{tenant_id: "acme", dedupe_key: "d1"})
      globex = envelope(%{tenant_id: "globex", dedupe_key: "d1"})

      refute Envelope.idempotency_key(acme) == Envelope.idempotency_key(globex)
    end

    test "no tenant scopes as \"default\"" do
      env = envelope(%{tenant_id: nil, dedupe_key: "evt_1"})
      assert Envelope.idempotency_key(env) == "default:stripe:evt_1"
    end

    test "no dedupe key falls back to the envelope id" do
      env = envelope(%{dedupe_key: nil})
      assert Envelope.idempotency_key(env) == env.id
    end

    test "an empty dedupe key falls back to the envelope id" do
      env = envelope(%{dedupe_key: ""})
      assert Envelope.idempotency_key(env) == env.id
    end
  end
end
