defmodule Ankusa.EnvelopeTest do
  use ExUnit.Case, async: true

  import Ankusa.TestHelpers, only: [envelope_from_the_future: 2]

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

  describe "to_binary/2 and from_binary/1" do
    test "the default encoding is the bare term every release up to 0.6 wrote" do
      env = envelope(%{dedupe_key: "evt_1"})
      legacy = :erlang.term_to_binary(Map.from_struct(env), [:deterministic])

      assert Envelope.to_binary(env) == legacy
      assert Envelope.from_binary(legacy) == {:ok, env}
    end

    test "v1 is the AE prefix, the version byte and the same term" do
      env = envelope(%{dedupe_key: "evt_1"})
      v1 = Envelope.to_binary(env, version: 1)

      assert <<"AE", 1, term::binary>> = v1
      assert term == Envelope.to_binary(env)
      assert Envelope.from_binary(v1) == {:ok, env}
    end

    for version <- [0, 1] do
      test "a field a newer node added is ignored (v#{version}), though its atom was never interned" do
        env = envelope(%{dedupe_key: "evt_1"})
        {field, bin} = envelope_from_the_future(env, unquote(version))

        assert_raise ArgumentError, fn -> String.to_existing_atom(field) end

        # What `binary_to_term(_, [:safe])` would have done with this row.
        term = if unquote(version) == 1, do: binary_part(bin, 3, byte_size(bin) - 3), else: bin
        assert_raise ArgumentError, fn -> :erlang.binary_to_term(term, [:safe]) end

        assert Envelope.from_binary(bin) == {:ok, env}
      end
    end

    test "a version this node does not know is an error value, not a raise" do
      <<"AE", 1, term::binary>> = Envelope.to_binary(envelope(%{}), version: 1)

      assert Envelope.from_binary(<<"AE", 9, term::binary>>) ==
               {:error, :unsupported_envelope_version}

      assert Envelope.from_binary(<<"AE", 0, term::binary>>) ==
               {:error, :unsupported_envelope_version}
    end

    test "bytes that are no envelope are :undecodable, never a raise" do
      for bad <- [
            "",
            "garbage",
            "AE",
            <<"AE", 1>>,
            <<"AE", 1, "garbage">>,
            :erlang.term_to_binary(7),
            :erlang.term_to_binary([1, 2])
          ] do
        assert Envelope.from_binary(bad) == {:error, :undecodable}
      end
    end
  end
end
