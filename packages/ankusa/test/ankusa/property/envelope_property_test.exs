defmodule Ankusa.Property.EnvelopeTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  @moduletag timeout: 3_600_000

  alias Ankusa.Envelope
  alias Ankusa.Generators, as: G

  property "from_binary/1 inverts to_binary/1, field for field" do
    check all env <- G.envelope() do
      assert Envelope.from_binary(Envelope.to_binary(env)) == env
    end
  end

  property "the encoding is byte-stable: re-encoding what was decoded gives the same bytes" do
    check all env <- G.envelope() do
      bin = Envelope.to_binary(env)
      assert Envelope.to_binary(Envelope.from_binary(bin)) == bin
    end
  end

  property "the body travels verbatim, whatever bytes it holds" do
    check all env <- G.envelope(), body <- G.bytes(8192) do
      env = %{env | body: body, size: byte_size(body)}
      assert Envelope.from_binary(Envelope.to_binary(env)).body == body
    end
  end

  property "header/2 finds the first value of a name, case-insensitively, on raw header bytes" do
    check all headers <- list_of(G.raw_header(), max_length: 8),
              name <- one_of([G.token(), G.bytes(16)]) do
      env = %Envelope{
        id: "i",
        source_id: "s",
        received_at: 0,
        method: "POST",
        path: "/",
        headers: headers,
        body: ""
      }

      expected = Enum.find_value(headers, fn {k, v} -> if fold(k) == fold(name), do: v end)
      assert Envelope.header(env, name) == expected
    end
  end

  property "idempotency_key/1 is a binary for every envelope, and the id without a dedupe key" do
    check all env <- G.envelope() do
      key = Envelope.idempotency_key(env)
      assert is_binary(key)

      if is_nil(env.dedupe_key) or env.dedupe_key == "" do
        assert key == env.id
      else
        assert key ==
                 (env.tenant_id || "default") <> ":" <> env.source_id <> ":" <> env.dedupe_key
      end
    end
  end

  # What `Envelope.header/2` compares with: `String.downcase/1`, which leaves bytes
  # that are not UTF-8 alone.
  defp fold(bytes), do: String.downcase(bytes)
end
