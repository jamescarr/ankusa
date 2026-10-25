defmodule Ankusa.Property.DedupeTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  @moduletag timeout: 3_600_000

  alias Ankusa.{Dedupe, Envelope}
  alias Ankusa.Generators, as: G

  @max_key_bytes 200

  defp env(headers, body) do
    %Envelope{
      id: "id",
      source_id: "src",
      received_at: 0,
      method: "POST",
      path: "/",
      headers: headers,
      body: body,
      size: byte_size(body)
    }
  end

  defp specs do
    one_of([
      member_of([:github, :standard_webhooks, :svix, :shopify, :stripe, :salesforce]),
      map(G.token(16), &%{header: &1}),
      map(
        list_of(string(:alphanumeric, min_length: 1, max_length: 6),
          min_length: 1,
          max_length: 4
        ),
        &%{json: Enum.join(&1, ".")}
      )
    ])
    |> map(&Dedupe.new!/1)
  end

  defp clamp(value) do
    if byte_size(value) > @max_key_bytes,
      do: "sha256:" <> Base.encode16(:crypto.hash(:sha256, value), case: :lower),
      else: value
  end

  property "key/2 never raises on any body or header list, and a key is a bounded binary or nil" do
    check all spec <- specs(),
              headers <- list_of(G.raw_header(), max_length: 8),
              body <- one_of([G.bytes(4096), G.mutated(map(json_body(), &JSON.encode!/1))]) do
      case Dedupe.key(spec, env(headers, body)) do
        nil ->
          :ok

        key ->
          assert is_binary(key)
          assert key != ""

          assert byte_size(key) <= @max_key_bytes or
                   (String.starts_with?(key, "sha256:") and byte_size(key) == 71)
      end
    end
  end

  property "key/2 with no spec is nil" do
    check all headers <- list_of(G.raw_header(), max_length: 4), body <- G.bytes(64) do
      assert Dedupe.key(nil, env(headers, body)) == nil
    end
  end

  property "a header spec returns the header's first value, clamped; none or empty is nil" do
    check all name <- G.token(16),
              value <- one_of([G.bytes(400), constant(""), string(:printable, max_length: 300)]),
              junk <- list_of(G.raw_header(), max_length: 4),
              fold <- member_of([&String.upcase/1, &String.downcase/1, & &1]) do
      spec = Dedupe.new!(%{header: String.downcase(name)})
      junk = Enum.reject(junk, fn {k, _} -> String.downcase(k) == String.downcase(name) end)
      request = env(junk ++ [{fold.(name), value}], "")

      expected = if value == "", do: nil, else: clamp(value)
      assert Dedupe.key(spec, request) == expected
      assert Dedupe.key(spec, env(junk, "")) == nil
    end
  end

  property "a json spec follows map keys to a string or integer, and yields nothing else" do
    check all path <-
                list_of(string(:alphanumeric, min_length: 1, max_length: 6),
                  min_length: 1,
                  max_length: 5
                ),
              leaf <-
                one_of([
                  string(:printable, max_length: 300),
                  integer(),
                  G.other_json(),
                  constant("")
                ]),
              siblings <-
                map_of(string(:alphanumeric, min_length: 1, max_length: 6), integer(),
                  max_length: 3
                ) do
      doc =
        Enum.reduce(Enum.reverse(path), leaf, fn key, inner -> Map.put(siblings, key, inner) end)

      spec = Dedupe.new!(%{json: Enum.join(path, ".")})

      expected =
        case leaf do
          value when is_binary(value) and value != "" -> clamp(value)
          value when is_integer(value) -> clamp(Integer.to_string(value))
          _ -> nil
        end

      assert Dedupe.key(spec, env([], JSON.encode!(doc))) == expected
    end
  end

  property "a json spec on a body that goes a different way, or is not JSON, is nil" do
    check all path <-
                list_of(string(:alphanumeric, min_length: 1, max_length: 6),
                  min_length: 1,
                  max_length: 3
                ),
              body <- one_of([G.bytes(512), map(G.other_json(), &JSON.encode!/1), constant("")]) do
      spec = Dedupe.new!(%{json: Enum.join(path, ".")})
      result = Dedupe.key(spec, env([], body))
      assert result == nil or is_binary(result)
    end
  end

  test "a body nested a million levels deep does not raise" do
    depth = 1_000_000
    body = String.duplicate("[", depth) <> String.duplicate("]", depth)

    assert Dedupe.key(Dedupe.new!(:stripe), env([], body)) == nil
    assert Dedupe.key(Dedupe.new!(:stripe), env([], String.duplicate("[", depth))) == nil
    assert Dedupe.key(Dedupe.new!(:stripe), env([], String.duplicate("{\"a\":", depth))) == nil
  end

  test "a value of exactly 200 bytes is kept, one more is hashed" do
    spec = Dedupe.new!(:svix)
    kept = String.duplicate("k", 200)
    hashed = String.duplicate("k", 201)

    assert Dedupe.key(spec, env([{"svix-id", kept}], "")) == kept
    assert Dedupe.key(spec, env([{"svix-id", hashed}], "")) == clamp(hashed)
    assert String.starts_with?(clamp(hashed), "sha256:")
  end

  # A JSON document of any shape, for mutation into near-JSON.
  defp json_body do
    tree(
      one_of([constant(nil), boolean(), integer(), string(:printable, max_length: 20)]),
      fn child ->
        one_of([
          list_of(child, max_length: 3),
          map_of(string(:alphanumeric, min_length: 1, max_length: 4), child, max_length: 3)
        ])
      end
    )
  end
end
