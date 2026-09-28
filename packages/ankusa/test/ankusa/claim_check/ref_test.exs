defmodule Ankusa.ClaimCheck.RefTest do
  use ExUnit.Case, async: true

  alias Ankusa.ClaimCheck.Ref
  alias Ankusa.UUIDv7

  @sha String.duplicate("ab", 32)

  defp urn(tenant, id, offset, length, digest \\ "sha256-" <> @sha),
    do: "urn:ankusa:claim:v1:#{tenant}:#{id}:#{offset}:#{length}:#{digest}"

  test "parse/1 and to_string/1 round-trip, and the path drops the digest" do
    id = UUIDv7.generate()
    string = urn("Acme_Corp-1", id, 66, 3_145_728)

    assert {:ok, ref} = Ref.parse(string)
    assert ref.tenant_id == "Acme_Corp-1"
    assert ref.object_id == id
    assert {ref.offset, ref.length, ref.sha256} == {66, 3_145_728, @sha}
    assert Ref.to_string(ref) == string
    assert to_string(ref) == string
    assert Ref.path(ref) == "/v1/claims/Acme_Corp-1/#{id}/66/3145728"
  end

  test "parse/1 accepts offset 0" do
    assert {:ok, %Ref{offset: 0}} = Ref.parse(urn("acme", UUIDv7.generate(), 0, 1))
  end

  test "parse/1 rejects anything outside the v1 grammar" do
    id = UUIDv7.generate()

    for bad <- [
          # tenant: a colon would add a segment, % or / would need encoding
          "urn:ankusa:claim:v1:ac:me:#{id}:0:1:sha256-#{@sha}",
          urn("ac%2Fme", id, 0, 1),
          urn("ac/me", id, 0, 1),
          urn("", id, 0, 1),
          urn(String.duplicate("a", 65), id, 0, 1),
          # object id: must be a lowercase UUIDv7
          urn("acme", String.upcase(id), 0, 1),
          urn("acme", "00000000-0000-4000-8000-000000000000", 0, 1),
          # range: no leading zeros, no zero length, no signs
          urn("acme", id, "066", 1),
          urn("acme", id, 0, 0),
          urn("acme", id, -1, 1),
          urn("acme", id, 0, "01"),
          # digest: exactly 64 lowercase hex characters
          urn("acme", id, 0, 1, "sha256-" <> String.duplicate("a", 63)),
          urn("acme", id, 0, 1, "sha256-" <> String.upcase(@sha)),
          urn("acme", id, 0, 1, "md5-" <> @sha),
          # version and shape
          String.replace(urn("acme", id, 0, 1), ":v1:", ":v2:"),
          urn("acme", id, 0, 1) <> ":extra",
          "not a urn"
        ] do
      assert Ref.parse(bad) == {:error, :invalid_ref}, "accepted #{inspect(bad)}"
    end
  end

  test "the key's dt partition is the UTC date of the object id's timestamp" do
    last_ms_of_day = DateTime.to_unix(~U[2026-09-23 23:59:59.999Z], :millisecond)
    before_midnight = UUIDv7.generate(last_ms_of_day)
    after_midnight = UUIDv7.generate(last_ms_of_day + 1)

    assert Ref.object_key("acme", before_midnight) ==
             "claims/tenant=acme/dt=2026-09-23/#{before_midnight}"

    assert Ref.object_key("acme", after_midnight) ==
             "claims/tenant=acme/dt=2026-09-24/#{after_midnight}"
  end
end
