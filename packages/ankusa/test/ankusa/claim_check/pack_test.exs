defmodule Ankusa.ClaimCheck.PackTest do
  use ExUnit.Case, async: true

  alias Ankusa.ClaimCheck.Pack

  defp claim(id, body, content_type \\ "application/json") do
    %{
      id: id,
      body: body,
      sha256: Base.encode16(:crypto.hash(:sha256, body), case: :lower),
      content_type: content_type,
      received_at: 1_760_000_000_000
    }
  end

  test "each claim's bytes sit at its placement, so one range read returns exactly them" do
    claims = [
      claim("a-1", :crypto.strong_rand_bytes(70_000)),
      claim("b-22", "second body"),
      claim("c-333", :crypto.strong_rand_bytes(1))
    ]

    {data, placements} = Pack.build(claims)
    bin = IO.iodata_to_binary(data)

    for {claim, placement} <- Enum.zip(claims, placements) do
      assert placement.id == claim.id
      assert binary_part(bin, placement.offset, placement.length) == claim.body
    end
  end

  test "a pack is a standard ZIP: entries named by id, plus a manifest that matches the placements" do
    claims = [claim("a-1", "first"), claim("b-2", "second", nil)]
    {data, placements} = Pack.build(claims)

    {:ok, files} = :zip.unzip(IO.iodata_to_binary(data), [:memory])
    files = Map.new(files, fn {name, bytes} -> {to_string(name), bytes} end)

    assert files["a-1"] == "first"
    assert files["b-2"] == "second"

    %{"v" => 1, "claims" => rows} = JSON.decode!(files["manifest.json"])

    assert rows ==
             Enum.zip_with(claims, placements, fn claim, placement ->
               %{
                 "id" => claim.id,
                 "offset" => placement.offset,
                 "length" => placement.length,
                 "sha256" => claim.sha256,
                 "content_type" => claim.content_type,
                 "received_at" => claim.received_at
               }
             end)
  end
end
