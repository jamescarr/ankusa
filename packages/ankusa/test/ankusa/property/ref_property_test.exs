defmodule Ankusa.Property.RefTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  @moduletag timeout: 3_600_000

  alias Ankusa.ClaimCheck.Ref
  alias Ankusa.Generators, as: G

  property "parse/1 inverts to_string/1" do
    check all ref <- G.ref() do
      assert Ref.parse(Ref.to_string(ref)) == {:ok, ref}
      assert Ref.parse(to_string(ref)) == {:ok, ref}
    end
  end

  property "a claim id locates its pack and position, and the two rebuild the id" do
    check all pack <- G.pack_id(), index <- G.pack_index() do
      claim_id = Ref.claim_id(pack, index)

      assert Ref.locate(claim_id) == {:ok, pack, index}
      assert Ref.validate_pack_id(pack) == :ok

      assert Ref.validate_pack_id(claim_id) ==
               if(index == 0, do: :ok, else: {:error, :invalid_id})
    end
  end

  property "a ref's path and storage keys are pure functions of it, under both layouts" do
    check all ref <- G.ref() do
      {:ok, pack, _index} = Ref.locate(ref.claim_id)

      assert Ref.path(ref) == "/v1/claims/#{ref.tenant_id}/#{ref.claim_id}"

      for layout <- [:flat, :sharded] do
        key = Ref.key(ref, layout)
        assert key == Ref.object_key(ref.tenant_id, pack, layout)
        assert String.starts_with?(key, "claims/tenant=#{ref.tenant_id}/")
        assert String.ends_with?(key, "/" <> pack)
      end

      assert Ref.object_keys(ref.tenant_id, pack, :flat) ==
               Enum.reverse(Ref.object_keys(ref.tenant_id, pack, :sharded))
    end
  end

  property "parse/1 never raises, and whatever it accepts is the canonical spelling" do
    check all input <-
                one_of([
                  G.bytes(80),
                  string(:printable, max_length: 120),
                  map(G.ref(), &Ref.to_string/1),
                  G.mutated(map(G.ref(), &Ref.to_string/1))
                ]) do
      case Ref.parse(input) do
        {:ok, %Ref{} = ref} ->
          assert Ref.to_string(ref) == input
          assert Ref.valid_tenant?(ref.tenant_id)
          assert {:ok, _pack, _index} = Ref.locate(ref.claim_id)

        {:error, :invalid_ref} ->
          :ok
      end
    end
  end

  property "parse/1 refuses anything but a binary" do
    check all term <- one_of([integer(), atom(:alphanumeric), list_of(integer()), constant(nil)]) do
      assert Ref.parse(term) == {:error, :invalid_ref}
    end
  end

  property "validate_tenant/1 and locate/1 never raise on any term" do
    check all term <- one_of([G.bytes(80), G.simple_term()]) do
      assert Ref.validate_tenant(term) in [:ok, {:error, :invalid_tenant}]
      assert match?({:ok, _, _}, Ref.locate(term)) or Ref.locate(term) == {:error, :invalid_id}
      assert Ref.validate_pack_id(term) in [:ok, {:error, :invalid_id}]
    end
  end
end
