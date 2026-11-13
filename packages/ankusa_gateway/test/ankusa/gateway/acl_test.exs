defmodule Ankusa.Gateway.ACLTest do
  @moduledoc """
  The ACL document v1 (`Ankusa.Gateway.ACL`) as a value: the wire format, the
  document key and the grant id, expiry, and the subject matching table the
  `grants` authorizer decides with.
  """

  use ExUnit.Case, async: true

  alias Ankusa.Gateway.ACL
  alias Ankusa.Gateway.Authz.Request
  alias Ankusa.Principal

  @space "reports"
  @key "teams/eng/q3.pdf"
  @now 1_790_000_000
  @incarnation "01JD0Z2B7M3N4P5Q6R7S8T9V0W"

  defp new_doc(grants) do
    %{ACL.new(@space, {:object, @key}, @incarnation) | grants: grants, updated_at: @now}
  end

  defp grant(subject, actions, opts \\ []) do
    %{
      id: Keyword.get(opts, :id, ACL.grant_id({:object, @key})),
      subject: subject,
      actions: actions,
      created_by: Keyword.get(opts, :created_by, "alice"),
      created_at: @now,
      expires_at: Keyword.get(opts, :expires_at)
    }
  end

  defp subjects(grants, principal, via, action \\ :read) do
    request = %Request{
      principal: principal,
      via: via,
      subject_key: <<0::128>>,
      action: action,
      resource: %{kind: :object, space: @space, key: @key, tenant: nil}
    }

    grants |> new_doc() |> ACL.matching_grants(request, @now) |> Enum.map(& &1.subject)
  end

  describe "the wire format" do
    test "a document round trips through encode/1 and decode/1" do
      doc =
        new_doc([
          grant("user:bob", [:read]),
          grant("group:eng", [:read, :write], expires_at: @now + 60)
        ])

      assert {:ok, decoded} = ACL.decode(ACL.encode(doc))
      assert decoded == doc
    end

    test "an unknown version or malformed JSON is :invalid_acl" do
      encoded = ACL.encode(new_doc([grant("user:bob", [:read])]))

      assert {:ok, _doc} = ACL.decode(encoded)

      future = encoded |> JSON.decode!() |> Map.put("v", 2) |> JSON.encode!()
      assert ACL.decode(future) == {:error, :invalid_acl}
      assert ACL.decode("{not json") == {:error, :invalid_acl}
      assert ACL.decode("[]") == {:error, :invalid_acl}
    end

    test "a grant with an unknown action is :invalid_acl" do
      encoded = ACL.encode(new_doc([grant("user:bob", [:read])]))

      bad =
        encoded
        |> JSON.decode!()
        |> Map.update!("grants", fn [grant | rest] ->
          [Map.put(grant, "actions", ["read", "publish"]) | rest]
        end)
        |> JSON.encode!()

      assert ACL.decode(bad) == {:error, :invalid_acl}
    end

    test "a grant out of shape is :invalid_acl" do
      encoded = ACL.encode(new_doc([grant("user:bob", [:read])]))

      for path <- [["id"], ["subject"], ["created_at"]] do
        bad =
          encoded
          |> JSON.decode!()
          |> Map.update!("grants", fn [grant | rest] -> [Map.delete(grant, hd(path)) | rest] end)
          |> JSON.encode!()

        assert ACL.decode(bad) == {:error, :invalid_acl}, inspect(path)
      end
    end

    test "the five actions are the only grant actions" do
      assert ACL.actions() == [:read, :write, :delete, :list, :share]
      assert ACL.action("read") == {:ok, :read}
      assert ACL.action(:share) == {:ok, :share}
      assert ACL.action("publish") == :error
      assert ACL.action(1) == :error
    end
  end

  describe "the document key" do
    test "is the space's prefix plus the ACL tree, by kind and key hash" do
      hash = ACL.hash(@key)
      assert hash =~ ~r/\A[0-9a-f]{64}\z/

      assert ACL.doc_key(%{prefix: "docs/"}, {:object, @key}) ==
               "docs/.ankusa/acl/v1/o/" <> hash

      assert ACL.doc_key(%{prefix: "docs/"}, {:prefix, "teams/eng/"}) ==
               "docs/.ankusa/acl/v1/p/" <> ACL.hash("teams/eng/")

      assert ACL.doc_key(%{prefix: ""}, {:object, "a"}) == ".ankusa/acl/v1/o/" <> ACL.hash("a")
    end
  end

  describe "grant ids" do
    test "carry the kind, the resource hash and a ULID" do
      id = ACL.grant_id({:object, @key})
      assert {:ok, :object, hash} = ACL.parse_grant_id(id)
      assert hash == ACL.hash(@key)

      prefix_id = ACL.grant_id({:prefix, "teams/eng/"})
      assert {:ok, :prefix, prefix_hash} = ACL.parse_grant_id(prefix_id)
      assert prefix_hash == ACL.hash("teams/eng/")
    end

    test "refuse anything but <o|p><64 hex>.<26 ULID chars>" do
      hash = ACL.hash(@key)
      ulid = ACL.ulid()

      assert {:ok, :object, _hash} = ACL.parse_grant_id("o#{hash}.#{ulid}")

      for bad <- [
            "x#{hash}.#{ulid}",
            "o#{String.duplicate("g", 64)}.#{ulid}",
            "o#{String.duplicate("a", 63)}.#{ulid}",
            "o#{hash}.#{String.duplicate("I", 26)}",
            "o#{hash}.#{String.duplicate("Z", 25)}",
            "o#{hash}.#{String.duplicate("Z", 27)}",
            "o#{hash}#{ulid}",
            "O#{hash}.#{ulid}",
            "",
            nil
          ] do
        assert ACL.parse_grant_id(bad) == :error, inspect(bad)
      end
    end
  end

  describe "expiry" do
    test "live?/2 is false from the expiry second on" do
      assert ACL.live?(grant("user:bob", [:read]), @now)
      assert ACL.live?(grant("user:bob", [:read], expires_at: @now + 1), @now)
      refute ACL.live?(grant("user:bob", [:read], expires_at: @now), @now)
      refute ACL.live?(grant("user:bob", [:read], expires_at: @now - 1), @now)
    end

    test "matching_grants/3 drops an expired grant" do
      bob = Principal.new("bob", [])

      assert subjects([grant("user:bob", [:read], expires_at: @now)], bob, :token) == []

      assert subjects([grant("user:bob", [:read], expires_at: @now + 1)], bob, :token) ==
               ["user:bob"]
    end

    test "matching_grants/3 drops a grant that does not carry the action" do
      bob = Principal.new("bob", [])
      grant = grant("user:bob", [:read])

      assert subjects([grant], bob, :token, :read) == ["user:bob"]
      assert subjects([grant], bob, :token, :write) == []
    end
  end

  describe "subjects" do
    test "user:<id> matches that principal, however it authenticated" do
      grant = grant("user:bob", [:read])

      assert subjects([grant], Principal.new("bob", []), :token) == ["user:bob"]
      assert subjects([grant], Principal.new("bob", []), :jwt) == ["user:bob"]
      assert subjects([grant], Principal.new("alice", []), :token) == []
      # The suffix is the whole id: a prefix of it is another principal.
      assert subjects([grant], Principal.new("bo", []), :token) == []
    end

    test "group:<name> matches a principal in the group" do
      grant = grant("group:eng", [:read])
      in_group = %{Principal.new("bob", []) | groups: ["ops", "eng"]}

      assert subjects([grant], in_group, :jwt) == ["group:eng"]
      assert subjects([grant], %{Principal.new("bob", []) | groups: ["ops"]}, :jwt) == []
      assert subjects([grant], Principal.new("bob", []), :token) == []
    end

    test "tenant:<id> matches a principal holding that tenant, or all of them" do
      grant = grant("tenant:acme", [:read])

      assert subjects([grant], Principal.new("bob", [], ["acme"]), :jwt) == ["tenant:acme"]
      assert subjects([grant], Principal.new("bob", [], ["other"]), :jwt) == []
      assert subjects([grant], Principal.new("bob", []), :jwt) == []

      assert subjects([grant], %Principal{Principal.new("op", []) | tenants: :all}, :jwt) ==
               ["tenant:acme"]
    end

    test "anyone matches an authenticated principal, but neither anonymous nor a link" do
      grant = grant("anyone", [:read])
      bob = Principal.new("bob", [])

      assert subjects([grant], bob, :token) == ["anyone"]
      assert subjects([grant], bob, :jwt) == ["anyone"]
      assert subjects([grant], Principal.operator(), :anonymous) == []
      assert subjects([grant], %Principal{id: "link:" <> ACL.ulid()}, :link) == []
    end

    test "link matches only the share link of that very grant" do
      id = ACL.grant_id({:object, @key})
      grant = grant("link", [:read], id: id)
      link = %Principal{Principal.new("link:" <> id, []) | id: "link:" <> id}

      assert subjects([grant], link, :link) == ["link"]

      other = %Principal{Principal.new("link:" <> ACL.ulid(), []) | id: "link:nope"}
      assert subjects([grant], other, :link) == []

      # The same id through another authenticator is another kind of principal.
      assert subjects([grant], link, :jwt) == []
      assert subjects([grant], link, :token) == []
    end
  end
end
