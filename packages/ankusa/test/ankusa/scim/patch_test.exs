defmodule Ankusa.Scim.PatchTest do
  use ExUnit.Case, async: true

  alias Ankusa.Scim.Patch

  @schema "urn:ietf:params:scim:api:messages:2.0:PatchOp"
  @enterprise "urn:ietf:params:scim:schemas:extension:enterprise:2.0:User"

  defp body(ops), do: %{"schemas" => [@schema], "Operations" => ops}

  defp user(attrs \\ %{}) do
    Map.merge(
      %{
        "id" => "u1",
        "userName" => "jane@corp.com",
        "active" => true,
        "meta" => %{"version" => 1}
      },
      attrs
    )
  end

  defp group(attrs \\ %{}),
    do: Map.merge(%{"id" => "g1", "displayName" => "Ops", "meta" => %{"version" => 1}}, attrs)

  defp patch_user(resource, ops) do
    with {:ok, patched, []} <- Patch.apply(:user, resource, body(ops)), do: {:ok, patched}
  end

  describe "active, as the two big providers send it" do
    test "Entra replaces it with a path and the string \"False\"" do
      assert {:ok, %{"active" => false}} =
               patch_user(user(), [%{"op" => "Replace", "path" => "active", "value" => "False"}])

      assert {:ok, %{"active" => true}} =
               patch_user(user(%{"active" => false}), [
                 %{"op" => "Replace", "path" => "active", "value" => "True"}
               ])
    end

    test "Okta replaces it with no path and a real boolean" do
      assert {:ok, %{"active" => false}} =
               patch_user(user(), [%{"op" => "replace", "value" => %{"active" => false}}])
    end

    test "anything else is invalidValue" do
      assert {:error, "invalidValue", _} =
               patch_user(user(), [%{"op" => "replace", "path" => "active", "value" => "maybe"}])
    end
  end

  describe "attribute paths" do
    test "a complex attribute's sub-attribute, created and pruned" do
      assert {:ok, %{"name" => %{"givenName" => "Jane"}}} =
               patch_user(user(), [
                 %{"op" => "add", "path" => "name.givenName", "value" => "Jane"}
               ])

      with_name = user(%{"name" => %{"givenName" => "Jane"}})

      assert {:ok, patched} =
               patch_user(with_name, [%{"op" => "remove", "path" => "name.givenName"}])

      refute Map.has_key?(patched, "name")
    end

    test "paths and operations are case-insensitive" do
      assert {:ok, %{"displayName" => "Jane D"}} =
               patch_user(user(), [
                 %{"op" => "REPLACE", "path" => "DisplayName", "value" => "Jane D"}
               ])
    end

    test "the core schema URN may prefix a path" do
      assert {:ok, %{"title" => "SRE"}} =
               patch_user(user(), [
                 %{
                   "op" => "add",
                   "path" => "urn:ietf:params:scim:schemas:core:2.0:User:title",
                   "value" => "SRE"
                 }
               ])
    end

    test "enterprise extension attributes, by path and as an object" do
      assert {:ok, %{@enterprise => %{"department" => "Ops"}}} =
               patch_user(user(), [
                 %{"op" => "add", "path" => @enterprise <> ":department", "value" => "Ops"}
               ])

      assert {:ok, %{@enterprise => %{"department" => "Ops", "costCenter" => "7"}}} =
               patch_user(user(), [
                 %{
                   "op" => "add",
                   "value" => %{@enterprise => %{"department" => "Ops", "costCenter" => "7"}}
                 }
               ])

      assert {:ok, patched} =
               patch_user(user(%{@enterprise => %{"department" => "Ops"}}), [
                 %{"op" => "remove", "path" => @enterprise <> ":department"}
               ])

      refute Map.has_key?(patched, @enterprise)
    end

    test "an attribute Ankusa does not keep is ignored, as it is in a POST body" do
      assert {:ok, patched} =
               patch_user(user(), [
                 %{
                   "op" => "replace",
                   "path" => "addresses[type eq \"work\"].streetAddress",
                   "value" => "1 Main"
                 },
                 %{"op" => "replace", "path" => "password", "value" => "hunter2"},
                 %{"op" => "replace", "value" => %{"x-vendor" => 1}}
               ])

      assert patched == user()
    end
  end

  describe "multi-valued attributes" do
    test "add appends and skips a duplicate value" do
      work = %{"type" => "work", "value" => "j@corp.com"}

      assert {:ok, %{"emails" => [^work]}} =
               patch_user(user(), [%{"op" => "add", "path" => "emails", "value" => [work]}])

      assert {:ok, %{"emails" => [^work, %{"value" => "h@home.com"}]}} =
               patch_user(user(%{"emails" => [work]}), [
                 %{
                   "op" => "add",
                   "path" => "emails",
                   "value" => [work, %{"type" => "home", "value" => "h@home.com"}]
                 }
               ])
    end

    test "a filter addresses elements: replace, add when absent, remove" do
      emails = [
        %{"type" => "work", "value" => "old@corp.com"},
        %{"type" => "home", "value" => "h@home.com"}
      ]

      assert {:ok, %{"emails" => [%{"value" => "new@corp.com"}, %{"value" => "h@home.com"}]}} =
               patch_user(user(%{"emails" => emails}), [
                 %{
                   "op" => "replace",
                   "path" => ~s(emails[type eq "work"].value),
                   "value" => "new@corp.com"
                 }
               ])

      assert {:ok, %{"emails" => [%{"type" => "work", "value" => "w@corp.com"}]}} =
               patch_user(user(), [
                 %{
                   "op" => "add",
                   "path" => ~s(emails[type eq "work"].value),
                   "value" => "w@corp.com"
                 }
               ])

      assert {:ok, %{"emails" => [%{"type" => "work"}]}} =
               patch_user(user(%{"emails" => emails}), [
                 %{"op" => "remove", "path" => ~s(emails[type eq "home"])}
               ])

      assert {:ok, patched} =
               patch_user(user(%{"emails" => [hd(emails)]}), [
                 %{"op" => "remove", "path" => ~s(emails[type eq "work"])}
               ])

      refute Map.has_key?(patched, "emails")
    end
  end

  describe "group membership" do
    defp patch_group(ops), do: Patch.apply(:group, group(), body(ops))

    test "add, replace and remove become changes, never edits of the resource" do
      assert {:ok, patched, [{:add, ["u1", "u2"]}]} =
               patch_group([
                 %{
                   "op" => "add",
                   "path" => "members",
                   "value" => [%{"value" => "u1"}, %{"value" => "u2"}]
                 }
               ])

      assert patched == group()

      assert {:ok, _, [{:replace, ["u3"]}]} =
               patch_group([
                 %{"op" => "replace", "path" => "members", "value" => [%{"value" => "u3"}]}
               ])
    end

    test "removing one member by filter" do
      assert {:ok, _, [{:remove, ["u1"]}]} =
               patch_group([%{"op" => "remove", "path" => ~s(members[value eq "u1"])}])
    end

    test "Entra removes with a value list" do
      assert {:ok, _, [{:remove, ["u1", "u2"]}]} =
               patch_group([
                 %{
                   "op" => "Remove",
                   "path" => "members",
                   "value" => [%{"value" => "u1"}, %{"value" => "u2"}]
                 }
               ])
    end

    test "removing members with neither value nor filter removes them all" do
      assert {:ok, _, [{:replace, []}]} = patch_group([%{"op" => "remove", "path" => "members"}])
    end

    test "members as a key of a pathless value, a single member as an object" do
      assert {:ok, _, [{:add, ["u1"]}]} =
               patch_group([%{"op" => "add", "value" => %{"members" => [%{"value" => "u1"}]}}])

      assert {:ok, _, [{:add, ["u1"]}]} =
               patch_group([%{"op" => "add", "path" => "members", "value" => %{"value" => "u1"}}])
    end

    test "changes come back in the order asked, beside attribute changes" do
      assert {:ok, %{"displayName" => "Platform"}, [{:add, ["u1"]}, {:remove, ["u2"]}]} =
               patch_group([
                 %{"op" => "add", "path" => "members", "value" => [%{"value" => "u1"}]},
                 %{"op" => "replace", "path" => "displayName", "value" => "Platform"},
                 %{"op" => "remove", "path" => ~s(members[value eq "u2"])}
               ])
    end

    test "members are not an attribute of a user" do
      assert {:ok, patched, []} =
               Patch.apply(
                 :user,
                 user(),
                 body([%{"op" => "add", "path" => "members", "value" => [%{"value" => "u1"}]}])
               )

      assert patched == user()
    end

    test "malformed members" do
      assert {:error, "invalidValue", _} =
               patch_group([%{"op" => "add", "path" => "members", "value" => [%{"id" => "u1"}]}])

      assert {:error, "invalidValue", _} =
               patch_group([%{"op" => "add", "path" => "members", "value" => "u1"}])

      assert {:error, "invalidPath", _} =
               patch_group([%{"op" => "remove", "path" => ~s(members[display eq "x"])}])

      assert {:error, "invalidPath", _} =
               patch_group([%{"op" => "add", "path" => ~s(members[value eq "x"]), "value" => []}])
    end
  end

  describe "errors" do
    test "remove with no path has no target" do
      assert {:error, "noTarget", _} = patch_user(user(), [%{"op" => "remove"}])
    end

    test "id and meta are read-only" do
      for path <- ["id", "meta", "meta.version"] do
        assert {:error, "mutability", _} =
                 patch_user(user(), [%{"op" => "replace", "path" => path, "value" => "x"}])
      end

      assert {:error, "mutability", _} =
               patch_user(user(), [%{"op" => "replace", "value" => %{"id" => "evil"}}])
    end

    test "paths that do not parse" do
      for path <- [
            "emails[",
            "emails[type eq]",
            "a b",
            "name..givenName",
            ~s(emails[type co "x"])
          ] do
        assert {:error, "invalidPath", _} =
                 patch_user(user(), [%{"op" => "replace", "path" => path, "value" => "x"}]),
               path
      end
    end

    test "a pathless add or replace needs an object" do
      assert {:error, "invalidValue", _} = patch_user(user(), [%{"op" => "add", "value" => "x"}])
      assert {:error, "invalidValue", _} = patch_user(user(), [%{"op" => "replace"}])

      assert {:error, "invalidValue", _} =
               patch_user(user(), [%{"op" => "add", "path" => "title"}])
    end

    test "a patch may not leave the resource invalid" do
      assert {:error, "invalidValue", _} =
               patch_user(user(), [%{"op" => "replace", "path" => "userName", "value" => ""}])

      assert {:error, "invalidValue", _} =
               patch_user(user(), [%{"op" => "remove", "path" => "userName"}])

      assert {:error, "invalidValue", _} =
               patch_user(user(), [%{"op" => "replace", "path" => "userName", "value" => 42}])
    end

    test "the envelope" do
      assert {:error, "invalidSyntax", _} = Patch.apply(:user, user(), %{})
      assert {:error, "invalidSyntax", _} = Patch.apply(:user, user(), %{"Operations" => []})

      assert {:error, "invalidSyntax", _} =
               Patch.apply(:user, user(), %{
                 "schemas" => ["wrong"],
                 "Operations" => [%{"op" => "add"}]
               })

      assert {:error, "invalidSyntax", _} = patch_user(user(), [%{"op" => "move"}])
      assert {:error, "invalidSyntax", _} = patch_user(user(), [%{"path" => "title"}])
    end

    test "nothing is applied when any operation fails" do
      assert {:error, "mutability", _} =
               patch_user(user(), [
                 %{"op" => "replace", "path" => "title", "value" => "SRE"},
                 %{"op" => "replace", "path" => "id", "value" => "x"}
               ])
    end
  end
end
