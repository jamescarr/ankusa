defmodule Ankusa.Directory.Store.PersistentTest do
  @moduledoc """
  The persistent directory behind `Ankusa.Directory`, through the facade the
  SCIM router uses (so ids are assigned the way production assigns them).
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Directory
  alias Ankusa.Directory.Store.Persistent

  setup do
    config = test_config(roles: [:storage])
    put_config(config)
    start_supervised!({Ankusa.Store, instance: config.instance})
    start_supervised!({Persistent, instance: config.instance})
    %{inst: config.instance}
  end

  defp user!(inst, name, attrs \\ %{}) do
    {:ok, user} = Directory.create_user(inst, Map.merge(%{"userName" => name}, attrs))
    user
  end

  defp group!(inst, name, members \\ [], attrs \\ %{}) do
    {:ok, group} =
      Directory.create_group(inst, Map.merge(%{"displayName" => name}, attrs), members)

    group
  end

  describe "users" do
    test "create assigns the id and the meta, and get reads it back", %{inst: inst} do
      user = user!(inst, "Jane@Corp.com", %{"active" => true, "name" => %{"givenName" => "Jane"}})

      assert byte_size(user["id"]) == 36
      assert %{"resourceType" => "User", "version" => 1, "created" => created} = user["meta"]
      assert user["meta"]["lastModified"] == created
      assert Directory.get_user(inst, user["id"]) == {:ok, user}
      assert Directory.get_user(inst, "nope") == :not_found
    end

    test "a body cannot choose the id", %{inst: inst} do
      {:ok, user} =
        Directory.create_user(inst, %{"id" => "chosen", "userName" => "a@corp.com"})

      refute user["id"] == "chosen"
    end

    test "userName is found case-insensitively, shown as it was given", %{inst: inst} do
      user = user!(inst, "Jane@Corp.com")

      for query <- ["jane@corp.com", "JANE@CORP.COM", "Jane@Corp.com"] do
        assert {:ok, found} = Directory.find_user(inst, :user_name, query)
        assert found["id"] == user["id"]
        assert found["userName"] == "Jane@Corp.com"
      end

      assert Directory.find_user(inst, :user_name, "jane@corp.org") == :not_found
    end

    test "case-insensitive uniqueness", %{inst: inst} do
      user!(inst, "Jane@Corp.com")

      assert Directory.create_user(inst, %{"userName" => "JANE@corp.com"}) == {:error, :conflict}
      assert {:ok, [_only]} = Directory.list_users(inst)
    end

    test "externalId is indexed exactly, not required to be unique", %{inst: inst} do
      a = user!(inst, "a@corp.com", %{"externalId" => "00uA"})
      assert {:ok, %{"id" => id}} = Directory.find_user(inst, :external_id, "00uA")
      assert id == a["id"]
      assert Directory.find_user(inst, :external_id, "00ua") == :not_found

      # a second holder takes the index; deleting the first leaves it alone
      b = user!(inst, "b@corp.com", %{"externalId" => "00uA"})
      assert :ok = Directory.delete_user(inst, a["id"])
      assert {:ok, %{"id" => id}} = Directory.find_user(inst, :external_id, "00uA")
      assert id == b["id"]
    end

    test "list is in id order, the same every time", %{inst: inst} do
      for n <- ~w(c a b d e), do: user!(inst, "#{n}@corp.com")
      assert {:ok, users} = Directory.list_users(inst)
      ids = Enum.map(users, & &1["id"])
      assert length(ids) == 5
      assert ids == Enum.sort(ids)
      assert Directory.list_users(inst) == {:ok, users}
    end

    test "groups and password in a body are never stored", %{inst: inst} do
      user =
        user!(inst, "a@corp.com", %{
          "password" => "hunter2",
          "groups" => [%{"value" => "g"}],
          "members" => []
        })

      assert {:ok, stored} = Directory.get_user(inst, user["id"])
      refute Map.has_key?(stored, "password")
      refute Map.has_key?(stored, "groups")
      refute Map.has_key?(stored, "members")
    end

    test "replace keeps the id and creation time, bumps the version", %{inst: inst} do
      user = user!(inst, "a@corp.com", %{"title" => "SRE"})
      Process.sleep(2)

      assert {:ok, replaced} =
               Directory.replace_user(inst, user["id"], %{
                 "userName" => "a@corp.com",
                 "id" => "evil"
               })

      assert replaced["id"] == user["id"]
      assert replaced["meta"]["created"] == user["meta"]["created"]
      assert replaced["meta"]["version"] == 2
      assert replaced["meta"]["lastModified"] > user["meta"]["lastModified"]
      refute Map.has_key?(replaced, "title")
      assert Directory.get_user(inst, user["id"]) == {:ok, replaced}
    end

    test "a rename moves the name index", %{inst: inst} do
      user = user!(inst, "old@corp.com")
      assert {:ok, _} = Directory.replace_user(inst, user["id"], %{"userName" => "New@Corp.com"})

      assert Directory.find_user(inst, :user_name, "old@corp.com") == :not_found
      assert {:ok, %{"id" => id}} = Directory.find_user(inst, :user_name, "new@corp.com")
      assert id == user["id"]

      # the old name is free again, the new one is taken
      assert {:ok, _} = Directory.create_user(inst, %{"userName" => "OLD@corp.com"})
      assert Directory.create_user(inst, %{"userName" => "new@corp.com"}) == {:error, :conflict}
    end

    test "a rename onto someone else's name is a conflict and changes nothing", %{inst: inst} do
      user!(inst, "taken@corp.com")
      other = user!(inst, "other@corp.com")

      assert Directory.replace_user(inst, other["id"], %{"userName" => "TAKEN@corp.com"}) ==
               {:error, :conflict}

      assert {:ok, ^other} = Directory.get_user(inst, other["id"])
      assert {:ok, %{"id" => id}} = Directory.find_user(inst, :user_name, "other@corp.com")
      assert id == other["id"]
    end

    test "changing only the case of one's own name is fine", %{inst: inst} do
      user = user!(inst, "jane@corp.com")

      assert {:ok, %{"userName" => "JANE@corp.com"}} =
               Directory.replace_user(inst, user["id"], %{"userName" => "JANE@corp.com"})
    end

    test "an expected version that is not current is a conflict", %{inst: inst} do
      user = user!(inst, "a@corp.com")
      assert {:ok, _} = Directory.replace_user(inst, user["id"], %{"userName" => "a@corp.com"}, 1)

      assert Directory.replace_user(inst, user["id"], %{"userName" => "b@corp.com"}, 1) ==
               {:error, :version_conflict}

      assert {:ok, %{"userName" => "a@corp.com"}} = Directory.get_user(inst, user["id"])

      assert {:ok, %{"meta" => %{"version" => 3}}} =
               Directory.replace_user(inst, user["id"], %{"userName" => "a@corp.com"}, 2)
    end

    test "replacing what is not there", %{inst: inst} do
      assert Directory.replace_user(inst, "nope", %{"userName" => "a@corp.com"}) ==
               {:error, :not_found}
    end

    test "update_user retries the version it lost and gives up after five", %{inst: inst} do
      user = user!(inst, "a@corp.com")
      parent = self()

      # a competing writer lands between this update's read and its write
      assert {:ok, updated} =
               Directory.update_user(inst, user["id"], fn current ->
                 if current["meta"]["version"] == 1 do
                   {:ok, _} =
                     Directory.replace_user(inst, user["id"], %{
                       "userName" => "a@corp.com",
                       "title" => "rival"
                     })
                 end

                 send(parent, {:attempt, current["meta"]["version"]})
                 {:ok, Map.put(current, "nickName", "mine")}
               end)

      assert updated["nickName"] == "mine"
      assert updated["title"] == "rival"
      assert_received {:attempt, 1}
      assert_received {:attempt, 2}

      assert {:error, :version_conflict} =
               Directory.update_user(inst, user["id"], fn current ->
                 {:ok, _} =
                   Directory.replace_user(inst, user["id"], %{"userName" => "a@corp.com"})

                 send(parent, :tried)
                 {:ok, current}
               end)

      for _ <- 1..5, do: assert_received(:tried)
      refute_received :tried
    end

    test "update_user passes the function's own error through", %{inst: inst} do
      user = user!(inst, "a@corp.com")

      assert Directory.update_user(inst, user["id"], fn _ -> {:error, :nope} end) ==
               {:error, :nope}

      assert Directory.update_user(inst, "missing", fn u -> {:ok, u} end) == {:error, :not_found}
    end

    test "delete frees the name and the external id", %{inst: inst} do
      user = user!(inst, "a@corp.com", %{"externalId" => "x1"})
      assert :ok = Directory.delete_user(inst, user["id"])

      assert Directory.get_user(inst, user["id"]) == :not_found
      assert Directory.find_user(inst, :user_name, "a@corp.com") == :not_found
      assert Directory.find_user(inst, :external_id, "x1") == :not_found
      assert Directory.delete_user(inst, user["id"]) == {:error, :not_found}
      assert {:ok, _} = Directory.create_user(inst, %{"userName" => "a@corp.com"})
    end
  end

  describe "groups and membership" do
    test "create with members, read them back in both directions", %{inst: inst} do
      jane = user!(inst, "jane@corp.com")
      joe = user!(inst, "joe@corp.com")
      ops = group!(inst, "Ops", [jane["id"], joe["id"]])

      assert ops["meta"]["resourceType"] == "Group"
      refute Map.has_key?(ops, "members")

      assert {:ok, members} = Directory.members(inst, ops["id"])
      assert Enum.sort(members) == Enum.sort([jane["id"], joe["id"]])
      assert {:ok, [%{"displayName" => "Ops"}]} = Directory.groups_for_user(inst, jane["id"])
      assert {:ok, []} = Directory.groups_for_user(inst, "nobody")
    end

    test "displayName is unique case-insensitively and found that way", %{inst: inst} do
      ops = group!(inst, "Ops")
      assert Directory.create_group(inst, %{"displayName" => "OPS"}, []) == {:error, :conflict}
      assert {:ok, %{"id" => id}} = Directory.find_group(inst, :display_name, "oPs")
      assert id == ops["id"]
    end

    test "a group with an unknown member is not created at all", %{inst: inst} do
      jane = user!(inst, "jane@corp.com")

      assert Directory.create_group(inst, %{"displayName" => "Ops"}, [jane["id"], "ghost"]) ==
               {:error, {:unknown_members, ["ghost"]}}

      assert {:ok, []} = Directory.list_groups(inst)
      assert Directory.find_group(inst, :display_name, "ops") == :not_found
      assert {:ok, []} = Directory.groups_for_user(inst, jane["id"])
    end

    test "add_members is idempotent and all-or-nothing", %{inst: inst} do
      [a, b] = for n <- ~w(a b), do: user!(inst, "#{n}@corp.com")
      ops = group!(inst, "Ops")

      assert :ok = Directory.add_members(inst, ops["id"], [a["id"]])
      assert :ok = Directory.add_members(inst, ops["id"], [a["id"], b["id"]])
      assert {:ok, [_, _]} = Directory.members(inst, ops["id"])

      assert Directory.add_members(inst, ops["id"], [a["id"], "ghost", "phantom"]) ==
               {:error, {:unknown_members, ["ghost", "phantom"]}}

      assert Directory.add_members(inst, "no-group", [a["id"]]) == {:error, :not_found}
    end

    test "remove_members ignores non-members and unknown ids", %{inst: inst} do
      [a, b] = for n <- ~w(a b), do: user!(inst, "#{n}@corp.com")
      ops = group!(inst, "Ops", [a["id"], b["id"]])

      assert :ok = Directory.remove_members(inst, ops["id"], [a["id"], "ghost"])
      assert {:ok, [only]} = Directory.members(inst, ops["id"])
      assert only == b["id"]
      assert {:ok, []} = Directory.groups_for_user(inst, a["id"])
      assert Directory.remove_members(inst, "no-group", [a["id"]]) == {:error, :not_found}
    end

    test "replace_group with :keep leaves the members, with a list makes them exactly that", %{
      inst: inst
    } do
      [a, b, c] = for n <- ~w(a b c), do: user!(inst, "#{n}@corp.com")
      ops = group!(inst, "Ops", [a["id"], b["id"]])

      assert {:ok, %{"displayName" => "Platform"}} =
               Directory.replace_group(inst, ops["id"], %{"displayName" => "Platform"}, :keep)

      assert {:ok, kept} = Directory.members(inst, ops["id"])
      assert Enum.sort(kept) == Enum.sort([a["id"], b["id"]])

      assert {:ok, _} =
               Directory.replace_group(inst, ops["id"], %{"displayName" => "Platform"}, [
                 b["id"],
                 c["id"]
               ])

      assert {:ok, now} = Directory.members(inst, ops["id"])
      assert Enum.sort(now) == Enum.sort([b["id"], c["id"]])
      assert {:ok, []} = Directory.groups_for_user(inst, a["id"])
      assert {:ok, [_]} = Directory.groups_for_user(inst, c["id"])

      assert {:ok, _} =
               Directory.replace_group(inst, ops["id"], %{"displayName" => "Platform"}, [])

      assert {:ok, []} = Directory.members(inst, ops["id"])
    end

    test "replace_group rejects unknown members and changes nothing", %{inst: inst} do
      a = user!(inst, "a@corp.com")
      ops = group!(inst, "Ops", [a["id"]])

      assert Directory.replace_group(inst, ops["id"], %{"displayName" => "Renamed"}, ["ghost"]) ==
               {:error, {:unknown_members, ["ghost"]}}

      assert {:ok, %{"displayName" => "Ops"}} = Directory.get_group(inst, ops["id"])
      assert {:ok, [only]} = Directory.members(inst, ops["id"])
      assert only == a["id"]
    end

    test "renaming a group onto another's name is a conflict; version conflicts apply too", %{
      inst: inst
    } do
      group!(inst, "Ops")
      dev = group!(inst, "Dev")

      assert Directory.replace_group(inst, dev["id"], %{"displayName" => "ops"}, :keep) ==
               {:error, :conflict}

      assert Directory.replace_group(inst, dev["id"], %{"displayName" => "Dev2"}, :keep, 9) ==
               {:error, :version_conflict}
    end

    test "deleting a user takes them out of every group", %{inst: inst} do
      [a, b] = for n <- ~w(a b), do: user!(inst, "#{n}@corp.com")
      ops = group!(inst, "Ops", [a["id"], b["id"]])
      dev = group!(inst, "Dev", [a["id"]])

      assert :ok = Directory.delete_user(inst, a["id"])

      assert {:ok, [only]} = Directory.members(inst, ops["id"])
      assert only == b["id"]
      assert {:ok, []} = Directory.members(inst, dev["id"])
    end

    test "deleting a group leaves its members as users with no group", %{inst: inst} do
      a = user!(inst, "a@corp.com")
      ops = group!(inst, "Ops", [a["id"]])

      assert :ok = Directory.delete_group(inst, ops["id"])

      assert Directory.get_group(inst, ops["id"]) == :not_found
      assert Directory.members(inst, ops["id"]) == {:error, :not_found}
      assert {:ok, []} = Directory.groups_for_user(inst, a["id"])
      assert {:ok, _} = Directory.get_user(inst, a["id"])
      assert Directory.find_group(inst, :display_name, "ops") == :not_found
      assert Directory.delete_group(inst, ops["id"]) == {:error, :not_found}
    end

    test "concurrent add_members lose nobody", %{inst: inst} do
      users = for n <- 1..20, do: user!(inst, "u#{n}@corp.com")
      ops = group!(inst, "Ops")

      users
      |> Task.async_stream(&Directory.add_members(inst, ops["id"], [&1["id"]]),
        max_concurrency: 20
      )
      |> Enum.each(fn {:ok, result} -> assert result == :ok end)

      assert {:ok, members} = Directory.members(inst, ops["id"])
      assert Enum.sort(members) == users |> Enum.map(& &1["id"]) |> Enum.sort()
    end

    test "one user's name can differ only by case from a group's: they are separate namespaces",
         %{inst: inst} do
      user!(inst, "ops")
      assert {:ok, _} = Directory.create_group(inst, %{"displayName" => "OPS"}, [])
    end
  end

  describe "when the store is gone" do
    test "reads and writes are :unavailable, never a crash", %{inst: inst} do
      user = user!(inst, "a@corp.com")
      ops = group!(inst, "Ops")

      :ok = stop_supervised({Ankusa.Store, inst})

      assert Directory.get_user(inst, user["id"]) == {:error, :unavailable}
      assert Directory.find_user(inst, :user_name, "a@corp.com") == {:error, :unavailable}
      assert Directory.list_users(inst) == {:error, :unavailable}
      assert Directory.groups_for_user(inst, user["id"]) == {:error, :unavailable}
      assert Directory.create_user(inst, %{"userName" => "b@corp.com"}) == {:error, :unavailable}
      assert Directory.add_members(inst, ops["id"], [user["id"]]) == {:error, :unavailable}
      assert Directory.delete_group(inst, ops["id"]) == {:error, :unavailable}
    end

    test "writes are :unavailable when the directory process is gone", %{inst: inst} do
      :ok = stop_supervised(Persistent)
      assert Directory.create_user(inst, %{"userName" => "b@corp.com"}) == {:error, :unavailable}
    end
  end
end
