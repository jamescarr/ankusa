defmodule Ankusa.Directory.Store.RedisTest do
  @moduledoc """
  The Redis directory against a real Redis, through the `Ankusa.Directory`
  facade (so ids are assigned the way production assigns them). Two instances
  in one VM sharing a namespace stand in for two admin nodes.

  Needs the package's compose Redis (`docker compose up -d --wait`);
  `REDIS_URL` overrides the default `redis://localhost:6399`.
  """

  use ExUnit.Case, async: false

  alias Ankusa.Directory
  alias Ankusa.Directory.Store.Redis

  @url System.get_env("REDIS_URL", "redis://localhost:6399")

  # A node of the directory: its own instance, a namespace it may share.
  defp node!(namespace, url \\ @url) do
    instance = :"redis_dir#{System.unique_integer([:positive])}"

    config =
      Ankusa.Config.new(
        instance: instance,
        data_dir: Path.join(System.tmp_dir!(), "ankusa_#{instance}"),
        scim: %{directory: {Redis, url: url, namespace: namespace}}
      )

    Ankusa.put_config(config)
    start_supervised!({Redis, instance: instance}, id: instance)
    instance
  end

  setup do
    namespace =
      "ankusa:directory:test:#{System.unique_integer([:positive])}:#{System.os_time(:microsecond)}"

    %{inst: node!(namespace), namespace: namespace}
  end

  defp user!(inst, name, attrs \\ %{}) do
    {:ok, user} = Directory.create_user(inst, Map.merge(%{"userName" => name}, attrs))
    user
  end

  defp group!(inst, name, members \\ []) do
    {:ok, group} = Directory.create_group(inst, %{"displayName" => name}, members)
    group
  end

  describe "users" do
    test "create assigns the id and meta; get and find read it back", %{inst: inst} do
      user = user!(inst, "Jane@Corp.com", %{"active" => true, "emails" => []})

      assert byte_size(user["id"]) == 36
      assert %{"resourceType" => "User", "version" => 1} = user["meta"]
      assert Directory.get_user(inst, user["id"]) == {:ok, user}
      assert Directory.get_user(inst, "nope") == :not_found

      # an empty list survives the round trip (it is not a Lua table anywhere)
      assert {:ok, %{"emails" => []}} = Directory.get_user(inst, user["id"])

      for query <- ["jane@corp.com", "JANE@CORP.COM"] do
        assert {:ok, %{"id" => id, "userName" => "Jane@Corp.com"}} =
                 Directory.find_user(inst, :user_name, query)

        assert id == user["id"]
      end

      assert Directory.find_user(inst, :user_name, "other@corp.com") == :not_found
    end

    test "userName is unique case-insensitively", %{inst: inst} do
      user!(inst, "Jane@Corp.com")
      assert Directory.create_user(inst, %{"userName" => "JANE@corp.com"}) == {:error, :conflict}
      assert {:ok, [_only]} = Directory.list_users(inst)
    end

    test "externalId: indexed exactly, the newest holder owns it", %{inst: inst} do
      a = user!(inst, "a@corp.com", %{"externalId" => "00uA"})
      assert {:ok, %{"id" => id}} = Directory.find_user(inst, :external_id, "00uA")
      assert id == a["id"]
      assert Directory.find_user(inst, :external_id, "00ua") == :not_found

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

    test "list reads more than one chunk", %{inst: inst} do
      for n <- 1..520,
          do: user!(inst, "u#{String.pad_leading(Integer.to_string(n), 4, "0")}@corp.com")

      assert {:ok, users} = Directory.list_users(inst)
      assert length(users) == 520
    end

    test "groups, members and password in a body are not stored", %{inst: inst} do
      user = user!(inst, "a@corp.com", %{"password" => "x", "groups" => [1], "members" => []})
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

      assert Directory.replace_user(inst, "nope", %{"userName" => "z@corp.com"}) ==
               {:error, :not_found}
    end

    test "a rename moves the name index", %{inst: inst} do
      user = user!(inst, "old@corp.com")
      assert {:ok, _} = Directory.replace_user(inst, user["id"], %{"userName" => "New@Corp.com"})

      assert Directory.find_user(inst, :user_name, "old@corp.com") == :not_found
      assert {:ok, %{"id" => id}} = Directory.find_user(inst, :user_name, "new@corp.com")
      assert id == user["id"]
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

    test "update_user retries a lost race", %{inst: inst} do
      user = user!(inst, "a@corp.com")
      parent = self()

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
    end

    test "delete frees the name and the external id", %{inst: inst} do
      user = user!(inst, "a@corp.com", %{"externalId" => "x1"})
      assert :ok = Directory.delete_user(inst, user["id"])

      assert Directory.get_user(inst, user["id"]) == :not_found
      assert Directory.find_user(inst, :user_name, "a@corp.com") == :not_found
      assert Directory.find_user(inst, :external_id, "x1") == :not_found
      assert {:ok, []} = Directory.list_users(inst)
      assert Directory.delete_user(inst, user["id"]) == {:error, :not_found}
      assert {:ok, _} = Directory.create_user(inst, %{"userName" => "a@corp.com"})
    end
  end

  describe "groups and membership" do
    test "create with members; read them in both directions", %{inst: inst} do
      jane = user!(inst, "jane@corp.com")
      joe = user!(inst, "joe@corp.com")
      ops = group!(inst, "Ops", [jane["id"], joe["id"]])

      assert ops["meta"]["resourceType"] == "Group"
      refute Map.has_key?(ops, "members")
      assert {:ok, members} = Directory.members(inst, ops["id"])
      assert members == Enum.sort([jane["id"], joe["id"]])
      assert {:ok, [%{"displayName" => "Ops"}]} = Directory.groups_for_user(inst, jane["id"])
      assert {:ok, []} = Directory.groups_for_user(inst, "nobody")
      assert Directory.members(inst, "no-group") == {:error, :not_found}
    end

    test "displayName is unique case-insensitively", %{inst: inst} do
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

      assert Directory.add_members(inst, ops["id"], [a["id"], "ghost"]) ==
               {:error, {:unknown_members, ["ghost"]}}

      assert Directory.add_members(inst, "no-group", [a["id"]]) == {:error, :not_found}
    end

    test "remove_members ignores non-members", %{inst: inst} do
      [a, b] = for n <- ~w(a b), do: user!(inst, "#{n}@corp.com")
      ops = group!(inst, "Ops", [a["id"], b["id"]])

      assert :ok = Directory.remove_members(inst, ops["id"], [a["id"], "ghost"])
      assert {:ok, [only]} = Directory.members(inst, ops["id"])
      assert only == b["id"]
      assert {:ok, []} = Directory.groups_for_user(inst, a["id"])
      assert Directory.remove_members(inst, "no-group", [a["id"]]) == {:error, :not_found}
    end

    test "replace_group: :keep leaves members, a list makes them exactly that", %{inst: inst} do
      [a, b, c] = for n <- ~w(a b c), do: user!(inst, "#{n}@corp.com")
      ops = group!(inst, "Ops", [a["id"], b["id"]])

      assert {:ok, %{"displayName" => "Platform"}} =
               Directory.replace_group(inst, ops["id"], %{"displayName" => "Platform"}, :keep)

      assert {:ok, kept} = Directory.members(inst, ops["id"])
      assert kept == Enum.sort([a["id"], b["id"]])

      assert {:ok, _} =
               Directory.replace_group(inst, ops["id"], %{"displayName" => "Platform"}, [
                 b["id"],
                 c["id"]
               ])

      assert {:ok, now} = Directory.members(inst, ops["id"])
      assert now == Enum.sort([b["id"], c["id"]])
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

    test "group renames and version checks", %{inst: inst} do
      group!(inst, "Ops")
      dev = group!(inst, "Dev")

      assert Directory.replace_group(inst, dev["id"], %{"displayName" => "ops"}, :keep) ==
               {:error, :conflict}

      assert Directory.replace_group(inst, dev["id"], %{"displayName" => "Dev2"}, :keep, 9) ==
               {:error, :version_conflict}

      assert {:ok, _} =
               Directory.replace_group(inst, dev["id"], %{"displayName" => "Dev2"}, :keep, 1)

      assert Directory.find_group(inst, :display_name, "dev") == :not_found
      assert {:ok, _} = Directory.find_group(inst, :display_name, "DEV2")
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

    test "20 concurrent add_members lose nobody", %{inst: inst} do
      users = for n <- 1..20, do: user!(inst, "u#{n}@corp.com")
      ops = group!(inst, "Ops")

      users
      |> Task.async_stream(&Directory.add_members(inst, ops["id"], [&1["id"]]),
        max_concurrency: 20
      )
      |> Enum.each(fn {:ok, result} -> assert result == :ok end)

      assert {:ok, members} = Directory.members(inst, ops["id"])
      assert members == users |> Enum.map(& &1["id"]) |> Enum.sort()
    end

    test "concurrent creators of one userName: exactly one wins", %{inst: inst} do
      results =
        1..10
        |> Task.async_stream(
          fn n ->
            Directory.create_user(inst, %{"userName" => "Same@Corp.com", "title" => "#{n}"})
          end,
          max_concurrency: 10
        )
        |> Enum.map(fn {:ok, result} -> result end)

      assert Enum.count(results, &match?({:ok, _}, &1)) == 1
      assert Enum.count(results, &(&1 == {:error, :conflict})) == 9
      assert {:ok, [_only]} = Directory.list_users(inst)
    end
  end

  describe "two nodes, one namespace" do
    test "a change on one node is the answer on the other, at once", %{
      inst: a,
      namespace: namespace
    } do
      b = node!(namespace)

      user = user!(a, "jane@corp.com")

      assert {:ok, %{"active" => true}} =
               Directory.replace_user(a, user["id"], %{
                 "userName" => "jane@corp.com",
                 "active" => true
               })

      assert {:ok, %{"id" => id}} = Directory.find_user(b, :user_name, "JANE@corp.com")
      assert id == user["id"]

      assert {:ok, _} =
               Directory.replace_user(b, user["id"], %{
                 "userName" => "jane@corp.com",
                 "active" => false
               })

      assert {:ok, %{"active" => false}} = Directory.get_user(a, user["id"])

      assert :ok = Directory.delete_user(b, user["id"])
      assert Directory.get_user(a, user["id"]) == :not_found
    end

    test "different namespaces share nothing", %{inst: a} do
      other = node!("ankusa:directory:test:other:#{System.unique_integer([:positive])}")
      user!(a, "jane@corp.com")
      assert {:ok, []} = Directory.list_users(other)
      assert {:ok, _} = Directory.create_user(other, %{"userName" => "jane@corp.com"})
    end
  end

  describe "when Redis is unreachable" do
    test "every call is :unavailable, never a crash" do
      # nothing listens on this port
      inst = node!("ankusa:directory:test:dead", "redis://localhost:6390")

      assert Directory.get_user(inst, "x") == {:error, :unavailable}
      assert Directory.find_user(inst, :user_name, "a@corp.com") == {:error, :unavailable}
      assert Directory.list_users(inst) == {:error, :unavailable}
      assert Directory.members(inst, "g") == {:error, :unavailable}
      assert Directory.groups_for_user(inst, "u") == {:error, :unavailable}
      assert Directory.create_user(inst, %{"userName" => "a@corp.com"}) == {:error, :unavailable}
      assert Directory.add_members(inst, "g", ["u"]) == {:error, :unavailable}
      assert Directory.delete_user(inst, "u") == {:error, :unavailable}
      assert Directory.update_user(inst, "u", fn u -> {:ok, u} end) == {:error, :unavailable}
    end
  end

  test "the password in the URL is read back on connect, never held in a child spec" do
    # the compose Redis has no password, so this node never gets a connection;
    # the question is only where the password lives
    inst = node!("ankusa:directory:test:pw", "redis://:s3cret-pw@localhost:6399")
    sup = Ankusa.via(inst, :directory_sup)

    assert Redis.password(inst) == "s3cret-pw"
    assert {:ok, spec} = :supervisor.get_childspec(sup, Redix)
    refute inspect(spec) =~ "s3cret-pw"

    assert inspect(spec) =~
             "password: {Ankusa.Directory.Store.Redis, :password, [#{inspect(inst)}]}"
  end
end
