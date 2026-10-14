defmodule Ankusa.Scim.RouterTest do
  @moduledoc """
  The SCIM endpoint as an identity provider uses it, through the admin router
  (so the guard's token check, the forward and the audit entry are in the path),
  against the persistent directory.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Admin.Router
  alias Ankusa.TestHelpers.TokenTable

  @token String.duplicate("t", 40)
  @patch_schema "urn:ietf:params:scim:api:messages:2.0:PatchOp"
  @enterprise "urn:ietf:params:scim:schemas:extension:enterprise:2.0:User"

  setup do
    config =
      test_config(
        roles: [:storage],
        admin: %{enabled: true, port: 0},
        auth: %{enabled: true, verifier: {TokenTable, tokens: %{}}},
        scim: %{enabled: true, token: @token},
        audit: %{enabled: true}
      )

    start_supervised!({Ankusa.Instance, config})
    %{config: config}
  end

  defp scim(config, method, path, body \\ nil, opts \\ []) do
    payload = if is_map(body) or is_list(body), do: JSON.encode!(body), else: body || ""
    conn = Plug.Test.conn(method, "/scim/v2" <> path, payload)

    conn =
      case Keyword.get(opts, :token, @token) do
        nil -> conn
        token -> Plug.Conn.put_req_header(conn, "authorization", "Bearer " <> token)
      end

    conn = Plug.Conn.put_req_header(conn, "content-type", "application/scim+json")
    Router.call(conn, Router.init(instance: config.instance))
  end

  defp json(conn), do: JSON.decode!(conn.resp_body)

  defp create_user(config, name, attrs \\ %{}) do
    conn = scim(config, :post, "/Users", Map.merge(%{"userName" => name}, attrs))
    assert conn.status == 201, conn.resp_body
    json(conn)
  end

  defp create_group(config, name, member_ids \\ []) do
    conn =
      scim(config, :post, "/Groups", %{
        "displayName" => name,
        "members" => Enum.map(member_ids, &%{"value" => &1})
      })

    assert conn.status == 201, conn.resp_body
    json(conn)
  end

  defp patch_ops(ops), do: %{"schemas" => [@patch_schema], "Operations" => ops}

  defp member_ids(config, group_id) do
    conn = scim(config, :get, "/Groups/#{group_id}")
    assert conn.status == 200
    json(conn)["members"] |> Enum.map(& &1["value"]) |> Enum.sort()
  end

  describe "users" do
    test "Okta's probe for a user that does not exist is an empty list, not a 404", %{
      config: config
    } do
      conn =
        scim(
          config,
          :get,
          "/Users?" <> URI.encode_query(%{"filter" => ~s(userName eq "Jane@Corp.com")})
        )

      assert conn.status == 200
      assert [content_type] = Plug.Conn.get_resp_header(conn, "content-type")
      assert content_type =~ "application/scim+json"

      assert %{
               "schemas" => ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
               "totalResults" => 0,
               "startIndex" => 1,
               "itemsPerPage" => 0,
               "Resources" => []
             } = json(conn)
    end

    test "create answers 201 with the resource, its location and its version", %{config: config} do
      conn =
        scim(config, :post, "/Users", %{
          "userName" => "Jane@Corp.com",
          "active" => true,
          "emails" => [%{"value" => "j@corp.com", "primary" => true}]
        })

      assert conn.status == 201
      user = json(conn)

      assert user["userName"] == "Jane@Corp.com"
      assert user["schemas"] == ["urn:ietf:params:scim:schemas:core:2.0:User"]
      assert user["groups"] == []
      assert user["meta"]["resourceType"] == "User"
      assert user["meta"]["version"] == ~s(W/"1")
      assert user["meta"]["location"] == "http://www.example.com:80/scim/v2/Users/#{user["id"]}"
      assert Plug.Conn.get_resp_header(conn, "location") == [user["meta"]["location"]]
    end

    test "meta.location uses scim.base_url when it is set" do
      config =
        test_config(
          roles: [:storage],
          admin: %{enabled: true, port: 0},
          auth: %{enabled: true, verifier: {TokenTable, tokens: %{}}},
          scim: %{enabled: true, token: @token, base_url: "https://admin.example.com/"}
        )

      start_supervised!({Ankusa.Instance, config}, id: :other)
      user = create_user(config, "a@corp.com")
      assert user["meta"]["location"] == "https://admin.example.com/scim/v2/Users/#{user["id"]}"
    end

    test "the user is found by userName in any case, with its original casing", %{config: config} do
      created = create_user(config, "Jane@Corp.com")

      for name <- ["jane@corp.com", "JANE@CORP.COM"] do
        conn =
          scim(
            config,
            :get,
            "/Users?" <> URI.encode_query(%{"filter" => ~s(userName eq "#{name}")})
          )

        assert %{"totalResults" => 1, "Resources" => [found]} = json(conn)
        assert found["id"] == created["id"]
        assert found["userName"] == "Jane@Corp.com"
      end
    end

    test "externalId and id filters", %{config: config} do
      created = create_user(config, "a@corp.com", %{"externalId" => "00u1"})

      for filter <- [~s(externalId eq "00u1"), ~s(id eq "#{created["id"]}")] do
        conn = scim(config, :get, "/Users?" <> URI.encode_query(%{"filter" => filter}))
        assert %{"totalResults" => 1} = json(conn)
      end

      conn = scim(config, :get, "/Users?" <> URI.encode_query(%{"filter" => ~s(id eq "nope")}))
      assert %{"totalResults" => 0} = json(conn)
    end

    test "a duplicate userName in another case is 409 uniqueness", %{config: config} do
      create_user(config, "Jane@Corp.com")

      conn = scim(config, :post, "/Users", %{"userName" => "JANE@corp.com"})
      assert conn.status == 409

      assert %{
               "schemas" => ["urn:ietf:params:scim:api:messages:2.0:Error"],
               "status" => "409",
               "scimType" => "uniqueness"
             } = json(conn)
    end

    test "the body is sanitized: no chosen id, no password, no stranger attributes", %{
      config: config
    } do
      conn =
        scim(config, :post, "/Users", %{
          "id" => "chosen",
          "userName" => "a@corp.com",
          "password" => "hunter2",
          "groups" => [%{"value" => "g"}],
          "x-vendor" => "?",
          "meta" => %{"version" => 99}
        })

      user = json(conn)
      refute user["id"] == "chosen"
      refute Map.has_key?(user, "password")
      refute Map.has_key?(user, "x-vendor")
      assert user["groups"] == []
      assert user["meta"]["version"] == ~s(W/"1")
    end

    test "a body that is not a valid user", %{config: config} do
      for body <- [
            %{},
            %{"userName" => ""},
            %{"userName" => 42},
            %{"userName" => "a", "active" => "maybe"},
            %{"userName" => String.duplicate("x", 257)}
          ] do
        conn = scim(config, :post, "/Users", body)
        assert conn.status == 400, inspect(body)
        assert %{"scimType" => "invalidValue", "status" => "400"} = json(conn)
      end

      conn = scim(config, :post, "/Users", "not json")
      assert conn.status == 400
      assert %{"scimType" => "invalidSyntax"} = json(conn)

      conn = scim(config, :post, "/Users", ~s(["a"]))
      assert conn.status == 400
    end

    test "active arrives as a string from some providers", %{config: config} do
      user = create_user(config, "a@corp.com", %{"active" => "False"})
      assert user["active"] == false
    end

    test "a body over 4 MiB is refused", %{config: config} do
      conn = scim(config, :post, "/Users", ~s({"userName":"#{String.duplicate("a", 4_194_304)}"}))
      assert conn.status == 413
      assert %{"status" => "413"} = json(conn)
    end

    test "get, put and delete", %{config: config} do
      created = create_user(config, "a@corp.com", %{"title" => "SRE"})
      id = created["id"]

      assert %{"userName" => "a@corp.com", "title" => "SRE"} =
               json(scim(config, :get, "/Users/#{id}"))

      conn = scim(config, :put, "/Users/#{id}", %{"userName" => "b@corp.com", "active" => false})
      assert conn.status == 200
      replaced = json(conn)
      assert replaced["userName"] == "b@corp.com"
      assert replaced["active"] == false
      refute Map.has_key?(replaced, "title")
      assert replaced["meta"]["version"] == ~s(W/"2")

      assert scim(config, :delete, "/Users/#{id}").status == 204
      assert scim(config, :get, "/Users/#{id}").status == 404
      assert scim(config, :delete, "/Users/#{id}").status == 404
      assert scim(config, :put, "/Users/#{id}", %{"userName" => "c@corp.com"}).status == 404
    end

    test "put onto another user's name is a conflict", %{config: config} do
      create_user(config, "a@corp.com")
      b = create_user(config, "b@corp.com")

      conn = scim(config, :put, "/Users/#{b["id"]}", %{"userName" => "A@corp.com"})
      assert conn.status == 409
      assert %{"scimType" => "uniqueness"} = json(conn)
    end

    test "Entra's PATCH of active to \"False\"", %{config: config} do
      user = create_user(config, "a@corp.com")

      conn =
        scim(
          config,
          :patch,
          "/Users/#{user["id"]}",
          patch_ops([%{"op" => "Replace", "path" => "active", "value" => "False"}])
        )

      assert conn.status == 200
      assert json(conn)["active"] == false
      assert json(scim(config, :get, "/Users/#{user["id"]}"))["active"] == false
    end

    test "PATCH of nested, filtered and extension attributes", %{config: config} do
      user = create_user(config, "a@corp.com")

      conn =
        scim(
          config,
          :patch,
          "/Users/#{user["id"]}",
          patch_ops([
            %{"op" => "add", "path" => "name.givenName", "value" => "Jane"},
            %{"op" => "add", "path" => ~s(emails[type eq "work"].value), "value" => "j@corp.com"},
            %{"op" => "add", "path" => @enterprise <> ":department", "value" => "Ops"}
          ])
        )

      assert conn.status == 200

      assert %{
               "name" => %{"givenName" => "Jane"},
               "emails" => [%{"type" => "work", "value" => "j@corp.com"}],
               "schemas" => schemas
             } = patched = json(conn)

      assert patched[@enterprise] == %{"department" => "Ops"}
      assert @enterprise in schemas
    end

    test "a PATCH that fails changes nothing, and says why", %{config: config} do
      user = create_user(config, "a@corp.com")

      for {ops, type} <- [
            {[%{"op" => "remove"}], "noTarget"},
            {[%{"op" => "replace", "path" => "id", "value" => "x"}], "mutability"},
            {[%{"op" => "replace", "path" => "emails[", "value" => "x"}], "invalidPath"},
            {[%{"op" => "replace", "path" => "userName", "value" => ""}], "invalidValue"}
          ] do
        conn = scim(config, :patch, "/Users/#{user["id"]}", patch_ops(ops))
        assert conn.status == 400
        assert %{"scimType" => ^type} = json(conn)
      end

      conn = scim(config, :patch, "/Users/#{user["id"]}", %{"Operations" => []})
      assert %{"scimType" => "invalidSyntax"} = json(conn)

      assert %{"userName" => "a@corp.com", "meta" => %{"version" => ~s(W/"1")}} =
               json(scim(config, :get, "/Users/#{user["id"]}"))
    end

    test "a PATCH onto another user's name is a conflict, onto a missing id a 404", %{
      config: config
    } do
      create_user(config, "a@corp.com")
      b = create_user(config, "b@corp.com")

      conn =
        scim(
          config,
          :patch,
          "/Users/#{b["id"]}",
          patch_ops([%{"op" => "replace", "path" => "userName", "value" => "A@corp.com"}])
        )

      assert conn.status == 409

      conn =
        scim(
          config,
          :patch,
          "/Users/nope",
          patch_ops([%{"op" => "replace", "path" => "title", "value" => "x"}])
        )

      assert conn.status == 404
      assert %{"status" => "404"} = json(conn)
    end

    test "unsupported filters are 400 invalidFilter", %{config: config} do
      for filter <- [
            ~s(name co "x"),
            ~s(userName sw "x"),
            ~s(userName eq "a" and active eq "true")
          ] do
        conn = scim(config, :get, "/Users?" <> URI.encode_query(%{"filter" => filter}))
        assert conn.status == 400, filter
        assert %{"scimType" => "invalidFilter"} = json(conn)
      end
    end

    test "paging", %{config: config} do
      for n <- 1..5, do: create_user(config, "u#{n}@corp.com")

      # the order is by id, whatever it is; paging slices that order
      all = json(scim(config, :get, "/Users"))["Resources"]
      assert length(all) == 5
      assert all == Enum.sort_by(all, & &1["id"])

      assert %{
               "totalResults" => 5,
               "startIndex" => 1,
               "itemsPerPage" => 2,
               "Resources" => first_two
             } = json(scim(config, :get, "/Users?count=2"))

      assert first_two == Enum.take(all, 2)

      assert %{
               "totalResults" => 5,
               "startIndex" => 4,
               "itemsPerPage" => 2,
               "Resources" => last_two
             } = json(scim(config, :get, "/Users?startIndex=4&count=10"))

      assert last_two == Enum.drop(all, 3)

      assert %{"itemsPerPage" => 0, "Resources" => [], "totalResults" => 5} =
               json(scim(config, :get, "/Users?count=0"))

      assert %{"startIndex" => 1} = json(scim(config, :get, "/Users?startIndex=-3"))
      assert %{"itemsPerPage" => 0} = json(scim(config, :get, "/Users?startIndex=50"))

      conn = scim(config, :get, "/Users?count=lots")
      assert conn.status == 400
      assert %{"scimType" => "invalidValue"} = json(conn)
    end
  end

  describe "groups" do
    test "create with members, and the user shows the group", %{config: config} do
      jane = create_user(config, "Jane@Corp.com")

      conn =
        scim(config, :post, "/Groups", %{
          "displayName" => "Ops",
          "members" => [%{"value" => jane["id"]}]
        })

      assert conn.status == 201
      group = json(conn)
      assert group["schemas"] == ["urn:ietf:params:scim:schemas:core:2.0:Group"]
      assert group["members"] == [%{"value" => jane["id"], "display" => "Jane@Corp.com"}]
      assert group["meta"]["location"] =~ "/scim/v2/Groups/#{group["id"]}"

      assert %{"groups" => [%{"value" => gid, "display" => "Ops"}]} =
               json(scim(config, :get, "/Users/#{jane["id"]}"))

      assert gid == group["id"]
    end

    test "a duplicate displayName is 409; an unknown member is 400 and creates nothing", %{
      config: config
    } do
      create_group(config, "Ops")

      conn = scim(config, :post, "/Groups", %{"displayName" => "OPS"})
      assert conn.status == 409
      assert %{"scimType" => "uniqueness"} = json(conn)

      conn =
        scim(config, :post, "/Groups", %{
          "displayName" => "Dev",
          "members" => [%{"value" => "ghost"}]
        })

      assert conn.status == 400
      assert %{"scimType" => "invalidValue", "detail" => detail} = json(conn)
      assert detail =~ "ghost"

      assert %{"totalResults" => 0} =
               json(
                 scim(
                   config,
                   :get,
                   "/Groups?" <> URI.encode_query(%{"filter" => ~s(displayName eq "dev")})
                 )
               )
    end

    test "lookup by displayName, excludedAttributes=members", %{config: config} do
      jane = create_user(config, "jane@corp.com")
      group = create_group(config, "Ops", [jane["id"]])

      conn =
        scim(
          config,
          :get,
          "/Groups?" <>
            URI.encode_query(%{
              "filter" => ~s(displayName eq "OPS"),
              "excludedAttributes" => "members"
            })
        )

      assert %{"totalResults" => 1, "Resources" => [found]} = json(conn)
      assert found["id"] == group["id"]
      refute Map.has_key?(found, "members")

      assert Map.has_key?(json(scim(config, :get, "/Groups/#{group["id"]}")), "members")

      refute Map.has_key?(
               json(scim(config, :get, "/Groups/#{group["id"]}?excludedAttributes=members")),
               "members"
             )
    end

    test "20 concurrent PATCHes each adding a member lose none", %{config: config} do
      users = for n <- 1..20, do: create_user(config, "u#{n}@corp.com")
      group = create_group(config, "Ops")

      users
      |> Task.async_stream(
        fn user ->
          scim(
            config,
            :patch,
            "/Groups/#{group["id"]}",
            patch_ops([
              %{"op" => "add", "path" => "members", "value" => [%{"value" => user["id"]}]}
            ])
          )
        end,
        max_concurrency: 20
      )
      |> Enum.each(fn {:ok, conn} -> assert conn.status == 200, conn.resp_body end)

      assert member_ids(config, group["id"]) == users |> Enum.map(& &1["id"]) |> Enum.sort()
    end

    test "removing members: by filter, by value list, and all", %{config: config} do
      [a, b, c, d] = for n <- ~w(a b c d), do: create_user(config, "#{n}@corp.com")
      group = create_group(config, "Ops", [a["id"], b["id"], c["id"], d["id"]])
      path = "/Groups/#{group["id"]}"

      conn =
        scim(
          config,
          :patch,
          path,
          patch_ops([%{"op" => "remove", "path" => ~s(members[value eq "#{a["id"]}"])}])
        )

      assert conn.status == 200
      assert member_ids(config, group["id"]) == Enum.sort([b["id"], c["id"], d["id"]])

      # Entra's form
      scim(
        config,
        :patch,
        path,
        patch_ops([
          %{
            "op" => "Remove",
            "path" => "members",
            "value" => [%{"value" => b["id"]}, %{"value" => c["id"]}]
          }
        ])
      )

      assert member_ids(config, group["id"]) == [d["id"]]

      scim(config, :patch, path, patch_ops([%{"op" => "remove", "path" => "members"}]))
      assert member_ids(config, group["id"]) == []
    end

    test "a PATCH renaming the group and changing its members at once", %{config: config} do
      [a, b] = for n <- ~w(a b), do: create_user(config, "#{n}@corp.com")
      group = create_group(config, "Ops", [a["id"]])

      conn =
        scim(
          config,
          :patch,
          "/Groups/#{group["id"]}",
          patch_ops([
            %{"op" => "replace", "path" => "displayName", "value" => "Platform"},
            %{"op" => "replace", "path" => "members", "value" => [%{"value" => b["id"]}]}
          ])
        )

      assert conn.status == 200
      assert %{"displayName" => "Platform", "members" => [%{"value" => bid}]} = json(conn)
      assert bid == b["id"]
    end

    test "PATCH adding an unknown member is 400 and adds nothing", %{config: config} do
      a = create_user(config, "a@corp.com")
      group = create_group(config, "Ops")

      conn =
        scim(
          config,
          :patch,
          "/Groups/#{group["id"]}",
          patch_ops([
            %{
              "op" => "add",
              "path" => "members",
              "value" => [%{"value" => a["id"]}, %{"value" => "ghost"}]
            }
          ])
        )

      assert conn.status == 400
      assert %{"scimType" => "invalidValue"} = json(conn)
      assert member_ids(config, group["id"]) == []
    end

    test "PUT without members keeps them, with members replaces them", %{config: config} do
      [a, b] = for n <- ~w(a b), do: create_user(config, "#{n}@corp.com")
      group = create_group(config, "Ops", [a["id"]])
      path = "/Groups/#{group["id"]}"

      assert scim(config, :put, path, %{"displayName" => "Ops2"}).status == 200
      assert member_ids(config, group["id"]) == [a["id"]]

      conn =
        scim(config, :put, path, %{"displayName" => "Ops2", "members" => [%{"value" => b["id"]}]})

      assert conn.status == 200
      assert member_ids(config, group["id"]) == [b["id"]]

      scim(config, :put, path, %{"displayName" => "Ops2", "members" => []})
      assert member_ids(config, group["id"]) == []
    end

    test "deleting a user removes them from the group; deleting a group removes the membership",
         %{config: config} do
      [a, b] = for n <- ~w(a b), do: create_user(config, "#{n}@corp.com")
      group = create_group(config, "Ops", [a["id"], b["id"]])

      assert scim(config, :delete, "/Users/#{a["id"]}").status == 204
      assert member_ids(config, group["id"]) == [b["id"]]

      assert scim(config, :delete, "/Groups/#{group["id"]}").status == 204
      assert scim(config, :get, "/Groups/#{group["id"]}").status == 404
      assert json(scim(config, :get, "/Users/#{b["id"]}"))["groups"] == []
    end
  end

  describe "discovery" do
    test "the service provider config says what is and is not supported", %{config: config} do
      conn = scim(config, :get, "/ServiceProviderConfig")
      assert conn.status == 200

      assert %{
               "patch" => %{"supported" => true},
               "bulk" => %{"supported" => false},
               "filter" => %{"supported" => true, "maxResults" => 1000},
               "changePassword" => %{"supported" => false},
               "sort" => %{"supported" => false},
               "etag" => %{"supported" => false},
               "authenticationSchemes" => [%{"type" => "oauthbearertoken", "primary" => true}]
             } = json(conn)
    end

    test "resource types and schemas", %{config: config} do
      assert %{"totalResults" => 2, "Resources" => types} =
               json(scim(config, :get, "/ResourceTypes"))

      assert types |> Enum.map(& &1["id"]) |> Enum.sort() == ["Group", "User"]
      assert %{"endpoint" => "/Users"} = json(scim(config, :get, "/ResourceTypes/User"))
      assert scim(config, :get, "/ResourceTypes/Nope").status == 404

      assert %{"totalResults" => 3, "Resources" => schemas} = json(scim(config, :get, "/Schemas"))
      urns = Enum.map(schemas, & &1["id"])
      assert "urn:ietf:params:scim:schemas:core:2.0:User" in urns
      assert "urn:ietf:params:scim:schemas:core:2.0:Group" in urns

      conn = scim(config, :get, "/Schemas/urn:ietf:params:scim:schemas:core:2.0:User")
      assert conn.status == 200
      assert json(conn)["attributes"] |> Enum.any?(&(&1["name"] == "userName" and &1["required"]))
      assert scim(config, :get, "/Schemas/urn:nope").status == 404
    end
  end

  describe "audit" do
    test "every provisioning change is recorded with the resource it touched", %{config: config} do
      user = create_user(config, "a@corp.com")
      group = create_group(config, "Ops", [user["id"]])

      scim(
        config,
        :patch,
        "/Users/#{user["id"]}",
        patch_ops([%{"op" => "replace", "path" => "title", "value" => "SRE"}])
      )

      scim(config, :delete, "/Groups/#{group["id"]}")
      scim(config, :get, "/Users")

      {:ok, %{entries: entries}} = Ankusa.Audit.list(config.instance, %{})

      assert entries |> Enum.map(&{&1["action"], &1["target"], &1["outcome"]}) |> Enum.reverse() ==
               [
                 {"scim.user.create", user["id"], "success"},
                 {"scim.group.create", group["id"], "success"},
                 {"scim.user.update", user["id"], "success"},
                 {"scim.group.delete", group["id"], "success"}
               ]

      assert Enum.all?(entries, &(&1["actor"]["type"] == "scim"))
    end
  end

  describe "when the directory is down" do
    test "the client is told to retry", %{config: config} do
      :ok =
        Supervisor.terminate_child(
          Ankusa.via(config.instance, :instance),
          {Ankusa.Store, config.instance}
        )

      conn = scim(config, :get, "/Users")
      assert conn.status == 503
      assert Plug.Conn.get_resp_header(conn, "retry-after") == ["1"]
      assert %{"status" => "503"} = json(conn)
    end
  end
end
