defmodule Ankusa.Gateway.SharingRouterTest do
  @moduledoc """
  The Sharing API and share links through `Ankusa.Gateway.Router`, over a
  LocalFS space: ownership, grants, links, revocation, audit records, and the
  holes an earlier draft of the design had.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Admin.Auth.Token
  alias Ankusa.BlobStore.LocalFS
  alias Ankusa.Gateway.{ACL, Router}

  @moduletag :capture_log
  @moduletag :tmp_dir

  @keys ["0123456789abcdef0123", "previous-key-0123456"]

  # LocalFS that refuses to write audit records while armed.
  defmodule AuditDownStore do
    @moduledoc false
    defdelegate head_object(i, k, o), to: LocalFS
    defdelegate get_object(i, k, o), to: LocalFS
    defdelegate delete_object(i, k, o), to: LocalFS
    defdelegate list_page(i, p, o), to: LocalFS

    def put_object(instance, key, data, opts) do
      if String.contains?(key, ".ankusa/audit/") and
           :persistent_term.get({__MODULE__, opts[:root]}, false),
         do: {:error, {:unavailable, :down}},
         else: LocalFS.put_object(instance, key, data, opts)
    end
  end

  setup %{tmp_dir: dir} = context do
    config = test_config(roles: [])
    start_supervised!({Ankusa.Instance, config})
    inst = config.instance
    root = Path.join(dir, "store")

    tokens =
      for {id, groups, scopes} <- [
            {"alice", ["eng"], []},
            {"bob", ["sales"], []},
            {"erin", ["eng"], []},
            {"rita", ["readers"], []}
          ],
          do: %{id: id, sha256: Token.hash(id), scopes: scopes, groups: groups}

    team_actions = Map.get(context, :team_actions, [:read, :write, :delete, :list, :share])

    rules = [
      %{
        effect: :allow,
        actions: team_actions,
        keys: ["teams/{team}/**"],
        when: %{groups_any: ["{{ vars.team }}"]}
      },
      %{
        effect: :allow,
        actions: [:read, :share],
        keys: ["shared/**"],
        when: %{groups_any: ["readers"]}
      }
    ]

    chain = if Map.get(context, :anonymous), do: [], else: [%{type: :token, tokens: tokens}]

    gateway =
      Ankusa.Gateway.Config.new(
        instance: inst,
        port: 0,
        auth: %{authenticators: chain},
        authz: %{
          authorizers: %{
            "team" => {Ankusa.Gateway.Authz.Rules, %{rules: rules}},
            "allow_all" =>
              {Ankusa.Gateway.Authz.Rules, %{rules: [%{effect: :allow, actions: ["*"]}]}}
          },
          policies: %{
            "docs" => %{mode: :first, steps: Map.get(context, :steps, ["grants", "team"])},
            "grants_only" => %{mode: :first, steps: ["grants"]}
          }
        },
        spaces: %{
          "docs" => %{store: {AuditDownStore, root: root}, prefix: "docs/", policy: "docs"}
        },
        sharing: %{
          enabled: true,
          signing_keys: @keys,
          acl_cache_ttl_ms: Map.get(context, :acl_cache_ttl_ms, 0)
        }
      )

    start_supervised!({Ankusa.Gateway, gateway})
    on_exit(fn -> :persistent_term.erase({AuditDownStore, root}) end)
    %{inst: inst, root: root}
  end

  defp call(inst, method, path, opts \\ []) do
    body = Keyword.get(opts, :body)
    body = if is_map(body), do: JSON.encode!(body), else: body
    conn = Plug.Test.conn(method, path, body)

    headers =
      Keyword.get(opts, :headers, []) ++
        if(body, do: [{"content-length", Integer.to_string(byte_size(body))}], else: [])

    conn = Enum.reduce(headers, conn, fn {k, v}, c -> Plug.Conn.put_req_header(c, k, v) end)

    conn =
      case Keyword.get(opts, :as, "alice") do
        nil -> conn
        token -> Plug.Conn.put_req_header(conn, "authorization", "Bearer " <> token)
      end

    Router.call(conn, Router.init(instance: inst))
  end

  defp put(inst, key, body, as \\ "alice"),
    do: call(inst, :put, "/v1/objects/docs/" <> key, body: body, as: as)

  defp grant(inst, resource, subject, actions, as \\ "alice") do
    call(inst, :post, "/v1/spaces/docs/grants",
      as: as,
      body: %{resource: resource, subject: subject, actions: actions, expires_in_s: nil}
    )
  end

  defp link(inst, key, expires_in_s \\ 60, as \\ "alice"),
    do:
      call(inst, :post, "/v1/spaces/docs/links",
        as: as,
        body: %{key: key, expires_in_s: expires_in_s}
      )

  defp json(conn), do: JSON.decode!(conn.resp_body)

  defp audit_records(root) do
    Path.wildcard(Path.join(root, "docs/.ankusa/audit/v1/dt=*/*.json"))
    |> Enum.map(&(&1 |> File.read!() |> JSON.decode!()))
    |> Enum.sort_by(& &1["at"])
  end

  describe "ownership and grants" do
    test "the uploader owns a new object; a grant lets another principal read it", %{
      inst: inst,
      root: root
    } do
      assert put(inst, "teams/eng/b.txt", "hello").status == 200
      assert call(inst, :get, "/v1/objects/docs/teams/eng/b.txt", as: "bob").status == 403

      conn = grant(inst, %{key: "teams/eng/b.txt"}, "user:bob", ["read"])
      assert conn.status == 201

      %{"id" => id, "subject" => "user:bob", "actions" => ["read"], "created_by" => "alice"} =
        json(conn)

      assert call(inst, :get, "/v1/objects/docs/teams/eng/b.txt", as: "bob").resp_body == "hello"

      assert call(inst, :put, "/v1/objects/docs/teams/eng/b.txt", as: "bob", body: "x").status ==
               403

      acl = inst |> call(:get, "/v1/spaces/docs/acl?key=teams/eng/b.txt") |> json()
      assert acl["owner"] == "alice"
      assert [%{"id" => ^id}] = acl["grants"]

      assert call(inst, :delete, "/v1/spaces/docs/grants/#{id}?key=teams/eng/b.txt").status == 204
      assert call(inst, :get, "/v1/objects/docs/teams/eng/b.txt", as: "bob").status == 403
      # Idempotent.
      assert call(inst, :delete, "/v1/spaces/docs/grants/#{id}?key=teams/eng/b.txt").status == 204

      assert [
               %{"action" => "grant.create", "actor" => "alice", "space" => "docs"} = created,
               %{"action" => "grant.revoke", "actor" => "alice"},
               %{"action" => "grant.revoke"}
             ] =
               audit_records(root)

      assert created["resource"] == %{"kind" => "object", "key" => "teams/eng/b.txt"}
      assert created["grant"]["subject"] == "user:bob"
      assert is_binary(created["request_id"])
    end

    test "a prefix grant covers every key under it", %{inst: inst} do
      put(inst, "teams/eng/x/deep/c.txt", "c")
      assert grant(inst, %{prefix: "teams/eng/x/"}, "group:sales", ["read"]).status == 201

      assert call(inst, :get, "/v1/objects/docs/teams/eng/x/deep/c.txt", as: "bob").status == 200
      assert call(inst, :get, "/v1/objects/docs/teams/eng/y.txt", as: "bob").status in [403, 404]

      acl = inst |> call(:get, "/v1/spaces/docs/acl?prefix=teams/eng/x/") |> json()
      assert acl["owner"] == nil
      assert [%{"subject" => "group:sales"}] = acl["grants"]
    end

    @tag team_actions: [:read, :write, :delete, :list]
    test "the uploader can share without the policy's share; another team member cannot", %{
      inst: inst
    } do
      put(inst, "teams/eng/own.txt", "x")
      assert grant(inst, %{key: "teams/eng/own.txt"}, "user:bob", ["read"]).status == 201

      conn = grant(inst, %{key: "teams/eng/own.txt"}, "user:bob", ["read"], "erin")
      assert {conn.status, json(conn)} == {403, %{"error" => "forbidden"}}
    end

    test "nobody hands out what they do not hold", %{inst: inst, root: root} do
      {:ok, _} = LocalFS.put_object(:x, "docs/shared/s.txt", "s", root: root)

      conn = grant(inst, %{key: "shared/s.txt"}, "user:bob", ["read", "write"], "rita")
      assert {conn.status, json(conn)} == {403, %{"error" => "forbidden", "action" => "write"}}
      assert grant(inst, %{key: "shared/s.txt"}, "user:bob", ["read"], "rita").status == 201
    end

    @tag team_actions: [:read, :write, :delete, :list]
    test "overwriting an object that has no owner does not make the writer its owner", %{
      inst: inst,
      root: root
    } do
      {:ok, _} = LocalFS.put_object(:x, "docs/teams/eng/legacy.txt", "old", root: root)
      assert put(inst, "teams/eng/legacy.txt", "new").status == 200
      assert grant(inst, %{key: "teams/eng/legacy.txt"}, "user:bob", ["read"]).status == 403

      assert inst
             |> call(:get, "/v1/spaces/docs/acl?key=teams/eng/legacy.txt")
             |> Map.get(:status) == 403
    end

    test "a deleted object's grants do not reach an object re-created under its key", %{
      inst: inst,
      root: root
    } do
      put(inst, "teams/eng/r.txt", "one")
      grant(inst, %{key: "teams/eng/r.txt"}, "user:bob", ["read"])
      assert call(inst, :get, "/v1/objects/docs/teams/eng/r.txt", as: "bob").status == 200

      # Through the API: the document goes with the object.
      doc_key = ACL.doc_key(%{prefix: "docs/"}, {:object, "teams/eng/r.txt"})
      assert {:ok, _, _} = LocalFS.get_object(:x, doc_key, root: root)
      assert call(inst, :delete, "/v1/objects/docs/teams/eng/r.txt").status == 204
      assert LocalFS.get_object(:x, doc_key, root: root) == {:error, :not_found}
      put(inst, "teams/eng/r.txt", "two")
      assert call(inst, :get, "/v1/objects/docs/teams/eng/r.txt", as: "bob").status == 403

      # Out of band, the old document left in the store: dead by its incarnation.
      grant(inst, %{key: "teams/eng/r.txt"}, "user:bob", ["read"])
      assert {:ok, _, _} = LocalFS.get_object(:x, doc_key, root: root)
      :ok = LocalFS.delete_object(:x, "docs/teams/eng/r.txt", root: root)
      put(inst, "teams/eng/r.txt", "three")

      assert {:ok, _, _} = LocalFS.get_object(:x, doc_key, root: root)
      assert call(inst, :get, "/v1/objects/docs/teams/eng/r.txt", as: "bob").status == 403

      assert inst
             |> call(:get, "/v1/spaces/docs/acl?key=teams/eng/r.txt")
             |> json()
             |> Map.get("grants") == []
    end
  end

  describe "validation" do
    test "every route is 401 without a credential and 403 without share", %{inst: inst} do
      put(inst, "teams/eng/v.txt", "v")

      id =
        inst
        |> grant(%{key: "teams/eng/v.txt"}, "user:erin", ["read"])
        |> json()
        |> Map.fetch!("id")

      routes = [
        {:get, "/v1/spaces/docs/acl?key=teams/eng/v.txt", nil},
        {:post, "/v1/spaces/docs/grants",
         %{
           resource: %{key: "teams/eng/v.txt"},
           subject: "user:bob",
           actions: ["read"],
           expires_in_s: nil
         }},
        {:delete, "/v1/spaces/docs/grants/#{id}?key=teams/eng/v.txt", nil},
        {:post, "/v1/spaces/docs/links", %{key: "teams/eng/v.txt", expires_in_s: 60}}
      ]

      for {method, path, body} <- routes do
        assert call(inst, method, path, as: nil, body: body).status == 401, "#{method} #{path}"
        assert call(inst, method, path, as: "bob", body: body).status == 403, "#{method} #{path}"
      end
    end

    test "a grant id must be the named resource's", %{inst: inst} do
      put(inst, "teams/eng/a.txt", "a")
      put(inst, "teams/eng/b.txt", "b")

      id =
        inst
        |> grant(%{key: "teams/eng/a.txt"}, "user:bob", ["read"])
        |> json()
        |> Map.fetch!("id")

      for path <- [
            "/v1/spaces/docs/grants/#{id}?key=teams/eng/b.txt",
            "/v1/spaces/docs/grants/#{id}?prefix=teams/eng/",
            "/v1/spaces/docs/grants/not-an-id?key=teams/eng/a.txt",
            "/v1/spaces/docs/grants/#{id}"
          ] do
        conn = call(inst, :delete, path)
        assert {conn.status, json(conn)} == {400, %{"error" => "invalid_grant"}}, path
      end

      assert call(inst, :get, "/v1/objects/docs/teams/eng/a.txt", as: "bob").status == 200
    end

    test "bad grant bodies are 400; a missing object is 404", %{inst: inst} do
      put(inst, "teams/eng/a.txt", "a")

      for body <- [
            %{resource: %{key: "teams/eng/a.txt"}, subject: "link", actions: ["read"]},
            %{resource: %{key: "teams/eng/a.txt"}, subject: "user:", actions: ["read"]},
            %{resource: %{key: "teams/eng/a.txt"}, subject: "role:x", actions: ["read"]},
            %{resource: %{key: "teams/eng/a.txt"}, subject: "user:bob", actions: []},
            %{resource: %{key: "teams/eng/a.txt"}, subject: "user:bob", actions: ["fly"]},
            %{resource: %{prefix: "teams/eng"}, subject: "user:bob", actions: ["read"]},
            %{resource: %{key: "../x"}, subject: "user:bob", actions: ["read"]},
            %{
              resource: %{key: "teams/eng/a.txt"},
              subject: "user:bob",
              actions: ["read"],
              extra: 1
            }
          ] do
        conn = call(inst, :post, "/v1/spaces/docs/grants", body: body)
        assert {conn.status, json(conn)} == {400, %{"error" => "invalid_grant"}}, inspect(body)
      end

      assert grant(inst, %{key: "teams/eng/missing.txt"}, "user:bob", ["read"]).status == 404
    end

    test "sharing routes are 404 on an unknown space", %{inst: inst} do
      assert call(inst, :get, "/v1/spaces/nope/acl?key=a").status == 404
    end
  end

  describe "share links" do
    test "create, redeem without credentials, refuse everything else, revoke", %{
      inst: inst,
      root: root
    } do
      put(inst, "teams/eng/b.txt", "hello")
      conn = link(inst, "teams/eng/b.txt")
      assert conn.status == 201
      %{"id" => id, "path" => path, "expires_at" => expires_at} = json(conn)
      assert expires_at > System.os_time(:second)
      assert path =~ ~r"^/v1/objects/docs/teams/eng/b\.txt\?link=.+&exp=\d+&sig=.{43}$"

      assert {200, "hello"} = with_status(call(inst, :get, path, as: nil))
      assert call(inst, :head, path, as: nil).status == 200

      # Never a write, a delete, a listing or a sharing route.
      assert call(inst, :put, path, as: nil, body: "x").status == 401
      assert call(inst, :delete, path, as: nil).status == 401

      # Another key under the same link.
      [_, query] = String.split(path, "?")

      assert call(inst, :get, "/v1/objects/docs/teams/eng/other.txt?" <> query, as: nil).status ==
               401

      # Tampered.
      assert call(inst, :get, String.replace(path, "exp=", "exp=1"), as: nil).status == 401

      assert call(inst, :delete, "/v1/spaces/docs/grants/#{id}?key=teams/eng/b.txt").status == 204
      assert call(inst, :get, path, as: nil).status == 403

      assert Enum.map(audit_records(root), & &1["action"]) == ["link.create", "grant.revoke"]
    end

    @tag acl_cache_ttl_ms: 200
    test "a revoked link stops within acl_cache_ttl_ms", %{inst: inst} do
      put(inst, "teams/eng/b.txt", "hello")
      %{"id" => id, "path" => path} = inst |> link("teams/eng/b.txt") |> json()
      assert call(inst, :get, path, as: nil).status == 200

      assert call(inst, :delete, "/v1/spaces/docs/grants/#{id}?key=teams/eng/b.txt").status == 204
      Process.sleep(250)
      assert call(inst, :get, path, as: nil).status == 403
    end

    test "an expired link is 410", %{inst: inst} do
      put(inst, "teams/eng/b.txt", "hello")
      %{"path" => path} = inst |> link("teams/eng/b.txt", 1) |> json()
      Process.sleep(2_100)
      conn = call(inst, :get, path, as: nil)
      assert {conn.status, json(conn)} == {410, %{"error" => "link_expired"}}
    end

    test "a link's lifetime is bounded and needs read", %{inst: inst, root: root} do
      put(inst, "teams/eng/b.txt", "hello")
      assert link(inst, "teams/eng/b.txt", 0).status == 400
      assert link(inst, "teams/eng/b.txt", 604_801).status == 400
      {:ok, _} = LocalFS.put_object(:x, "docs/shared/s.txt", "s", root: root)
      assert link(inst, "shared/s.txt", 60, "rita").status == 201
    end

    @tag steps: ["allow_all", "grants"]
    test "under [allow-all rules, grants] a revoked link is refused", %{inst: inst} do
      put(inst, "teams/eng/b.txt", "hello")
      %{"id" => id, "path" => path} = inst |> link("teams/eng/b.txt") |> json()
      assert call(inst, :get, path, as: nil).status == 200
      call(inst, :delete, "/v1/spaces/docs/grants/#{id}?key=teams/eng/b.txt")
      assert call(inst, :get, path, as: nil).status == 403
    end
  end

  describe "audit" do
    test "an audit write that fails changes nothing", %{inst: inst, root: root} do
      put(inst, "teams/eng/b.txt", "hello")
      :persistent_term.put({AuditDownStore, root}, true)

      conn = grant(inst, %{key: "teams/eng/b.txt"}, "user:bob", ["read"])
      assert {conn.status, json(conn)} == {503, %{"error" => "audit_unavailable"}}
      assert [retry_after] = Plug.Conn.get_resp_header(conn, "retry-after")
      assert retry_after in ~w(1 2 3)

      :persistent_term.put({AuditDownStore, root}, false)

      assert inst
             |> call(:get, "/v1/spaces/docs/acl?key=teams/eng/b.txt")
             |> json()
             |> Map.get("grants") == []

      assert call(inst, :get, "/v1/objects/docs/teams/eng/b.txt", as: "bob").status == 403
    end
  end

  describe "an empty chain" do
    @tag anonymous: true, steps: ["grants", "allow_all"]
    test "the anonymous principal never owns and never matches anyone", %{inst: inst, root: root} do
      assert put(inst, "open/a.txt", "a", nil).status == 200
      {:ok, meta} = LocalFS.head_object(:x, "docs/open/a.txt", root: root)
      assert is_binary(meta.metadata["ankusa-incarnation"])
      refute Map.has_key?(meta.metadata, "ankusa-owner")

      space = Ankusa.Gateway.config(inst).spaces["docs"]

      {:ok, _} =
        Ankusa.Gateway.ACL.Store.update(inst, space, :prefix, "", fn nil ->
          doc = ACL.new("docs", {:prefix, ""}, nil)

          grant = %{
            id: ACL.grant_id({:prefix, ""}),
            subject: "anyone",
            actions: [:read],
            created_by: "op",
            created_at: 0,
            expires_at: nil
          }

          {:ok, %{doc | grants: [grant]}}
        end)

      request = fn principal, via ->
        %Ankusa.Gateway.Authz.Request{
          principal: principal,
          via: via,
          subject_key: Ankusa.Gateway.Authn.subject_key(via, principal),
          action: :read,
          resource: %{kind: :object, space: "docs", key: "open/a.txt", tenant: nil}
        }
      end

      operator = Ankusa.Principal.operator()

      assert {:deny, _} =
               Ankusa.Gateway.Authz.decide(inst, "grants_only", request.(operator, :anonymous))

      someone = Ankusa.Principal.new("someone", [])

      assert {:allow, %{authorizer: "grants"}} =
               Ankusa.Gateway.Authz.decide(inst, "grants_only", request.(someone, :token))
    end
  end

  defp with_status(conn), do: {conn.status, conn.resp_body}
end
