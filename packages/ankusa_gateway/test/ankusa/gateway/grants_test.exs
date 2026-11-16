defmodule Ankusa.Gateway.GrantsTest do
  @moduledoc """
  The `grants` authorizer through `Ankusa.Gateway.Authz.decide/4`: ownership,
  grants on the object and on every ancestor prefix, expiry, the abstain, the
  claim abstain, an unavailable store, a dead incarnation, and the store reads
  a deep key costs cold and warm.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.BlobStore.LocalFS
  alias Ankusa.Gateway.ACL
  alias Ankusa.Gateway.ACL.Store
  alias Ankusa.Gateway.{Authn, Authz}
  alias Ankusa.Gateway.Authz.Request
  alias Ankusa.Principal

  @moduletag :capture_log
  @moduletag :tmp_dir

  @key "teams/eng/q3.pdf"
  @now 1_790_000_000

  # A LocalFS that tells the test about every head and document read.
  defmodule CountingStore do
    @moduledoc false

    alias Ankusa.BlobStore.LocalFS

    def head_object(instance, key, opts) do
      count(opts, :head, key)
      LocalFS.head_object(instance, key, opts)
    end

    def get_object(instance, key, opts) do
      count(opts, :get, key)
      LocalFS.get_object(instance, key, opts)
    end

    def put_object(instance, key, data, opts), do: LocalFS.put_object(instance, key, data, opts)
    def delete_object(instance, key, opts), do: LocalFS.delete_object(instance, key, opts)
    def list_page(instance, prefix, opts), do: LocalFS.list_page(instance, prefix, opts)

    defp count(opts, kind, key) do
      case Keyword.get(opts, :test) do
        nil -> :ok
        pid -> send(pid, {:store_read, kind, key})
      end
    end
  end

  # A store whose document reads fail; the head still answers.
  defmodule BrokenStore do
    @moduledoc false

    alias Ankusa.BlobStore.LocalFS

    def head_object(instance, key, opts), do: LocalFS.head_object(instance, key, opts)
    def put_object(instance, key, data, opts), do: LocalFS.put_object(instance, key, data, opts)
    def delete_object(instance, key, opts), do: LocalFS.delete_object(instance, key, opts)
    def list_page(instance, prefix, opts), do: LocalFS.list_page(instance, prefix, opts)

    def get_object(_instance, _key, _opts), do: {:error, {:status, 500, "boom"}}
  end

  defp boot(dir, opts \\ []) do
    config = test_config(roles: [])
    start_supervised!({Ankusa.Instance, config})
    inst = config.instance

    authz =
      Map.merge(
        %{policies: %{"sharing" => %{mode: :first, steps: ["grants"]}}},
        Map.new(Keyword.get(opts, :authz, []))
      )

    sharing =
      Map.merge(
        %{enabled: true, signing_keys: [String.duplicate("k", 32)]},
        Map.new(Keyword.get(opts, :sharing, []))
      )

    store = Keyword.get(opts, :store, LocalFS)
    store_opts = [root: dir] ++ Keyword.get(opts, :store_opts, [])

    gateway =
      Ankusa.Gateway.Config.new(
        instance: inst,
        port: 0,
        authz: authz,
        spaces: %{
          "docs" => %{store: {store, store_opts}, prefix: "docs/", policy: "sharing"}
        },
        sharing: sharing
      )

    start_supervised!({Ankusa.Gateway, gateway})
    %{inst: inst, space: gateway.spaces["docs"], gateway: gateway}
  end

  defp object_resource(key), do: %{kind: :object, space: "docs", key: key, tenant: nil}
  defp prefix_resource(key), do: %{kind: :prefix, space: "docs", key: key, tenant: nil}

  defp claim_resource(tenant, id),
    do: %{kind: :claim, space: "claims", key: tenant <> "/" <> id, tenant: tenant}

  defp request(opts) do
    principal = Keyword.get(opts, :principal, Principal.new("bob", []))
    via = Keyword.get(opts, :via, :token)

    %Request{
      principal: principal,
      via: via,
      subject_key: Authn.subject_key(via, principal),
      action: Keyword.get(opts, :action, :read),
      resource: Keyword.fetch!(opts, :resource)
    }
  end

  defp decide(inst, request), do: Authz.decide(inst, "sharing", request, [])

  defp grant(subject, actions, opts \\ []) do
    %{
      id: Keyword.get(opts, :id, "g-" <> ACL.ulid()),
      subject: subject,
      actions: actions,
      created_by: "tester",
      created_at: @now,
      expires_at: Keyword.get(opts, :expires_at)
    }
  end

  defp put_object(inst, space, key, metadata) do
    {module, store_opts} = space.store

    {:ok, _version} =
      module.put_object(inst, space.prefix <> key, "body", store_opts ++ [metadata: metadata])

    :ok
  end

  # Out-of-band document write, with no cache to outrun (tests write first).
  defp write_doc(inst, space, kind, key, grants, incarnation \\ nil) do
    {module, store_opts} = space.store
    doc = %{ACL.new("docs", {kind, key}, incarnation) | grants: grants, updated_at: @now}

    {:ok, _version} =
      module.put_object(
        inst,
        ACL.doc_key(space, {kind, key}),
        ACL.encode(doc),
        store_opts ++ [content_type: "application/json"]
      )

    :ok
  end

  defp drain(acc \\ []) do
    receive do
      {:store_read, kind, key} -> drain([{kind, key} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  describe "ownership" do
    test "the owner may do anything on the object, but not by link nor anonymously",
         %{tmp_dir: dir} do
      %{inst: inst, space: space} = boot(dir)

      put_object(inst, space, @key, %{
        "ankusa-owner" => "alice",
        "ankusa-incarnation" => "INC-1"
      })

      alice = Principal.new("alice", [])

      for action <- [:read, :write, :delete, :list, :share] do
        assert {:allow, %{authorizer: "grants"}} =
                 decide(
                   inst,
                   request(principal: alice, action: action, resource: object_resource(@key))
                 ),
               inspect(action)
      end

      assert {:deny, _meta} =
               decide(
                 inst,
                 request(principal: Principal.new("bob", []), resource: object_resource(@key))
               )

      # The same kind of principal through a share link is not the owner.
      link = Principal.new("link:" <> ACL.ulid(), [])

      assert {:deny, _meta} =
               decide(
                 inst,
                 request(principal: link, via: :link, resource: object_resource(@key))
               )

      # Nor is an anonymous caller.
      assert {:deny, _meta} =
               decide(
                 inst,
                 request(
                   principal: Principal.operator(),
                   via: :anonymous,
                   resource: object_resource(@key)
                 )
               )
    end

    test "an owner does not own a prefix request", %{tmp_dir: dir} do
      %{inst: inst, space: space} = boot(dir)

      put_object(inst, space, @key, %{
        "ankusa-owner" => "alice",
        "ankusa-incarnation" => "INC-1"
      })

      assert {:deny, _meta} =
               decide(
                 inst,
                 request(
                   principal: Principal.new("alice", []),
                   action: :list,
                   resource: prefix_resource("teams/")
                 )
               )
    end
  end

  describe "grants" do
    test "user, group, tenant and anyone grants on the object and each ancestor allow their actions",
         %{tmp_dir: dir} do
      %{inst: inst, space: space} = boot(dir)

      put_object(inst, space, @key, %{"ankusa-incarnation" => "INC-1"})

      write_doc(inst, space, :object, @key, [grant("user:bob", [:read])], "INC-1")
      write_doc(inst, space, :prefix, "teams/", [grant("group:eng", [:read, :write])])
      write_doc(inst, space, :prefix, "", [grant("tenant:acme", [:read])])
      write_doc(inst, space, :prefix, "teams/eng/", [grant("anyone", [:list])])

      bob = Principal.new("bob", [])
      eng = %Principal{Principal.new("erin", []) | groups: ["eng"]}
      acme = Principal.new("carl", [], ["acme"])

      # The object's own grant: its action only.
      assert {:allow, _} =
               decide(
                 inst,
                 request(principal: bob, action: :read, resource: object_resource(@key))
               )

      assert {:deny, _} =
               decide(
                 inst,
                 request(principal: bob, action: :write, resource: object_resource(@key))
               )

      # The group grant on teams/.
      for action <- [:read, :write] do
        assert {:allow, _} =
                 decide(
                   inst,
                   request(principal: eng, action: action, resource: object_resource(@key))
                 ),
               inspect(action)
      end

      assert {:deny, _} =
               decide(
                 inst,
                 request(principal: eng, action: :delete, resource: object_resource(@key))
               )

      # The tenant grant on the root prefix applies to every key.
      assert {:allow, _} =
               decide(
                 inst,
                 request(principal: acme, resource: object_resource("other/x.txt"))
               )

      assert {:deny, _} =
               decide(
                 inst,
                 request(
                   principal: Principal.new("dan", [], ["other"]),
                   resource: object_resource("other/x.txt")
                 )
               )

      # anyone on a prefix: a credential yes, an anonymous caller no.
      assert {:allow, _} =
               decide(
                 inst,
                 request(principal: bob, action: :list, resource: object_resource(@key))
               )

      assert {:deny, _} =
               decide(
                 inst,
                 request(
                   principal: Principal.operator(),
                   via: :anonymous,
                   action: :list,
                   resource: object_resource(@key)
                 )
               )
    end

    test "a grant on a prefix a partial name does not reach, one on the full prefix does",
         %{tmp_dir: dir} do
      %{inst: inst, space: space} = boot(dir)
      write_doc(inst, space, :prefix, "teams/", [grant("user:bob", [:list])])
      bob = Principal.new("bob", [])

      assert {:allow, _} =
               decide(
                 inst,
                 request(principal: bob, action: :list, resource: prefix_resource("teams/"))
               )

      assert {:deny, _} =
               decide(
                 inst,
                 request(principal: bob, action: :list, resource: prefix_resource("teams"))
               )
    end

    test "an expired grant allows nothing", %{tmp_dir: dir} do
      %{inst: inst, space: space} = boot(dir)
      put_object(inst, space, @key, %{"ankusa-incarnation" => "INC-1"})

      write_doc(
        inst,
        space,
        :object,
        @key,
        [grant("user:bob", [:read], expires_at: System.os_time(:second) - 1)],
        "INC-1"
      )

      assert {:deny, _meta} =
               decide(
                 inst,
                 request(principal: Principal.new("bob", []), resource: object_resource(@key))
               )
    end

    test "no grant at all abstains (a deny under a first-match policy of one step)",
         %{tmp_dir: dir} do
      %{inst: inst, space: space} = boot(dir)
      put_object(inst, space, @key, %{"ankusa-incarnation" => "INC-1"})

      assert {:deny, %{authorizer: nil}} =
               decide(
                 inst,
                 request(principal: Principal.new("bob", []), resource: object_resource(@key))
               )
    end

    test "a claim request abstains", %{tmp_dir: dir} do
      %{inst: inst} = boot(dir)

      assert {:deny, %{authorizer: nil}} =
               decide(
                 inst,
                 request(
                   principal: Principal.new("bob", ["claims:read"]),
                   resource: claim_resource("acme", "01M39VMD8RA3C5HR4RBV67Y002")
                 )
               )
    end
  end

  describe "failures" do
    test "a store that cannot read a document is unavailable, never an abstain",
         %{tmp_dir: dir} do
      %{inst: inst} = boot(dir, store: BrokenStore)

      assert {:error, {:unavailable, :acl}} =
               decide(
                 inst,
                 request(principal: Principal.new("bob", []), resource: object_resource(@key))
               )
    end

    test "a grant on a deleted-then-recreated object does not apply", %{tmp_dir: dir} do
      %{inst: inst, space: space} = boot(dir)
      bob = Principal.new("bob", [])

      put_object(inst, space, @key, %{"ankusa-incarnation" => "INC-1"})
      write_doc(inst, space, :object, @key, [grant("user:bob", [:read])], "INC-1")

      assert {:allow, _} =
               decide(inst, request(principal: bob, resource: object_resource(@key)))

      {module, store_opts} = space.store
      :ok = module.delete_object(inst, space.prefix <> @key, store_opts)
      put_object(inst, space, @key, %{"ankusa-incarnation" => "INC-2"})
      :ok = Store.invalidate(inst, space, @key)

      # The old document is still in the store, but its incarnation is dead.
      assert {:deny, _meta} =
               decide(inst, request(principal: bob, resource: object_resource(@key)))
    end
  end

  describe "store reads" do
    test "a key 32 segments deep authorizes with at most 34 reads cold and none warm",
         %{tmp_dir: dir} do
      %{inst: inst} =
        boot(dir, store: CountingStore, store_opts: [test: self()], authz: [deadline_ms: 5_000])

      key = 1..32 |> Enum.map_join("/", &"s#{&1}")
      request = request(principal: Principal.new("bob", []), resource: object_resource(key))

      drain()

      assert {:deny, _meta} = decide(inst, request)

      cold = drain()
      assert length(cold) <= 34
      # 32 ancestor prefixes, the object's head and its document.
      assert length(cold) == 34
      assert Enum.count(cold, &match?({:head, _key}, &1)) == 1

      assert {:deny, _meta} = decide(inst, request)
      assert drain() == []
    end

    test "a claim request reads no store at all", %{tmp_dir: dir} do
      %{inst: inst} = boot(dir, store: CountingStore, store_opts: [test: self()])
      drain()

      assert {:deny, %{authorizer: nil}} =
               decide(
                 inst,
                 request(resource: claim_resource("acme", "01M39VMD8RA3C5HR4RBV67Y002"))
               )

      assert drain() == []
    end
  end
end
