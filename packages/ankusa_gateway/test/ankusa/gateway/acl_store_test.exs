defmodule Ankusa.Gateway.ACLStoreTest do
  @moduledoc """
  `Ankusa.Gateway.ACL.Store`: compare-and-swap updates of an ACL document
  through a space's own store, the object's incarnation, and the `:acl` cache
  with its TTL (the cross-node bound of a revocation).
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.BlobStore.LocalFS
  alias Ankusa.Gateway.ACL
  alias Ankusa.Gateway.ACL.Store

  @moduletag :capture_log
  @moduletag :tmp_dir

  @key "teams/eng/q3.pdf"
  @now 1_790_000_000

  defp boot(dir, sharing_opts \\ []) do
    config = test_config(roles: [])
    start_supervised!({Ankusa.Instance, config})
    inst = config.instance

    gateway =
      Ankusa.Gateway.Config.new(
        instance: inst,
        port: 0,
        authz: %{policies: %{"sharing" => %{mode: :first, steps: ["grants"]}}},
        spaces: %{
          "docs" => %{
            store: {LocalFS, root: dir},
            prefix: "docs/",
            policy: "sharing"
          }
        },
        sharing:
          Map.merge(
            %{enabled: true, signing_keys: [String.duplicate("k", 32)]},
            Map.new(sharing_opts)
          )
      )

    start_supervised!({Ankusa.Gateway, gateway})
    %{inst: inst, gateway: gateway, space: gateway.spaces["docs"]}
  end

  defp deadline, do: System.monotonic_time(:millisecond) + 5_000

  defp grant(id) do
    %{
      id: id,
      subject: "user:" <> id,
      actions: [:read],
      created_by: "tester",
      created_at: @now,
      expires_at: nil
    }
  end

  defp add_grant(nil, kind, key, id) do
    %{ACL.new("docs", {kind, key}, nil) | grants: [grant(id)]}
  end

  defp add_grant(doc, _kind, _key, id), do: %{doc | grants: doc.grants ++ [grant(id)]}

  defp ids(doc), do: Enum.map(doc.grants, & &1.id)

  # Out-of-band writes: straight to LocalFS, past the store's cache.
  defp object_put(inst, space, key, data, opts) do
    {module, store_opts} = space.store
    {:ok, version} = module.put_object(inst, space.prefix <> key, data, store_opts ++ opts)
    version
  end

  defp doc_put(inst, space, kind, key, doc) do
    {module, store_opts} = space.store

    {:ok, _version} =
      module.put_object(
        inst,
        ACL.doc_key(space, {kind, key}),
        ACL.encode(doc),
        store_opts ++ [content_type: "application/json"]
      )

    :ok
  end

  describe "update/5" do
    test "20 concurrent updates on one document all land or report :conflict, none lost",
         %{tmp_dir: dir} do
      %{inst: inst, space: space} = boot(dir)

      results =
        1..20
        |> Task.async_stream(
          fn i ->
            Store.update(inst, space, :prefix, "shared/", fn doc ->
              {:ok, add_grant(doc, :prefix, "shared/", "g#{i}")}
            end)
          end,
          max_concurrency: 20,
          timeout: 30_000
        )
        |> Enum.map(fn {:ok, result} -> result end)

      assert Enum.all?(results, &(match?({:ok, _doc}, &1) or &1 == {:error, :conflict}))

      landed =
        for {i, {:ok, _doc}} <- Enum.zip(1..20, results), do: "g#{i}"

      assert landed != []

      assert {:ok, doc} = Store.fetch(inst, space, :prefix, "shared/", deadline())
      assert Enum.sort(ids(doc)) == Enum.sort(landed)
    end

    test "an update on an object that does not exist is :not_found", %{tmp_dir: dir} do
      %{inst: inst, space: space} = boot(dir)

      assert Store.update(inst, space, :object, "nope.txt", fn current ->
               {:ok, add_grant(current, :object, "nope.txt", "g1")}
             end) == {:error, :not_found}
    end

    test "the document carries the object's current incarnation; a dead document reads as nil",
         %{tmp_dir: dir} do
      %{inst: inst, space: space} = boot(dir)
      parent = self()

      fun = fn current ->
        send(parent, {:current, current})
        {:ok, add_grant(current, :object, @key, "g#{System.unique_integer([:positive])}")}
      end

      object_put(inst, space, @key, "one", metadata: %{"ankusa-incarnation" => "INC-1"})

      assert {:ok, doc} = Store.update(inst, space, :object, @key, fun)
      assert_received {:current, nil}
      assert doc.incarnation == "INC-1"
      assert doc.resource == %{kind: :object, key: @key}
      assert length(doc.grants) == 1

      # A document written on another incarnation of the object is dead: the
      # update reads it as nil and replaces it.
      dead = %{ACL.new("docs", {:object, @key}, "INC-OTHER") | grants: [grant("dead")]}
      doc_put(inst, space, :object, @key, dead)

      assert {:ok, replaced} = Store.update(inst, space, :object, @key, fun)
      assert_received {:current, nil}
      assert replaced.incarnation == "INC-1"
      refute "dead" in ids(replaced)
      assert length(replaced.grants) == 1

      # A live document of the current incarnation is handed to `fun`.
      assert {:ok, _live} = Store.update(inst, space, :object, @key, fun)
      assert_received {:current, %{incarnation: "INC-1"}}
    end

    test "a fun that answers {:error, reason} stops the update with that reason", %{tmp_dir: dir} do
      %{inst: inst, space: space} = boot(dir)

      assert {:error, :nope} =
               Store.update(inst, space, :prefix, "shared/", fn _current -> {:error, :nope} end)

      assert {:ok, nil} = Store.fetch(inst, space, :prefix, "shared/", deadline())
    end
  end

  describe "the :acl cache" do
    test "a read is served from the cache until acl_cache_ttl_ms passes", %{tmp_dir: dir} do
      %{inst: inst, space: space} = boot(dir, acl_cache_ttl_ms: 250)

      assert {:ok, doc1} =
               Store.update(inst, space, :prefix, "shared/", fn nil ->
                 {:ok, add_grant(nil, :prefix, "shared/", "g1")}
               end)

      assert {:ok, cached} = Store.fetch(inst, space, :prefix, "shared/", deadline())
      assert ids(cached) == ["g1"]

      # Another node (or an operator) rewrites the document out of band.
      doc_put(inst, space, :prefix, "shared/", add_grant(doc1, :prefix, "shared/", "g2"))

      assert {:ok, still_cached} = Store.fetch(inst, space, :prefix, "shared/", deadline())
      assert ids(still_cached) == ["g1"]

      Process.sleep(400)

      assert {:ok, fresh} = Store.fetch(inst, space, :prefix, "shared/", deadline())
      assert Enum.sort(ids(fresh)) == ["g1", "g2"]
    end

    test "a second node sees an update within acl_cache_ttl_ms", %{tmp_dir: dir} do
      a = boot(dir, acl_cache_ttl_ms: 300)
      b = boot(dir, acl_cache_ttl_ms: 300)

      assert {:ok, _doc1} =
               Store.update(a.inst, a.space, :prefix, "shared/", fn nil ->
                 {:ok, add_grant(nil, :prefix, "shared/", "g1")}
               end)

      assert {:ok, seen} = Store.fetch(b.inst, b.space, :prefix, "shared/", deadline())
      assert ids(seen) == ["g1"]

      assert {:ok, doc2} =
               Store.update(a.inst, a.space, :prefix, "shared/", fn doc ->
                 {:ok, add_grant(doc, :prefix, "shared/", "g2")}
               end)

      assert ids(doc2) == ["g1", "g2"]

      assert {:ok, stale} = Store.fetch(b.inst, b.space, :prefix, "shared/", deadline())
      assert ids(stale) == ["g1"]

      Process.sleep(600)

      assert {:ok, fresh} = Store.fetch(b.inst, b.space, :prefix, "shared/", deadline())
      assert Enum.sort(ids(fresh)) == ["g1", "g2"]
    end

    test "acl_cache_ttl_ms: 0 reads the store every time", %{tmp_dir: dir} do
      %{inst: inst, space: space} = boot(dir, acl_cache_ttl_ms: 0)

      assert {:ok, doc1} =
               Store.update(inst, space, :prefix, "shared/", fn nil ->
                 {:ok, add_grant(nil, :prefix, "shared/", "g1")}
               end)

      assert {:ok, first} = Store.fetch(inst, space, :prefix, "shared/", deadline())
      assert ids(first) == ["g1"]

      doc_put(inst, space, :prefix, "shared/", add_grant(doc1, :prefix, "shared/", "g2"))

      assert {:ok, second} = Store.fetch(inst, space, :prefix, "shared/", deadline())
      assert Enum.sort(ids(second)) == ["g1", "g2"]
    end

    test "invalidate/3 drops this node's head and document entries", %{tmp_dir: dir} do
      %{inst: inst, space: space} = boot(dir)

      object_put(inst, space, @key, "one", metadata: %{"ankusa-incarnation" => "INC-1"})
      assert {:ok, %{incarnation: "INC-1"} = head} = Store.head(inst, space, @key, deadline())

      assert {:ok, ^head} = Store.head(inst, space, @key, deadline())
      assert Store.invalidate(inst, space, @key) == :ok

      # The object is replaced; the next read is the store's, not the cache's.
      object_put(inst, space, @key, "two", metadata: %{"ankusa-incarnation" => "INC-2"})
      assert {:ok, %{incarnation: "INC-2"}} = Store.head(inst, space, @key, deadline())
    end

    test "head/4 of an absent object is nil, cached as a miss too", %{tmp_dir: dir} do
      %{inst: inst, space: space} = boot(dir)

      assert {:ok, nil} = Store.head(inst, space, "missing.txt", deadline())
      assert {:ok, nil} = Store.head(inst, space, "missing.txt", deadline())
    end
  end
end
