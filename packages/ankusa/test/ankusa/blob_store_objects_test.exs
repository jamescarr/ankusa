defmodule Ankusa.BlobStoreObjectsTest do
  @moduledoc """
  The optional object callbacks `Ankusa.BlobStore` adds beside its immutable
  pack and segment API: `Ankusa.BlobStore.LocalFS` here, and the shared status
  and header helpers of `Ankusa.BlobStore.HTTP`. The four HTTP stores' request
  shapes and status mappings are the sibling describes in this file.
  """

  use ExUnit.Case, async: true

  alias Ankusa.BlobStore.{HTTP, LocalFS}

  describe "Ankusa.BlobStore.LocalFS object operations" do
    setup do
      inst = Ankusa.TestHelpers.unique_instance()
      root = Ankusa.TestHelpers.unique_data_dir(inst)
      on_exit(fn -> File.rm_rf(root) end)

      %{inst: inst, root: root, opts: [root: root]}
    end

    test "put then head and get round-trip content type, metadata and version", ctx do
      assert {:ok, version} =
               LocalFS.put_object(ctx.inst, "docs/a.txt", "hello",
                 root: ctx.root,
                 content_type: "text/plain",
                 metadata: %{"team" => "eng"}
               )

      assert String.length(version) == 26

      assert {:ok, meta} = LocalFS.head_object(ctx.inst, "docs/a.txt", ctx.opts)
      assert meta.size == 5
      assert meta.version == version
      assert meta.content_type == "text/plain"
      assert is_integer(meta.last_modified)
      assert meta.metadata == %{"team" => "eng"}

      assert {:ok, same_meta, "hello"} = LocalFS.get_object(ctx.inst, "docs/a.txt", ctx.opts)
      assert same_meta == meta

      # The pack/segment API reads the same object.
      assert {:ok, "hello"} = LocalFS.get(ctx.inst, "docs/a.txt", ctx.opts)
    end

    test "content type defaults to application/octet-stream", ctx do
      assert {:ok, _version} = LocalFS.put_object(ctx.inst, "a.bin", <<0, 1, 2>>, ctx.opts)
      assert {:ok, meta} = LocalFS.head_object(ctx.inst, "a.bin", ctx.opts)
      assert meta.content_type == "application/octet-stream"
      assert meta.metadata == %{}
    end

    test "get returns the requested inclusive range and always the whole size", ctx do
      assert {:ok, version} =
               LocalFS.put_object(ctx.inst, "digits", "0123456789", ctx.opts)

      assert {:ok, meta, "2345"} =
               LocalFS.get_object(ctx.inst, "digits", range: {2, 5}, root: ctx.root)

      assert meta.size == 10
      assert meta.version == version

      # An end past the object is cut at its last byte.
      assert {:ok, meta, "89"} =
               LocalFS.get_object(ctx.inst, "digits", range: {8, 100}, root: ctx.root)

      assert meta.size == 10

      assert {:error, :range_not_satisfiable} =
               LocalFS.get_object(ctx.inst, "digits", range: {10, 20}, root: ctx.root)
    end

    test "an empty object reads as an empty body and has no satisfiable range", ctx do
      assert {:ok, version} = LocalFS.put_object(ctx.inst, "empty", "", ctx.opts)

      assert {:ok, %{size: 0, version: ^version}, ""} =
               LocalFS.get_object(ctx.inst, "empty", ctx.opts)

      assert {:error, :range_not_satisfiable} =
               LocalFS.get_object(ctx.inst, "empty", range: {0, 0}, root: ctx.root)
    end

    test "max_bytes refuses a body or range past it", ctx do
      assert {:ok, _version} = LocalFS.put_object(ctx.inst, "ten", "0123456789", ctx.opts)

      assert {:error, :too_large} =
               LocalFS.get_object(ctx.inst, "ten", max_bytes: 5, root: ctx.root)

      assert {:ok, _meta, "012"} =
               LocalFS.get_object(ctx.inst, "ten", range: {0, 2}, max_bytes: 3, root: ctx.root)

      assert {:error, :too_large} =
               LocalFS.get_object(ctx.inst, "ten", range: {0, 2}, max_bytes: 2, root: ctx.root)
    end

    test "put conditions: :absent refuses an existing object and leaves it alone", ctx do
      assert {:ok, version} =
               LocalFS.put_object(ctx.inst, "once.txt", "first",
                 root: ctx.root,
                 if: :absent
               )

      assert {:error, :already_exists} =
               LocalFS.put_object(ctx.inst, "once.txt", "second", root: ctx.root, if: :absent)

      assert {:ok, meta, "first"} = LocalFS.get_object(ctx.inst, "once.txt", ctx.opts)
      assert meta.version == version
    end

    test "put conditions: a version precondition succeeds only on the current version", ctx do
      assert {:ok, version} = LocalFS.put_object(ctx.inst, "v.txt", "first", ctx.opts)

      assert {:error, :version_mismatch} =
               LocalFS.put_object(ctx.inst, "v.txt", "second",
                 root: ctx.root,
                 if: {:version, "01ARZ3NDEKTSV4RRFFQ69G5FAV"}
               )

      assert {:ok, next_version} =
               LocalFS.put_object(ctx.inst, "v.txt", "second",
                 root: ctx.root,
                 if: {:version, version}
               )

      refute next_version == version

      assert {:ok, %{version: ^next_version}, "second"} =
               LocalFS.get_object(ctx.inst, "v.txt", ctx.opts)
    end

    test "a missing object is :not_found for head and get, and :version_mismatch for a versioned put",
         ctx do
      assert {:error, :not_found} = LocalFS.head_object(ctx.inst, "nope", ctx.opts)
      assert {:error, :not_found} = LocalFS.get_object(ctx.inst, "nope", ctx.opts)

      assert {:error, :version_mismatch} =
               LocalFS.put_object(ctx.inst, "nope", "x",
                 root: ctx.root,
                 if: {:version, "01ARZ3NDEKTSV4RRFFQ69G5FAV"}
               )
    end

    test "50 concurrent :absent writers yield exactly one winner", ctx do
      results =
        1..50
        |> Task.async_stream(
          &LocalFS.put_object(ctx.inst, "race.txt", "writer #{&1}", root: ctx.root, if: :absent),
          max_concurrency: 50,
          ordered: false,
          timeout: 60_000
        )
        |> Enum.map(fn {:ok, result} -> result end)

      assert [{:ok, winner}] = Enum.filter(results, &match?({:ok, _}, &1))
      assert Enum.count(results, &(&1 == {:error, :already_exists})) == 49

      assert {:ok, %{version: ^winner}, body} = LocalFS.get_object(ctx.inst, "race.txt", ctx.opts)
      assert body =~ "writer "
    end

    test "50 concurrent versioned writers yield exactly one winner per round", ctx do
      assert {:ok, first} = LocalFS.put_object(ctx.inst, "race2.txt", "initial", ctx.opts)

      results =
        1..50
        |> Task.async_stream(
          &LocalFS.put_object(ctx.inst, "race2.txt", "writer #{&1}",
            root: ctx.root,
            if: {:version, first}
          ),
          max_concurrency: 50,
          ordered: false,
          timeout: 60_000
        )
        |> Enum.map(fn {:ok, result} -> result end)

      assert [{:ok, winner}] = Enum.filter(results, &match?({:ok, _}, &1))
      assert Enum.count(results, &(&1 == {:error, :version_mismatch})) == 49

      assert {:ok, %{version: ^winner}, _body} =
               LocalFS.get_object(ctx.inst, "race2.txt", ctx.opts)
    end

    test "an object written through put/4 has no sidecar and a hashed version", ctx do
      assert :ok = LocalFS.put(ctx.inst, "plain.txt", "hello", ctx.opts)

      assert {:ok, meta} = LocalFS.head_object(ctx.inst, "plain.txt", ctx.opts)
      assert meta.version == "x" <> Base.encode16(:crypto.hash(:sha256, "hello"), case: :lower)
      assert meta.size == 5
      assert meta.content_type == nil
      assert meta.metadata == %{}

      # And a condition on that version is satisfied.
      assert {:ok, _meta, "hello"} =
               LocalFS.get_object(ctx.inst, "plain.txt",
                 root: ctx.root,
                 if: {:version, meta.version}
               )
    end

    test "an object rewritten out of band reports a new hashed version and fails the old one",
         ctx do
      assert {:ok, version} = LocalFS.put_object(ctx.inst, "oob.txt", "hello", ctx.opts)

      File.write!(Path.join(ctx.root, "oob.txt"), "a much longer body")

      assert {:ok, meta} = LocalFS.head_object(ctx.inst, "oob.txt", ctx.opts)
      assert meta.version =~ ~r/\Ax[0-9a-f]{64}\z/
      refute meta.version == version
      assert meta.size == byte_size("a much longer body")

      assert {:error, :version_mismatch} =
               LocalFS.get_object(ctx.inst, "oob.txt", root: ctx.root, if: {:version, version})

      assert {:ok, _restored} =
               LocalFS.put_object(ctx.inst, "oob.txt", "restored",
                 root: ctx.root,
                 if: {:version, meta.version}
               )

      assert {:ok, _meta, "restored"} = LocalFS.get_object(ctx.inst, "oob.txt", ctx.opts)
    end

    test "list_page returns sorted keys with sizes and follows cursors and limits", ctx do
      keys = ["a/1.txt", "a/2.txt", "a/b/3.txt", "b.txt", "z/4.txt"]
      for key <- keys, do: {:ok, _version} = LocalFS.put_object(ctx.inst, key, key, ctx.opts)

      assert {:ok, %{objects: objects, prefixes: [], next: nil}} =
               LocalFS.list_page(ctx.inst, "", ctx.opts)

      assert Enum.map(objects, & &1.key) == keys
      assert Enum.map(objects, & &1.size) == Enum.map(keys, &byte_size/1)

      assert {:ok, %{objects: first, next: cursor}} =
               LocalFS.list_page(ctx.inst, "", root: ctx.root, limit: 2)

      assert Enum.map(first, & &1.key) == ["a/1.txt", "a/2.txt"]
      assert cursor == "a/2.txt"

      assert {:ok, %{objects: second, next: cursor}} =
               LocalFS.list_page(ctx.inst, "", root: ctx.root, limit: 2, cursor: cursor)

      assert Enum.map(second, & &1.key) == ["a/b/3.txt", "b.txt"]

      assert {:ok, %{objects: third, next: nil}} =
               LocalFS.list_page(ctx.inst, "", root: ctx.root, limit: 2, cursor: cursor)

      assert Enum.map(third, & &1.key) == ["z/4.txt"]
    end

    test "list_page with a delimiter groups one directory level into prefixes", ctx do
      keys = ["a/1.txt", "a/2.txt", "a/b/3.txt", "b.txt", "z/4.txt"]
      for key <- keys, do: {:ok, _version} = LocalFS.put_object(ctx.inst, key, key, ctx.opts)

      assert {:ok, %{objects: [%{key: "b.txt"}], prefixes: ["a/", "z/"], next: nil}} =
               LocalFS.list_page(ctx.inst, "", root: ctx.root, delimiter: "/")

      assert {:ok, %{objects: nested, prefixes: ["a/b/"], next: nil}} =
               LocalFS.list_page(ctx.inst, "a/", root: ctx.root, delimiter: "/")

      assert Enum.map(nested, & &1.key) == ["a/1.txt", "a/2.txt"]
    end

    test "list_page pages the grouped entries of a delimiter listing", ctx do
      keys = ["a/1.txt", "b.txt", "z/4.txt"]
      for key <- keys, do: {:ok, _version} = LocalFS.put_object(ctx.inst, key, key, ctx.opts)

      assert {:ok, %{objects: [], prefixes: ["a/"], next: "a/"}} =
               LocalFS.list_page(ctx.inst, "", root: ctx.root, delimiter: "/", limit: 1)

      assert {:ok, %{objects: [%{key: "b.txt"}], prefixes: [], next: "b.txt"}} =
               LocalFS.list_page(ctx.inst, "",
                 root: ctx.root,
                 delimiter: "/",
                 limit: 1,
                 cursor: "a/"
               )

      assert {:ok, %{objects: [], prefixes: ["z/"], next: nil}} =
               LocalFS.list_page(ctx.inst, "",
                 root: ctx.root,
                 delimiter: "/",
                 limit: 1,
                 cursor: "b.txt"
               )
    end

    test "list_page never returns the sidecar tree", ctx do
      assert {:ok, _version} = LocalFS.put_object(ctx.inst, "docs/a.txt", "hello", ctx.opts)
      assert File.exists?(Path.join([ctx.root, ".ankusa-meta", "docs", "a.txt.json"]))

      assert {:ok, %{objects: [%{key: "docs/a.txt"}], prefixes: [], next: nil}} =
               LocalFS.list_page(ctx.inst, "", ctx.opts)

      assert {:ok, %{objects: [], prefixes: [], next: nil}} =
               LocalFS.list_page(ctx.inst, ".ankusa-meta/", ctx.opts)
    end

    test "delete_object removes data and sidecar, and a missing object is :ok", ctx do
      assert {:ok, _version} = LocalFS.put_object(ctx.inst, "gone.txt", "bye", ctx.opts)
      sidecar = Path.join([ctx.root, ".ankusa-meta", "gone.txt.json"])
      assert File.exists?(sidecar)

      assert :ok = LocalFS.delete_object(ctx.inst, "gone.txt", ctx.opts)
      assert {:error, :not_found} = LocalFS.head_object(ctx.inst, "gone.txt", ctx.opts)
      refute File.exists?(sidecar)

      assert :ok = LocalFS.delete_object(ctx.inst, "gone.txt", ctx.opts)
    end
  end

  describe "Ankusa.BlobStore.HTTP.object_reply/2" do
    test "a 2xx passes through as it came" do
      headers = %{"etag" => ["\"v1\""]}

      assert HTTP.object_reply({:ok, 200, headers, "body"}, nil) == {:ok, 200, headers, "body"}

      assert HTTP.object_reply({:ok, 206, headers, "part"}, {:version, "v1"}) ==
               {:ok, 206, headers, "part"}
    end

    test "a 404 is :not_found whatever the condition" do
      assert HTTP.object_reply({:ok, 404, %{}, ""}, nil) == {:error, :not_found}
      assert HTTP.object_reply({:ok, 404, %{}, ""}, {:version, "v"}) == {:error, :not_found}
    end

    test "a 412 is :already_exists for the absent condition and :version_mismatch otherwise" do
      assert HTTP.object_reply({:ok, 412, %{}, ""}, :absent) == {:error, :already_exists}

      assert HTTP.object_reply({:ok, 412, %{}, ""}, {:version, "v"}) ==
               {:error, :version_mismatch}

      assert HTTP.object_reply({:ok, 412, %{}, ""}, nil) == {:error, :version_mismatch}
    end

    test "a 409 is :version_mismatch only for a conditional request" do
      assert HTTP.object_reply({:ok, 409, %{}, ""}, {:version, "v"}) ==
               {:error, :version_mismatch}

      assert HTTP.object_reply({:ok, 409, %{}, "conflict"}, nil) ==
               {:error, {:status, 409, "conflict"}}
    end

    test "a 416 is :range_not_satisfiable" do
      assert HTTP.object_reply({:ok, 416, %{}, ""}, nil) == {:error, :range_not_satisfiable}
    end

    test "every other status, and a transport error, is HTTP.reply/1's answer" do
      assert HTTP.object_reply({:ok, 500, %{}, "boom"}, nil) == {:error, {:status, 500, "boom"}}
      assert HTTP.object_reply({:error, :deadline}, nil) == {:error, :deadline}

      assert HTTP.object_reply({:error, :breaker_open}, {:version, "v"}) ==
               {:error, :breaker_open}
    end
  end

  describe "Ankusa.BlobStore.HTTP helpers" do
    test "version/1 strips quotes and a weak marker, and passes nil through" do
      assert HTTP.version(nil) == nil
      assert HTTP.version("\"abc\"") == "abc"
      assert HTTP.version("W/\"abc\"") == "abc"
      assert HTTP.version(" \"abc\" ") == "abc"
    end

    test "object_size/1 prefers content-range's total over content-length" do
      assert HTTP.object_size(%{"content-range" => ["bytes 0-9/1234"]}) == 1234
      assert HTTP.object_size(%{"content-length" => ["42"]}) == 42

      assert HTTP.object_size(%{
               "content-range" => ["bytes 0-9/1234"],
               "content-length" => ["10"]
             }) == 1234

      assert HTTP.object_size(%{"content-range" => ["garbage"], "content-length" => ["7"]}) == 7
      assert HTTP.object_size(%{}) == nil
    end

    test "http_date/1 parses an IMF-fixdate and answers nil for anything else" do
      expected = DateTime.new!(~D[1994-11-06], ~T[08:49:37], "Etc/UTC") |> DateTime.to_unix()

      assert HTTP.http_date("Sun, 06 Nov 1994 08:49:37 GMT") == expected
      assert HTTP.http_date("not a date") == nil
      assert HTTP.http_date(nil) == nil
    end

    test "metadata/2 strips the prefix, lowercases names and ignores other headers" do
      headers = %{
        "x-amz-meta-team" => ["eng"],
        "x-amz-meta-owner" => ["alice", "bob"],
        "content-type" => ["text/plain"]
      }

      assert HTTP.metadata(headers, "x-amz-meta-") == %{"team" => "eng", "owner" => "alice"}
      assert HTTP.metadata(%{}, "x-amz-meta-") == %{}
    end
  end
end
