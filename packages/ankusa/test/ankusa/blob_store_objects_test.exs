defmodule Ankusa.BlobStoreObjectsTest do
  @moduledoc """
  The optional object callbacks `Ankusa.BlobStore` adds beside its immutable
  pack and segment API: `Ankusa.BlobStore.LocalFS` here, and the shared status
  and header helpers of `Ankusa.BlobStore.HTTP`. The four HTTP stores' request
  shapes and status mappings are the sibling describes in this file.
  """

  use ExUnit.Case, async: true

  alias Ankusa.BlobStore.{Azure, GCS, HTTP, LocalFS, OCI, S3}
  alias Ankusa.Test.BlobStoreAdapters, as: Adapters

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

    test "a 409 maps by condition, and only for a conditional request" do
      assert HTTP.object_reply({:ok, 409, %{}, ""}, :absent) == {:error, :already_exists}

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

  # ── S3 and GCS ─────────────────────────────────────────────────────────────

  @last_modified "Sun, 06 Nov 1994 08:49:37 GMT"
  @last_modified_unix DateTime.to_unix(~U[1994-11-06 08:49:37Z])

  # ── stubs ──────────────────────────────────────────────────────────────────

  defp s3_stub(respond) do
    test = self()

    Req.Test.stub(__MODULE__, fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      send(
        test,
        {:s3, conn.method, conn.request_path, conn.query_string, Adapters.headers(conn), body}
      )

      respond.(conn, body)
    end)
  end

  defp gcs_stub(respond) do
    test = self()

    Req.Test.stub(__MODULE__, fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      send(
        test,
        {:gcs, conn.method, conn.request_path, conn.query_string, Adapters.headers(conn), body}
      )

      respond.(conn, body)
    end)
  end

  defp s3_requests(acc \\ []) do
    receive do
      {:s3, method, path, query, headers, body} ->
        s3_requests([{method, path, query, headers, body} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp gcs_requests(acc \\ []) do
    receive do
      {:gcs, method, path, query, headers, body} ->
        gcs_requests([{method, path, query, headers, body} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp s3_method?(requests, method), do: Enum.any?(requests, &(elem(&1, 0) == method))
  defp gcs_method?(requests, method), do: Enum.any?(requests, &(elem(&1, 0) == method))

  # The `CompleteMultipartUpload` request of `upload_id` (a part's query carries
  # the same token, so the method is part of the match).
  defp s3_completion?({method, _path, query, _headers, _body}, upload_id),
    do: method == "POST" and String.contains?(query, "uploadId=" <> upload_id)

  defp s3_opts(tag, extra \\ []), do: Adapters.opts(S3, tag, __MODULE__, extra)
  defp gcs_opts(tag, extra \\ []), do: Adapters.opts(GCS, tag, __MODULE__, extra)

  @s3_page """
  <?xml version="1.0" encoding="UTF-8"?>
  <ListBucketResult>
    <Name>b</Name>
    <Prefix>docs/</Prefix>
    <KeyCount>2</KeyCount>
    <MaxKeys>2</MaxKeys>
    <IsTruncated>true</IsTruncated>
    <Contents><Key>docs/a.txt</Key><Size>5</Size></Contents>
    <Contents><Key>docs/b.txt</Key><Size>7</Size></Contents>
    <CommonPrefixes><Prefix>docs/team/</Prefix></CommonPrefixes>
    <NextContinuationToken>tok-2</NextContinuationToken>
  </ListBucketResult>
  """

  # ── S3 ─────────────────────────────────────────────────────────────────────

  describe "s3 object operations" do
    test "head_object reports the object's metadata" do
      s3_stub(fn conn, _body ->
        conn
        |> Plug.Conn.put_resp_header("etag", ~s("v1"))
        |> Plug.Conn.put_resp_header("content-type", "text/plain")
        |> Plug.Conn.put_resp_header("content-length", "42")
        |> Plug.Conn.put_resp_header("last-modified", @last_modified)
        |> Plug.Conn.put_resp_header("x-amz-meta-color", "blue")
        |> Plug.Conn.send_resp(200, "")
      end)

      {mod, opts} = s3_opts("obj-s3-head")

      assert {:ok, meta} = mod.head_object(:i, "docs/a.txt", opts)

      assert meta == %{
               size: 42,
               version: "v1",
               content_type: "text/plain",
               last_modified: @last_modified_unix,
               metadata: %{"color" => "blue"}
             }

      assert [{"HEAD", "/b/docs/a.txt", "", _headers, ""}] = s3_requests()
    end

    test "head_object on a missing key is :not_found" do
      s3_stub(fn conn, _body -> Plug.Conn.send_resp(conn, 404, "") end)

      {mod, opts} = s3_opts("obj-s3-head-missing")
      assert {:error, :not_found} = mod.head_object(:i, "docs/missing.txt", opts)
    end

    test "get_object sends a range and an If-Match, and reports the whole size" do
      s3_stub(fn conn, _body ->
        conn
        |> Plug.Conn.put_resp_header("etag", ~s("v1"))
        |> Plug.Conn.put_resp_header("content-range", "bytes 10-19/100")
        |> Plug.Conn.put_resp_header("content-type", "text/plain")
        |> Plug.Conn.put_resp_header("last-modified", @last_modified)
        |> Plug.Conn.put_resp_header("x-amz-meta-color", "blue")
        |> Plug.Conn.send_resp(206, "0123456789")
      end)

      {mod, opts} = s3_opts("obj-s3-get-range")

      assert {:ok, meta, "0123456789"} =
               mod.get_object(:i, "docs/a.txt", opts ++ [range: {10, 19}, if: {:version, "v1"}])

      assert meta.size == 100
      assert meta.version == "v1"
      assert meta.content_type == "text/plain"
      assert meta.last_modified == @last_modified_unix
      assert meta.metadata == %{"color" => "blue"}

      assert [{"GET", "/b/docs/a.txt", "", headers, ""}] = s3_requests()
      assert headers["range"] == "bytes=10-19"
      assert headers["if-match"] == ~s("v1")
      refute Map.has_key?(headers, "x-amz-checksum-mode")
    end

    test "get_object verifies the checksum of a whole read" do
      body = "hello"
      digest = Ankusa.BlobStore.Checksum.sha256_base64(body)

      s3_stub(fn conn, _body ->
        conn
        |> Plug.Conn.put_resp_header("etag", ~s("v1"))
        |> Plug.Conn.put_resp_header("content-length", "5")
        |> Plug.Conn.put_resp_header("x-amz-checksum-sha256", digest)
        |> Plug.Conn.send_resp(200, body)
      end)

      {mod, opts} = s3_opts("obj-s3-get-checksum")

      assert {:ok, %{size: 5}, "hello"} = mod.get_object(:i, "docs/a.txt", opts)

      assert [{"GET", _, "", headers, ""}] = s3_requests()
      assert headers["x-amz-checksum-mode"] == "ENABLED"
    end

    test "get_object with a stale version is :version_mismatch" do
      s3_stub(fn conn, _body -> Plug.Conn.send_resp(conn, 412, "") end)

      {mod, opts} = s3_opts("obj-s3-get-412")

      assert {:error, :version_mismatch} =
               mod.get_object(:i, "docs/a.txt", opts ++ [if: {:version, "old"}])
    end

    test "get_object past the object's end is :range_not_satisfiable" do
      s3_stub(fn conn, _body -> Plug.Conn.send_resp(conn, 416, "") end)

      {mod, opts} = s3_opts("obj-s3-get-416")

      assert {:error, :range_not_satisfiable} =
               mod.get_object(:i, "docs/a.txt", opts ++ [range: {100, 199}])
    end

    test "get_object on a missing key is :not_found" do
      s3_stub(fn conn, _body -> Plug.Conn.send_resp(conn, 404, "") end)

      {mod, opts} = s3_opts("obj-s3-get-404")
      assert {:error, :not_found} = mod.get_object(:i, "docs/missing.txt", opts)
    end

    test "get_object maps a body past max_bytes to :too_large" do
      s3_stub(fn conn, _body -> Plug.Conn.send_resp(conn, 200, String.duplicate("x", 64)) end)

      {mod, opts} = s3_opts("obj-s3-get-capped")
      assert {:error, :too_large} = mod.get_object(:i, "docs/big", opts ++ [max_bytes: 16])
    end

    test "put_object sends content type, metadata and the absent condition, and returns the ETag" do
      s3_stub(fn conn, _body ->
        conn
        |> Plug.Conn.put_resp_header("etag", ~s("v2"))
        |> Plug.Conn.send_resp(200, "")
      end)

      {mod, opts} = s3_opts("obj-s3-put")

      assert {:ok, "v2"} =
               mod.put_object(
                 :i,
                 "docs/a.txt",
                 "hello",
                 opts ++ [content_type: "text/plain", metadata: %{"Color" => "blue"}, if: :absent]
               )

      assert [{"PUT", "/b/docs/a.txt", "", headers, "hello"}] = s3_requests()
      assert headers["content-type"] == "text/plain"
      assert headers["x-amz-meta-color"] == "blue"
      assert headers["if-none-match"] == "*"
    end

    test "put_object under a version condition sends If-Match" do
      s3_stub(fn conn, _body -> Plug.Conn.send_resp(conn, 412, "") end)

      {mod, opts} = s3_opts("obj-s3-put-version")

      assert {:error, :version_mismatch} =
               mod.put_object(:i, "docs/a.txt", "hello", opts ++ [if: {:version, "v1"}])

      assert [{"PUT", _, _, headers, _}] = s3_requests()
      assert headers["if-match"] == ~s("v1")
    end

    test "put_object maps 412 to :already_exists under the absent condition" do
      s3_stub(fn conn, _body -> Plug.Conn.send_resp(conn, 412, "") end)

      {mod, opts} = s3_opts("obj-s3-put-absent")

      assert {:error, :already_exists} =
               mod.put_object(:i, "docs/a.txt", "hello", opts ++ [if: :absent])
    end

    test "delete_object is :ok, and a missing key is :ok too" do
      s3_stub(fn conn, _body -> Plug.Conn.send_resp(conn, 204, "") end)

      {mod, opts} = s3_opts("obj-s3-delete")
      assert :ok = mod.delete_object(:i, "docs/a.txt", opts)
      assert [{"DELETE", "/b/docs/a.txt", "", _headers, ""}] = s3_requests()

      s3_stub(fn conn, _body -> Plug.Conn.send_resp(conn, 404, "") end)
      assert :ok = mod.delete_object(:i, "docs/missing.txt", opts)
    end

    test "list_page sends the ListObjectsV2 parameters and decodes a page" do
      s3_stub(fn conn, _body -> Plug.Conn.send_resp(conn, 200, @s3_page) end)

      {mod, opts} = s3_opts("obj-s3-list")

      assert {:ok, page} =
               mod.list_page(:i, "docs/", opts ++ [cursor: "tok-1", limit: 2, delimiter: "/"])

      assert page == %{
               objects: [%{key: "docs/a.txt", size: 5}, %{key: "docs/b.txt", size: 7}],
               prefixes: ["docs/team/"],
               next: "tok-2"
             }

      assert [{"GET", "/b", query, _headers, ""}] = s3_requests()

      assert URI.decode_query(query) == %{
               "list-type" => "2",
               "prefix" => "docs/",
               "max-keys" => "2",
               "delimiter" => "/",
               "continuation-token" => "tok-1"
             }
    end

    test "list_page's first page carries no continuation token" do
      s3_stub(fn conn, _body ->
        Plug.Conn.send_resp(
          conn,
          200,
          "<ListBucketResult><Contents><Key>a</Key><Size>1</Size></Contents></ListBucketResult>"
        )
      end)

      {mod, opts} = s3_opts("obj-s3-list-first")

      assert {:ok, %{objects: [%{key: "a", size: 1}], prefixes: [], next: nil}} =
               mod.list_page(:i, "", opts)

      assert [{"GET", _, query, _, _}] = s3_requests()
      refute Map.has_key?(URI.decode_query(query), "continuation-token")
    end

    test "a multipart put_object carries metadata on the start and the condition when completing" do
      s3_multipart_stub(
        "up-obj",
        "<CompleteMultipartUploadResult><ETag>\"multi\"</ETag></CompleteMultipartUploadResult>"
      )

      {mod, opts} = s3_opts("obj-s3-multipart", multipart_threshold_bytes: 16)

      assert {:ok, "multi"} =
               mod.put_object(
                 :i,
                 "docs/big",
                 String.duplicate("x", 32),
                 opts ++ [content_type: "text/plain", metadata: %{"color" => "blue"}, if: :absent]
               )

      requests = s3_requests()

      assert [{"POST", "/b/docs/big", start_query, start_headers, _body} | _rest] = requests
      assert start_query =~ "uploads"
      assert start_headers["content-type"] == "text/plain"
      assert start_headers["x-amz-meta-color"] == "blue"

      assert {_method, _path, _query, complete_headers, _body} =
               Enum.find(requests, &s3_completion?(&1, "up-obj"))

      assert complete_headers["if-none-match"] == "*"
      refute s3_method?(requests, "HEAD")
    end

    test "a multipart put_object under a version condition sends If-Match when completing" do
      s3_multipart_stub("up-version", nil, 412)

      {mod, opts} = s3_opts("obj-s3-multipart-version", multipart_threshold_bytes: 16)

      assert {:error, :version_mismatch} =
               mod.put_object(
                 :i,
                 "docs/big",
                 String.duplicate("x", 32),
                 opts ++ [content_type: "text/plain", if: {:version, "v7"}]
               )

      requests = s3_requests()

      assert {_method, _path, _query, complete_headers, _body} =
               Enum.find(requests, &s3_completion?(&1, "up-version"))

      assert complete_headers["if-match"] == ~s("v7")
      refute s3_method?(requests, "HEAD")
    end

    test "a multipart put_object whose completing request answers 412 under :absent is :already_exists" do
      s3_multipart_stub("up-absent", nil, 412)

      {mod, opts} = s3_opts("obj-s3-multipart-absent", multipart_threshold_bytes: 16)

      assert {:error, :already_exists} =
               mod.put_object(:i, "docs/big", String.duplicate("x", 32), opts ++ [if: :absent])
    end

    test "put_object's metadata is what head_object reports" do
      s3_stub(fn conn, body ->
        if conn.method == "PUT" do
          headers = Adapters.headers(conn)

          metadata =
            for {name, value} <- headers,
                String.starts_with?(name, "x-amz-meta-"),
                into: %{},
                do: {String.replace_prefix(name, "x-amz-meta-", ""), value}

          Process.put(:s3_object, %{
            content_type: headers["content-type"],
            metadata: metadata,
            size: byte_size(body)
          })

          conn
          |> Plug.Conn.put_resp_header("etag", ~s("v1"))
          |> Plug.Conn.send_resp(200, "")
        else
          stored = Process.get(:s3_object)

          Enum.reduce(stored.metadata, conn, fn {name, value}, conn ->
            Plug.Conn.put_resp_header(conn, "x-amz-meta-" <> name, value)
          end)
          |> Plug.Conn.put_resp_header("etag", ~s("v1"))
          |> Plug.Conn.put_resp_header("content-type", stored.content_type)
          |> Plug.Conn.put_resp_header("content-length", Integer.to_string(stored.size))
          |> Plug.Conn.send_resp(200, "")
        end
      end)

      {mod, opts} = s3_opts("obj-s3-roundtrip")

      assert {:ok, "v1"} =
               mod.put_object(
                 :i,
                 "docs/a.txt",
                 "hello",
                 opts ++ [content_type: "text/plain", metadata: %{"color" => "blue"}]
               )

      assert {:ok, meta} = mod.head_object(:i, "docs/a.txt", opts)
      assert meta.size == 5
      assert meta.content_type == "text/plain"
      assert meta.metadata == %{"color" => "blue"}
      assert meta.version == "v1"
    end
  end

  # Create, one part, complete: the completing response's body (or status) is
  # what the test controls.
  defp s3_multipart_stub(upload_id, complete_body, complete_status \\ 200) do
    s3_stub(fn conn, _body ->
      query = Adapters.query(conn)

      cond do
        conn.method == "POST" and Map.has_key?(query, "uploads") ->
          Plug.Conn.send_resp(
            conn,
            200,
            "<InitiateMultipartUploadResult><UploadId>#{upload_id}</UploadId></InitiateMultipartUploadResult>"
          )

        conn.method == "PUT" and Map.has_key?(query, "partNumber") ->
          conn
          |> Plug.Conn.put_resp_header("etag", ~s("part-1"))
          |> Plug.Conn.send_resp(200, "")

        conn.method == "POST" and query["uploadId"] == upload_id ->
          Plug.Conn.send_resp(conn, complete_status, complete_body || "")

        conn.method == "DELETE" and query["uploadId"] == upload_id ->
          Plug.Conn.send_resp(conn, 204, "")
      end
    end)
  end

  # ── GCS ────────────────────────────────────────────────────────────────────

  describe "gcs object operations" do
    test "head_object reads the metadata document" do
      gcs_stub(fn conn, _body ->
        Plug.Conn.send_resp(
          conn,
          200,
          JSON.encode!(%{
            "name" => "docs/a.txt",
            "size" => "42",
            "generation" => "17",
            "contentType" => "text/plain",
            "updated" => "1994-11-06T08:49:37.000Z",
            "metadata" => %{"color" => "blue"}
          })
        )
      end)

      {mod, opts} = gcs_opts("obj-gcs-head")

      assert {:ok, meta} = mod.head_object(:i, "docs/a.txt", opts)

      assert meta == %{
               size: 42,
               version: "17",
               content_type: "text/plain",
               last_modified: @last_modified_unix,
               metadata: %{"color" => "blue"}
             }

      assert [{"GET", "/storage/v1/b/b/o/docs%2Fa.txt", "", _headers, ""}] = gcs_requests()
    end

    test "head_object on a missing key is :not_found" do
      gcs_stub(fn conn, _body -> Plug.Conn.send_resp(conn, 404, "") end)

      {mod, opts} = gcs_opts("obj-gcs-head-missing")
      assert {:error, :not_found} = mod.head_object(:i, "docs/missing.txt", opts)
    end

    test "get_object reads a range under ifGenerationMatch and takes the version from the response" do
      gcs_stub(fn conn, _body ->
        conn
        |> Plug.Conn.put_resp_header("content-range", "bytes 10-19/100")
        |> Plug.Conn.put_resp_header("x-goog-generation", "17")
        |> Plug.Conn.put_resp_header("content-type", "text/plain")
        |> Plug.Conn.put_resp_header("last-modified", @last_modified)
        |> Plug.Conn.put_resp_header("x-goog-meta-color", "blue")
        |> Plug.Conn.send_resp(206, "0123456789")
      end)

      {mod, opts} = gcs_opts("obj-gcs-get-range")

      assert {:ok, meta, "0123456789"} =
               mod.get_object(:i, "docs/a.txt", opts ++ [range: {10, 19}, if: {:version, "17"}])

      assert meta.size == 100
      assert meta.version == "17"
      assert meta.content_type == "text/plain"
      assert meta.last_modified == @last_modified_unix
      assert meta.metadata == %{"color" => "blue"}

      assert [{"GET", "/storage/v1/b/b/o/docs%2Fa.txt", query, headers, ""}] = gcs_requests()
      assert URI.decode_query(query) == %{"alt" => "media", "ifGenerationMatch" => "17"}
      assert headers["range"] == "bytes=10-19"
    end

    test "get_object reads the metadata document when the media response has no generation" do
      gcs_stub(fn conn, _body ->
        if Adapters.query(conn)["alt"] == "media" do
          Plug.Conn.send_resp(conn, 200, "hello")
        else
          Plug.Conn.send_resp(
            conn,
            200,
            JSON.encode!(%{
              "name" => "docs/a.txt",
              "size" => "5",
              "generation" => "18",
              "contentType" => "text/plain",
              "updated" => "1994-11-06T08:49:37.000Z",
              "metadata" => %{"color" => "blue"}
            })
          )
        end
      end)

      {mod, opts} = gcs_opts("obj-gcs-get-fallback")

      assert {:ok, meta, "hello"} = mod.get_object(:i, "docs/a.txt", opts)
      assert meta.version == "18"
      assert meta.size == 5
      assert meta.content_type == "text/plain"
      assert meta.metadata == %{"color" => "blue"}

      assert [{_, _, "alt=media", _, _}, {_, _, "", _, _}] = gcs_requests()
    end

    test "get_object with a stale generation is :version_mismatch" do
      gcs_stub(fn conn, _body -> Plug.Conn.send_resp(conn, 412, "") end)

      {mod, opts} = gcs_opts("obj-gcs-get-412")

      assert {:error, :version_mismatch} =
               mod.get_object(:i, "docs/a.txt", opts ++ [if: {:version, "1"}])
    end

    test "get_object past the object's end is :range_not_satisfiable" do
      gcs_stub(fn conn, _body -> Plug.Conn.send_resp(conn, 416, "") end)

      {mod, opts} = gcs_opts("obj-gcs-get-416")

      assert {:error, :range_not_satisfiable} =
               mod.get_object(:i, "docs/a.txt", opts ++ [range: {100, 199}])
    end

    test "get_object on a missing key is :not_found" do
      gcs_stub(fn conn, _body -> Plug.Conn.send_resp(conn, 404, "") end)

      {mod, opts} = gcs_opts("obj-gcs-get-404")
      assert {:error, :not_found} = mod.get_object(:i, "docs/missing.txt", opts)
    end

    test "get_object maps a body past max_bytes to :too_large" do
      gcs_stub(fn conn, _body ->
        Plug.Conn.send_resp(conn, 200, String.duplicate("x", 64))
      end)

      {mod, opts} = gcs_opts("obj-gcs-get-capped")
      assert {:error, :too_large} = mod.get_object(:i, "docs/big", opts ++ [max_bytes: 16])
    end

    test "put_object uploads a multipart document with content type, metadata and the condition" do
      gcs_stub(fn conn, _body ->
        Plug.Conn.send_resp(conn, 200, JSON.encode!(%{"generation" => "18", "size" => "5"}))
      end)

      {mod, opts} = gcs_opts("obj-gcs-put")

      assert {:ok, "18"} =
               mod.put_object(
                 :i,
                 "docs/a.txt",
                 "hello",
                 opts ++ [content_type: "text/plain", metadata: %{"color" => "blue"}, if: :absent]
               )

      assert [{"POST", "/upload/storage/v1/b/b/o", query, headers, body}] = gcs_requests()

      assert URI.decode_query(query) == %{"uploadType" => "multipart", "ifGenerationMatch" => "0"}
      assert [_, boundary] = Regex.run(~r/boundary="([^"]+)"/, headers["content-type"])

      parts = String.split(body, "--" <> boundary)

      document =
        parts
        |> Enum.find(&String.contains?(&1, "application/json"))
        |> String.split("\r\n\r\n", parts: 2)
        |> List.last()
        |> JSON.decode!()

      assert document == %{
               "name" => "docs/a.txt",
               "contentType" => "text/plain",
               "metadata" => %{"color" => "blue"}
             }

      media = Enum.find(parts, &String.contains?(&1, "content-type: text/plain"))
      assert media =~ "\r\n\r\nhello\r\n"
    end

    test "put_object under a version condition sends ifGenerationMatch and maps 412 to :version_mismatch" do
      gcs_stub(fn conn, _body -> Plug.Conn.send_resp(conn, 412, "") end)

      {mod, opts} = gcs_opts("obj-gcs-put-version")

      assert {:error, :version_mismatch} =
               mod.put_object(:i, "docs/a.txt", "hello", opts ++ [if: {:version, "17"}])

      assert [{"POST", _, query, _, _}] = gcs_requests()
      assert URI.decode_query(query)["ifGenerationMatch"] == "17"
    end

    test "put_object maps 412 to :already_exists under the absent condition" do
      gcs_stub(fn conn, _body -> Plug.Conn.send_resp(conn, 412, "") end)

      {mod, opts} = gcs_opts("obj-gcs-put-absent")

      assert {:error, :already_exists} =
               mod.put_object(:i, "docs/a.txt", "hello", opts ++ [if: :absent])
    end

    test "delete_object is :ok, and a missing key is :ok too" do
      gcs_stub(fn conn, _body -> Plug.Conn.send_resp(conn, 204, "") end)

      {mod, opts} = gcs_opts("obj-gcs-delete")
      assert :ok = mod.delete_object(:i, "docs/a.txt", opts)
      assert [{"DELETE", "/storage/v1/b/b/o/docs%2Fa.txt", "", _headers, ""}] = gcs_requests()

      gcs_stub(fn conn, _body -> Plug.Conn.send_resp(conn, 404, "") end)
      assert :ok = mod.delete_object(:i, "docs/missing.txt", opts)
    end

    test "list_page sends prefix, maxResults, delimiter and the page token" do
      gcs_stub(fn conn, _body ->
        Plug.Conn.send_resp(
          conn,
          200,
          JSON.encode!(%{
            "items" => [
              %{"name" => "docs/a.txt", "size" => "5"},
              %{"name" => "docs/b.txt", "size" => "7"}
            ],
            "prefixes" => ["docs/team/"],
            "nextPageToken" => "tok-2"
          })
        )
      end)

      {mod, opts} = gcs_opts("obj-gcs-list")

      assert {:ok, page} =
               mod.list_page(:i, "docs/", opts ++ [cursor: "tok-1", limit: 2, delimiter: "/"])

      assert page == %{
               objects: [%{key: "docs/a.txt", size: 5}, %{key: "docs/b.txt", size: 7}],
               prefixes: ["docs/team/"],
               next: "tok-2"
             }

      assert [{"GET", "/storage/v1/b/b/o", query, _headers, ""}] = gcs_requests()

      assert URI.decode_query(query) == %{
               "prefix" => "docs/",
               "maxResults" => "2",
               "delimiter" => "/",
               "pageToken" => "tok-1"
             }
    end

    test "a multipart put_object initiates a resumable session with contentType, metadata and the condition" do
      gcs_resumable_stub("obj-gcs-resumable", JSON.encode!(%{"generation" => "19"}))

      {mod, opts} = gcs_opts("obj-gcs-resumable", multipart_threshold_bytes: 16)

      assert {:ok, "19"} =
               mod.put_object(
                 :i,
                 "docs/big",
                 String.duplicate("x", 32),
                 opts ++ [content_type: "text/plain", metadata: %{"color" => "blue"}, if: :absent]
               )

      requests = gcs_requests()

      assert [{"POST", "/upload/storage/v1/b/b/o", query, headers, init_body} | _rest] = requests
      assert URI.decode_query(query)["uploadType"] == "resumable"
      assert URI.decode_query(query)["ifGenerationMatch"] == "0"
      assert headers["x-upload-content-type"] == "text/plain"

      assert JSON.decode!(init_body) == %{
               "name" => "docs/big",
               "contentType" => "text/plain",
               "metadata" => %{"color" => "blue"}
             }

      refute gcs_method?(requests, "HEAD")
      refute gcs_method?(requests, "GET")
    end

    test "a multipart put_object whose final chunk answers 412 is :already_exists under :absent" do
      gcs_resumable_stub("obj-gcs-resumable-absent", JSON.encode!(%{"generation" => "19"}), 412)

      {mod, opts} = gcs_opts("obj-gcs-resumable-absent", multipart_threshold_bytes: 16)

      assert {:error, :already_exists} =
               mod.put_object(:i, "docs/big", String.duplicate("x", 32), opts ++ [if: :absent])
    end

    test "a multipart put_object whose final chunk answers 412 is :version_mismatch under a version condition" do
      gcs_resumable_stub("obj-gcs-resumable-version", JSON.encode!(%{"generation" => "19"}), 412)

      {mod, opts} = gcs_opts("obj-gcs-resumable-version", multipart_threshold_bytes: 16)

      assert {:error, :version_mismatch} =
               mod.put_object(
                 :i,
                 "docs/big",
                 String.duplicate("x", 32),
                 opts ++ [if: {:version, "17"}]
               )

      assert [{"POST", _, query, _, _} | _rest] = gcs_requests()
      assert URI.decode_query(query)["ifGenerationMatch"] == "17"
    end

    test "put_object's metadata is what head_object reports" do
      gcs_stub(fn conn, body ->
        if conn.method == "POST" do
          document =
            body
            |> String.split("--" <> gcs_boundary_of(conn))
            |> Enum.find(&String.contains?(&1, "application/json"))
            |> String.split("\r\n\r\n", parts: 2)
            |> List.last()
            |> String.trim()
            |> JSON.decode!()

          Process.put(:gcs_object, Map.put(document, "generation", "18"))

          Plug.Conn.send_resp(conn, 200, JSON.encode!(%{"generation" => "18"}))
        else
          document = Process.get(:gcs_object)
          size = byte_size("hello")

          Plug.Conn.send_resp(
            conn,
            200,
            JSON.encode!(
              Map.merge(document, %{
                "size" => Integer.to_string(size),
                "updated" => "1994-11-06T08:49:37.000Z"
              })
            )
          )
        end
      end)

      {mod, opts} = gcs_opts("obj-gcs-roundtrip")

      assert {:ok, "18"} =
               mod.put_object(
                 :i,
                 "docs/a.txt",
                 "hello",
                 opts ++ [content_type: "text/plain", metadata: %{"color" => "blue"}]
               )

      assert {:ok, meta} = mod.head_object(:i, "docs/a.txt", opts)
      assert meta.size == 5
      assert meta.version == "18"
      assert meta.content_type == "text/plain"
      assert meta.metadata == %{"color" => "blue"}
    end
  end

  # An initiation answered `location`, then the final chunk's response is what
  # the test controls.
  defp gcs_resumable_stub(session, final_body, final_status \\ 200) do
    gcs_stub(fn conn, _body ->
      query = Adapters.query(conn)

      cond do
        conn.method == "POST" and query["uploadType"] == "resumable" ->
          conn
          |> Plug.Conn.put_resp_header("location", "http://#{conn.host}/upload/#{session}")
          |> Plug.Conn.send_resp(200, "")

        conn.method == "PUT" and conn.request_path == "/upload/#{session}" ->
          Plug.Conn.send_resp(conn, final_status, final_body)

        conn.method == "DELETE" and conn.request_path == "/upload/#{session}" ->
          Plug.Conn.send_resp(conn, 204, "")
      end
    end)
  end

  defp gcs_boundary_of(conn) do
    [_, boundary] =
      Regex.run(~r/boundary="([^"]+)"/, Adapters.headers(conn)["content-type"])

    boundary
  end

  # ── Azure and OCI ──────────────────────────────────────────────────────────

  @azure_stub :ankusa_azure_objects_stub
  @oci_stub :ankusa_oci_objects_stub

  @azure_page """
  <?xml version="1.0" encoding="utf-8"?>
  <EnumerationResults ContainerName="c">
    <Blobs>
      <Blob><Name>docs/a.txt</Name><Properties><Content-Length>5</Content-Length></Properties></Blob>
      <BlobPrefix><Name>docs/sub/</Name></BlobPrefix>
    </Blobs>
    <NextMarker>page-2</NextMarker>
  </EnumerationResults>
  """

  @azure_last_page """
  <?xml version="1.0" encoding="utf-8"?>
  <EnumerationResults ContainerName="c">
    <Blobs>
      <Blob><Name>docs/b.txt</Name><Properties><Content-Length>7</Content-Length></Properties></Blob>
    </Blobs>
    <NextMarker></NextMarker>
  </EnumerationResults>
  """

  @oci_page ~s({"objects":[{"name":"docs/a.txt","size":5}],"prefixes":["docs/sub/"],"nextStartWith":"page-2"})
  @oci_last_page ~s({"objects":[{"name":"docs/b.txt","size":7}],"prefixes":[],"nextStartWith":null})

  # ── helpers ───────────────────────────────────────────────────────────────

  defp azure_serve(fun) do
    test = self()

    Req.Test.stub(@azure_stub, fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn, length: 16 * 1_048_576)

      send(
        test,
        {:azure_request, conn.method, conn.request_path, conn.query_string,
         Adapters.headers(conn), body}
      )

      fun.(conn, body)
    end)
  end

  defp azure_requests do
    receive do
      {:azure_request, method, path, query, headers, body} ->
        [{method, path, query, headers, body} | azure_requests()]
    after
      0 -> []
    end
  end

  defp azure_opts(tag, extra \\ []),
    do: Adapters.opts(Azure, tag, @azure_stub, extra)

  defp oci_serve(fun) do
    test = self()

    Req.Test.stub(@oci_stub, fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn, length: 16 * 1_048_576)

      send(
        test,
        {:oci_request, conn.method, conn.request_path, conn.query_string, Adapters.headers(conn),
         body}
      )

      fun.(conn, body)
    end)
  end

  defp oci_requests do
    receive do
      {:oci_request, method, path, query, headers, body} ->
        [{method, path, query, headers, body} | oci_requests()]
    after
      0 -> []
    end
  end

  defp oci_opts(tag, extra \\ []), do: Adapters.opts(OCI, tag, @oci_stub, extra)

  defp respond(conn, status, body, headers) do
    conn =
      Enum.reduce(headers, conn, fn {name, value}, conn ->
        Plug.Conn.put_resp_header(conn, header_name(name), value)
      end)

    Plug.Conn.send_resp(conn, status, body)
  end

  defp header_name(name) when is_atom(name),
    do: name |> Atom.to_string() |> String.replace("_", "-")

  defp header_name(name) when is_binary(name), do: name

  defp md5(body), do: Base.encode64(:crypto.hash(:md5, body))

  # ── azure ─────────────────────────────────────────────────────────────────

  describe "azure object operations" do
    test "head_object reads the blob's size, version, type and metadata" do
      azure_serve(fn conn, _body ->
        respond(conn, 200, "",
          "content-length": "12",
          etag: ~s("0x8D1"),
          "content-type": "text/plain",
          "last-modified": "Sun, 06 Nov 1994 08:49:37 GMT",
          x_ms_meta_owner: "alice"
        )
      end)

      {_mod, opts} = azure_opts("az-object-head")

      assert {:ok, meta} = Azure.head_object(:i, "docs/a.txt", opts)
      assert meta.size == 12
      assert meta.version == "0x8D1"
      assert meta.content_type == "text/plain"
      assert meta.last_modified == 784_111_777
      assert meta.metadata == %{"owner" => "alice"}

      assert [{"HEAD", "/acct/c/docs/a.txt", "", headers, ""}] = azure_requests()
      assert headers["x-ms-version"] == "2024-11-04"
    end

    test "head_object on a missing blob is :not_found" do
      azure_serve(fn conn, _body -> Plug.Conn.send_resp(conn, 404, "") end)

      {_mod, opts} = azure_opts("az-object-head-missing")
      assert {:error, :not_found} = Azure.head_object(:i, "docs/none", opts)
    end

    test "get_object reads the whole blob and checks its md5" do
      body = "hello world"

      azure_serve(fn conn, _sent ->
        respond(conn, 200, body,
          etag: ~s("v1"),
          "content-length": Integer.to_string(byte_size(body)),
          content_type: "text/plain",
          content_md5: md5(body)
        )
      end)

      {_mod, opts} = azure_opts("az-object-get")

      assert {:ok, meta, ^body} = Azure.get_object(:i, "docs/a.txt", opts)
      assert meta.size == byte_size(body)
      assert meta.version == "v1"
      assert meta.content_type == "text/plain"

      assert [{"GET", "/acct/c/docs/a.txt", "", headers, ""}] = azure_requests()
      refute Map.has_key?(headers, "range")
      refute Map.has_key?(headers, "if-match")
    end

    test "get_object of a whole blob whose md5 is wrong is an :integrity error" do
      azure_serve(fn conn, _sent ->
        respond(conn, 200, "tampered", content_md5: md5("original"))
      end)

      {_mod, opts} = azure_opts("az-object-get-bad-md5")
      assert {:error, :integrity} = Azure.get_object(:i, "docs/a.txt", opts)
    end

    test "get_object with :range sends the byte window and reports the whole size" do
      azure_serve(fn conn, _sent ->
        respond(conn, 206, "0123",
          etag: ~s("v1"),
          content_range: "bytes 10-13/99",
          content_length: "4",
          # a window's bytes never match the whole blob's digest; it is not checked
          content_md5: md5("not the window")
        )
      end)

      {_mod, opts} = azure_opts("az-object-get-range")

      assert {:ok, meta, "0123"} =
               Azure.get_object(:i, "docs/a.txt", opts ++ [range: {10, 13}])

      assert meta.size == 99

      assert [{"GET", _path, "", headers, ""}] = azure_requests()
      assert headers["range"] == "bytes=10-13"
    end

    test "get_object with :if sends if-match, and a 412 is :version_mismatch" do
      azure_serve(fn conn, _sent ->
        if Adapters.headers(conn)["if-match"] == ~s("v2") do
          Plug.Conn.send_resp(conn, 412, "")
        else
          Plug.Conn.send_resp(conn, 200, "unexpected")
        end
      end)

      {_mod, opts} = azure_opts("az-object-get-if")

      assert {:error, :version_mismatch} =
               Azure.get_object(:i, "docs/a.txt", opts ++ [if: {:version, "v2"}])

      assert [{"GET", _path, "", headers, _body}] = azure_requests()
      assert headers["if-match"] == ~s("v2")
    end

    test "get_object on a range past the end is :range_not_satisfiable" do
      azure_serve(fn conn, _sent -> Plug.Conn.send_resp(conn, 416, "") end)

      {_mod, opts} = azure_opts("az-object-get-416")

      assert {:error, :range_not_satisfiable} =
               Azure.get_object(:i, "docs/a.txt", opts ++ [range: {90, 99}])
    end

    test "get_object with :max_bytes refuses a body past it" do
      azure_serve(fn conn, _sent ->
        Plug.Conn.send_resp(conn, 200, String.duplicate("x", 4096))
      end)

      {_mod, opts} = azure_opts("az-object-get-max")

      assert {:error, :too_large} =
               Azure.get_object(:i, "docs/a.txt", opts ++ [max_bytes: 1024])
    end

    test "put_object sends the content type and metadata, and returns the new version" do
      azure_serve(fn conn, _sent ->
        respond(conn, 201, "", etag: ~s("0xNEW"))
      end)

      {_mod, opts} = azure_opts("az-object-put")

      assert {:ok, "0xNEW"} =
               Azure.put_object(
                 :i,
                 "docs/a.txt",
                 "hello",
                 opts ++ [content_type: "text/plain", metadata: %{"owner" => "alice"}]
               )

      assert [{"PUT", "/acct/c/docs/a.txt", "", headers, "hello"}] = azure_requests()
      assert headers["x-ms-blob-type"] == "BlockBlob"
      assert headers["content-type"] == "text/plain"
      assert headers["x-ms-meta-owner"] == "alice"
      refute Map.has_key?(headers, "if-none-match")
      refute Map.has_key?(headers, "if-match")
    end

    test "put_object writes the default content type when none is given" do
      azure_serve(fn conn, _sent -> respond(conn, 201, "", etag: ~s("0xNEW")) end)

      {_mod, opts} = azure_opts("az-object-put-default")
      assert {:ok, _version} = Azure.put_object(:i, "docs/a.txt", "hello", opts)

      assert [{"PUT", _path, "", headers, "hello"}] = azure_requests()
      assert headers["content-type"] == "application/octet-stream"
    end

    test "put_object maps a losing condition by the condition it sent" do
      azure_serve(fn conn, _sent ->
        case Adapters.headers(conn) do
          %{"if-none-match" => "*"} -> Plug.Conn.send_resp(conn, 412, "")
          _version_condition -> Plug.Conn.send_resp(conn, 409, "")
        end
      end)

      {_mod, opts} = azure_opts("az-object-put-condition")

      assert {:error, :already_exists} =
               Azure.put_object(:i, "docs/a.txt", "x", opts ++ [if: :absent])

      assert {:error, :version_mismatch} =
               Azure.put_object(:i, "docs/a.txt", "x", opts ++ [if: {:version, "v1"}])

      assert [{"PUT", _, "", absent_headers, _}, {"PUT", _, "", version_headers, _}] =
               azure_requests()

      assert absent_headers["if-none-match"] == "*"
      assert version_headers["if-match"] == ~s("v1")
    end

    test "put_object above the threshold carries type, metadata and condition on the Put Block List" do
      azure_serve(fn conn, _sent ->
        case URI.decode_query(conn.query_string)["comp"] do
          "block" -> Plug.Conn.send_resp(conn, 201, "")
          "blocklist" -> respond(conn, 201, "", etag: ~s("0xMP"))
        end
      end)

      {_mod, opts} = azure_opts("az-object-put-multipart", multipart_threshold_bytes: 4)

      assert {:ok, "0xMP"} =
               Azure.put_object(
                 :i,
                 "docs/big",
                 "0123456789",
                 opts ++
                   [content_type: "text/plain", metadata: %{"owner" => "alice"}, if: :absent]
               )

      requests = azure_requests()

      assert [{"PUT", _, block_query, _block_headers, _}, {"PUT", _, list_query, list_headers, _}] =
               requests

      assert URI.decode_query(block_query)["comp"] == "block"
      assert URI.decode_query(list_query)["comp"] == "blocklist"
      assert list_headers["x-ms-blob-content-type"] == "text/plain"
      assert list_headers["x-ms-blob-content-md5"] == md5("0123456789")
      assert list_headers["x-ms-meta-owner"] == "alice"
      assert list_headers["if-none-match"] == "*"

      refute Enum.any?(requests, fn {method, _path, _query, _headers, _body} ->
               method == "HEAD"
             end)
    end

    test "put_object whose Put Block List answers 412 maps by the condition it sent" do
      azure_serve(fn conn, _sent ->
        case URI.decode_query(conn.query_string)["comp"] do
          "block" -> Plug.Conn.send_resp(conn, 201, "")
          "blocklist" -> Plug.Conn.send_resp(conn, 412, "")
        end
      end)

      {_mod, opts} = azure_opts("az-object-put-multipart-cond", multipart_threshold_bytes: 4)

      assert {:error, :already_exists} =
               Azure.put_object(:i, "docs/big", "0123456789", opts ++ [if: :absent])

      assert {:error, :version_mismatch} =
               Azure.put_object(:i, "docs/big", "0123456789", opts ++ [if: {:version, "v1"}])
    end

    test "get_object on a missing blob is :not_found" do
      azure_serve(fn conn, _sent -> Plug.Conn.send_resp(conn, 404, "") end)

      {_mod, opts} = azure_opts("az-object-get-missing")
      assert {:error, :not_found} = Azure.get_object(:i, "docs/none", opts)
    end

    test "delete_object is :ok for a delete and for a blob that is already gone" do
      azure_serve(fn conn, _sent ->
        if String.ends_with?(conn.request_path, "gone") do
          Plug.Conn.send_resp(conn, 404, "")
        else
          Plug.Conn.send_resp(conn, 202, "")
        end
      end)

      {_mod, opts} = azure_opts("az-object-delete")

      assert :ok = Azure.delete_object(:i, "docs/a.txt", opts)
      assert :ok = Azure.delete_object(:i, "docs/gone", opts)

      assert [{"DELETE", _, "", _, ""}, {"DELETE", _, "", _, ""}] = azure_requests()
    end

    test "list_page sends comp=list with the page's parameters and parses objects, prefixes and the cursor" do
      azure_serve(fn conn, _sent -> Plug.Conn.send_resp(conn, 200, @azure_page) end)

      {_mod, opts} = azure_opts("az-object-list")

      assert {:ok, page} =
               Azure.list_page(:i, "docs/", opts ++ [limit: 2, delimiter: "/", cursor: "page-1"])

      assert page.objects == [%{key: "docs/a.txt", size: 5}]
      assert page.prefixes == ["docs/sub/"]
      assert page.next == "page-2"

      assert [{"GET", "/acct/c", query, _headers, ""}] = azure_requests()

      assert URI.decode_query(query) == %{
               "restype" => "container",
               "comp" => "list",
               "prefix" => "docs/",
               "maxresults" => "2",
               "delimiter" => "/",
               "marker" => "page-1"
             }
    end

    test "list_page's last page has no cursor, and the defaults send no delimiter" do
      azure_serve(fn conn, _sent -> Plug.Conn.send_resp(conn, 200, @azure_last_page) end)

      {_mod, opts} = azure_opts("az-object-list-last")

      assert {:ok, page} = Azure.list_page(:i, "docs/", opts)
      assert page.objects == [%{key: "docs/b.txt", size: 7}]
      assert page.prefixes == []
      assert page.next == nil

      assert [{"GET", _path, query, _headers, ""}] = azure_requests()
      params = URI.decode_query(query)
      assert params["maxresults"] == "1000"
      refute Map.has_key?(params, "delimiter")
      refute Map.has_key?(params, "marker")
    end
  end

  # ── oci ───────────────────────────────────────────────────────────────────

  describe "oci object operations" do
    test "head_object reads the object's size, version, type and metadata" do
      oci_serve(fn conn, _body ->
        respond(conn, 200, "",
          "content-length": "12",
          etag: ~s("etag-1"),
          "content-type": "text/plain",
          "last-modified": "Sun, 06 Nov 1994 08:49:37 GMT",
          opc_meta_owner: "alice"
        )
      end)

      {_mod, opts} = oci_opts("oci-object-head")

      assert {:ok, meta} = OCI.head_object(:i, "docs/a.txt", opts)
      assert meta.size == 12
      assert meta.version == "etag-1"
      assert meta.content_type == "text/plain"
      assert meta.last_modified == 784_111_777
      assert meta.metadata == %{"owner" => "alice"}

      assert [{"HEAD", "/n/ns/b/b/o/docs/a.txt", "", headers, ""}] = oci_requests()
      assert headers["authorization"] =~ ~r{keyId=}
    end

    test "head_object on a missing object is :not_found" do
      oci_serve(fn conn, _body -> Plug.Conn.send_resp(conn, 404, "") end)

      {_mod, opts} = oci_opts("oci-object-head-missing")
      assert {:error, :not_found} = OCI.head_object(:i, "docs/none", opts)
    end

    test "get_object reads the whole object and checks its opc-content-md5" do
      body = "hello world"

      oci_serve(fn conn, _sent ->
        respond(conn, 200, body,
          etag: ~s("etag-1"),
          "content-length": Integer.to_string(byte_size(body)),
          content_type: "text/plain",
          opc_content_md5: md5(body)
        )
      end)

      {_mod, opts} = oci_opts("oci-object-get")

      assert {:ok, meta, ^body} = OCI.get_object(:i, "docs/a.txt", opts)
      assert meta.size == byte_size(body)
      assert meta.version == "etag-1"

      assert [{"GET", "/n/ns/b/b/o/docs/a.txt", "", headers, ""}] = oci_requests()
      refute Map.has_key?(headers, "range")
    end

    test "get_object of a whole object whose md5 is wrong is an :integrity error" do
      oci_serve(fn conn, _sent ->
        respond(conn, 200, "tampered", opc_content_md5: md5("original"))
      end)

      {_mod, opts} = oci_opts("oci-object-get-bad-md5")
      assert {:error, :integrity} = OCI.get_object(:i, "docs/a.txt", opts)
    end

    test "get_object with :range sends the byte window and reports the whole size" do
      oci_serve(fn conn, _sent ->
        respond(conn, 206, "0123",
          etag: ~s("etag-1"),
          content_range: "bytes 10-13/99",
          content_length: "4",
          opc_content_md5: md5("not the window")
        )
      end)

      {_mod, opts} = oci_opts("oci-object-get-range")

      assert {:ok, meta, "0123"} =
               OCI.get_object(:i, "docs/a.txt", opts ++ [range: {10, 13}])

      assert meta.size == 99

      assert [{"GET", _path, "", headers, ""}] = oci_requests()
      assert headers["range"] == "bytes=10-13"
    end

    test "get_object with :if sends if-match, and a 412 is :version_mismatch" do
      oci_serve(fn conn, _sent ->
        if Adapters.headers(conn)["if-match"] == ~s("etag-2") do
          Plug.Conn.send_resp(conn, 412, "")
        else
          Plug.Conn.send_resp(conn, 200, "unexpected")
        end
      end)

      {_mod, opts} = oci_opts("oci-object-get-if")

      assert {:error, :version_mismatch} =
               OCI.get_object(:i, "docs/a.txt", opts ++ [if: {:version, "etag-2"}])

      assert [{"GET", _path, "", headers, _body}] = oci_requests()
      assert headers["if-match"] == ~s("etag-2")
    end

    test "get_object on a range past the end is :range_not_satisfiable" do
      oci_serve(fn conn, _sent -> Plug.Conn.send_resp(conn, 416, "") end)

      {_mod, opts} = oci_opts("oci-object-get-416")

      assert {:error, :range_not_satisfiable} =
               OCI.get_object(:i, "docs/a.txt", opts ++ [range: {90, 99}])
    end

    test "get_object with :max_bytes refuses a body past it" do
      oci_serve(fn conn, _sent ->
        Plug.Conn.send_resp(conn, 200, String.duplicate("x", 4096))
      end)

      {_mod, opts} = oci_opts("oci-object-get-max")

      assert {:error, :too_large} = OCI.get_object(:i, "docs/a.txt", opts ++ [max_bytes: 1024])
    end

    test "put_object sends the content type and metadata, and returns the new version" do
      oci_serve(fn conn, _sent -> respond(conn, 200, "", etag: ~s("etag-new")) end)

      {_mod, opts} = oci_opts("oci-object-put")

      assert {:ok, "etag-new"} =
               OCI.put_object(
                 :i,
                 "docs/a.txt",
                 "hello",
                 opts ++ [content_type: "text/plain", metadata: %{"owner" => "alice"}]
               )

      assert [{"PUT", "/n/ns/b/b/o/docs/a.txt", "", headers, "hello"}] = oci_requests()
      assert headers["content-type"] == "text/plain"
      assert headers["opc-meta-owner"] == "alice"
      assert headers["content-md5"] == md5("hello")
      assert headers["x-content-sha256"] == Base.encode64(:crypto.hash(:sha256, "hello"))
      refute Map.has_key?(headers, "if-none-match")
      refute Map.has_key?(headers, "if-match")
    end

    test "put_object maps a losing condition by the condition it sent" do
      oci_serve(fn conn, _sent ->
        case Adapters.headers(conn) do
          %{"if-none-match" => "*"} -> Plug.Conn.send_resp(conn, 412, "")
          _version_condition -> Plug.Conn.send_resp(conn, 409, "")
        end
      end)

      {_mod, opts} = oci_opts("oci-object-put-condition")

      assert {:error, :already_exists} =
               OCI.put_object(:i, "docs/a.txt", "x", opts ++ [if: :absent])

      assert {:error, :version_mismatch} =
               OCI.put_object(:i, "docs/a.txt", "x", opts ++ [if: {:version, "etag-1"}])

      assert [{"PUT", _, "", absent_headers, _}, {"PUT", _, "", version_headers, _}] =
               oci_requests()

      assert absent_headers["if-none-match"] == "*"
      assert version_headers["if-match"] == ~s("etag-1")
    end

    test "put_object above the threshold starts the upload with type and metadata, and commits under the condition" do
      oci_serve(fn conn, _sent ->
        query = Adapters.query(conn)

        cond do
          conn.method == "POST" and String.ends_with?(conn.request_path, "/b/b/u") ->
            Plug.Conn.send_resp(conn, 200, JSON.encode!(%{"uploadId" => "up-1"}))

          conn.method == "PUT" and Map.has_key?(query, "uploadPartNum") ->
            respond(conn, 200, "", etag: "etag-part")

          conn.method == "POST" and query["uploadId"] == "up-1" ->
            respond(conn, 200, "", etag: ~s("etag-complete"))
        end
      end)

      {_mod, opts} = oci_opts("oci-object-put-multipart", multipart_threshold_bytes: 4)

      assert {:ok, "etag-complete"} =
               OCI.put_object(
                 :i,
                 "docs/big",
                 "0123456789",
                 opts ++
                   [content_type: "text/plain", metadata: %{"owner" => "alice"}, if: :absent]
               )

      requests = oci_requests()

      assert [
               {"POST", "/n/ns/b/b/u", "", create_headers, create_body},
               {"PUT", _, part_query, _part_headers, "0123456789"},
               {"POST", _, commit_query, commit_headers, commit_body}
             ] = requests

      assert JSON.decode!(create_body) == %{
               "object" => "docs/big",
               "contentType" => "text/plain",
               "metadata" => %{"owner" => "alice"}
             }

      assert create_headers["content-type"] == "application/json"
      assert URI.decode_query(part_query)["uploadPartNum"] == "1"
      assert URI.decode_query(commit_query)["uploadId"] == "up-1"

      assert JSON.decode!(commit_body) == %{
               "partsToCommit" => [%{"partNum" => 1, "etag" => "etag-part"}]
             }

      assert commit_headers["if-none-match"] == "*"

      refute Enum.any?(requests, fn {method, _path, _query, _headers, _body} ->
               method == "HEAD"
             end)
    end

    test "put_object whose CommitMultipartUpload answers 412 maps by the condition it sent" do
      oci_serve(fn conn, _sent ->
        query = Adapters.query(conn)

        cond do
          conn.method == "POST" and String.ends_with?(conn.request_path, "/b/b/u") ->
            Plug.Conn.send_resp(conn, 200, JSON.encode!(%{"uploadId" => "up-1"}))

          conn.method == "PUT" ->
            respond(conn, 200, "", etag: "etag-part")

          conn.method == "POST" and query["uploadId"] == "up-1" ->
            Plug.Conn.send_resp(conn, 412, "")

          conn.method == "DELETE" ->
            Plug.Conn.send_resp(conn, 204, "")
        end
      end)

      {_mod, opts} = oci_opts("oci-object-put-multipart-cond", multipart_threshold_bytes: 4)

      assert {:error, :already_exists} =
               OCI.put_object(:i, "docs/big", "0123456789", opts ++ [if: :absent])

      # the failed upload was aborted, so no orphan parts are kept
      assert [_, _, _, {"DELETE", _path, abort_query, _headers, ""}] = oci_requests()
      assert URI.decode_query(abort_query)["uploadId"] == "up-1"

      assert {:error, :version_mismatch} =
               OCI.put_object(:i, "docs/big", "0123456789", opts ++ [if: {:version, "etag-1"}])
    end

    test "get_object on a missing object is :not_found" do
      oci_serve(fn conn, _sent -> Plug.Conn.send_resp(conn, 404, "") end)

      {_mod, opts} = oci_opts("oci-object-get-missing")
      assert {:error, :not_found} = OCI.get_object(:i, "docs/none", opts)
    end

    test "delete_object is :ok for a delete and for an object that is already gone" do
      oci_serve(fn conn, _sent ->
        if String.ends_with?(conn.request_path, "gone") do
          Plug.Conn.send_resp(conn, 404, "")
        else
          Plug.Conn.send_resp(conn, 204, "")
        end
      end)

      {_mod, opts} = oci_opts("oci-object-delete")

      assert :ok = OCI.delete_object(:i, "docs/a.txt", opts)
      assert :ok = OCI.delete_object(:i, "docs/gone", opts)

      assert [{"DELETE", _, "", _, ""}, {"DELETE", _, "", _, ""}] = oci_requests()
    end

    test "list_page sends limit, delimiter, start and fields, and parses objects, prefixes and the cursor" do
      oci_serve(fn conn, _sent -> Plug.Conn.send_resp(conn, 200, @oci_page) end)

      {_mod, opts} = oci_opts("oci-object-list")

      assert {:ok, page} =
               OCI.list_page(:i, "docs/", opts ++ [limit: 2, delimiter: "/", cursor: "page-1"])

      assert page.objects == [%{key: "docs/a.txt", size: 5}]
      assert page.prefixes == ["docs/sub/"]
      assert page.next == "page-2"

      assert [{"GET", "/n/ns/b/b/o", query, _headers, ""}] = oci_requests()

      assert URI.decode_query(query) == %{
               "prefix" => "docs/",
               "limit" => "2",
               "delimiter" => "/",
               "start" => "page-1",
               "fields" => "name,size"
             }
    end

    test "list_page's last page has no cursor, and the defaults send no delimiter" do
      oci_serve(fn conn, _sent -> Plug.Conn.send_resp(conn, 200, @oci_last_page) end)

      {_mod, opts} = oci_opts("oci-object-list-last")

      assert {:ok, page} = OCI.list_page(:i, "docs/", opts)
      assert page.objects == [%{key: "docs/b.txt", size: 7}]
      assert page.prefixes == []
      assert page.next == nil

      assert [{"GET", _path, query, _headers, ""}] = oci_requests()
      params = URI.decode_query(query)
      assert params["limit"] == "1000"
      refute Map.has_key?(params, "delimiter")
      refute Map.has_key?(params, "start")
    end
  end
end
