defmodule Ankusa.Gateway.ObjectsRouterTest do
  @moduledoc """
  The objects API through `Ankusa.Gateway.Router`, over LocalFS spaces in a
  temporary directory: reads (whole, ranged, conditional, in two store reads),
  writes and their conditions, deletes, listings, the key rules, the byte
  budget, and the answers for a missing credential, a policy's deny and a
  store that refuses this node.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Admin.Auth.Token
  alias Ankusa.BlobStore.LocalFS
  alias Ankusa.Gateway.Objects.Budget
  alias Ankusa.Gateway.Router

  @moduletag :capture_log
  @moduletag :tmp_dir

  # A LocalFS store that tells the test about every object call and, once
  # armed, rewrites the object right after the first ranged read: the second
  # read of a two-read GET then meets another version.
  defmodule RacyStore do
    @moduledoc false

    defdelegate put_object(instance, key, data, opts), to: LocalFS
    defdelegate delete_object(instance, key, opts), to: LocalFS
    defdelegate list_page(instance, prefix, opts), to: LocalFS

    # Armed with `{:head, data}`, rewrites the object right after a HEAD: a
    # suffix range's size is then stale by its first read.
    def head_object(instance, key, opts) do
      result = LocalFS.head_object(instance, key, opts)

      case opts[:race] && :persistent_term.get({__MODULE__, opts[:race]}, nil) do
        {:head, data} ->
          :persistent_term.put({__MODULE__, opts[:race]}, :done)
          {:ok, _} = LocalFS.put_object(instance, key, data, opts)

        _ ->
          :ok
      end

      result
    end

    def get_object(instance, key, opts) do
      if test = opts[:test], do: send(test, {:get_object, opts[:range], opts[:if]})
      result = LocalFS.get_object(instance, key, opts)

      case opts[:race] && :persistent_term.get({__MODULE__, opts[:race]}, nil) do
        {:armed, data} ->
          :persistent_term.put({__MODULE__, opts[:race]}, :done)
          {:ok, _} = LocalFS.put_object(instance, key, data, opts)

        _ ->
          :ok
      end

      result
    end
  end

  # A store whose credential is refused.
  defmodule ForbiddenStore do
    @moduledoc false
    def head_object(_i, _k, _o), do: {:error, {:status, 403, "denied"}}
    def get_object(_i, _k, _o), do: {:error, {:status, 403, "denied"}}
    def put_object(_i, _k, _d, _o), do: {:error, {:status, 403, "denied"}}
    def delete_object(_i, _k, _o), do: {:error, {:status, 403, "denied"}}
    def list_page(_i, _p, _o), do: {:error, {:status, 403, "denied"}}
  end

  setup %{tmp_dir: dir} = context do
    config = test_config(roles: [])
    start_supervised!({Ankusa.Instance, config})
    inst = config.instance
    race = {inst, make_ref()}

    tokens = [
      %{id: "writer", sha256: Token.hash("writer"), scopes: [], groups: ["writers"]},
      %{id: "reader", sha256: Token.hash("reader"), scopes: [], groups: ["readers"]},
      %{id: "nobody", sha256: Token.hash("nobody"), scopes: [], groups: []}
    ]

    rules = [
      %{
        effect: :allow,
        actions: [:read, :write, :delete, :list],
        when: %{groups_any: ["writers"]}
      },
      %{effect: :allow, actions: [:read, :list], when: %{groups_any: ["readers"]}},
      %{effect: :deny, actions: ["*"]}
    ]

    store = {RacyStore, [root: Path.join(dir, "store"), test: self(), race: race]}

    spaces =
      Map.merge(
        %{
          "docs" => %{
            store: store,
            prefix: "docs/",
            policy: "objects",
            max_object_bytes: Map.get(context, :max_object_bytes, 65_536),
            read_first_bytes: Map.get(context, :read_first_bytes, 1024)
          },
          "archive" => %{
            store: store,
            prefix: "archive/",
            policy: "objects",
            max_object_bytes: Map.get(context, :max_object_bytes, 65_536),
            read_only: true
          },
          "locked" => %{
            store: {ForbiddenStore, []},
            policy: "objects",
            max_object_bytes: Map.get(context, :max_object_bytes, 65_536)
          }
        },
        Map.get(context, :spaces, %{})
      )

    gateway =
      Ankusa.Gateway.Config.new(
        instance: inst,
        port: 0,
        auth: %{authenticators: [%{type: :token, tokens: tokens}]},
        authz: %{
          authorizers: %{"rules" => {Ankusa.Gateway.Authz.Rules, %{rules: rules}}},
          policies: %{"objects" => %{mode: :first, steps: ["rules"]}}
        },
        spaces: spaces,
        objects: Map.get(context, :objects, %{})
      )

    start_supervised!({Ankusa.Gateway, gateway})
    %{inst: inst, root: Path.join(dir, "store"), race: race}
  end

  defp call(inst, method, path, opts \\ []) do
    conn = Plug.Test.conn(method, path, Keyword.get(opts, :body))

    conn =
      Enum.reduce(Keyword.get(opts, :headers, []), conn, fn {k, v}, conn ->
        Plug.Conn.put_req_header(conn, k, v)
      end)

    conn =
      case Keyword.get(opts, :as, "writer") do
        nil -> conn
        token -> Plug.Conn.put_req_header(conn, "authorization", "Bearer " <> token)
      end

    Router.call(conn, Router.init(instance: inst))
  end

  defp put(inst, path, body, headers \\ []) do
    headers = [{"content-length", Integer.to_string(byte_size(body))} | headers]
    call(inst, :put, path, body: body, headers: headers)
  end

  defp json(conn), do: JSON.decode!(conn.resp_body)
  defp header(conn, name), do: conn |> Plug.Conn.get_resp_header(name) |> List.first()

  defp drain_gets do
    receive do
      {:get_object, _range, _if} -> drain_gets()
    after
      0 -> :ok
    end
  end

  describe "a round trip" do
    test "PUT then GET carries the content type, the metadata, the version and safe headers",
         %{inst: inst} do
      conn =
        put(inst, "/v1/objects/docs/reports/q1.csv", "a,b\n1,2\n", [
          {"content-type", "text/csv"},
          {"x-ankusa-meta-owner", "alice"}
        ])

      assert conn.status == 200

      assert %{"space" => "docs", "key" => "reports/q1.csv", "size" => 8, "version" => version} =
               json(conn)

      assert header(conn, "etag") == ~s("#{version}")

      conn = call(inst, :get, "/v1/objects/docs/reports/q1.csv")
      assert conn.status == 200
      assert conn.resp_body == "a,b\n1,2\n"
      assert header(conn, "content-type") == "text/csv"
      assert header(conn, "etag") == ~s("#{version}")
      assert header(conn, "x-ankusa-meta-owner") == "alice"
      assert header(conn, "accept-ranges") == "bytes"
      assert header(conn, "cache-control") == "private, no-cache"
      assert header(conn, "x-content-type-options") == "nosniff"
      assert header(conn, "content-disposition") == "attachment"
      assert header(conn, "content-security-policy") == "default-src 'none'; sandbox"
      assert header(conn, "last-modified") =~ ~r/GMT$/

      head = call(inst, :head, "/v1/objects/docs/reports/q1.csv")
      assert head.status == 200
      assert head.resp_body == ""
      assert header(head, "content-length") == "8"
      assert header(head, "etag") == ~s("#{version}")

      assert call(inst, :delete, "/v1/objects/docs/reports/q1.csv").status == 204
      assert call(inst, :get, "/v1/objects/docs/reports/q1.csv").status == 404
      assert call(inst, :delete, "/v1/objects/docs/reports/q1.csv").status == 204
    end

    test "If-None-Match with the current version is 304", %{inst: inst} do
      %{"version" => version} = inst |> put("/v1/objects/docs/a.txt", "hello") |> json()

      conn =
        call(inst, :get, "/v1/objects/docs/a.txt", headers: [{"if-none-match", ~s("#{version}")}])

      assert conn.status == 304
      assert conn.resp_body == ""

      conn = call(inst, :get, "/v1/objects/docs/a.txt", headers: [{"if-none-match", ~s("other")}])
      assert conn.status == 200
    end

    test "metadata names and values are checked", %{inst: inst} do
      for header <- [
            {"x-ankusa-meta-ankusa-x", "v"},
            {"x-ankusa-meta-bad_name", "v"},
            {"x-ankusa-meta-ok", "tab\there"},
            {"x-ankusa-meta-ok", String.duplicate("v", 1025)}
          ] do
        conn = put(inst, "/v1/objects/docs/m.txt", "x", [header])

        assert {conn.status, json(conn)} == {400, %{"error" => "invalid_metadata"}},
               inspect(header)
      end

      many = for i <- 1..17, do: {"x-ankusa-meta-k#{i}", "v"}
      assert put(inst, "/v1/objects/docs/m.txt", "x", many).status == 400
    end
  end

  describe "ranges" do
    test "a span, an open end and a suffix are 206 with content-range", %{inst: inst} do
      put(inst, "/v1/objects/docs/r.txt", "0123456789")

      for {range, body, content_range} <- [
            {"bytes=2-4", "234", "bytes 2-4/10"},
            {"bytes=7-", "789", "bytes 7-9/10"},
            {"bytes=-3", "789", "bytes 7-9/10"},
            {"bytes=8-100", "89", "bytes 8-9/10"}
          ] do
        conn = call(inst, :get, "/v1/objects/docs/r.txt", headers: [{"range", range}])

        assert {conn.status, conn.resp_body, header(conn, "content-range")} ==
                 {206, body, content_range}
      end
    end

    test "a range past the end is 416 with the size", %{inst: inst} do
      put(inst, "/v1/objects/docs/r.txt", "0123456789")

      for range <- ["bytes=10-12", "bytes=-0"] do
        conn = call(inst, :get, "/v1/objects/docs/r.txt", headers: [{"range", range}])
        assert conn.status == 416, range
        assert header(conn, "content-range") == "bytes */10"
      end
    end

    test "an empty object is an empty 200 whole, and 416 for any range", %{inst: inst} do
      put(inst, "/v1/objects/docs/empty", "")

      conn = call(inst, :get, "/v1/objects/docs/empty")
      assert {conn.status, conn.resp_body} == {200, ""}
      assert header(conn, "etag")

      conn = call(inst, :get, "/v1/objects/docs/empty", headers: [{"range", "bytes=0-0"}])
      assert conn.status == 416
    end
  end

  describe "large objects" do
    test "an object past read_first_bytes is read in two store reads, the second held to the version",
         %{inst: inst} do
      body = :crypto.strong_rand_bytes(3000)
      %{"version" => version} = inst |> put("/v1/objects/docs/big.bin", body) |> json()
      drain_gets()

      conn = call(inst, :get, "/v1/objects/docs/big.bin")
      assert {conn.status, conn.resp_body} == {200, body}
      assert_received {:get_object, {0, 1023}, nil}
      assert_received {:get_object, {1024, 2999}, {:version, ^version}}
      refute_received {:get_object, _, _}
    end

    test "an overwrite between the two reads restarts the read once", %{inst: inst, race: race} do
      put(inst, "/v1/objects/docs/big.bin", String.duplicate("a", 3000))
      newer = String.duplicate("b", 2500)
      :persistent_term.put({RacyStore, race}, {:armed, newer})
      drain_gets()

      conn = call(inst, :get, "/v1/objects/docs/big.bin")
      assert {conn.status, conn.resp_body} == {200, newer}
      # first, mismatch, then the restart's two reads
      assert_received {:get_object, {0, 1023}, nil}
      assert_received {:get_object, {1024, 2999}, {:version, _}}
      assert_received {:get_object, {0, 1023}, nil}
      assert_received {:get_object, {1024, 2499}, {:version, _}}
    end

    test "a suffix range whose object changes after its HEAD restarts on the new size", %{
      inst: inst,
      race: race
    } do
      put(inst, "/v1/objects/docs/s.txt", "0123456789")
      :persistent_term.put({RacyStore, race}, {:head, "abcdefghijklmnop"})

      conn = call(inst, :get, "/v1/objects/docs/s.txt", headers: [{"range", "bytes=-3"}])
      assert {conn.status, conn.resp_body} == {206, "nop"}
      assert header(conn, "content-range") == "bytes 13-15/16"
    end

    @tag max_object_bytes: 2048
    test "past max_object_bytes a PUT is 413 and a GET is 422", %{inst: inst, root: root} do
      conn = put(inst, "/v1/objects/docs/huge", String.duplicate("x", 2049))
      assert {conn.status, json(conn)} == {413, %{"error" => "too_large", "max_bytes" => 2048}}

      {:ok, _} = LocalFS.put_object(:x, "docs/huge", String.duplicate("x", 4000), root: root)
      conn = call(inst, :get, "/v1/objects/docs/huge")

      assert {conn.status, json(conn)} ==
               {422, %{"error" => "object_too_large", "max_bytes" => 2048}}

      # A range within the bound is still served.
      conn = call(inst, :get, "/v1/objects/docs/huge", headers: [{"range", "bytes=0-99"}])
      assert conn.status == 206
    end
  end

  describe "writes" do
    test "a PUT without content-length is 411", %{inst: inst} do
      conn = call(inst, :put, "/v1/objects/docs/a.txt", body: "hello")
      assert {conn.status, json(conn)} == {411, %{"error" => "length_required"}}
    end

    test "if-none-match: * and if-match are conditions; a failed one is 412", %{inst: inst} do
      absent = [{"if-none-match", "*"}]
      %{"version" => v1} = inst |> put("/v1/objects/docs/c.txt", "one", absent) |> json()

      conn = put(inst, "/v1/objects/docs/c.txt", "two", absent)
      assert {conn.status, json(conn)} == {412, %{"error" => "precondition_failed"}}

      %{"version" => v2} =
        inst |> put("/v1/objects/docs/c.txt", "two", [{"if-match", ~s("#{v1}")}]) |> json()

      assert v2 != v1
      conn = put(inst, "/v1/objects/docs/c.txt", "three", [{"if-match", ~s("#{v1}")}])
      assert conn.status == 412
      assert call(inst, :get, "/v1/objects/docs/c.txt").resp_body == "two"
    end

    test "a read_only space refuses PUT and DELETE with 405", %{inst: inst} do
      assert put(inst, "/v1/objects/archive/a.txt", "x").status == 405
      assert call(inst, :delete, "/v1/objects/archive/a.txt").status == 405
      assert call(inst, :get, "/v1/objects/archive/a.txt").status == 404
    end

    test "another method is 405", %{inst: inst} do
      assert call(inst, :post, "/v1/objects/docs/a.txt").status == 405
    end
  end

  describe "keys" do
    test "invalid keys are 400 on every route", %{inst: inst} do
      for path <- [
            "/v1/objects/docs/.ankusa/x",
            "/v1/objects/docs/a//b",
            "/v1/objects/docs/a/",
            "/v1/objects/docs/../x",
            "/v1/objects/docs/a/./b",
            "/v1/objects/docs/a%2Fb",
            "/v1/objects/docs/a%5Cb",
            "/v1/objects/docs/x/.ankusa/acl/v1/o/abc",
            "/v1/objects/docs/" <> Enum.map_join(1..33, "/", &"s#{&1}")
          ],
          method <- [:get, :head, :put, :delete] do
        conn =
          if method == :put,
            do: put(inst, path, "x"),
            else: call(inst, method, path)

        assert conn.status == 400, "#{method} #{path}"
      end
    end

    test "a percent-encoded key and the same key in a query name one object", %{inst: inst} do
      assert put(inst, "/v1/objects/docs/my%20file.txt", "x").status == 200
      assert call(inst, :get, "/v1/objects/docs/my%20file.txt").resp_body == "x"

      body = inst |> call(:get, "/v1/objects/docs?prefix=my+file.txt") |> json()
      assert body["objects"] == [%{"key" => "my file.txt", "size" => 1}]
    end

    test "a malformed escape is 400", %{inst: inst} do
      assert call(inst, :get, "/v1/objects/docs/%zz").status == 400
      assert put(inst, "/v1/objects/docs/a%zzb", "x").status == 400
    end
  end

  describe "listing" do
    test "pages, with and without a delimiter, never showing .ankusa trees", %{
      inst: inst,
      root: root
    } do
      for key <- ["a/1", "a/2", "b/1", "c"], do: put(inst, "/v1/objects/docs/" <> key, "x")
      {:ok, _} = LocalFS.put_object(:x, "docs/.ankusa/acl/v1/o/abc", "{}", root: root)

      page = inst |> call(:get, "/v1/objects/docs?limit=2") |> json()
      assert page["objects"] == [%{"key" => "a/1", "size" => 1}, %{"key" => "a/2", "size" => 1}]
      assert is_binary(page["next_cursor"])

      rest =
        inst
        |> call(
          :get,
          "/v1/objects/docs?limit=10&cursor=" <> URI.encode_www_form(page["next_cursor"])
        )
        |> json()

      assert Enum.map(rest["objects"], & &1["key"]) == ["b/1", "c"]
      assert rest["next_cursor"] == nil

      grouped = inst |> call(:get, "/v1/objects/docs?delimiter=/") |> json()
      assert grouped["prefixes"] == ["a/", "b/"]
      assert grouped["objects"] == [%{"key" => "c", "size" => 1}]

      under = inst |> call(:get, "/v1/objects/docs?prefix=a/") |> json()
      assert Enum.map(under["objects"], & &1["key"]) == ["a/1", "a/2"]
    end

    test "bad listing parameters are 400", %{inst: inst} do
      for {query, error} <- [
            {"prefix=../x", "invalid_key"},
            {"delimiter=%2C", "invalid_delimiter"},
            {"limit=0", "invalid_limit"},
            {"limit=1001", "invalid_limit"},
            {"cursor=" <> String.duplicate("c", 2049), "invalid_cursor"}
          ] do
        conn = call(inst, :get, "/v1/objects/docs?" <> query)
        assert {conn.status, json(conn)} == {400, %{"error" => error}}, query
      end
    end
  end

  describe "who may" do
    test "an unknown space is 404", %{inst: inst} do
      assert call(inst, :get, "/v1/objects/nope/a.txt").status == 404
      assert call(inst, :get, "/v1/objects/nope").status == 404
    end

    test "no credential is 401 and a rules deny is 403, on every route", %{inst: inst} do
      put(inst, "/v1/objects/docs/a.txt", "x")

      routes = [
        {:get, "/v1/objects/docs/a.txt"},
        {:head, "/v1/objects/docs/a.txt"},
        {:put, "/v1/objects/docs/a.txt"},
        {:delete, "/v1/objects/docs/a.txt"},
        {:get, "/v1/objects/docs"}
      ]

      for {method, path} <- routes do
        assert call(inst, method, path, as: nil).status == 401, "#{method} #{path}"

        headers = [{"content-length", "1"}]
        assert call(inst, method, path, as: "nobody", body: "x", headers: headers).status == 403
      end

      # A reader may read and list, not write.
      assert call(inst, :get, "/v1/objects/docs/a.txt", as: "reader").status == 200
      assert put(inst, "/v1/objects/docs/a.txt", "y") |> Map.get(:status) == 200
      assert call(inst, :delete, "/v1/objects/docs/a.txt", as: "reader").status == 403
    end

    test "a store that refuses this node's credential is 503 store_forbidden", %{inst: inst} do
      conn = call(inst, :get, "/v1/objects/locked/a.txt")
      assert {conn.status, json(conn)} == {503, %{"error" => "store_forbidden"}}
      assert header(conn, "retry-after") == "60"
    end
  end

  describe "the byte budget" do
    @tag max_object_bytes: 4096, read_first_bytes: 1024, objects: %{max_inflight_bytes: 4096}
    test "a read that would pass max_inflight_bytes is 503 overloaded; a dead holder is swept",
         %{inst: inst} do
      put(inst, "/v1/objects/docs/a.txt", "hello")
      test = self()

      holder =
        spawn(fn ->
          :ok = Budget.reserve(inst, 3500, 4096)
          send(test, :held)
          Process.sleep(:infinity)
        end)

      assert_receive :held
      conn = call(inst, :get, "/v1/objects/docs/a.txt")
      assert {conn.status, json(conn)} == {503, %{"error" => "overloaded"}}

      Process.exit(holder, :kill)
      Process.sleep(10)
      assert Budget.sweep(inst) == 0
      assert call(inst, :get, "/v1/objects/docs/a.txt").status == 200
      assert Budget.inflight(inst) == 0
    end
  end

  test "every answer is a request event with api: :objects", %{inst: inst} do
    handler = "objects-#{inst}"
    test = self()

    :telemetry.attach(
      handler,
      [:ankusa, :gateway, :request],
      fn _, _, meta, _ -> send(test, {:request, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    call(inst, :get, "/v1/objects/docs/missing")
    assert_received {:request, %{api: :objects, method: "GET", status: 404, instance: ^inst}}
  end
end
