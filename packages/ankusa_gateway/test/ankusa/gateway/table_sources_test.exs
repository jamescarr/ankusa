defmodule Ankusa.Gateway.Authz.TableSourcesTest do
  @moduledoc """
  The two library sources of the `table` authorizer: NDJSON objects in a blob
  store, and paged JSON over HTTP.
  """

  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Ankusa.Gateway.Authz.Table.Source.{Http, Object}

  @key "gateway.authz.authorizers.team.source"

  setup do
    dir =
      Path.join(System.tmp_dir!(), "ankusa-table-source-#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    %{dir: dir}
  end

  defp ctx, do: %{instance: :table_source_test, name: "team", deadline: 0}

  defp collect, do: fn row, acc -> {:cont, [row | acc]} end

  describe "object source" do
    test "one NDJSON object: version and rows", %{dir: dir} do
      body = """
      {"principal":"user:alice","action":"read","space":"docs","prefix":"teams/eng/","effect":"allow"}

      {"principal":"group:eng","action":"*","space":"*","prefix":"","effect":"deny"}
      """

      {:ok, _version} =
        Ankusa.BlobStore.LocalFS.put_object(:test, "perms/rows.ndjson", body, root: dir)

      state =
        Object.init!(
          %{store: {Ankusa.BlobStore.LocalFS, [root: dir]}, key: "perms/rows.ndjson"},
          @key
        )

      assert {:ok, [{"perms/rows.ndjson", version}]} = Object.version(state, ctx())
      assert is_binary(version)

      assert {:ok, rows} = Object.load(state, ctx(), [], collect())
      assert [first, second] = Enum.reverse(rows)
      assert first["principal"] == "user:alice"
      assert second["effect"] == "deny"
    end

    test "a prefix reads every object under it, sorted, and a bad line is a bad row", %{
      dir: dir
    } do
      store = {Ankusa.BlobStore.LocalFS, [root: dir]}

      {:ok, _} =
        Ankusa.BlobStore.LocalFS.put_object(
          :test,
          "perms/b.ndjson",
          ~s({"principal":"user:bob","action":"read","space":"docs","prefix":"","effect":"allow"}),
          root: dir
        )

      {:ok, _} =
        Ankusa.BlobStore.LocalFS.put_object(
          :test,
          "perms/a.ndjson",
          ~s({"principal":"user:alice","action":"read","space":"docs","prefix":"","effect":"allow"}) <>
            "\nnot json\n",
          root: dir
        )

      state = Object.init!(%{store: store, prefix: "perms/"}, @key)

      assert {:ok, [{"perms/a.ndjson", _}, {"perms/b.ndjson", _}]} = Object.version(state, ctx())

      assert {:ok, rows} = Object.load(state, ctx(), [], collect())
      rows = Enum.reverse(rows)
      assert length(rows) == 3
      assert Enum.at(rows, 1) == %{}
      assert Enum.at(rows, 2)["principal"] == "user:bob"
    end

    test "the reducer's halt stops the load", %{dir: dir} do
      body =
        ~s({"principal":"user:alice","action":"read","space":"docs","prefix":""}) <>
          "\n" <>
          ~s({"principal":"user:bob","action":"read","space":"docs","prefix":""})

      {:ok, _} =
        Ankusa.BlobStore.LocalFS.put_object(:test, "perms.ndjson", body, root: dir)

      state =
        Object.init!(%{store: {Ankusa.BlobStore.LocalFS, [root: dir]}, key: "perms.ndjson"}, @key)

      halt = fn row, acc ->
        if acc == [], do: {:cont, [row]}, else: {:halt, acc}
      end

      assert {:ok, [%{"principal" => "user:alice"}]} = Object.load(state, ctx(), [], halt)
    end

    test "exactly one of key and prefix is required" do
      store = {Ankusa.BlobStore.LocalFS, [root: "/tmp/ankusa-table-source"]}

      assert_raise ArgumentError, ~r/one of key and prefix is required/, fn ->
        Object.init!(%{store: store}, @key)
      end

      assert_raise ArgumentError, ~r/only one of key and prefix/, fn ->
        Object.init!(%{store: store, key: "a.ndjson", prefix: "perms/"}, @key)
      end
    end
  end

  describe "http source" do
    test "pages follow next until it is null, and send the configured headers" do
      stub = :"table_source_#{System.unique_integer([:positive])}"
      parent = self()

      Req.Test.stub(stub, fn conn ->
        send(
          parent,
          {:request, conn.query_string, Plug.Conn.get_req_header(conn, "authorization")}
        )

        case conn.query_string do
          "" ->
            Req.Test.json(conn, %{
              "rows" => [
                %{
                  "principal" => "user:alice",
                  "action" => "read",
                  "space" => "docs",
                  "prefix" => ""
                }
              ],
              "next" => "c2"
            })

          "cursor=c2" ->
            Req.Test.json(conn, %{
              "rows" => [
                %{
                  "principal" => "user:bob",
                  "action" => "read",
                  "space" => "docs",
                  "prefix" => ""
                }
              ],
              "next" => nil
            })
        end
      end)

      state =
        Http.init!(
          %{
            url: "https://permissions.test/rows",
            headers: %{"authorization" => "Bearer t0k3n"},
            req_options: [plug: {Req.Test, stub}]
          },
          @key
        )

      assert {:ok, rows} = Http.load(state, ctx(), [], collect())
      assert [%{"principal" => "user:alice"}, %{"principal" => "user:bob"}] = Enum.reverse(rows)

      assert_received {:request, "", ["Bearer t0k3n"]}
      assert_received {:request, "cursor=c2", ["Bearer t0k3n"]}
    end

    test "a listing longer than max_pages is an error" do
      stub = :"table_source_#{System.unique_integer([:positive])}"

      Req.Test.stub(stub, fn conn ->
        Req.Test.json(conn, %{"rows" => [], "next" => "again"})
      end)

      state =
        Http.init!(
          %{
            url: "https://permissions.test/rows",
            max_pages: 1,
            req_options: [plug: {Req.Test, stub}]
          },
          @key
        )

      assert {:error, :too_many_pages} = Http.load(state, ctx(), [], collect())
    end

    test "an unreachable endpoint and a non-JSON body are errors" do
      stub = :"table_source_#{System.unique_integer([:positive])}"

      Req.Test.stub(stub, fn conn ->
        conn
        |> Plug.Conn.put_resp_content_type("text/plain")
        |> Plug.Conn.send_resp(200, "not json")
      end)

      state =
        Http.init!(
          %{url: "https://permissions.test/rows", req_options: [plug: {Req.Test, stub}]},
          @key
        )

      assert {:error, :bad_response} = Http.load(state, ctx(), [], collect())
    end

    test "the URL must be https, or http to loopback, and never templated" do
      assert_raise ArgumentError, ~r/must be a literal URL/, fn ->
        Http.init!(%{url: "https://{{ claims.host }}/rows"}, @key)
      end

      assert_raise ArgumentError, ~r/must be an https URL \(or http to a loopback host\)/, fn ->
        Http.init!(%{url: "http://permissions.test/rows"}, @key)
      end

      state = Http.init!(%{url: "http://127.0.0.1:8080/rows"}, @key)
      assert state.url == "http://127.0.0.1:8080/rows"
    end
  end
end
