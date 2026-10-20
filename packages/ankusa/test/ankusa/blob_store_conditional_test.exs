defmodule Ankusa.BlobStore.ConditionalTest do
  @moduledoc """
  `put_if_absent/4` on each HTTP object store, against a `Req.Test` stub: the
  store's own precondition is sent, its "already there" answer is
  `{:error, :already_exists}` with the original left alone, and an object that
  holds exactly the bytes being written is `:ok` (a retry whose first attempt
  landed). A plain `put/4` sends no precondition.
  """

  use ExUnit.Case, async: true

  alias Ankusa.BlobStore.{Azure, GCS, OCI, S3}
  alias Ankusa.Test.BlobStoreAdapters, as: Adapters

  # What each store answers a failed precondition with.
  @exists %{S3 => 412, GCS => 412, Azure => 409, OCI => 412}

  # Serves one object: `stored` is what a GET returns and what a conditional PUT
  # collides with (`nil`: the key is free). Every request is sent to the test.
  defp serve(adapter, stored) do
    test = self()

    Req.Test.stub(__MODULE__, fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      send(test, {:request, conn.method, conn.query_string, Adapters.headers(conn), body})

      cond do
        conn.method == "GET" and stored != nil ->
          Plug.Conn.send_resp(conn, 200, stored)

        conn.method == "GET" ->
          Plug.Conn.send_resp(conn, 404, "")

        conditional?(conn) and stored != nil ->
          Plug.Conn.send_resp(conn, @exists[adapter], "exists")

        true ->
          Plug.Conn.send_resp(conn, 200, "")
      end
    end)
  end

  defp conditional?(conn) do
    Adapters.headers(conn)["if-none-match"] == "*" or
      Adapters.query(conn)["ifGenerationMatch"] == "0"
  end

  # Every request the stub saw so far, oldest first, as `{method, query, headers, body}`.
  defp requests do
    receive do
      {:request, method, query, headers, body} -> [{method, query, headers, body} | requests()]
    after
      0 -> []
    end
  end

  defp put_request do
    assert [{method, query, headers, body}] = requests()
    assert method in ["PUT", "POST"]
    {query, headers, body}
  end

  for adapter <- Adapters.all() do
    describe inspect(adapter) do
      test "a free key is written, with the store's precondition on the request" do
        serve(unquote(adapter), nil)
        {mod, opts} = Adapters.opts(unquote(adapter), "cond-free", __MODULE__)

        assert :ok = mod.put_if_absent(:i, "claims/p", "new", opts)

        {query, headers, "new"} = put_request()

        case unquote(adapter) do
          GCS -> assert query =~ "ifGenerationMatch=0"
          _precondition_header -> assert headers["if-none-match"] == "*"
        end
      end

      test "an existing key is {:error, :already_exists}, and the original is not touched" do
        serve(unquote(adapter), "original")
        {mod, opts} = Adapters.opts(unquote(adapter), "cond-exists", __MODULE__)

        assert {:error, :already_exists} = mod.put_if_absent(:i, "claims/p", "new", opts)

        # One conditional write, then the read-back; nothing else was sent.
        assert [{put, _, _, "new"}, {"GET", _, _, _}] = requests()
        assert put in ["PUT", "POST"]
      end

      test "an existing key that holds exactly these bytes is :ok" do
        serve(unquote(adapter), "same bytes")
        {mod, opts} = Adapters.opts(unquote(adapter), "cond-same", __MODULE__)

        assert :ok = mod.put_if_absent(:i, "claims/p", "same bytes", opts)
      end

      test "a plain put sends no precondition, and overwrites" do
        serve(unquote(adapter), "original")
        {mod, opts} = Adapters.opts(unquote(adapter), "cond-plain", __MODULE__)

        assert :ok = mod.put(:i, "seg/a", "new", opts)

        {query, headers, "new"} = put_request()
        refute query =~ "ifGenerationMatch"
        refute Map.has_key?(headers, "if-none-match")
      end
    end
  end

  test "LocalFS refuses to replace a file, and accepts the same bytes" do
    dir = Path.join(System.tmp_dir!(), "ankusa_cond_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf(dir) end)
    opts = [root: dir]

    assert :ok = Ankusa.BlobStore.LocalFS.put_if_absent(:i, "claims/p", "first", opts)

    assert {:error, :already_exists} =
             Ankusa.BlobStore.LocalFS.put_if_absent(:i, "claims/p", "second", opts)

    assert :ok = Ankusa.BlobStore.LocalFS.put_if_absent(:i, "claims/p", "first", opts)
    assert {:ok, "first"} = Ankusa.BlobStore.LocalFS.get(:i, "claims/p", opts)
  end
end
