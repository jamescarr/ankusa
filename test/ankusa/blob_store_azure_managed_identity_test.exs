defmodule Ankusa.BlobStore.Azure.ManagedIdentityTest do
  @moduledoc """
  The managed-identity token provider against a stubbed IMDS endpoint: request
  shape (the required `Metadata: true` header, api-version, the Blob Storage
  resource), token caching, refresh inside the expiry window, and the
  user-assigned `client_id` variant.
  """

  use ExUnit.Case, async: false

  alias Ankusa.BlobStore.Azure.ManagedIdentity

  @endpoint "http://imds.test/token"

  setup do
    # The module caches tokens in a named ETS table; wipe it so tests cannot
    # observe each other's cached tokens.
    case :ets.whereis(ManagedIdentity) do
      :undefined -> :ok
      tid -> :ets.delete_all_objects(tid)
    end

    :ok
  end

  defp opts(extra \\ []) do
    [endpoint: @endpoint, req_options: [plug: {Req.Test, __MODULE__}]] ++ extra
  end

  defp respond(conn, token, expires_in) do
    expires_on = Integer.to_string(System.system_time(:second) + expires_in)

    Plug.Conn.send_resp(
      conn,
      200,
      JSON.encode!(%{"access_token" => token, "expires_on" => expires_on})
    )
  end

  test "fetches a token from IMDS with Metadata: true and the Blob Storage resource" do
    {:ok, capture} = Agent.start_link(fn -> [] end)

    Req.Test.stub(__MODULE__, fn conn ->
      Agent.update(capture, fn seen ->
        [{conn.method, conn.request_path, conn.query_string, conn.req_headers} | seen]
      end)

      respond(conn, "tok", 3600)
    end)

    assert {:ok, "tok"} = ManagedIdentity.token(opts())

    assert [{"GET", path, query, headers}] = Agent.get(capture, & &1)
    assert path == "/token"
    assert query =~ "api-version=2018-02-01"
    assert query =~ "resource=https%3A%2F%2Fstorage.azure.com"
    assert Map.new(headers)["metadata"] == "true"
  end

  test "caches the token: two calls, one IMDS fetch" do
    {:ok, count} = Agent.start_link(fn -> 0 end)

    Req.Test.stub(__MODULE__, fn conn ->
      Agent.update(count, &(&1 + 1))
      respond(conn, "tok", 3600)
    end)

    assert {:ok, "tok"} = ManagedIdentity.token(opts())
    assert {:ok, "tok"} = ManagedIdentity.token(opts())
    assert Agent.get(count, & &1) == 1
  end

  test "refreshes once the cached token is inside the refresh window" do
    {:ok, count} = Agent.start_link(fn -> 0 end)

    Req.Test.stub(__MODULE__, fn conn ->
      n = Agent.get_and_update(count, fn n -> {n + 1, n + 1} end)
      # first token expires in 10s (< 5 min window) → must be refetched
      respond(conn, "tok", if(n == 1, do: 10, else: 3600))
    end)

    assert {:ok, "tok"} = ManagedIdentity.token(opts())
    assert {:ok, "tok"} = ManagedIdentity.token(opts())
    assert Agent.get(count, & &1) == 2
  end

  test "appends client_id for a user-assigned identity" do
    {:ok, capture} = Agent.start_link(fn -> [] end)

    Req.Test.stub(__MODULE__, fn conn ->
      Agent.update(capture, fn seen -> [conn.query_string | seen] end)
      respond(conn, "tok", 3600)
    end)

    assert {:ok, "tok"} = ManagedIdentity.token(opts(client_id: "0123abcd"))
    assert [query] = Agent.get(capture, & &1)
    assert query =~ "client_id=0123abcd"
  end

  test "returns :error when IMDS does not answer with a token" do
    Req.Test.stub(__MODULE__, fn conn -> Plug.Conn.send_resp(conn, 500, "boom") end)
    assert :error = ManagedIdentity.token(opts())
  end
end
