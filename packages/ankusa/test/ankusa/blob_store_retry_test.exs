defmodule Ankusa.BlobStore.RetryTest do
  @moduledoc """
  `Ankusa.BlobStore.Retry` as each HTTP object store uses it, against a
  `Req.Test` plug: which failures are retried, how many attempts `:retries`
  allows, that the last answer keeps its usual mapping, and that a `put` sends
  iodata as its bytes.
  """

  use ExUnit.Case, async: true

  alias Ankusa.BlobStore.{Azure, GCS, OCI, S3}
  alias Ankusa.Test.OCIKey

  @adapters %{
    S3 =>
      {S3,
       bucket: "b",
       region: "us-east-1",
       endpoint: "http://s3.test",
       access_key_id: "AKID",
       secret_access_key: "secret"},
    GCS => {GCS, bucket: "b", endpoint: "http://gcs.test"},
    Azure => {Azure, account_name: "acct", container: "c", endpoint: "http://azure.test/acct"},
    OCI =>
      {OCI,
       region: "us-ashburn-1",
       namespace: "ns",
       bucket: "b",
       tenancy_ocid: "ocid1.tenancy.oc1..t",
       user_ocid: "ocid1.user.oc1..u",
       key_fingerprint: "aa:bb:cc",
       endpoint: "http://oci.test"}
  }

  # Answers each request with the next status of `statuses` (the last one
  # repeats) and records what it saw.
  defp stub(statuses) do
    {:ok, agent} = Agent.start_link(fn -> {statuses, []} end)

    Req.Test.stub(__MODULE__, fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      status =
        Agent.get_and_update(agent, fn {[status | rest], seen} ->
          next = if rest == [], do: [status], else: rest
          {status, {next, [{conn.req_headers, body} | seen]}}
        end)

      Plug.Conn.send_resp(conn, status, "status #{status}")
    end)

    agent
  end

  defp seen(agent), do: agent |> Agent.get(&elem(&1, 1)) |> Enum.reverse()

  defp opts(name, extra \\ []) do
    {mod, opts} = Map.fetch!(@adapters, name)
    opts = if mod == OCI, do: [{:private_key, OCIKey.private_key_pem()} | opts], else: opts
    {mod, opts ++ [req_options: [plug: {Req.Test, __MODULE__}]] ++ extra}
  end

  for name <- [S3, GCS, Azure, OCI] do
    describe inspect(name) do
      test "503, 503, then 200: the put succeeds on its third attempt" do
        agent = stub([503, 503, 200])
        {mod, opts} = opts(unquote(name))

        assert :ok = mod.put(:i, "seg/a.seg", "abc", opts)
        assert length(seen(agent)) == 3
      end

      test "retries: 0 is a single attempt, and the status is kept" do
        agent = stub([503, 200])
        {mod, opts} = opts(unquote(name), retries: 0)

        assert {:error, {:status, 503, "status 503"}} = mod.get(:i, "seg/a.seg", opts)
        assert length(seen(agent)) == 1
      end

      test "a 404 is answered at once as :not_found" do
        agent = stub([404, 200])
        {mod, opts} = opts(unquote(name))

        assert {:error, :not_found} = mod.get(:i, "seg/a.seg", opts)
        assert length(seen(agent)) == 1
      end

      test "a put sends iodata as its bytes" do
        agent = stub([200])
        {mod, opts} = opts(unquote(name))

        assert :ok = mod.put(:i, "seg/a.seg", ["ab", ["c", ?d]], opts)
        assert [{headers, "abcd"}] = seen(agent)

        if mod == OCI do
          sha = Base.encode64(:crypto.hash(:sha256, "abcd"))
          assert {"x-content-sha256", sha} in headers
        end

        if mod == S3 do
          sha = Base.encode16(:crypto.hash(:sha256, "abcd"), case: :lower)
          assert {"x-amz-content-sha256", sha} in headers
        end
      end
    end
  end

  test "a transport error is retried like a 5xx" do
    {:ok, attempts} = Agent.start_link(fn -> 0 end)

    Req.Test.stub(__MODULE__, fn conn ->
      case Agent.get_and_update(attempts, &{&1 + 1, &1 + 1}) do
        1 -> Req.Test.transport_error(conn, :econnrefused)
        _ -> Plug.Conn.send_resp(conn, 200, "ok")
      end
    end)

    {mod, opts} = opts(Azure)
    assert {:ok, "ok"} = mod.get(:i, "seg/a.seg", opts)
    assert Agent.get(attempts, & &1) == 2
  end

  test "500, 502, 504 with retries: 2 is three attempts, then the last answer" do
    agent = stub([500, 502, 504, 200])
    {mod, opts} = opts(GCS)

    assert {:error, {:status, 504, "status 504"}} = mod.get(:i, "seg/a.seg", opts)
    assert length(seen(agent)) == 3
  end

  test "a :retries that is not a non-negative integer raises" do
    stub([200])
    {mod, opts} = opts(GCS, retries: -1)

    assert_raise ArgumentError, ~r/:retries must be a non-negative integer/, fn ->
      mod.get(:i, "seg/a.seg", opts)
    end
  end
end
