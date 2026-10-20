defmodule Ankusa.BlobStore.RetryTest do
  @moduledoc """
  `Ankusa.BlobStore.Retry` as each HTTP object store uses it, against a
  `Req.Test` plug: which failures are retried, how many attempts `:retries`
  allows, that the last answer keeps its usual mapping, and that a `put` sends
  iodata as its bytes.
  """

  use ExUnit.Case, async: true

  alias Ankusa.BlobStore.{Azure, Breaker, GCS, OCI, Retry, S3}
  alias Ankusa.Test.OCIKey

  setup do
    # The breaker table is node-wide, and several tests here share an endpoint.
    Breaker.reset()
    :ok
  end

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
    {mod, Keyword.merge(opts ++ [req_options: [plug: {Req.Test, __MODULE__}]], extra)}
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

  test "each retry emits [:ankusa, :blob_store, :retry] with a jittered delay" do
    handler = "blob-store-retry-#{System.unique_integer([:positive])}"
    test = self()

    :ok =
      :telemetry.attach(
        handler,
        [:ankusa, :blob_store, :retry],
        fn _event, measurements, meta, pid -> send(pid, {:retry, measurements, meta}) end,
        test
      )

    on_exit(fn -> :telemetry.detach(handler) end)

    stub([503, 200])
    {mod, opts} = opts(S3)

    assert :ok = mod.put(:retry_telemetry, "seg/a.seg", "abc", opts)

    assert_receive {:retry, %{delay_ms: delay},
                    %{instance: :retry_telemetry, attempt: 1, status: 503}}

    # Full jitter: anywhere from no wait to the whole 200 ms step.
    assert delay in 0..200
    refute_received {:retry, _, %{instance: :retry_telemetry}}
  end

  test "a :retries that is not a non-negative integer raises" do
    stub([200])
    {mod, opts} = opts(GCS, retries: -1)

    assert_raise ArgumentError, ~r/:retries must be a non-negative integer/, fn ->
      mod.get(:i, "seg/a.seg", opts)
    end
  end

  # ── Retry-After, the deadline, the breaker ──────────────────────────────────

  # Answers each request with the next `{status, headers}` of `script` (the last
  # one repeats), after `delay_ms`, and counts the requests.
  defp scripted(script, delay_ms \\ 0) do
    {:ok, agent} = Agent.start_link(fn -> {script, 0} end)

    Req.Test.stub(__MODULE__, fn conn ->
      {status, headers} =
        Agent.get_and_update(agent, fn {[entry | rest], n} ->
          {entry, {if(rest == [], do: [entry], else: rest), n + 1}}
        end)

      if delay_ms > 0, do: Process.sleep(delay_ms)

      conn =
        Enum.reduce(headers, conn, fn {k, v}, conn -> Plug.Conn.put_resp_header(conn, k, v) end)

      Plug.Conn.send_resp(conn, status, "status #{status}")
    end)

    fn -> Agent.get(agent, &elem(&1, 1)) end
  end

  defp attach_retry do
    handler = "blob-store-retry-after-#{System.unique_integer([:positive])}"
    test = self()

    :ok =
      :telemetry.attach(
        handler,
        [:ankusa, :blob_store, :retry],
        fn _event, measurements, meta, pid -> send(pid, {:retry, measurements, meta}) end,
        test
      )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  test "Retry-After: 2 is honoured: the retry waits the two seconds" do
    attach_retry()
    requests = scripted([{503, [{"retry-after", "2"}]}, {200, []}])
    {mod, opts} = opts(S3)

    {elapsed_us, result} =
      :timer.tc(fn -> mod.put(:retry_after_two, "seg/a.seg", "abc", opts) end)

    assert result == :ok
    assert requests.() == 2
    assert_receive {:retry, %{delay_ms: 2000}, %{instance: :retry_after_two, status: 503}}
    assert elapsed_us >= 2_000_000
    assert elapsed_us < 4_000_000
  end

  for name <- [S3, GCS, Azure, OCI] do
    test "#{inspect(name)}: a Retry-After that does not fit the deadline is {:throttled, seconds} at once" do
      requests = scripted([{429, [{"retry-after", "3600"}]}, {200, []}])
      {mod, opts} = opts(unquote(name), op_deadline_ms: 5_000)

      {elapsed_us, result} = :timer.tc(fn -> mod.get(:i, "seg/a.seg", opts) end)

      assert result == {:error, {:throttled, 3600}}
      assert requests.() == 1
      assert elapsed_us < 1_000_000
    end
  end

  test "a Retry-After as an HTTP date is read, and a past one waits no longer than the jitter" do
    requests = scripted([{503, [{"retry-after", "Sun, 06 Nov 1994 08:49:37 GMT"}]}, {200, []}])
    {mod, opts} = opts(GCS)

    assert {:ok, "status 200"} = mod.get(:i, "seg/a.seg", opts)
    assert requests.() == 2
  end

  test "the attempts spent on a throttle that carried a Retry-After is {:throttled, seconds}" do
    requests = scripted([{503, [{"retry-after", "1"}]}])
    {mod, opts} = opts(S3, retries: 1)

    assert {:error, {:throttled, 1}} = mod.get(:i, "seg/a.seg", opts)
    assert requests.() == 2
  end

  test "one deadline covers every attempt: a failing store is given up on at op_deadline_ms" do
    requests = scripted([{500, []}])
    {mod, opts} = opts(S3, retries: 10, op_deadline_ms: 1_000, breaker_failures: 0)

    {elapsed_us, result} = :timer.tc(fn -> mod.get(:i, "seg/a.seg", opts) end)

    assert {:error, {:status, 500, _body}} = result
    assert requests.() >= 2
    assert elapsed_us < 1_500_000
  end

  test "an operation past its deadline sends nothing" do
    requests = scripted([{200, []}])
    {mod, opts} = opts(S3, deadline: System.monotonic_time(:millisecond) - 1)

    assert {:error, :deadline} = mod.get(:i, "seg/a.seg", opts)
    assert requests.() == 0
  end

  test "each attempt runs under its kind's timeout, or the time left if that is less" do
    run = fn kind, extra ->
      opts = [timeout_ms: 111, upload_timeout_ms: 2_222, retries: 0] ++ extra
      opts = Ankusa.BlobStore.Options.with_deadline(opts)

      Retry.run(:i, opts, {"http://timeouts.test", "b"}, kind, fn timeout ->
        send(self(), {:timeout, timeout})
        {:ok, 200, %{}, ""}
      end)

      assert_received {:timeout, timeout}
      timeout
    end

    assert run.(:small, []) == 111
    assert run.(:bulk, []) == 2_222
    assert run.(:bulk, op_deadline_ms: 1_000) <= 1_000
  end

  test "the breaker opens after breaker_failures failures, then lets one probe through" do
    handler = "blob-store-breaker-#{System.unique_integer([:positive])}"
    test = self()

    :ok =
      :telemetry.attach(
        handler,
        [:ankusa, :blob_store, :breaker],
        fn _event, _measurements, meta, pid -> send(pid, {:breaker, meta}) end,
        test
      )

    on_exit(fn -> :telemetry.detach(handler) end)

    requests = scripted([{500, []}, {500, []}, {200, []}])

    {mod, opts} =
      opts(S3,
        endpoint: "http://breaker-failures.test",
        retries: 0,
        breaker_failures: 2,
        breaker_open_ms: 300
      )

    assert {:error, {:status, 500, _}} = mod.get(:breaker_failures, "k", opts)
    assert {:error, {:status, 500, _}} = mod.get(:breaker_failures, "k", opts)

    assert_receive {:breaker,
                    %{
                      instance: :breaker_failures,
                      endpoint: "http://breaker-failures.test",
                      bucket: "b",
                      state: :open,
                      reason: {:failures, 2}
                    }}

    # Open: answered without a request.
    assert {:error, :breaker_open} = mod.get(:breaker_failures, "k", opts)
    assert requests.() == 2

    # Another bucket on the same endpoint is not behind it.
    {_mod, other} =
      opts(S3, endpoint: "http://breaker-failures.test", bucket: "other", retries: 0)

    assert {:ok, "status 200"} = mod.get(:breaker_failures, "k", other)

    Process.sleep(350)
    assert {:ok, "status 200"} = mod.get(:breaker_failures, "k", opts)
    assert_receive {:breaker, %{state: :closed, reason: nil}}
  end

  test "the breaker also opens on p99 latency, not only on failures" do
    scripted([{200, []}], 30)

    {mod, opts} =
      opts(GCS, endpoint: "http://breaker-slow.test", breaker_slow_ms: 10, breaker_open_ms: 5_000)

    for _ <- 1..20, do: assert({:ok, _} = mod.get(:breaker_slow, "k", opts))

    assert {:error, :breaker_open} = mod.get(:breaker_slow, "k", opts)
  end

  test "breaker_failures: 0 turns the breaker off, the latency trip with it" do
    scripted([{500, []}])

    {mod, opts} =
      opts(Azure, endpoint: "http://breaker-off.test", retries: 0, breaker_failures: 0)

    for _ <- 1..8 do
      assert {:error, {:status, 500, _}} = mod.get(:breaker_off, "k", opts)
    end
  end

  test "a throttled answer is a reachable store: it never opens the breaker" do
    scripted([{429, []}])

    {mod, opts} =
      opts(OCI, endpoint: "http://breaker-throttle.test", retries: 0, breaker_failures: 2)

    for _ <- 1..6 do
      assert {:error, {:status, 429, _}} = mod.get(:breaker_throttle, "k", opts)
    end
  end
end
