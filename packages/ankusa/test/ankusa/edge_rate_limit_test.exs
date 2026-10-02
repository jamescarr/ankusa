defmodule Ankusa.Edge.RateLimitTest do
  @moduledoc """
  Per-tenant ingest rate limits, end to end: a real edge instance, real POSTs
  through the router, and the store as the witness for "nothing was stored".

  Timing matters to one property only — that a bucket refills — so the limits
  here are chosen to make that observable in well under a second, and every
  other test asserts on a bucket that is deliberately far from refilling.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Edge.{Ingest, RateLimiter, Router}

  @secret "whsec_" <> Base.encode64("supersecret-key")

  defp start_edge(
         rate_limits,
         sources \\ %{
           "demo" => [verifier: {Ankusa.Verifier.None, []}, sinks: [{Ankusa.Sink.Log, []}]]
         }
       ) do
    config =
      test_config(
        roles: [:edge],
        route_resolver: {Ankusa.RouteResolver.TenantPath, []},
        rate_limits: rate_limits,
        source_store: {Ankusa.SourceStore.Static, sources: sources}
      )

    start_supervised!({Ankusa.Instance, config})
    config
  end

  defp post(config, path, body, headers \\ []) do
    conn =
      Plug.Test.conn(:post, path, body)
      |> then(fn c ->
        Enum.reduce(headers, c, fn {k, v}, c -> Plug.Conn.put_req_header(c, k, v) end)
      end)

    Router.call(conn, Router.init(instance: config.instance))
  end

  defp statuses(config, tenant, source_id, count) do
    for _ <- 1..count do
      post(config, "/webhooks/#{tenant}/#{source_id}", "{}").status
    end
  end

  test "over the limit is 429, stores nothing, and leaves other tenants alone" do
    config = start_edge(%{tenants: %{"acme" => %{rate: 0.001, burst: 2}}})

    :telemetry.attach(
      "rate-limit-rejected",
      [:ankusa, :rate_limit, :rejected],
      fn event, measurements, meta, pid -> send(pid, {event, measurements, meta}) end,
      self()
    )

    on_exit(fn -> :telemetry.detach("rate-limit-rejected") end)

    assert statuses(config, "acme", "demo", 3) == [201, 201, 429]

    # The 429 is a real denial, not a hint: the third hook never reached the log.
    assert length(stored_ids(config.instance)) == 2

    denied = post(config, "/webhooks/acme/demo", "{}")
    assert Plug.Conn.get_resp_header(denied, "retry-after") == ["1000"]
    assert JSON.decode!(denied.resp_body) == %{"error" => "rate_limited"}

    # A tenant with no limit of its own is untouched by acme's.
    assert post(config, "/webhooks/globex/demo", "{}").status == 201

    assert_receive {[:ankusa, :rate_limit, :rejected], %{},
                    %{tenant_id: "acme", source_id: "demo"}}
  end

  test "the default applies unless the tenant has its own limit" do
    config =
      start_edge(%{
        default: %{rate: 0.001, burst: 1},
        tenants: %{"vip" => %{rate: 0.001, burst: 3}}
      })

    assert statuses(config, "globex", "demo", 2) == [201, 429]
    assert statuses(config, "vip", "demo", 4) == [201, 201, 201, 429]
  end

  test "the bucket refills at the configured rate" do
    config = start_edge(%{tenants: %{"acme" => %{rate: 2, burst: 1}}})

    assert post(config, "/webhooks/acme/demo", "{}").status == 201
    assert post(config, "/webhooks/acme/demo", "{}").status == 429

    # rate: 2/s, burst: 1 — one half-second's worth of refill admits the next.
    Process.sleep(600)
    assert post(config, "/webhooks/acme/demo", "{}").status == 201
  end

  test "only hooks verification accepted spend the tenant's budget" do
    sources = %{
      "signed" => [
        verifier: {Ankusa.Verifier.Hmac, scheme: :standard_webhooks, secret: @secret},
        on_verify_failure: :reject
      ]
    }

    config = start_edge(%{tenants: %{"acme" => %{rate: 0.001, burst: 1}}}, sources)

    forged = [
      {"webhook-id", "msg_forged"},
      {"webhook-timestamp", to_string(System.system_time(:second))},
      {"webhook-signature", "v1,deadbeef"}
    ]

    # Two forgeries: both 401, neither charged, so a flood of them can never
    # exhaust the tenant's budget.
    assert post(config, "/webhooks/acme/signed", "{}", forged).status == 401
    assert post(config, "/webhooks/acme/signed", "{}", forged).status == 401

    body = ~s({"event":"real"})

    assert post(
             config,
             "/webhooks/acme/signed",
             body,
             standard_webhooks_headers("msg_1", body, @secret)
           ).status == 201

    signed_2 = standard_webhooks_headers("msg_2", body, @secret)
    assert post(config, "/webhooks/acme/signed", body, signed_2).status == 429
  end

  test "concurrent hits never overshoot the burst" do
    config = start_edge(%{tenants: %{"acme" => %{rate: 0.001, burst: 50}}})

    results =
      1..200
      |> Enum.map(fn _ -> Task.async(fn -> RateLimiter.hit(config.instance, "acme") end) end)
      |> Task.await_many(5_000)

    assert Enum.count(results, &(&1 == :ok)) == 50
    assert Enum.count(results, &match?({:error, {:rate_limited, _}}, &1)) == 150
  end

  test "an override applies at once, survives a restart, and can be deleted" do
    config = test_config(rate_limits: %{tenants: %{"acme" => %{rate: 0.001, burst: 1}}})
    put_config(config)
    start_store!(config)

    {:ok, pid} = start_limiter(config)
    inst = config.instance

    assert RateLimiter.hit(inst, "acme") == :ok
    assert {:error, {:rate_limited, _}} = RateLimiter.hit(inst, "acme")

    assert {:ok, %{rate: 0.001, burst: 2}} =
             RateLimiter.put_override(inst, "acme", %{"rate" => 0.001, "burst" => 2})

    # The bucket was reset by the write: a raised limit is not held back by the
    # old limit's accumulated debt.
    assert RateLimiter.hit(inst, "acme") == :ok
    assert RateLimiter.hit(inst, "acme") == :ok
    assert {:error, {:rate_limited, _}} = RateLimiter.hit(inst, "acme")

    assert {_, :override} = RateLimiter.effective(inst, "acme")

    # A full restart: the limiter and the store under it both stop.
    GenServer.stop(pid)
    stop_supervised!({Ankusa.Store, inst})
    start_store!(config)
    {:ok, _pid} = start_limiter(config)

    assert {_, :override} = RateLimiter.effective(inst, "acme")

    assert RateLimiter.delete_override(inst, "acme") == :ok
    assert RateLimiter.effective(inst, "acme") == {%{rate: 0.001, burst: 1}, :config}
    assert RateLimiter.delete_override(inst, "acme") == {:error, :not_found}
  end

  test "an override row that cannot be read back is skipped, not fatal" do
    config = test_config()
    put_config(config)
    inst = config.instance
    start_store!(config)

    :ok =
      Ankusa.Store.write(
        inst,
        [{:put, :default, Ankusa.Store.Keys.rate_limit("acme"), "not json"}],
        sync: true
      )

    start_limiter(config)

    assert RateLimiter.effective(inst, "acme") == {nil, :none}

    assert {:ok, %{rate: 1, burst: 2}} =
             RateLimiter.put_override(inst, "acme", %{"rate" => 1, "burst" => 2})
  end

  @tag :capture_log
  test "the limiter refuses to boot when the store cannot be read" do
    config = test_config()
    put_config(config)
    Process.flag(:trap_exit, true)

    assert {:error, {:rate_limits_load_failed, :store_unavailable}} =
             RateLimiter.start_link(instance: config.instance, config: config)
  end

  defp start_store!(config) do
    start_supervised!({Ankusa.Store, instance: config.instance, config: config})
  end

  defp start_limiter(config) do
    {:ok, pid} = RateLimiter.start_link(instance: config.instance, config: config)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    {:ok, pid}
  end

  # The router is one of several `Ingest.ingest/2` callers; this is the call
  # that says so, without a socket.
  test "Ingest.ingest/2 itself returns the denial" do
    config = start_edge(%{tenants: %{"acme" => %{rate: 0.001, burst: 1}}})

    req = Map.put(request("demo", "{}"), :tenant_id, "acme")

    assert {:ok, _env} = Ingest.ingest(config.instance, req)
    assert {:error, {:rate_limited, _}} = Ingest.ingest(config.instance, req)
  end
end
