defmodule Ankusa.Edge.RateLimitTest do
  @moduledoc """
  Ingest admission, end to end: a real edge instance, real POSTs through the
  router, and the store as the witness for "nothing was stored". The per-tenant
  rate limits, and (TEN-2) what runs before the body is read — per address, per
  node, per source — and the tenant's byte quota, body cap and queue share.

  Timing matters to one property only — that a bucket refills — so the limits
  here are chosen to make that observable in well under a second, and every
  other test asserts on a bucket that is deliberately far from refilling.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Edge.{Ingest, RateLimiter, Router}

  @secret "whsec_" <> Base.encode64("supersecret-key")

  # `TenantPath` refuses to boot without a default limit, and most of these
  # tests are about one tenant's limit, not the default: unless a test sets its
  # own default, every other tenant gets one that never bites.
  @open %{rate: 1_000_000, burst: 1_000_000}

  @demo %{
    "demo" => [
      verifier: {Ankusa.Verifier.None, []},
      trust_url_tenant: true,
      sinks: [{Ankusa.Sink.Log, []}]
    ]
  }

  defp start_edge(rate_limits, sources \\ @demo, overrides \\ []) do
    config =
      test_config(
        [
          roles: [:edge],
          route_resolver: {Ankusa.RouteResolver.TenantPath, []},
          rate_limits: Map.put_new(rate_limits, :default, @open),
          source_store: {Ankusa.SourceStore.Static, sources: sources}
        ] ++ overrides
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
             RateLimiter.start_link(instance: config.instance)
  end

  defp start_store!(config) do
    # a restart of the store restarts the audit process with it
    _ = stop_supervised({Ankusa.Audit, config.instance})
    store = start_supervised!({Ankusa.Store, instance: config.instance})
    start_supervised!({Ankusa.Audit, instance: config.instance})
    store
  end

  defp start_limiter(config) do
    {:ok, pid} = RateLimiter.start_link(instance: config.instance)
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

  # ── TEN-2: admission before the body, and the tenant's share of the node ────

  # One request over a real socket: the request head, then `sent` bytes of a
  # body that claims to be `declared` long. The rest is never sent, so a reply
  # can only come from a server that did not wait for it.
  defp raw_post(port, path, declared, sent) do
    {:ok, sock} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false])

    :ok =
      :gen_tcp.send(
        sock,
        "POST #{path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: #{declared}\r\n\r\n#{sent}"
      )

    reply = :gen_tcp.recv(sock, 0, 2_000)
    :ok = :gen_tcp.close(sock)
    reply
  end

  defp post_from(config, remote_ip, forwarded_for \\ nil) do
    conn = %{Plug.Test.conn(:post, "/webhooks/acme/demo", "{}") | remote_ip: remote_ip}

    conn =
      if forwarded_for,
        do: Plug.Conn.put_req_header(conn, "x-forwarded-for", forwarded_for),
        else: conn

    Router.call(conn, Router.init(instance: config.instance))
  end

  defp error(conn), do: JSON.decode!(conn.resp_body)["error"]

  defp attach_refused do
    :telemetry.attach(
      "admission-refused",
      [:ankusa, :ingest, :refused],
      fn _event, _measurements, meta, pid -> send(pid, {:refused, meta.reason}) end,
      self()
    )

    on_exit(fn -> :telemetry.detach("admission-refused") end)
  end

  defp held_by(batcher) do
    state = :sys.get_state(batcher)

    state.count +
      Enum.reduce(state.inflight, 0, fn {_ref, %{entries: entries}}, n -> n + length(entries) end)
  end

  defp wait_until(fun, tries \\ 200) do
    cond do
      fun.() ->
        :ok

      tries == 0 ->
        flunk("condition never held")

      true ->
        Process.sleep(10)
        wait_until(fun, tries - 1)
    end
  end

  test "per ip pre-read: an address over its limit is 429 before its body is read" do
    attach_refused()
    config = start_edge(%{per_ip: %{rate: 0.001, burst: 1}}, @demo, port: free_port())

    assert {:ok, "HTTP/1.1 201" <> _} = raw_post(config.port, "/webhooks/acme/demo", 2, "{}")

    # Declares a 1 000 byte body and sends none of it: the 429 can only come
    # from a server that refused without waiting for it.
    assert {:ok, "HTTP/1.1 429" <> _ = reply} =
             raw_post(config.port, "/webhooks/acme/demo", 1_000, "")

    assert reply =~ ~r/retry-after: \d+/
    assert_receive {:refused, :ip_rate_limited}
    assert length(stored_ids(config.instance)) == 1
  end

  test "per ip pre-read: addresses are separate buckets, and X-Forwarded-For counts only behind a trusted proxy" do
    config =
      start_edge(%{per_ip: %{rate: 0.001, burst: 1}}, @demo,
        routes: %{trusted_proxies: ["10.0.0.0/8"]}
      )

    peer = {192, 0, 2, 1}
    assert post_from(config, peer).status == 201

    denied = post_from(config, peer)
    assert denied.status == 429
    assert error(denied) == "ip_rate_limited"
    assert [_] = Plug.Conn.get_resp_header(denied, "retry-after")

    # An untrusted peer's header is not read: it cannot choose its own bucket.
    assert post_from(config, peer, "198.51.100.9").status == 429
    assert post_from(config, {192, 0, 2, 2}).status == 201

    # Behind a trusted proxy the client is the address it forwarded.
    proxy = {10, 0, 0, 1}
    assert post_from(config, proxy, "198.51.100.7").status == 201
    assert post_from(config, proxy, "198.51.100.8").status == 201
    assert post_from(config, proxy, "198.51.100.7").status == 429
  end

  test "per ip pre-read: an IPv6 address counts as its /64" do
    config = start_edge(%{per_ip: %{rate: 0.001, burst: 1}})

    assert post_from(config, {0x2001, 0xDB8, 0, 1, 0, 0, 0, 1}).status == 201
    assert post_from(config, {0x2001, 0xDB8, 0, 1, 0, 0, 0, 2}).status == 429
    assert post_from(config, {0x2001, 0xDB8, 0, 2, 0, 0, 0, 1}).status == 201
  end

  test "a request with no per ip limit configured is never charged to an address" do
    config = start_edge(%{})
    assert for(_ <- 1..5, do: post_from(config, {192, 0, 2, 1}).status) == List.duplicate(201, 5)
  end

  test "node global: one bucket for every tenant and source, refused before the read" do
    attach_refused()
    config = start_edge(%{node: %{rate: 0.001, burst: 2}})

    assert post(config, "/webhooks/acme/demo", "{}").status == 201
    assert post(config, "/webhooks/globex/demo", "{}").status == 201

    denied = post(config, "/webhooks/initech/demo", "{}")
    assert denied.status == 429
    assert error(denied) == "node_rate_limited"
    assert Plug.Conn.get_resp_header(denied, "retry-after") == ["1000"]
    assert_receive {:refused, :node_rate_limited}
    assert length(stored_ids(config.instance)) == 2
  end

  test "an address refused by per ip has not spent the node's budget" do
    config = start_edge(%{per_ip: %{rate: 0.001, burst: 1}, node: %{rate: 0.001, burst: 2}})

    assert post_from(config, {192, 0, 2, 1}).status == 201
    assert post_from(config, {192, 0, 2, 1}).status == 429
    assert post_from(config, {192, 0, 2, 1}).status == 429

    # One of the node's two tokens is left for somebody else.
    assert post_from(config, {192, 0, 2, 2}).status == 201
    assert error(post_from(config, {192, 0, 2, 3})) == "node_rate_limited"
  end

  test "per source bytes: a declared length is charged before the read, an undeclared one after it" do
    attach_refused()
    sources = Map.put(@demo, "chunked", @demo["demo"])
    config = start_edge(%{sources: %{default: %{bytes_per_second: 1_000}}}, sources)

    body = String.duplicate("x", 600)
    declared = [{"content-length", "600"}]

    assert post(config, "/webhooks/acme/demo", body, declared).status == 201

    denied = post(config, "/webhooks/acme/demo", body, declared)
    assert denied.status == 429
    assert error(denied) == "source_rate_limited"
    assert [_] = Plug.Conn.get_resp_header(denied, "retry-after")
    assert_receive {:refused, :source_rate_limited}

    # Another source has its own bucket; no content-length means the bytes are
    # charged once they have been read.
    assert post(config, "/webhooks/acme/chunked", body).status == 201
    chunked = post(config, "/webhooks/acme/chunked", body)
    assert chunked.status == 429
    assert error(chunked) == "source_rate_limited"

    assert length(stored_ids(config.instance)) == 2
  end

  test "per source bytes: a body bigger than the bucket is admitted into a full one, not never" do
    config = start_edge(%{sources: %{default: %{bytes_per_second: 100, burst_bytes: 100}}})

    body = String.duplicate("x", 5_000)
    declared = [{"content-length", "5000"}]

    assert post(config, "/webhooks/acme/demo", body, declared).status == 201
    assert post(config, "/webhooks/acme/demo", body, declared).status == 429
  end

  test "the source ceiling can be switched off" do
    config = start_edge(%{sources: %{default: nil}})
    body = String.duplicate("x", 600)

    assert for(_ <- 1..3, do: post(config, "/webhooks/acme/demo", body).status) ==
             [201, 201, 201]
  end

  test "tenant bytes: bytes_per_second is a quota charged after verification, with the hook" do
    config =
      start_edge(%{
        tenants: %{"acme" => %{rate: 1_000_000, burst: 1_000_000, bytes_per_second: 1_000}}
      })

    :telemetry.attach(
      "bytes-rejected",
      [:ankusa, :rate_limit, :rejected],
      fn _event, _measurements, meta, pid -> send(pid, {:rejected, meta.tenant_id}) end,
      self()
    )

    on_exit(fn -> :telemetry.detach("bytes-rejected") end)

    body = String.duplicate("x", 600)
    assert post(config, "/webhooks/acme/demo", body).status == 201

    denied = post(config, "/webhooks/acme/demo", body)
    assert denied.status == 429
    assert error(denied) == "rate_limited"
    assert [_] = Plug.Conn.get_resp_header(denied, "retry-after")
    assert_receive {:rejected, "acme"}
    assert length(stored_ids(config.instance)) == 1

    # Another tenant's bytes are its own.
    assert post(config, "/webhooks/globex/demo", body).status == 201
    assert post(config, "/webhooks/globex/demo", body).status == 201
  end

  test "tenant bytes: a forged request spends none of the tenant's bytes" do
    sources = %{
      "signed" => [
        verifier: {Ankusa.Verifier.Hmac, scheme: :standard_webhooks, secret: @secret},
        on_verify_failure: :reject
      ]
    }

    config =
      start_edge(
        %{tenants: %{"acme" => %{rate: 1_000_000, burst: 1_000_000, bytes_per_second: 1_000}}},
        sources
      )

    body = String.duplicate("x", 900)

    forged = [
      {"webhook-id", "msg_forged"},
      {"webhook-timestamp", to_string(System.system_time(:second))},
      {"webhook-signature", "v1,deadbeef"}
    ]

    for _ <- 1..3, do: assert(post(config, "/webhooks/acme/signed", body, forged).status == 401)

    headers = standard_webhooks_headers("msg_1", body, @secret)
    assert post(config, "/webhooks/acme/signed", body, headers).status == 201
  end

  test "tenant body cap: a tenant's max_body_bytes is 413, before the read when declared" do
    attach_refused()

    config =
      start_edge(
        %{tenants: %{"acme" => %{rate: 1_000_000, burst: 1_000_000, max_body_bytes: 16}}},
        @demo,
        port: free_port()
      )

    big = String.duplicate("x", 100)

    # No declared length: the read itself stops at the tenant's cap.
    capped = post(config, "/webhooks/acme/demo", big)
    assert capped.status == 413
    assert JSON.decode!(capped.resp_body) == %{"error" => "payload_too_large", "limit" => 16}
    assert post(config, "/webhooks/acme/demo", "{}").status == 201

    # Another tenant is held to the instance's limit alone.
    assert post(config, "/webhooks/globex/demo", big).status == 201

    # Declared: refused on the header, so the body that never arrives is not waited for.
    assert {:ok, "HTTP/1.1 413" <> _} = raw_post(config.port, "/webhooks/acme/demo", 100, "x")
    assert_receive {:refused, :payload_too_large}

    assert length(stored_ids(config.instance)) == 2
  end

  test "tenant body cap: a refused body spends none of the pre-read budgets" do
    config =
      start_edge(
        %{
          node: %{rate: 0.001, burst: 1},
          tenants: %{"acme" => %{rate: 1_000_000, burst: 1_000_000, max_body_bytes: 16}}
        },
        @demo
      )

    declared = [{"content-length", "100"}]
    body = String.duplicate("x", 100)

    assert post(config, "/webhooks/acme/demo", body, declared).status == 413
    assert post(config, "/webhooks/acme/demo", body, declared).status == 413
    assert post(config, "/webhooks/globex/demo", "{}").status == 201
  end

  test "tenant overload isolates: a tenant past its queue share is 429 while another is 201" do
    attach_ingest_outcomes()

    config =
      start_edge(
        %{tenants: %{"acme" => %{rate: 1_000_000, burst: 1_000_000, max_queue: 2}}},
        @demo,
        batcher: %{partitions: 1, max_batch: 1, max_delay_ms: 0}
      )

    inst = config.instance
    writer = Ankusa.whereis(inst, :queue_writer)
    batcher = Ankusa.whereis(inst, {:batcher, 0})

    # Nothing commits while the writer is stuck, so what is admitted is held.
    :ok = :sys.suspend(writer)
    acme = for _ <- 1..2, do: Task.async(fn -> post(config, "/webhooks/acme/demo", "{}") end)
    wait_until(fn -> held_by(batcher) == 2 end)

    # Acme holds its whole share: its own 429, not the node's 503.
    over = post(config, "/webhooks/acme/demo", "{}")
    assert over.status == 429
    assert error(over) == "tenant_overload"
    assert Plug.Conn.get_resp_header(over, "retry-after") == ["1"]
    assert_receive {:outcome, :tenant_overload}

    # Globex is not in acme's queue share: it gets in behind it.
    globex = Task.async(fn -> post(config, "/webhooks/globex/demo", "{}") end)
    wait_until(fn -> held_by(batcher) == 3 end)

    :ok = :sys.resume(writer)
    assert Enum.map(Task.await_many(acme ++ [globex], 30_000), & &1.status) == [201, 201, 201]

    # What was committed gave its share back; the 429 was never stored.
    assert post(config, "/webhooks/acme/demo", "{}").status == 201
    assert length(stored_ids(inst)) == 4
  end

  test "tenant overload isolates: max_queue_bytes counts bodies, and a lone big hook still gets in" do
    config =
      start_edge(
        %{tenants: %{"acme" => %{rate: 1_000_000, burst: 1_000_000, max_queue_bytes: 1_000}}},
        @demo,
        batcher: %{partitions: 1, max_batch: 1, max_delay_ms: 0}
      )

    inst = config.instance
    writer = Ankusa.whereis(inst, :queue_writer)
    batcher = Ankusa.whereis(inst, {:batcher, 0})
    :ok = :sys.suspend(writer)

    # Bigger than the share on its own, and the tenant holds nothing else.
    big = String.duplicate("x", 5_000)
    first = Task.async(fn -> post(config, "/webhooks/acme/demo", big) end)
    wait_until(fn -> held_by(batcher) == 1 end)

    assert error(post(config, "/webhooks/acme/demo", "{}")) == "tenant_overload"

    :ok = :sys.resume(writer)
    assert Task.await(first, 30_000).status == 201
    assert post(config, "/webhooks/acme/demo", "{}").status == 201
  end

  test "a tenant over its queue share does not turn into the node's load shed" do
    config =
      start_edge(
        %{tenants: %{"acme" => %{rate: 1_000_000, burst: 1_000_000, max_queue: 1}}},
        @demo,
        batcher: %{partitions: 1, max_batch: 1, max_delay_ms: 0, max_queue: 100}
      )

    inst = config.instance
    writer = Ankusa.whereis(inst, :queue_writer)
    batcher = Ankusa.whereis(inst, {:batcher, 0})
    :ok = :sys.suspend(writer)

    held = Task.async(fn -> post(config, "/webhooks/acme/demo", "{}") end)
    wait_until(fn -> held_by(batcher) == 1 end)

    statuses = for _ <- 1..20, do: post(config, "/webhooks/acme/demo", "{}").status
    assert Enum.uniq(statuses) == [429]

    :ok = :sys.resume(writer)
    assert Task.await(held, 30_000).status == 201
  end

  defp attach_ingest_outcomes do
    :telemetry.attach(
      "admission-outcomes",
      [:ankusa, :ingest, :stop],
      fn _event, _measurements, meta, pid -> send(pid, {:outcome, meta.outcome}) end,
      self()
    )

    on_exit(fn -> :telemetry.detach("admission-outcomes") end)
  end

  test "queue shares: counted per tenant, refused past the limit, given back, never negative" do
    config =
      start_edge(%{
        tenants: %{
          "acme" => %{rate: 1, burst: 1, max_queue: 2},
          "bulk" => %{rate: 1, burst: 1, max_queue_bytes: 100}
        }
      })

    inst = config.instance

    assert {:ok, first} = RateLimiter.acquire_queue(inst, "acme", 10)
    assert {:ok, second} = RateLimiter.acquire_queue(inst, "acme", 10)
    assert {:error, {:tenant_overload, 1_000}} = RateLimiter.acquire_queue(inst, "acme", 10)

    # The refusal counted nothing: one release makes room for exactly one more.
    assert :ok = RateLimiter.release_queue(inst, first)
    assert {:ok, third} = RateLimiter.acquire_queue(inst, "acme", 10)
    assert {:error, {:tenant_overload, _}} = RateLimiter.acquire_queue(inst, "acme", 10)

    # Everything given back, and then some: a counter never goes below zero, so a
    # stray release (a limiter that restarted under in-flight hooks) cannot
    # grant the tenant more than its share afterwards.
    assert :ok = RateLimiter.release_queue(inst, second)
    assert :ok = RateLimiter.release_queue(inst, third)
    assert :ok = RateLimiter.release_queue(inst, first)
    assert {:ok, _} = RateLimiter.acquire_queue(inst, "acme", 10)
    assert {:ok, _} = RateLimiter.acquire_queue(inst, "acme", 10)
    assert {:error, {:tenant_overload, _}} = RateLimiter.acquire_queue(inst, "acme", 10)

    # A tenant with no queue limit of its own counts nothing at all.
    assert {:ok, nil} = RateLimiter.acquire_queue(inst, "globex", 10)
    assert :ok = RateLimiter.release_queue(inst, nil)

    # Bytes: a lone hook over the share is admitted; a second is not.
    assert {:ok, lone} = RateLimiter.acquire_queue(inst, "bulk", 500)
    assert {:error, {:tenant_overload, _}} = RateLimiter.acquire_queue(inst, "bulk", 10)
    assert :ok = RateLimiter.release_queue(inst, lone)
    assert {:ok, _} = RateLimiter.acquire_queue(inst, "bulk", 90)
    assert {:ok, _} = RateLimiter.acquire_queue(inst, "bulk", 10)
    assert {:error, {:tenant_overload, _}} = RateLimiter.acquire_queue(inst, "bulk", 1)
  end

  test "concurrent queue charges never overshoot the share" do
    config =
      start_edge(%{tenants: %{"acme" => %{rate: 1, burst: 1, max_queue: 50}}})

    results =
      1..200
      |> Enum.map(fn _ ->
        Task.async(fn -> RateLimiter.acquire_queue(config.instance, "acme", 1) end)
      end)
      |> Task.await_many(5_000)

    assert Enum.count(results, &match?({:ok, _}, &1)) == 50
    assert Enum.count(results, &match?({:error, {:tenant_overload, _}}, &1)) == 150
  end

  test "an override carries the optional keys, replaces the limit whole, and survives a restart" do
    config = test_config(rate_limits: %{tenants: %{"acme" => %{rate: 1, burst: 1, max_queue: 5}}})
    put_config(config)
    start_store!(config)

    {:ok, pid} = start_limiter(config)
    inst = config.instance

    assert RateLimiter.body_limit(inst, "acme") == nil

    attrs = %{
      "rate" => 5,
      "burst" => 5,
      "bytes_per_second" => 1_000,
      "burst_bytes" => 2_000,
      "max_body_bytes" => 1_000,
      "max_queue" => 3,
      "max_queue_bytes" => 4_000
    }

    assert {:ok, limit} = RateLimiter.put_override(inst, "acme", attrs)
    assert limit.max_queue == 3 and limit.max_body_bytes == 1_000
    assert RateLimiter.body_limit(inst, "acme") == 1_000

    GenServer.stop(pid)
    stop_supervised!({Ankusa.Store, inst})
    start_store!(config)
    {:ok, _pid} = start_limiter(config)

    assert {^limit, :override} = RateLimiter.effective(inst, "acme")

    # The override replaced the tenant's limit whole: the config's max_queue is gone.
    assert RateLimiter.delete_override(inst, "acme") == :ok
    assert {%{max_queue: 5}, :config} = RateLimiter.effective(inst, "acme")
  end

  test "an override with a bad optional key is refused with the parser's message" do
    config = start_edge(%{})
    inst = config.instance
    base = %{"rate" => 1, "burst" => 1}

    for {extra, message} <- [
          {%{"max_queue" => 0}, "max_queue must be an integer of at least 1, got 0"},
          {%{"max_queue_bytes" => "big"},
           "max_queue_bytes must be an integer of at least 1, got \"big\""},
          {%{"bytes_per_second" => -1},
           "bytes_per_second must be a number greater than 0, got -1"},
          {%{"burst_bytes" => 10}, "burst_bytes needs bytes_per_second"},
          {%{"max_body_bytes" => 8_000_001},
           "max_body_bytes must be an integer of at least 1 and at most the instance's " <>
             "max_body_bytes (8000000), got 8000001"},
          {%{"per" => 1}, "unknown field \"per\""}
        ] do
      assert {:error, :invalid, ^message} =
               RateLimiter.put_override(inst, "acme", Map.merge(base, extra))
    end
  end

  describe "the bucket sweep (PERF-1)" do
    # Expired rows (a TAT in the past is a full bucket) in the shard each key
    # hashes to, as the limiter itself would have left them.
    defp seed_expired(instance, keys) do
      expired = System.monotonic_time() - System.convert_time_unit(10, :second, :native)

      for {shard, rows} <- Enum.group_by(keys, &RateLimiter.bucket_shard/1, &{&1, expired}) do
        :ets.insert(RateLimiter.bucket_table(instance, shard), rows)
      end

      :ok
    end

    defp shard_sizes(instance) do
      for shard <- 0..15, do: :ets.info(RateLimiter.bucket_table(instance, shard), :size)
    end

    test "a tenant's buckets hash to one shard, and the shards are spread" do
      assert RateLimiter.bucket_shard("acme") == RateLimiter.bucket_shard("acme")

      shards = for i <- 1..2_000, uniq: true, do: RateLimiter.bucket_shard("tenant-#{i}")
      assert Enum.sort(shards) == Enum.to_list(0..15)
    end

    test "a tick sweeps one shard, in turn, and leaves a live bucket alone" do
      config = start_edge(%{tenants: %{"acme" => %{rate: 0.001, burst: 1}}})
      inst = config.instance
      assert :ok = RateLimiter.hit(inst, "acme")

      seed_expired(inst, for(i <- 1..4_000, do: {:ip, i}))
      before = shard_sizes(inst)
      assert Enum.sum(before) == 4_000 + 1

      pid = GenServer.whereis(Ankusa.via(inst, :rate_limiter))
      send(pid, :sweep)
      _ = :sys.get_state(pid)
      after_one = shard_sizes(inst)

      live_shard = RateLimiter.bucket_shard("acme")
      assert hd(after_one) == if(live_shard == 0, do: 1, else: 0)
      assert tl(after_one) == tl(before)

      send(pid, :sweep)
      _ = :sys.get_state(pid)
      assert shard_sizes(inst) |> Enum.take(2) |> Enum.sum() == if(live_shard < 2, do: 1, else: 0)

      for _ <- 3..16, do: send(pid, :sweep)
      _ = :sys.get_state(pid)
      assert Enum.sum(shard_sizes(inst)) == 1
      assert {:error, {:rate_limited, _}} = RateLimiter.hit(inst, "acme")
    end

    test "a table of a million expired rows sweeps in 16 ticks, each under 50 ms" do
      config = start_edge(%{})
      inst = config.instance
      seed_expired(inst, for(i <- 1..1_000_000, do: {:ip, i}))
      assert Enum.sum(shard_sizes(inst)) == 1_000_000

      millis =
        for shard <- 0..15 do
          {micros, deleted} = :timer.tc(fn -> RateLimiter.sweep(inst, shard) end)
          assert deleted > 0
          micros / 1_000
        end

      assert Enum.max(millis) < 50
      assert Enum.sum(shard_sizes(inst)) == 0
    end
  end
end
