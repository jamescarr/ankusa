# The audit harness: an instance that makes every kind of audited change through
# its routers and then waits to be killed, for `test/ankusa/admin/audit_test.exs`
# ("audit row survives kill").
#
# It runs as its own OS process, with no Mix and no `:ankusa` application, so the
# process that kills it gets no chance to flush anything:
#
#     ERL_LIBS=<build>/lib DATA_DIR=/data elixir audit_harness.exs serve
#
# `serve` boots one `:edge` + `:dispatch` instance with the admin API, the
# route-management API and a writable source store on `DATA_DIR`, makes the
# changes in `steps/0` through `Ankusa.Admin.Router` and `Ankusa.Routes.Router`
# (the response is the router's own), prints `STEP <n> <method> <path> <status>`
# after each response and `READY` after the last, then sleeps. The parent
# SIGKILLs it the moment it reads `READY`.
#
# Required by `test_helper.exs` for `Ankusa.AuditHarness.config/1` and
# `expected_actions/0`: the file only runs a command when its argv is exactly
# `serve`.
defmodule Ankusa.AuditHarness do
  @moduledoc false

  @instance :audit_harness
  @canary "audit-harness-canary-secret"
  @rotated "audit-harness-rotated-secret"

  @doc "A value that must never reach an audit row."
  def canary, do: @canary
  def rotated, do: @rotated

  @doc false
  def decoder(_source_id, _spec),
    do: [verifier: {Ankusa.Verifier.None, []}, sinks: [{Ankusa.Sink.Log, []}]]

  @doc "The instance config both the serving child and a reopening parent use."
  def config(data_dir) do
    Ankusa.Config.new(
      instance: @instance,
      data_dir: data_dir,
      port: 0,
      roles: [:edge, :dispatch],
      source_store: {Ankusa.SourceStore.Persistent, decoder: &__MODULE__.decoder/2},
      routes: %{enabled: true, admin: %{port: 0}},
      admin: %{enabled: true, port: 0}
    )
  end

  @doc "What the steps leave in the trail, oldest first."
  def expected_actions do
    ~w(source.create source.update rate_limit.put rate_limit.delete quarantine.purge
       replay.create replay.cancel route.create route.update route.update ip_rules.put
       route.delete source.delete)
  end

  @doc "Boot the instance, make every change once, print `READY`, wait to be killed."
  def serve do
    config = config(System.fetch_env!("DATA_DIR"))

    # stdout carries only `STEP`/`READY` lines; log output goes to stderr.
    :ok = :logger.remove_handler(:default)
    :ok = :logger.add_handler(:default, :logger_std_h, %{config: %{type: :standard_error}})

    {:ok, _} = Application.ensure_all_started(:bandit)
    # The route guard's decision cache is a Nebulex cache.
    {:ok, _} = Application.ensure_all_started(:nebulex)
    {:ok, _} = Registry.start_link(keys: :unique, name: Ankusa.Registry)
    Ankusa.put_config(config)
    {:ok, _} = Ankusa.Instance.start_link(config)

    steps()
    IO.puts("READY")
    Process.sleep(:infinity)
  end

  defp steps do
    admin = Ankusa.Admin.Router
    routes = Ankusa.Routes.Router

    source = fn secret ->
      %{"verify" => %{"type" => "hmac", "secret" => secret}, "sinks" => [%{"type" => "log"}]}
    end

    step(
      1,
      admin,
      :post,
      "/v1/tenants/acme/sources",
      Map.put(source.(@canary), "name", "billing")
    )

    step(2, admin, :put, "/v1/tenants/acme/sources/billing", source.(@rotated))
    step(3, admin, :put, "/v1/tenants/acme/rate-limit", %{"rate" => 5, "burst" => 10})
    step(4, admin, :delete, "/v1/tenants/acme/rate-limit", nil)
    step(5, admin, :delete, "/v1/quarantine", nil)

    job = step(6, admin, :post, "/v1/replays", %{"kind" => "dlq"})
    step(7, admin, :patch, "/v1/replays/#{job["id"]}", %{"state" => "cancelled"})

    step(8, routes, :post, "/admin/routes", %{
      "id" => "stripe",
      "path" => "/hooks/stripe",
      "methods" => ["POST"]
    })

    step(9, routes, :put, "/admin/routes/stripe", %{
      "path" => "/hooks/stripe",
      "methods" => ["POST"],
      "enabled" => false
    })

    step(10, routes, :patch, "/admin/routes/stripe", %{"metadata" => %{"owner" => "payments"}})
    step(11, routes, :put, "/admin/ip-rules", %{"default" => "allow", "rules" => []})
    step(12, routes, :delete, "/admin/routes/stripe", nil)
    step(13, admin, :delete, "/v1/tenants/acme/sources/billing", nil)
  end

  defp step(n, router, method, path, body) do
    conn =
      method
      |> Plug.Test.conn(path, if(body, do: JSON.encode!(body), else: ""))
      |> Plug.Conn.put_req_header("content-type", "application/json")

    conn = router.call(conn, router.init(instance: @instance))
    IO.puts("STEP #{n} #{method} #{path} #{conn.status}")

    case JSON.decode(conn.resp_body) do
      {:ok, %{} = decoded} -> decoded
      _ -> nil
    end
  end
end

case System.argv() do
  ["serve"] -> Ankusa.AuditHarness.serve()
  _ -> :ok
end
