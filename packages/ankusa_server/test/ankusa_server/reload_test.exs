defmodule AnkusaServer.ReloadTest do
  @moduledoc """
  Config reload from the operator's side: a running instance booted from a YAML
  file, the file edited, then `POST /v1/config/reload` (through the admin router,
  with a bearer token) or a real `SIGHUP`. What may change takes effect with no
  restart; what may not is refused with the key named, and nothing is applied.
  """

  # The Logger level and the signal handlers are the VM's.
  use ExUnit.Case, async: false

  alias Ankusa.Admin.Router
  alias Ankusa.Edge.RateLimiter
  alias AnkusaServer.Reload

  @moduletag :tmp_dir
  @moduletag capture_log: true

  setup do
    original = Logger.level()
    Logger.configure(level: :info)
    on_exit(fn -> Logger.configure(level: original) end)
  end

  defp yaml(dir, opts \\ []) do
    """
    node: {roles: [edge, dispatch], data_dir: "#{dir}/data"}
    log: {level: #{opts[:level] || "info"}}
    http: {port: #{opts[:port] || 4000}, max_body_bytes: #{opts[:max_body_bytes] || 1_000_000}}
    admin:
      enabled: false
      auth:
        mode: token
        tokens:
          - {id: operator, token: operator-token, scopes: [config:write, audit:read, "tenant:*"]}
          - {id: reader, token: reader-token, scopes: [config:read, "tenant:*"]}
          - {id: scoped, token: scoped-token, scopes: [config:write, "tenant:acme"]}
    routes:
      enabled: true
      admin: {port: 0}
      seed:
        - {id: a, path: "#{opts[:seed_path] || "/hooks/a"}"}
    rate_limits:
      tenants:
        acme: {rate: #{opts[:rate] || 1}, burst: #{opts[:burst] || 2}}
    dispatch: {concurrency: #{opts[:concurrency] || 4}}
    sources:
      gh:
        verify: {type: github, secret: #{opts[:secret] || "s1"}}
        sinks: [{type: log}]
    """
  end

  defp boot(%{tmp_dir: dir}) do
    inst = :"reload_#{System.unique_integer([:positive])}"
    path = Path.join(dir, "ankusa.yml")
    File.write!(path, yaml(dir))

    loaded = AnkusaServer.Config.load!(path: path, env: %{})
    start_supervised!({Ankusa.Instance, %{loaded.config | instance: inst, port: 0}})

    Reload.remember(inst, loaded)
    Ankusa.Reload.register(inst, &Reload.reload(inst, audit: &1, path: path, env: %{}))
    %{inst: inst, path: path, dir: dir}
  end

  defp rewrite(%{path: path, dir: dir}, opts), do: File.write!(path, yaml(dir, opts))

  defp call(inst, method, path, token \\ "operator-token") do
    conn = Plug.Test.conn(method, path, "")

    conn =
      if token,
        do: Plug.Conn.put_req_header(conn, "authorization", "Bearer " <> token),
        else: conn

    Router.call(conn, Router.init(instance: inst))
  end

  defp json(conn), do: JSON.decode!(conn.resp_body)

  defp reload!(inst), do: call(inst, :post, "/v1/config/reload")

  defp rows(inst), do: json(call(inst, :get, "/v1/audit?action=config.reload"))["entries"]

  defp eventually(fun, tries \\ 100) do
    cond do
      fun.() ->
        :ok

      tries == 0 ->
        flunk("did not happen in time")

      true ->
        Process.sleep(50)
        eventually(fun, tries - 1)
    end
  end

  test "applies reloadable: log.level, a tenant rate limit, a source secret, the routes seed and dispatch.concurrency take effect with no restart",
       ctx do
    %{inst: inst} = node = boot(ctx)
    assert {%{rate: 1, burst: 2}, :config} = RateLimiter.effective(inst, "acme")
    assert Logger.level() == :info

    rewrite(node,
      level: "debug",
      rate: 9,
      burst: 20,
      secret: "s2",
      seed_path: "/hooks/a2",
      concurrency: 8
    )

    conn = reload!(inst)
    assert conn.status == 200

    assert %{"status" => "reloaded", "keys" => keys, "sources" => ["gh"], "kept" => []} =
             json(conn)

    assert keys == [
             "dispatch.concurrency",
             "log.level",
             "rate_limits.tenants.acme.burst",
             "rate_limits.tenants.acme.rate",
             "routes.seed[0].path",
             "sources.gh.verify.secret"
           ]

    assert Logger.level() == :debug
    assert {%{rate: 9, burst: 20}, :config} = RateLimiter.effective(inst, "acme")
    assert {:ok, %{path: "/hooks/a2"}} = Ankusa.Routes.get(inst, "a")
    assert :sys.get_state(Ankusa.via(inst, :dispatch)).config.dispatch.concurrency == 8

    {Ankusa.SourceStore.Static, opts} = Ankusa.config(inst).source_store
    assert %{verifier: {_hmac, verifier}} = Ankusa.Source.new("gh", opts[:sources]["gh"])
    assert verifier[:secret] == "s2"

    # one audit row, by the caller, naming what changed
    assert [row] = rows(inst)
    assert %{"actor" => "operator", "result" => "ok", "detail" => %{"keys" => ^keys}} = row
  end

  test "a reload that leaves `log` alone keeps the level an operator set at runtime", ctx do
    %{inst: inst} = node = boot(ctx)
    Logger.configure(level: :error)

    rewrite(node, rate: 3)
    assert reload!(inst).status == 200

    assert Logger.level() == :error
    assert {%{rate: 3}, :config} = RateLimiter.effective(inst, "acme")
  end

  test "refuses non-reloadable: a changed port is a 409 naming the key, and nothing in the file is applied",
       ctx do
    %{inst: inst} = node = boot(ctx)

    # `log.level` and the rate limit would be reloadable on their own
    rewrite(node, port: 4001, max_body_bytes: 2_000_000, level: "debug", rate: 9, burst: 20)

    conn = reload!(inst)
    assert conn.status == 409

    assert %{"error" => "not_reloadable", "keys" => ["http.max_body_bytes", "http.port"]} =
             json(conn)

    assert Logger.level() == :info
    assert {%{rate: 1, burst: 2}, :config} = RateLimiter.effective(inst, "acme")
    assert rows(inst) == []

    # put the port back and the same file reloads
    rewrite(node, level: "debug", rate: 9, burst: 20)
    assert reload!(inst).status == 200
    assert Logger.level() == :debug
    assert {%{rate: 9, burst: 20}, :config} = RateLimiter.effective(inst, "acme")
  end

  test "a file that no longer loads is a 422 and applies nothing", %{tmp_dir: dir} = ctx do
    %{inst: inst, path: path} = boot(ctx)
    File.write!(path, "log: {level: loud}\n")

    conn = reload!(inst)
    assert conn.status == 422
    assert %{"error" => "invalid_config", "message" => message} = json(conn)
    assert message =~ "log.level"

    File.write!(path, yaml(dir, secret: "${NOT_SET_ANYWHERE}"))
    assert reload!(inst).status == 422
    assert Logger.level() == :info
    assert rows(inst) == []
  end

  test "the route is 401 without a credential and 403 without config:write and tenant:*", ctx do
    %{inst: inst} = boot(ctx)

    assert call(inst, :post, "/v1/config/reload", nil).status == 401
    assert call(inst, :post, "/v1/config/reload", "reader-token").status == 403
    assert call(inst, :post, "/v1/config/reload", "scoped-token").status == 403
    assert rows(inst) == []
  end

  test "SIGHUP reloads the config as the system, and the node keeps running", ctx do
    %{inst: inst} = node = boot(ctx)
    start_supervised!({Reload.Signal, instance: inst, reload: [path: node.path, env: %{}]})

    rewrite(node, level: "warning", rate: 5, burst: 6)
    {_, 0} = System.cmd("kill", ["-HUP", System.pid()])

    eventually(fn -> Logger.level() == :warning end)
    assert {%{rate: 5, burst: 6}, :config} = RateLimiter.effective(inst, "acme")

    assert [%{"actor" => "system", "result" => "ok"}] = rows(inst)

    # and a second signal is just another reload
    rewrite(node, level: "info", rate: 5, burst: 6)
    {_, 0} = System.cmd("kill", ["-HUP", System.pid()])
    eventually(fn -> Logger.level() == :info end)
  end
end
