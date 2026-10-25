defmodule Ankusa.ReloadTest do
  @moduledoc """
  `Ankusa.Reload`: the reloadable fields of a config are applied to a running
  instance and the rest is kept, a config that would not boot applies nothing,
  one audit row brackets a reload, and `POST /v1/config/reload` maps the
  loader's answers to HTTP.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Admin.Auth.Token
  alias Ankusa.Admin.Router
  alias Ankusa.Audit.Context
  alias Ankusa.Edge.RateLimiter
  alias Ankusa.Reload
  alias Ankusa.Routes

  @every_scope Ankusa.Principal.scopes()

  defp token(id, scopes), do: %{id: id, sha256: Token.hash(id), scopes: scopes}

  defp start(extra \\ []) do
    tokens = [
      token("operator", @every_scope ++ ["tenant:*"]),
      token("acme", @every_scope ++ ["tenant:acme"]),
      token("nobody", ["dlq:read"])
    ]

    config =
      test_config(
        Keyword.merge(
          [
            roles: [:edge, :dispatch],
            admin: %{enabled: true, port: 0, auth: %{mode: :token, tokens: tokens}},
            routes: %{
              enabled: true,
              admin: %{port: 0},
              seed: [
                %{"id" => "a", "path" => "/hooks/a"},
                %{"id" => "b", "path" => "/hooks/b"}
              ]
            },
            source_store:
              {Ankusa.SourceStore.Static,
               sources: %{"acme.hooks" => [tenant_id: "acme", sinks: [{Ankusa.Sink.Log, []}]]}},
            rate_limits: %{
              default: %{rate: 5, burst: 10},
              tenants: %{"acme" => %{rate: 1, burst: 2}}
            },
            dispatch: %{concurrency: 4, sink_concurrency: 4}
          ],
          extra
        )
      )

    put_config(config)
    start_supervised!({Ankusa.Instance, config})
    config.instance
  end

  defp call(inst, method, path, token) do
    method
    |> Plug.Test.conn(path, "")
    |> Plug.Conn.put_req_header("authorization", "Bearer " <> token)
    |> then(&Router.call(&1, Router.init(instance: inst)))
  end

  defp json(conn), do: JSON.decode!(conn.resp_body)

  defp rows(inst),
    do: json(call(inst, :get, "/v1/audit?action=config.reload", "operator"))["entries"]

  defp operator, do: %Context{actor: "operator", request_id: "r-1"}

  describe "apply_config/3" do
    test "applies the reloadable set, keeps every other field, and records one audit row" do
      inst = start()
      current = Ankusa.config(inst)
      assert {%{rate: 1, burst: 2}, :config} = RateLimiter.effective(inst, "acme")

      poller = Ankusa.whereis(inst, :metrics_poller)
      reporter = Process.whereis(Ankusa.Metrics.reporter_name(inst))
      assert is_pid(poller) and is_pid(reporter)

      next = %{
        current
        | # not reloadable: the instance's shape
          port: current.port + 1,
          rate_limits: %{
            current.rate_limits
            | scope: :fleet,
              tenants: %{"acme" => %{rate: 7, burst: 9}}
          },
          dispatch: %{
            current.dispatch
            | concurrency: 3,
              sink_concurrency: 2,
              max_inflight: current.dispatch.max_inflight + 1,
              tenants: Map.put(current.dispatch.tenants, "acme", %{concurrency: 1})
          },
          metrics: %{current.metrics | detail: :source},
          admin: %{current.admin | gauge_interval_ms: 20_000}
      }

      keys = ["rate_limits.tenants.acme.rate", "dispatch.concurrency"]

      assert {:ok, %{sources: []}} =
               Reload.apply_config(inst, next, audit: operator(), keys: keys)

      # takes effect on the very next request
      assert {%{rate: 7, burst: 9}, :config} = RateLimiter.effective(inst, "acme")

      applied = Ankusa.config(inst)
      assert applied.dispatch.concurrency == 3
      assert applied.dispatch.sink_concurrency == 2
      assert applied.metrics.detail == :source
      assert applied.admin.gauge_interval_ms == 20_000

      # everything else is the running config's
      assert applied.port == current.port
      assert applied.rate_limits.scope == :node
      assert applied.dispatch.max_inflight == current.dispatch.max_inflight

      # the dispatch pipeline took its new limits without a restart
      state = :sys.get_state(Ankusa.via(inst, :dispatch))
      assert state.config.dispatch.concurrency == 3
      assert state.tenant_overrides["acme"].concurrency == 1

      # the gauge poller and the metric definitions were rebuilt
      assert Ankusa.whereis(inst, :metrics_poller) not in [nil, poller]
      assert Process.whereis(Ankusa.Metrics.reporter_name(inst)) not in [nil, reporter]

      assert [row] = rows(inst)

      assert %{"action" => "config.reload", "actor" => "operator", "result" => "ok"} = row
      assert row["detail"] == %{"keys" => keys}
    end

    test "a config that would not boot applies nothing and writes no audit row" do
      inst = start()
      current = Ankusa.config(inst)

      next = %{current | dispatch: %{current.dispatch | sink_concurrency: 0}}

      assert {:error, {:invalid_config, message}} =
               Reload.apply_config(inst, next, audit: operator())

      assert message =~ "dispatch.sink_concurrency"
      assert Ankusa.config(inst) == current
      assert rows(inst) == []
    end

    test "a changed static source is published, a removed one is gone" do
      inst = start()
      current = Ankusa.config(inst)
      {Ankusa.SourceStore.Static, opts} = current.source_store
      sources = Keyword.fetch!(opts, :sources)

      added = Map.put(sources, "new.hooks", sinks: [{Ankusa.Sink.Log, []}])
      next = %{current | source_store: {Ankusa.SourceStore.Static, sources: added}}

      assert {:ok, %{sources: ["new.hooks"]}} = Reload.apply_config(inst, next)
      assert {:ok, _source} = Ankusa.SourceStore.Static.fetch(inst, "new.hooks")

      gone = %{next | source_store: {Ankusa.SourceStore.Static, sources: %{}}}
      assert {:ok, %{sources: ["acme.hooks", "new.hooks"]}} = Reload.apply_config(inst, gone)
      assert :error = Ankusa.SourceStore.Static.fetch(inst, "new.hooks")
    end

    test "a source the instance would refuse is refused with the old sources still served" do
      inst = start()
      current = Ankusa.config(inst)

      bad = %{
        "gh" => [
          verifier: {Ankusa.Verifier.Hmac, scheme: :standard_webhooks, secret: "whsec_!!!"}
        ]
      }

      next = %{current | source_store: {Ankusa.SourceStore.Static, sources: bad}}

      assert {:error, {:invalid_config, message}} = Reload.apply_config(inst, next)
      assert message =~ "gh"
      assert {:ok, _source} = Ankusa.SourceStore.Static.fetch(inst, "acme.hooks")
    end

    test "the seed's difference is applied; routes made through the API are left alone" do
      inst = start()
      assert {:ok, _route} = Routes.create(inst, %{"id" => "api", "path" => "/hooks/api"})
      current = Ankusa.config(inst)

      seed = [
        %{"id" => "a", "path" => "/hooks/a2"},
        %{"id" => "c", "path" => "/hooks/c"}
      ]

      next = %{current | routes: %{current.routes | seed: seed}}
      assert {:ok, %{sources: []}} = Reload.apply_config(inst, next, audit: operator())

      assert {:ok, %{path: "/hooks/a2"}} = Routes.get(inst, "a")
      assert {:ok, %{path: "/hooks/c"}} = Routes.get(inst, "c")
      assert {:error, :not_found} = Routes.get(inst, "b")
      assert {:ok, %{path: "/hooks/api"}} = Routes.get(inst, "api")
      assert Ankusa.config(inst).routes.seed == seed
    end

    test "a seed route without an id cannot be compared and is refused" do
      inst = start()
      current = Ankusa.config(inst)
      next = %{current | routes: %{current.routes | seed: [%{"path" => "/hooks/x"}]}}

      assert {:error, {:invalid_config, message}} = Reload.apply_config(inst, next)
      assert message =~ "routes.seed[0] needs an id"
      assert Ankusa.config(inst) == current
    end

    test "a seed route that collides with an API route fails the reload and is recorded as failed" do
      inst = start()
      assert {:ok, _route} = Routes.create(inst, %{"id" => "api", "path" => "/hooks/c"})
      current = Ankusa.config(inst)

      seed = current.routes.seed ++ [%{"id" => "c", "path" => "/hooks/c"}]
      next = %{current | routes: %{current.routes | seed: seed}}

      assert {:error, {:invalid_config, "routes.seed route c conflicts with another route"}} =
               Reload.apply_config(inst, next, audit: operator(), keys: ["routes.seed[2].id"])

      assert Ankusa.config(inst) == current
      assert [%{"result" => "failed", "actor" => "operator"}] = rows(inst)
    end
  end

  describe "run/2" do
    test "an instance nobody registered a reload for is unsupported" do
      assert {:error, :unsupported} = Reload.run(:never_registered, Context.system())
    end

    test "the registered function gets the caller's context" do
      me = self()
      summary = %{keys: [], sources: [], kept: []}

      :ok =
        Reload.register(:registered_reload, fn context ->
          send(me, {:ran, context})
          {:ok, summary}
        end)

      assert {:ok, ^summary} = Reload.run(:registered_reload, operator())
      assert_received {:ran, %Context{actor: "operator", request_id: "r-1"}}
    end
  end

  describe "POST /v1/config/reload" do
    test "is 401 without a credential and 403 without config:write and tenant:*" do
      inst = start()

      assert :post
             |> Plug.Test.conn("/v1/config/reload", "")
             |> then(&Router.call(&1, Router.init(instance: inst)))
             |> Map.fetch!(:status) == 401

      assert call(inst, :post, "/v1/config/reload", "nobody").status == 403
      assert call(inst, :post, "/v1/config/reload", "acme").status == 403
    end

    test "an instance with no reload behind it is 409 reload_unavailable" do
      inst = start()
      conn = call(inst, :post, "/v1/config/reload", "operator")
      assert conn.status == 409
      assert %{"error" => "reload_unavailable"} = json(conn)
    end

    test "each answer of the loader has its status" do
      inst = start()
      me = self()

      answer = fn result ->
        Reload.register(inst, fn ctx ->
          send(me, {:ctx, ctx})
          result
        end)
      end

      answer.(
        {:ok, %{keys: ["log.level"], sources: ["gh"], kept: [%{path: "a.b", reason: "enoent"}]}}
      )

      conn = call(inst, :post, "/v1/config/reload", "operator")
      assert conn.status == 200
      assert_received {:ctx, %Context{actor: "operator"}}

      assert json(conn) == %{
               "status" => "reloaded",
               "keys" => ["log.level"],
               "sources" => ["gh"],
               "kept" => [%{"path" => "a.b", "reason" => "enoent"}]
             }

      answer.({:error, {:not_reloadable, ["http.port", "wal.type"]}})
      conn = call(inst, :post, "/v1/config/reload", "operator")
      assert conn.status == 409
      assert %{"error" => "not_reloadable", "keys" => ["http.port", "wal.type"]} = json(conn)

      answer.({:error, {:invalid_config, "sources.gh: broken"}})
      conn = call(inst, :post, "/v1/config/reload", "operator")
      assert conn.status == 422
      assert %{"error" => "invalid_config", "message" => "sources.gh: broken"} = json(conn)

      answer.({:error, :audit_backlog})
      conn = call(inst, :post, "/v1/config/reload", "operator")
      assert conn.status == 503
      assert Plug.Conn.get_resp_header(conn, "retry-after") == ["60"]

      answer.({:error, :store_unavailable})
      conn = call(inst, :post, "/v1/config/reload", "operator")
      assert conn.status == 503
      assert json(conn) == %{"error" => "store_unavailable"}
    end
  end
end
