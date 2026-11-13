defmodule Ankusa.Gateway.Authz.HttpAuthorizerTest do
  @moduledoc """
  The `http` authorizer, captured with `Req.Test`: the exact requests the
  AuthZEN and template protocols send, how a verdict, a `max-age` hint and a
  `ttl_path` hint become a decision, the batch endpoint and its fallback, the
  failure mappings, `forward_authorization`, and — through the gateway — an
  outage's `503 authz_unavailable`, `on_unavailable: deny`'s `403` and a
  `serve_stale_ms` answer.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Admin.Auth.Token
  alias Ankusa.BlobStore.LocalFS
  alias Ankusa.Gateway.Authz
  alias Ankusa.Gateway.Authz.{Http, Request}
  alias Ankusa.Gateway.Router
  alias Ankusa.Principal

  @key "gateway.authz.authorizers.pdp"

  @moduletag :capture_log
  @moduletag :tmp_dir

  # ── helpers ─────────────────────────────────────────────────────────────────

  defp principal(fields \\ []) do
    %Principal{
      id: Keyword.get(fields, :id, "alice"),
      scopes: Keyword.get(fields, :scopes, ["claims:read"]),
      tenants: Keyword.get(fields, :tenants, ["acme"]),
      groups: Keyword.get(fields, :groups, ["eng"]),
      claims: Keyword.get(fields, :claims, %{})
    }
  end

  defp request(fields \\ []) do
    %Request{
      principal: Keyword.get(fields, :principal, principal()),
      via: :jwt,
      subject_key: Keyword.get(fields, :subject_key, <<1::128>>),
      action: Keyword.get(fields, :action, :read),
      resource:
        Keyword.get(fields, :resource, %{
          kind: :object,
          space: "docs",
          key: "a.txt",
          tenant: nil
        }),
      conn: Keyword.get(fields, :conn),
      # What `Request.from_conn/3` reads from a connection.
      authorization:
        fields
        |> Keyword.get(:conn, Plug.Test.conn(:get, "/"))
        |> Plug.Conn.get_req_header("authorization")
        |> List.first()
    }
  end

  defp ctx, do: %{instance: :default, name: "pdp", deadline: 0}

  # A verdict without the `max-age` hint: Plug's test adapter sends
  # `cache-control: max-age=0, private, must-revalidate` on every response, and
  # a hint of 0 correctly means "do not cache".
  defp verdict({:ok, decision, meta}), do: {:ok, decision, Map.delete(meta, :ttl_ms)}
  defp verdict(other), do: other

  # A fresh stub name, so tests in this file never share one.
  defp stub_name, do: make_ref()

  defp state(opts \\ []) do
    name = stub_name()

    state =
      Http.init!(
        %{url: "https://pdp.example", req_options: [plug: {Req.Test, name}]}
        |> Map.merge(Map.new(opts)),
        @key
      )

    {state, name}
  end

  defp capture(conn, test) do
    {:ok, body, conn} = Plug.Conn.read_body(conn)

    send(
      test,
      {:pdp,
       %{method: conn.method, path: conn.request_path, headers: conn.req_headers, body: body}}
    )

    conn
  end

  # A PDP that sends no cache-control: the decision cache's `allow_ttl_ms`
  # then governs (a `Plug.Test` conn starts with `max-age=0`, which correctly
  # means "do not cache").
  defp json(conn, value) do
    conn
    |> Plug.Conn.delete_resp_header("cache-control")
    |> Plug.Conn.send_resp(200, JSON.encode!(value))
  end

  # A PDP that names the freshness of its allow, the plan's `max-age` hint.
  defp json(conn, value, max_age) do
    conn
    |> Plug.Conn.put_resp_header("cache-control", "max-age=#{max_age}")
    |> Plug.Conn.send_resp(200, JSON.encode!(value))
  end

  defp header(%{headers: headers}, name), do: List.first(for {k, v} <- headers, k == name, do: v)

  # ── authzen ─────────────────────────────────────────────────────────────────

  describe "the authzen protocol" do
    test "one evaluation is exactly the documented JSON, with content-type and a request id" do
      {state, name} = state()
      test = self()

      Req.Test.stub(name, fn conn ->
        conn = capture(conn, test)
        json(conn, %{"decision" => true}, 30)
      end)

      assert {:ok, :allow, %{ttl_ms: 30_000}} = Http.decide(request(), state, ctx())

      assert_receive {:pdp, sent}
      assert sent.method == "POST"
      assert sent.path == "/access/v1/evaluation"
      assert header(sent, "content-type") == "application/json"
      assert [request_id] = List.wrap(header(sent, "x-request-id"))
      assert byte_size(request_id) == 26

      assert JSON.decode!(sent.body) == %{
               "subject" => %{
                 "type" => "user",
                 "id" => "alice",
                 "properties" => %{"groups" => ["eng"], "tenants" => ["acme"]}
               },
               "resource" => %{
                 "type" => "object",
                 "id" => "docs/a.txt",
                 "properties" => %{"space" => "docs", "key" => "a.txt"}
               },
               "action" => %{"name" => "read"},
               "context" => %{}
             }
    end

    test "a JSON true allows and a JSON false denies; nothing else is a verdict" do
      {state, name} = state()

      for {body, expected} <- [
            {~s({"decision": true}), {:ok, :allow, %{}}},
            {~s({"decision": false}), {:ok, :deny, %{}}},
            {~s({"decision": "true"}), {:error, {:unavailable, :bad_response}}},
            {~s({"decision": null}), {:error, {:unavailable, :bad_response}}},
            {~s({"allowed": true}), {:error, {:unavailable, :bad_response}}},
            {"not json", {:error, {:unavailable, :bad_response}}}
          ] do
        Req.Test.stub(name, fn conn -> Plug.Conn.send_resp(conn, 200, body) end)
        assert verdict(Http.decide(request(), state, ctx())) == expected, body
      end
    end

    test "an action and a resource may be configured, and an unrenderable one denies unmappable" do
      {state, name} =
        state(
          actions: %{read: "check_claim"},
          resource: %{"type" => "claim", "id" => "{{ tenant }}/{{ key }}"}
        )

      Req.Test.stub(name, fn conn -> json(conn, %{"decision" => true}) end)

      claim = %{kind: :claim, space: "claims", key: "acme/01ABC", tenant: "acme"}
      assert {:ok, :allow, _meta} = Http.decide(request(resource: claim), state, ctx())

      # `principal.tenants` of `:all` renders as `["*"]`; an empty groups list
      # has no value, so the default subject is unmappable for that caller.
      nobody = %Principal{id: "nobody", scopes: [], tenants: :all, groups: []}

      assert {:ok, :deny, %{reason: :unmappable}} =
               Http.decide(request(principal: nobody), state, ctx())
    end

    test "a batch is one evaluations request, answered in order" do
      {state, name} = state(batch: true)
      test = self()

      Req.Test.stub(name, fn conn ->
        conn = capture(conn, test)
        json(conn, %{"evaluations" => [%{"decision" => true}, %{"decision" => false}]})
      end)

      requests = [
        request(),
        request(resource: %{kind: :object, space: "docs", key: "b.txt", tenant: nil})
      ]

      assert [{:ok, :allow, _allow}, {:ok, :deny, _deny}] =
               Http.decide_many(requests, state, ctx())

      assert_receive {:pdp, sent}
      assert sent.path == "/access/v1/evaluations"

      assert %{
               "evaluations" => [first, second],
               "options" => %{"evaluations_semantic" => "execute_all"}
             } = JSON.decode!(sent.body)

      assert first["resource"]["id"] == "docs/a.txt"
      assert second["resource"]["id"] == "docs/b.txt"
    end

    test "a batch answer of another length is bad_batch for every request" do
      {state, name} = state()

      Req.Test.stub(name, fn conn ->
        json(conn, %{"evaluations" => [%{"decision" => true}]})
      end)

      requests = [
        request(),
        request(resource: %{kind: :object, space: "docs", key: "b.txt", tenant: nil})
      ]

      assert [{:error, {:unavailable, :bad_batch}}, {:error, {:unavailable, :bad_batch}}] =
               Http.decide_many(requests, state, ctx())
    end

    test "a batch element with context.error, or without a boolean decision, is unavailable for itself" do
      {state, name} = state()

      Req.Test.stub(name, fn conn ->
        json(conn, %{
          "evaluations" => [
            %{"decision" => true, "context" => %{"error" => "no"}},
            %{"decision" => true},
            %{"decision" => "true"}
          ]
        })
      end)

      requests =
        for key <- ~w(a b c),
            do: request(resource: %{kind: :object, space: "docs", key: key, tenant: nil})

      assert [
               {:error, {:unavailable, :bad_response}},
               {:ok, :allow, _meta},
               {:error, {:unavailable, :bad_response}}
             ] = Http.decide_many(requests, state, ctx())
    end

    test "a non-200 status, a transport failure and a configured header travel as they are" do
      {state, name} = state(headers: %{"x-api-key" => "s3cret"}, timeout_ms: 50)
      test = self()

      Req.Test.stub(name, fn conn ->
        conn = capture(conn, test)
        Plug.Conn.send_resp(conn, 429, "slow down")
      end)

      assert {:error, {:unavailable, {:status, 429}}} = Http.decide(request(), state, ctx())
      assert_receive {:pdp, sent}
      assert header(sent, "x-api-key") == "s3cret"

      Req.Test.stub(name, fn conn -> Plug.Conn.send_resp(conn, 500, "boom") end)
      assert {:error, {:unavailable, {:status, 500}}} = Http.decide(request(), state, ctx())

      Req.Test.stub(name, fn conn -> Req.Test.transport_error(conn, :timeout) end)
      assert {:error, {:unavailable, _reason}} = Http.decide(request(), state, ctx())
    end
  end

  # ── template ────────────────────────────────────────────────────────────────

  describe "the template protocol" do
    test "one check is the rendered request, and allow_path and ttl_path decide" do
      {state, name} =
        state(
          protocol: :template,
          url: "https://perm.example/v1/check",
          request: %{
            "user" => "{{ principal.id }}",
            "org" => "{{ claims.org ?? none }}",
            "groups" => "{{ principal.groups }}",
            "action" => "{{ action }}",
            "key" => "{{ space }}/{{ key }}"
          },
          allow_path: "data.allow",
          ttl_path: "data.ttl"
        )

      test = self()

      Req.Test.stub(name, fn conn ->
        conn = capture(conn, test)
        json(conn, %{"data" => [%{"allow" => true, "ttl" => 12}]})
      end)

      request = request(principal: principal(claims: %{"org" => "acme"}))

      # `data` is a map, `data.allow` a path into it: the array form must not match.
      assert {:error, {:unavailable, :bad_response}} = Http.decide(request, state, ctx())

      Req.Test.stub(name, fn conn ->
        conn = capture(conn, test)
        json(conn, %{"data" => %{"allow" => true, "ttl" => 12}})
      end)

      assert {:ok, :allow, %{ttl_ms: 12_000}} = Http.decide(request, state, ctx())

      assert_receive {:pdp, sent}
      assert sent.path == "/v1/check"

      assert JSON.decode!(sent.body) == %{
               "user" => "alice",
               "org" => "acme",
               "groups" => ["eng"],
               "action" => "read",
               "key" => "docs/a.txt"
             }

      Req.Test.stub(name, fn conn -> json(conn, %{"data" => %{"allow" => false}}) end)
      assert {:ok, :deny, _meta} = Http.decide(request, state, ctx())
    end

    test "a bulk endpoint takes every rendered request and answers in order" do
      {state, name} =
        state(
          protocol: :template,
          url: "https://perm.example/check",
          request: %{"key" => "{{ key }}"},
          allow_path: "allow",
          batch: %{url: "https://perm.example/bulk", items_key: "q", results_path: "rows"}
        )

      test = self()

      Req.Test.stub(name, fn conn ->
        conn = capture(conn, test)
        json(conn, %{"rows" => [%{"allow" => true}, %{"allow" => false}]})
      end)

      requests =
        for key <- ~w(a.txt b.txt),
            do: request(resource: %{kind: :object, space: "docs", key: key, tenant: nil})

      assert [{:ok, :allow, _allow}, {:ok, :deny, _deny}] =
               Http.decide_many(requests, state, ctx())

      assert_receive {:pdp, sent}
      assert sent.path == "/bulk"
      assert JSON.decode!(sent.body) == %{"q" => [%{"key" => "a.txt"}, %{"key" => "b.txt"}]}
    end

    test "an array path indexes an array, and a non-boolean answer is bad_response" do
      {state, name} =
        state(
          protocol: :template,
          url: "https://perm.example/check",
          request: %{"key" => "{{ key }}"},
          allow_path: "rows.0.allow"
        )

      Req.Test.stub(name, fn conn -> json(conn, %{"rows" => [%{"allow" => true}]}) end)
      assert {:ok, :allow, _meta} = Http.decide(request(), state, ctx())

      Req.Test.stub(name, fn conn -> json(conn, %{"rows" => [%{"allow" => "yes"}]}) end)
      assert {:error, {:unavailable, :bad_response}} = Http.decide(request(), state, ctx())
    end

    test "a request that cannot render denies unmappable without a call" do
      {state, name} =
        state(
          protocol: :template,
          url: "https://perm.example/check",
          request: %{"org" => "{{ claims.org }}"},
          allow_path: "allow"
        )

      Req.Test.stub(name, fn conn -> json(conn, %{"allow" => true}) end)

      assert {:ok, :deny, %{reason: :unmappable}} = Http.decide(request(), state, ctx())
      refute_receive {:pdp, _}
    end
  end

  # ── validation ──────────────────────────────────────────────────────────────

  describe "validation" do
    test "the url is literal, https (or loopback http), and required" do
      assert_raise ArgumentError, ~r/\.url must be a literal URL/, fn ->
        Http.init!(%{url: "https://{{ claims.host }}/x"}, @key)
      end

      assert_raise ArgumentError, ~r/\.url must be an https URL/, fn ->
        Http.init!(%{url: "http://pdp.example/x"}, @key)
      end

      assert %{url: "http://127.0.0.1:8080/x"} =
               Http.init!(%{url: "http://127.0.0.1:8080/x"}, @key)

      assert %{url: "http://pdp.internal/x"} =
               Http.init!(%{url: "http://pdp.internal/x", allow_insecure_http: true}, @key)

      assert_raise ArgumentError, ~r/\.url must be a non-empty string/, fn ->
        Http.init!(%{}, @key)
      end
    end

    test "unknown keys, headers, actions and paths carry their dotted key" do
      assert_raise ArgumentError, "unknown Ankusa.Gateway.Config key: #{@key}.opa", fn ->
        Http.init!(%{url: "https://pdp.example", opa: true}, @key)
      end

      assert_raise ArgumentError, ~r/#{@key}\.headers must hold at most 16/, fn ->
        headers = Map.new(1..17, &{"x-#{&1}", "v"})
        Http.init!(%{url: "https://pdp.example", headers: headers}, @key)
      end

      assert_raise ArgumentError, ~r/#{@key}\.headers has an invalid header value/, fn ->
        Http.init!(%{url: "https://pdp.example", headers: %{"x" => "a\nb"}}, @key)
      end

      assert_raise ArgumentError, ~r/#{@key}\.actions has unknown action "fly"/, fn ->
        Http.init!(%{url: "https://pdp.example", actions: %{"fly" => "x"}}, @key)
      end

      assert_raise ArgumentError, ~r/#{@key}\.allow_path must be dotted segments/, fn ->
        Http.init!(
          %{
            url: "https://pdp.example",
            protocol: :template,
            request: %{"a" => "b"},
            allow_path: "a..b"
          },
          @key
        )
      end

      assert_raise ArgumentError, ~r/#{@key}\.protocol must be authzen or template/, fn ->
        Http.init!(%{url: "https://pdp.example", protocol: :opa}, @key)
      end

      assert_raise ArgumentError, ~r/#{@key}\.request must be a map/, fn ->
        Http.init!(%{url: "https://pdp.example", protocol: :template, allow_path: "a"}, @key)
      end

      assert_raise ArgumentError, ~r/#{@key}\.allow_path is required/, fn ->
        Http.init!(
          %{url: "https://pdp.example", protocol: :template, request: %{"a" => "b"}},
          @key
        )
      end

      assert_raise ArgumentError, ~r/#{@key}\.req_options does not accept :params/, fn ->
        Http.init!(%{url: "https://pdp.example", req_options: [params: %{}]}, @key)
      end
    end
  end

  # ── forward_authorization ───────────────────────────────────────────────────

  describe "forward_authorization" do
    test "the caller's authorization header reaches the PDP, and its absence denies unmappable" do
      {state, name} = state(forward_authorization: true)
      test = self()

      Req.Test.stub(name, fn conn ->
        conn = capture(conn, test)
        json(conn, %{"decision" => true})
      end)

      conn =
        Plug.Test.conn(:get, "/") |> Plug.Conn.put_req_header("authorization", "Bearer alice")

      assert {:ok, :allow, _meta} = Http.decide(request(conn: conn), state, ctx())
      assert_receive {:pdp, sent}
      assert header(sent, "authorization") == "Bearer alice"

      assert {:ok, :deny, %{reason: :unmappable}} =
               Http.decide(request(conn: Plug.Test.conn(:get, "/")), state, ctx())

      refute_receive {:pdp, _}
    end
  end

  # ── through the gateway ─────────────────────────────────────────────────────

  describe "through the gateway" do
    test "200 concurrent distinct checks arrive as at most 4 batch requests of 50", ctx do
      %{inst: inst, name: name} = boot(ctx, batch: true, batch_window_ms: 50)

      {:ok, counter} =
        start_supervised(Supervisor.child_spec({Agent, fn -> [] end}, id: :counter))

      Req.Test.stub(name, fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        %{"evaluations" => evaluations} = JSON.decode!(body)
        Agent.update(counter, &[length(evaluations) | &1])
        allowed = Enum.map(evaluations, fn _ -> %{"decision" => true} end)
        json(conn, %{"evaluations" => allowed})
      end)

      requests =
        for index <- 1..200 do
          request(resource: %{kind: :object, space: "docs", key: "k#{index}", tenant: nil})
        end

      results =
        requests
        |> Task.async_stream(&Authz.decide(inst, "docs", &1),
          max_concurrency: 200,
          timeout: 10_000
        )
        |> Enum.map(fn {:ok, result} -> result end)

      assert Enum.all?(results, &match?({:allow, _}, &1))

      batches = Agent.get(counter, & &1)
      assert length(batches) <= 4, "expected at most 4 batch requests, got #{inspect(batches)}"
      assert Enum.sum(batches) == 200
      assert Enum.all?(batches, &(&1 <= 50))
    end

    test "a bulk endpoint that answers 404 falls back to single checks and is not retried", ctx do
      %{inst: inst, name: name} = boot(ctx, batch: true)

      {:ok, counter} =
        start_supervised(Supervisor.child_spec({Agent, fn -> [] end}, id: :counter))

      Req.Test.stub(name, fn conn ->
        Agent.update(counter, &[conn.request_path | &1])

        case conn.request_path do
          "/access/v1/evaluations" -> Plug.Conn.send_resp(conn, 404, "no such route")
          "/access/v1/evaluation" -> json(conn, %{"decision" => true})
        end
      end)

      first = request()
      second = request(resource: %{kind: :object, space: "docs", key: "b.txt", tenant: nil})

      assert {:allow, _} = Authz.decide(inst, "docs", first)
      assert {:allow, _} = Authz.decide(inst, "docs", second)
      # One bulk attempt, then two single checks.
      assert Agent.get(counter, & &1) == [
               "/access/v1/evaluation",
               "/access/v1/evaluation",
               "/access/v1/evaluations"
             ]

      third = request(resource: %{kind: :object, space: "docs", key: "c.txt", tenant: nil})
      fourth = request(resource: %{kind: :object, space: "docs", key: "d.txt", tenant: nil})

      assert {:allow, _} = Authz.decide(inst, "docs", third)
      assert {:allow, _} = Authz.decide(inst, "docs", fourth)
      # The five-minute memo: two more single checks, no second bulk attempt.
      paths = Agent.get(counter, & &1)
      assert length(paths) == 5
      assert Enum.count(paths, &(&1 == "/access/v1/evaluations")) == 1
    end

    test "two tokens of one subject do not share a cached decision", ctx do
      %{inst: inst, name: name} = boot(ctx, forward_authorization: true, batch: false)
      {:ok, counter} = start_supervised(Supervisor.child_spec({Agent, fn -> 0 end}, id: :counter))

      Req.Test.stub(name, fn conn ->
        Agent.update(counter, &(&1 + 1))
        json(conn, %{"decision" => true})
      end)

      conn = fn token ->
        Plug.Test.conn(:get, "/") |> Plug.Conn.put_req_header("authorization", "Bearer #{token}")
      end

      alice = request(conn: conn.("alice-token"))

      # Same principal and subject key, another credential: another decision.
      bob = request(conn: conn.("bob-token"))

      assert {:allow, _} = Authz.decide(inst, "docs", alice)
      assert {:allow, _} = Authz.decide(inst, "docs", bob)
      assert Agent.get(counter, & &1) == 2

      # The same credential, once more, is a cache hit.
      assert {:allow, _} = Authz.decide(inst, "docs", alice)
      assert Agent.get(counter, & &1) == 2
    end

    test "an outage is 503 authz_unavailable, on_unavailable: deny is 403", ctx do
      %{inst: inst, name: name, root: root} =
        boot(ctx, spaces: %{"denied" => %{policy: "denied", prefix: "denied/"}})

      Req.Test.stub(name, fn conn -> Plug.Conn.send_resp(conn, 500, "down") end)

      put_object(inst, root, "docs/a.txt", "hello")
      put_object(inst, root, "denied/a.txt", "hello")

      conn = get(inst, "/v1/objects/docs/a.txt")
      assert {conn.status, json_body(conn)} == {503, %{"error" => "authz_unavailable"}}
      assert header_of(conn, "retry-after")

      conn = get(inst, "/v1/objects/denied/a.txt")
      assert {conn.status, json_body(conn)} == {403, %{"error" => "forbidden"}}
    end

    test "serve_stale_ms keeps answering with the last allow while the PDP is down", ctx do
      %{inst: inst, name: name, root: root, control: control} =
        boot(ctx,
          cache: %{allow_ttl_ms: 50, deny_ttl_ms: 50, serve_stale_ms: 200, refresh_ahead: false}
        )

      Req.Test.stub(name, fn conn ->
        Agent.get(control, & &1).(conn)
      end)

      put_object(inst, root, "docs/a.txt", "hello")

      assert get(inst, "/v1/objects/docs/a.txt").status == 200

      Agent.update(control, fn _ -> fn conn -> Plug.Conn.send_resp(conn, 500, "down") end end)

      Process.sleep(60)
      # Past its freshness, still inside serve_stale_ms: the old allow answers.
      assert get(inst, "/v1/objects/docs/a.txt").status == 200

      Process.sleep(250)
      assert get(inst, "/v1/objects/docs/a.txt").status == 503
    end
  end

  # ── boots ───────────────────────────────────────────────────────────────────

  # Core instance, the gateway's caches, and an `http` authorizer whose PDP is
  # a Req.Test stub the test controls; `docs` is a LocalFS space on `pdp`'s
  # policy.
  defp boot(%{tmp_dir: dir}, opts) do
    opts = Map.new(opts)
    config = test_config(roles: [])
    start_supervised!({Ankusa.Instance, config})
    inst = config.instance
    name = stub_name()

    # What the PDP answers, replaced by a test that needs an outage.
    control =
      start_supervised!(
        {Agent,
         fn ->
           fn conn ->
             conn
             |> Plug.Conn.delete_resp_header("cache-control")
             |> Plug.Conn.send_resp(200, ~s({"decision": true}))
           end
         end}
      )

    root = Path.join(dir, "store")
    tokens = [%{id: "alice", sha256: Token.hash("alice"), scopes: [], groups: []}]

    authorizer =
      Map.merge(
        %{
          url: "https://pdp.example",
          req_options: [plug: {Req.Test, name}],
          timeout_ms: 500,
          batch: Map.get(opts, :batch, false),
          forward_authorization: Map.get(opts, :forward_authorization, false)
        },
        Map.get(opts, :authorizer, %{})
      )

    authorizer =
      case Map.get(opts, :batch_window_ms) do
        nil -> authorizer
        window_ms -> Map.put(authorizer, :batch_window_ms, window_ms)
      end

    spaces =
      Map.merge(
        %{
          "docs" => %{policy: "docs", prefix: "docs/"},
          "denied" => %{policy: "denied", prefix: "denied/"}
        },
        Map.get(opts, :spaces, %{})
      )

    spaces =
      Map.new(spaces, fn {space, fields} ->
        store = {LocalFS, [root: root]}
        {space, Map.merge(%{store: store, max_object_bytes: 65_536}, fields)}
      end)

    gateway =
      Ankusa.Gateway.Config.new(
        instance: inst,
        port: 0,
        auth: %{authenticators: [%{type: :token, tokens: tokens}]},
        authz: %{
          authorizers: %{"pdp" => {Http, authorizer}},
          policies: %{
            "docs" => %{mode: :first, steps: ["pdp"]},
            "denied" => %{mode: :first, steps: [%{use: "pdp", on_unavailable: :deny}]}
          },
          cache: Map.get(opts, :cache, %{})
        },
        spaces: spaces
      )

    # The PDP is called from the authorizer's Caller process, not this one:
    # register the stub (the default answers allow) and let that process
    # resolve it.
    Req.Test.stub(name, fn conn -> Agent.get(control, & &1).(conn) end)

    start_supervised!({Ankusa.Gateway, gateway})

    caller = Ankusa.whereis(inst, {:gateway_caller, {:authz, "pdp"}})
    :ok = Req.Test.allow(name, self(), caller)

    %{inst: inst, name: name, root: root, control: control}
  end

  defp put_object(inst, root, key, body) do
    {:ok, _version} = LocalFS.put_object(inst, key, body, root: root)
  end

  defp get(inst, path), do: call(inst, path)

  defp call(inst, path) do
    conn =
      Plug.Test.conn(:get, path)
      |> Plug.Conn.put_req_header("authorization", "Bearer alice")

    Router.call(conn, Router.init(instance: inst))
  end

  defp json_body(conn), do: JSON.decode!(conn.resp_body)

  defp header_of(conn, name), do: conn |> Plug.Conn.get_resp_header(name) |> List.first()
end
