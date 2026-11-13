defmodule Ankusa.Gateway.Authz.OpenFGATest do
  @moduledoc """
  The `openfga` authorizer, captured with `Req.Test`: the tuple a request
  becomes (user, object through a `resources` mapping, relation,
  contextual tuples), the batch endpoint's correlation ids and per-request
  answers, the identifier checks that keep caller data out of a tuple, the
  boot-time validation, and — through the gateway's decision cache — one PDP
  call for every key of a folder.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Gateway.Authz
  alias Ankusa.Gateway.Authz.{OpenFGA, Request}
  alias Ankusa.Principal

  @key "gateway.authz.authorizers.fga"

  @moduletag :capture_log

  defp principal(fields \\ []) do
    %Principal{
      id: Keyword.get(fields, :id, "alice"),
      scopes: [],
      tenants: Keyword.get(fields, :tenants, ["acme"]),
      groups: Keyword.get(fields, :groups, ["eng"]),
      claims: %{}
    }
  end

  defp request(fields \\ []) do
    resource =
      Keyword.get(fields, :resource, %{kind: :object, space: "docs", key: "a.txt", tenant: nil})

    %Request{
      principal: Keyword.get(fields, :principal, principal()),
      via: :token,
      subject_key: Keyword.get(fields, :subject_key, <<2::128>>),
      action: Keyword.get(fields, :action, :read),
      resource: resource
    }
  end

  defp ctx, do: %{instance: :default, name: "fga", deadline: 0}

  defp state(opts \\ []) do
    name = make_ref()

    state =
      OpenFGA.init!(
        %{url: "https://fga.example", store_id: "store-1", req_options: [plug: {Req.Test, name}]}
        |> Map.merge(Map.new(opts)),
        @key
      )

    {state, name}
  end

  defp capture(conn, test) do
    {:ok, body, conn} = Plug.Conn.read_body(conn)

    send(
      test,
      {:fga,
       %{method: conn.method, path: conn.request_path, headers: conn.req_headers, body: body}}
    )

    conn
  end

  defp json(conn, value) do
    conn
    |> Plug.Conn.delete_resp_header("cache-control")
    |> Plug.Conn.send_resp(200, JSON.encode!(value))
  end

  defp header(%{headers: headers}, name),
    do: Enum.find_value(headers, fn {k, v} -> if k == name, do: v end)

  # ── one check ───────────────────────────────────────────────────────────────

  describe "one check" do
    test "is the documented tuple at <url>/stores/<store>/check, with the bearer token" do
      {state, name} = state(api_token: "tok", authorization_model_id: "model-1")
      test = self()

      Req.Test.stub(name, fn conn ->
        conn = capture(conn, test)
        json(conn, %{"allowed" => true})
      end)

      assert {:ok, :allow, %{}} = OpenFGA.decide(request(), state, ctx())

      assert_receive {:fga, sent}
      assert sent.method == "POST"
      assert sent.path == "/stores/store-1/check"
      assert header(sent, "content-type") == "application/json"
      assert header(sent, "authorization") == "Bearer tok"
      assert byte_size(header(sent, "x-request-id")) == 26

      assert JSON.decode!(sent.body) == %{
               "tuple_key" => %{
                 "user" => "user:alice",
                 "relation" => "viewer",
                 "object" => "object:docs/a.txt"
               },
               "consistency" => "MINIMIZE_LATENCY",
               "authorization_model_id" => "model-1"
             }
    end

    test "allowed is a boolean verdict; nothing else is" do
      {state, name} = state()

      for {body, expected} <- [
            {%{"allowed" => true}, {:ok, :allow, %{}}},
            {%{"allowed" => false}, {:ok, :deny, %{}}},
            {%{"allowed" => "true"}, {:error, {:unavailable, :bad_response}}},
            {%{"allowed" => nil}, {:error, {:unavailable, :bad_response}}},
            {%{"error" => "boom"}, {:error, {:unavailable, :bad_response}}},
            {"garbage", {:error, {:unavailable, :bad_response}}}
          ] do
        Req.Test.stub(name, fn conn -> json(conn, body) end)
        assert OpenFGA.decide(request(), state, ctx()) == expected, inspect(body)
      end
    end

    test "a non-200 status and a transport failure are unavailable" do
      {state, name} = state()

      Req.Test.stub(name, fn conn -> Plug.Conn.send_resp(conn, 500, "boom") end)
      assert {:error, {:unavailable, {:status, 500}}} = OpenFGA.decide(request(), state, ctx())

      Req.Test.stub(name, fn conn -> Req.Test.transport_error(conn, :econnrefused) end)
      assert {:error, {:unavailable, _reason}} = OpenFGA.decide(request(), state, ctx())
    end

    test "relations and consistency are the configured values" do
      {state, name} = state(relations: %{read: "can_read"}, consistency: :higher_consistency)
      test = self()

      Req.Test.stub(name, fn conn ->
        conn = capture(conn, test)
        json(conn, %{"allowed" => true})
      end)

      assert {:ok, :allow, %{}} = OpenFGA.decide(request(), state, ctx())

      assert_receive {:fga, sent}

      assert %{
               "tuple_key" => %{"relation" => "can_read"},
               "consistency" => "HIGHER_CONSISTENCY"
             } = JSON.decode!(sent.body)
    end

    test "a resources entry maps a space and a pattern to one object, its captures to vars" do
      {state, name} =
        state(
          resources: [
            %{
              space: "docs",
              match: "teams/{team}/**",
              object: "folder:{{ vars.team }}"
            }
          ]
        )

      test = self()

      Req.Test.stub(name, fn conn ->
        conn = capture(conn, test)
        json(conn, %{"allowed" => true})
      end)

      inside = %{kind: :object, space: "docs", key: "teams/eng/q1.pdf", tenant: nil}
      assert {:ok, :allow, %{}} = OpenFGA.decide(request(resource: inside), state, ctx())

      outside = %{kind: :object, space: "docs", key: "other.txt", tenant: nil}
      assert {:ok, :allow, %{}} = OpenFGA.decide(request(resource: outside), state, ctx())

      assert_receive {:fga, sent1}
      assert_receive {:fga, sent2}

      assert %{"tuple_key" => %{"object" => "folder:eng"}} = JSON.decode!(sent1.body)
      assert %{"tuple_key" => %{"object" => "object:docs/other.txt"}} = JSON.decode!(sent2.body)
    end

    test "contextual tuples expand one per group" do
      {state, name} =
        state(
          contextual_tuples: [
            %{user: "group:{{ principal.groups }}", relation: "member", object: "folder:eng"},
            %{user: "user:{{ principal.id }}", relation: "owner", object: "folder:eng"}
          ]
        )

      test = self()

      Req.Test.stub(name, fn conn ->
        conn = capture(conn, test)
        json(conn, %{"allowed" => true})
      end)

      principal = principal(groups: ["eng", "ops"])
      assert {:ok, :allow, %{}} = OpenFGA.decide(request(principal: principal), state, ctx())

      assert_receive {:fga, sent}

      assert %{"contextual_tuples" => %{"tuple_keys" => tuples}} = JSON.decode!(sent.body)

      assert tuples == [
               %{"user" => "group:eng", "relation" => "member", "object" => "folder:eng"},
               %{"user" => "group:ops", "relation" => "member", "object" => "folder:eng"},
               %{"user" => "user:alice", "relation" => "owner", "object" => "folder:eng"}
             ]
    end
  end

  # ── identifiers ─────────────────────────────────────────────────────────────

  describe "identifiers" do
    test "a wildcard, a userset separator or whitespace from the caller denies without a call" do
      {state, name} = state()

      Req.Test.stub(name, fn conn -> json(conn, %{"allowed" => true}) end)

      for id <- ["*", "a#member", "a b", "a\tb"] do
        assert {:ok, :deny, %{reason: :unmappable}} =
                 OpenFGA.decide(request(principal: principal(id: id)), state, ctx()),
               id
      end

      refute_receive {:fga, _}
    end

    test "a template that cannot render denies unmappable" do
      {state, name} = state(user: "user:{{ claims.sub }}")

      Req.Test.stub(name, fn conn -> json(conn, %{"allowed" => true}) end)

      assert {:ok, :deny, %{reason: :unmappable}} = OpenFGA.decide(request(), state, ctx())
      refute_receive {:fga, _}
    end
  end

  # ── batches ─────────────────────────────────────────────────────────────────

  describe "a batch" do
    test "is one batch-check, each request reading its correlation id" do
      {state, name} = state(api_token: "tok")
      test = self()

      Req.Test.stub(name, fn conn ->
        conn = capture(conn, test)

        json(conn, %{
          "result" => %{
            "c0" => %{"allowed" => true},
            "c1" => %{"allowed" => false}
          }
        })
      end)

      requests = [
        request(),
        request(resource: %{kind: :object, space: "docs", key: "b.txt", tenant: nil})
      ]

      assert [{:ok, :allow, %{}}, {:ok, :deny, %{}}] = OpenFGA.decide_many(requests, state, ctx())

      assert_receive {:fga, sent}
      assert sent.path == "/stores/store-1/batch-check"
      assert header(sent, "authorization") == "Bearer tok"

      assert %{"checks" => [first, second], "consistency" => "MINIMIZE_LATENCY"} =
               JSON.decode!(sent.body)

      assert first["correlation_id"] == "c0"
      assert first["tuple_key"]["object"] == "object:docs/a.txt"
      assert second["correlation_id"] == "c1"
      assert second["tuple_key"]["object"] == "object:docs/b.txt"
    end

    test "a missing correlation id is unavailable for that request only" do
      {state, name} = state()

      Req.Test.stub(name, fn conn ->
        json(conn, %{"results" => %{"c0" => %{"allowed" => true}}})
      end)

      requests = [
        request(),
        request(resource: %{kind: :object, space: "docs", key: "b.txt", tenant: nil})
      ]

      assert [{:ok, :allow, %{}}, {:error, {:unavailable, :bad_response}}] =
               OpenFGA.decide_many(requests, state, ctx())
    end

    test "an entry with a non-empty error is unavailable for its request" do
      {state, name} = state()

      Req.Test.stub(name, fn conn ->
        json(conn, %{
          "results" => %{
            "c0" => %{"allowed" => true, "error" => "internal"},
            "c1" => %{"allowed" => true, "error" => ""}
          }
        })
      end)

      requests = [
        request(),
        request(resource: %{kind: :object, space: "docs", key: "b.txt", tenant: nil})
      ]

      assert [{:error, {:unavailable, :bad_response}}, {:ok, :allow, %{}}] =
               OpenFGA.decide_many(requests, state, ctx())
    end
  end

  # ── validation ──────────────────────────────────────────────────────────────

  describe "validation" do
    test "url, store_id and consistency carry their dotted key" do
      assert_raise ArgumentError, ~r/\.url must be a literal URL/, fn ->
        state(url: "https://{{ claims.host }}/x")
      end

      assert_raise ArgumentError, ~r/\.store_id must be a non-empty string/, fn ->
        OpenFGA.init!(%{url: "https://fga.example"}, @key)
      end

      assert_raise ArgumentError, ~r/\.consistency must be minimize_latency/, fn ->
        state(consistency: :eventual)
      end

      assert %{url: "https://fga.example"} = state() |> elem(0)

      assert_raise ArgumentError, ~r/\.relations has unknown action "fly"/, fn ->
        state(relations: %{"fly" => "viewer"})
      end

      assert_raise ArgumentError, ~r/\.contextual_tuples must hold at most 8/, fn ->
        state(contextual_tuples: List.duplicate(%{user: "u", relation: "r", object: "o"}, 9))
      end

      assert_raise ArgumentError, ~r/\.resources\[0\]\.pattern/, fn ->
        state(resources: [%{match: "a", object: "b", pattern: "c"}])
      end
    end
  end

  # ── through the gateway ─────────────────────────────────────────────────────

  describe "through the gateway" do
    test "100 keys of one folder cost one PDP call" do
      %{inst: inst, name: name} = boot()
      {:ok, counter} = start_supervised(Supervisor.child_spec({Agent, fn -> 0 end}, id: :counter))

      Req.Test.stub(name, fn conn ->
        Agent.update(counter, &(&1 + 1))

        conn
        |> Plug.Conn.delete_resp_header("cache-control")
        |> Plug.Conn.send_resp(200, ~s({"allowed": true}))
      end)

      results =
        for index <- 1..100 do
          key = "teams/eng/doc-#{index}.pdf"
          resource = %{kind: :object, space: "docs", key: key, tenant: nil}
          Authz.decide(inst, "docs", request(resource: resource))
        end

      assert Enum.all?(results, &match?({:allow, _}, &1))
      assert Agent.get(counter, & &1) == 1
    end

    test "a batch-check that answers 404 falls back to single checks and is not retried" do
      %{inst: inst, name: name} = boot(batch: true)

      {:ok, counter} =
        start_supervised(Supervisor.child_spec({Agent, fn -> [] end}, id: :counter))

      Req.Test.stub(name, fn conn ->
        Agent.update(counter, &[conn.request_path | &1])

        case conn.request_path do
          "/stores/store-1/batch-check" ->
            Plug.Conn.send_resp(conn, 404, "no such route")

          "/stores/store-1/check" ->
            conn
            |> Plug.Conn.delete_resp_header("cache-control")
            |> Plug.Conn.send_resp(200, ~s({"allowed": true}))
        end
      end)

      assert {:allow, _} = Authz.decide(inst, "docs", request(subject_key: hash("a")))

      assert {:allow, _} =
               Authz.decide(
                 inst,
                 "docs",
                 request(
                   subject_key: hash("b"),
                   resource: %{kind: :object, space: "docs", key: "b.txt", tenant: nil}
                 )
               )

      assert {:allow, _} =
               Authz.decide(
                 inst,
                 "docs",
                 request(
                   subject_key: hash("c"),
                   resource: %{kind: :object, space: "docs", key: "c.txt", tenant: nil}
                 )
               )

      assert {:allow, _} =
               Authz.decide(
                 inst,
                 "docs",
                 request(
                   subject_key: hash("d"),
                   resource: %{kind: :object, space: "docs", key: "d.txt", tenant: nil}
                 )
               )

      paths = Agent.get(counter, & &1)
      assert length(paths) == 5
      assert Enum.count(paths, &(&1 == "/stores/store-1/batch-check")) == 1
    end
  end

  defp hash(value), do: :crypto.hash(:sha256, value)

  # Core instance, the gateway's caches, and an `openfga` authorizer whose
  # `docs` policy is decided by it; every key of `teams/eng/` maps to one
  # object.
  defp boot(opts \\ []) do
    opts = Map.new(opts)
    config = test_config(roles: [])
    start_supervised!({Ankusa.Instance, config})
    inst = config.instance
    name = make_ref()

    authorizer =
      Map.merge(
        %{
          url: "https://fga.example",
          store_id: "store-1",
          req_options: [plug: {Req.Test, name}],
          timeout_ms: 500,
          batch: Map.get(opts, :batch, false),
          resources: [
            %{space: "docs", match: "teams/{team}/**", object: "folder:{{ vars.team }}"}
          ]
        },
        Map.get(opts, :authorizer, %{})
      )

    gateway =
      Ankusa.Gateway.Config.new(
        instance: inst,
        port: 0,
        authz: %{
          authorizers: %{"fga" => {OpenFGA, authorizer}},
          policies: %{"docs" => %{mode: :first, steps: ["fga"]}},
          cache: %{allow_ttl_ms: 5_000, deny_ttl_ms: 1_000}
        }
      )

    Req.Test.stub(name, fn conn -> Plug.Conn.send_resp(conn, 200, ~s({"allowed": true})) end)

    start_supervised!({Ankusa.Gateway, gateway})

    caller = Ankusa.whereis(inst, {:gateway_caller, {:authz, "fga"}})
    :ok = Req.Test.allow(name, self(), caller)

    %{inst: inst, name: name}
  end
end
