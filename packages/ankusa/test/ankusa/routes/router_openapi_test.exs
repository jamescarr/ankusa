defmodule Ankusa.Routes.RouterOpenAPITest do
  @moduledoc """
  The contract test for the `routes` endpoints of
  `priv/openapi/admin.v1.yaml`: the document and `Ankusa.Routes.Router` cannot
  drift apart without failing here.

  Four things are checked, and each one catches a different kind of drift:

    1. **Every documented operation is executed.** The request is built from the
       operation's *own* `example`s — the path parameter's, the query
       parameters', the request body's — sent through the real router on a real
       instance (store, snapshot, and decision cache included), and the
       response's status and body are checked against the documented response
       schema and example. So an implementation that renames a field, changes a
       status code, or stops accepting a documented request fails here.
    2. **The documented method matrix is the served one.** For every path in the
       `routes` tag, the documented methods answer, and every other method is the
       same `404` an unknown path gets. So an endpoint added, removed, or moved
       to another method fails here. The probes for paths nobody documents are
       near-misses of the documented paths (`/admin/routes/stripe/extra`) rather
       than an enumeration: `Plug.Router` compiles its table into function
       clauses and exposes no route list, so that part stays a heuristic — the
       matrix is what makes it strong.
    3. **Every documented error response is executed.** `@error_probes` drives
       each documented `4xx` — and the `503` every write can answer — with a
       request that produces it, and validates the body against that response's
       schema and, where it has them, its examples. The success examples alone
       leave the error half of the document unexercised, and the coverage
       assertion below fails if the document grows an error status no probe
       drives.
    4. **Every example is an example of its own schema.** The document is
       validated against itself, so prose-level examples cannot silently rot into
       values the schemas forbid.

  Responses are read through `deref_response/3`: most error responses are `$ref`s
  into `components/responses`, and reading `content` off a `$ref` finds nothing —
  which is how a schema check quietly becomes a no-op. A documented JSON response
  with no schema is a failure, not "anything goes".

  The examples in the tag are a *sequence* — one route is created and then
  listed, read, dry-run, replaced, patched, and deleted — which is what makes
  point 1 executable. `@sequence` below is that order, and the coverage
  assertion at the end fails if the document grows an operation the sequence
  does not drive.

  Scope: the `routes` tag. The operator API's own operations (`dlq`,
  `quarantine`, `metrics`, `config`) carry no examples yet, so they cannot be
  driven the same way — adding examples to them is all it takes to bring them
  under this test.

  Examples are compared field by field, except for `format: date-time` fields,
  which are checked against the ISO 8601 shape: a real `inserted_at` cannot
  equal the example's literal timestamp.
  """

  use ExUnit.Case, async: false

  import Ankusa.TestHelpers

  alias Ankusa.Routes.Router

  @spec_path Path.expand("../../../priv/openapi/admin.v1.yaml", __DIR__)
  @spec_file Path.relative_to_cwd(@spec_path)

  # The order the documented examples are meant to be executed in: one route's
  # life. `:health` is the routes listener's own health, which shares a path with
  # the operator API's and is therefore documented on the operator tag.
  @sequence [
    {:createRoute, "created"},
    {:health, "routesPort"},
    {:listRoutes, "oneRoute"},
    {:getRoute, "stripe"},
    {:testRoute, "matched"},
    {:getIpRules, "empty"},
    {:putIpRules, "pinned"},
    {:replaceRoute, "replaced"},
    {:updateRoute, "reenabled"},
    {:deleteRoute, nil}
  ]

  # The table every error probe runs against: `stripe` has the documented
  # example's id and path, and `both` shares its path with a different method —
  # which is how a `PATCH` (whose `path` is immutable) can still be made to
  # collide, and how `PUT /admin/routes/both` can replace into a conflict.
  @probe_seed [
    %{"id" => "stripe", "path" => "/webhooks/stripe", "methods" => ["POST"]},
    %{"id" => "both", "path" => "/webhooks/stripe", "methods" => ["GET"]}
  ]

  # The request bodies that are too long to sit inline in the table below.
  @bad_ip_body %{"method" => "POST", "path" => "/webhooks/stripe", "ip" => "not-an-address"}
  @bad_cidr_body %{
    "default" => "allow",
    "rules" => [%{"action" => "allow", "cidr" => "10.0.0.0/33"}]
  }
  @unknown_rule_body %{
    "default" => "allow",
    "rules" => [%{"action" => "allow", "cidr" => "10.0.0.0/8", "cidrs" => []}]
  }
  @bad_action_body %{
    "default" => "allow",
    "rules" => [%{"action" => "maybe", "cidr" => "10.0.0.0/8"}]
  }
  @mapped_range_body %{
    "default" => "allow",
    "rules" => [%{"action" => "deny", "cidr" => "::ffff:10.0.0.0/104"}]
  }

  # Every documented error response, with the request that produces it. The
  # status is the one the probe must get; `:query` replaces the documented query
  # string, `:path` addresses a concrete path (a missing id, say), and `:body` is
  # the request body. Each probe runs against a fresh instance seeded with
  # `@probe_seed`, so a `404` probe addresses an id that really is missing and a
  # `409` probe really collides.
  #
  # The `503` probes need a store whose writes fail (`UnavailableStore`); the
  # rest need a working one, which is why the status picks the store.
  @error_probes [
    {:listRoutes, 400, [query: "limit=abc"]},
    {:createRoute, 400, [body: %{"path" => "hooks/not-rooted"}]},
    {:createRoute, 400, [body: %{"id" => "Not-A-Slug", "path" => "/x"}]},
    {:createRoute, 400, [body: %{"path" => "/x", "inserted_at" => "2026-01-01T00:00:00Z"}]},
    {:createRoute, 409, [body: %{"id" => "other", "path" => "/webhooks/stripe"}]},
    {:getRoute, 404, [path: "/admin/routes/missing"]},
    {:replaceRoute, 400, [body: %{"path" => "hooks/not-rooted"}]},
    {:replaceRoute, 409, [path: "/admin/routes/both", body: %{"path" => "/webhooks/stripe"}]},
    {:updateRoute, 400, [body: %{"path" => "/webhooks/moved"}]},
    {:updateRoute, 404, [path: "/admin/routes/missing", body: %{"enabled" => false}]},
    {:updateRoute, 409, [path: "/admin/routes/both", body: %{"methods" => ["POST"]}]},
    {:deleteRoute, 404, [path: "/admin/routes/missing"]},
    {:testRoute, 400, [body: @bad_ip_body]},
    {:putIpRules, 400, [body: %{"rules" => []}]},
    {:putIpRules, 400, [body: %{"default" => "allow"}]},
    {:putIpRules, 400, [body: %{"default" => "maybe", "rules" => []}]},
    {:putIpRules, 400, [body: %{"default" => "allow", "rules" => "nope"}]},
    {:putIpRules, 400, [body: @bad_cidr_body]},
    {:putIpRules, 400, [body: @mapped_range_body]},
    {:putIpRules, 400, [body: @bad_action_body]},
    {:putIpRules, 400, [body: @unknown_rule_body]},
    {:createRoute, 503, [body: %{"path" => "/hooks/any"}]},
    {:replaceRoute, 503, [body: %{"path" => "/webhooks/stripe"}]},
    {:updateRoute, 503, [body: %{"enabled" => false}]},
    {:deleteRoute, 503, []},
    {:putIpRules, 503, [body: %{"default" => "allow", "rules" => []}]}
  ]

  setup_all do
    {:ok, document} = YamlElixir.read_from_file(@spec_path)
    %{document: document}
  end

  defp start(routes_opts \\ []), do: start_routes(routes_opts)

  defp operations(document) do
    for {path, item} <- document["paths"],
        {method, operation} <- item,
        is_map(operation),
        "routes" in (operation["tags"] || []) do
      {path, item, method, operation}
    end
  end

  defp operation!(document, operation_id) do
    name = if is_atom(operation_id), do: Atom.to_string(operation_id), else: operation_id

    Enum.find_value(operations(document), fn {path, item, method, operation} ->
      if operation["operationId"] == name, do: {path, item, method, operation}
    end) || flunk("the document has no routes operation #{inspect(name)}")
  end

  # ── the sequence: every documented operation, driven by its own examples ────

  test "every documented operation answers what the document says", %{document: document} do
    config = start()
    state = %{}

    state =
      Enum.reduce(@sequence, state, fn {operation_id, example_name}, state ->
        case operation_id do
          :health -> exercise_health(config, document, state, example_name)
          _other -> exercise(config, document, operation_id, example_name, state)
        end
      end)

    assert state == %{created: "stripe", ip_rules_pinned: true}
  end

  test "the documented examples cover every documented routes operation", %{document: document} do
    documented = for {_p, _i, _m, operation} <- operations(document), do: operation["operationId"]

    driven =
      @sequence
      |> Enum.map(&elem(&1, 0))
      |> Kernel.--([:health])
      |> Enum.map(&Atom.to_string/1)

    assert Enum.sort(documented) == Enum.sort(driven),
           """
           the `routes` tag and @sequence disagree.

             documented: #{inspect(Enum.sort(documented))}
             driven:     #{inspect(Enum.sort(driven))}

           Add the new operation to @sequence (with its request built from the
           operation's examples) or remove it from the document.
           """
  end

  # The sequence sends the documented list query and checks the documented page;
  # this pins that each documented parameter does something, which the sequence
  # alone cannot: its table has one route, so a parameter that was ignored would
  # still produce the documented answer.
  test "the documented list query selects the documented page", %{document: document} do
    # A table where every documented parameter changes the answer: ids at or
    # below the `checkout` cursor, a disabled route inside the first `limit`
    # window, and more enabled routes than the documented `limit` (100).
    seed =
      [
        %{"id" => "aaa", "path" => "/hooks/aaa"},
        %{"id" => "checkout", "path" => "/hooks/checkout"}
      ] ++
        for index <- 0..119 do
          %{"id" => id_at(index), "path" => "/hooks/#{id_at(index)}", "enabled" => index != 50}
        end

    config = start(seed: seed)

    {path, _item, _method, operation} = operation!(document, "listRoutes")
    query = URI.encode_query(documented_query(operation))

    assert query == "enabled=true&limit=100&cursor=checkout",
           "the documented list query parameters changed; update this test"

    page = JSON.decode!(call(config, "get", path <> "?" <> query, nil).resp_body)
    ids = Enum.map(page["routes"], & &1["id"])

    assert ids == Enum.map(0..49, &id_at/1) ++ Enum.map(51..100, &id_at/1)
    assert page["next_cursor"] == "x100"

    # Every documented parameter earned its place: no id at or below the cursor,
    # no disabled route, and the page stops at `limit` even though 119 routes
    # matched.
    refute "aaa" in ids
    refute "checkout" in ids
    refute "x050" in ids
  end

  defp id_at(index), do: "x" <> String.pad_leading(to_string(index), 3, "0")

  # ── the error responses ─────────────────────────────────────────────────────

  test "every documented error response is executed and matches its schema and examples",
       %{document: document} do
    for {operation_id, status, request} <- @error_probes do
      config = probe_config(status)

      {path, _item, method, operation} = operation!(document, operation_id)
      where = "#{operation_id}: #{String.upcase(method)} #{path} → #{status}"
      conn = probe_call(config, method, path, request)

      assert conn.status == status,
             "#{where}: documented, but the router answered #{conn.status} " <>
               "(#{conn.resp_body})"

      response = documented_response!(document, operation, status, where)
      schema = json_schema!(document, response, where)
      body = JSON.decode!(conn.resp_body)

      assert validate(document, schema, body, []) == [],
             "#{where}: the body does not match the documented schema:\n" <>
               Enum.map_join(validate(document, schema, body, []), "\n", &"  #{&1}")

      # Where the response documents examples, the body must be one of them: an
      # SDK matches on `error`/`field`/`message`, so the documented message is
      # part of the contract, not decoration.
      examples = documented_example_values(response)

      if examples != [] do
        assert body in examples,
               "#{where}: the body is none of the documented examples:\n" <>
                 "  got:        #{inspect(body)}\n" <>
                 "  documented: #{inspect(examples)}"
      end
    end
  end

  test "the error probes cover every documented error response", %{document: document} do
    documented =
      for {_path, _item, _method, operation} <- operations(document),
          {status, _response} <- operation["responses"],
          not String.starts_with?(status, "2"),
          do: {operation["operationId"], String.to_integer(status)}

    covered =
      for {operation_id, status, _request} <- @error_probes,
          do: {to_string(operation_id), status}

    missing = Enum.uniq(documented) -- covered

    assert missing == [],
           """
           the document documents error responses no probe drives:

             #{inspect(missing)}

           Add them to @error_probes with a request that produces them, or stop
           documenting them.
           """
  end

  # ── the documented claims the sequence cannot drive ─────────────────────────
  #
  # The sequence exercises one route's life; these are the rest of the document's
  # promises — an id the examples never name, and the dry run's decisions, only
  # one of which the sequence happens to make.

  # The documented id `test` is not reserved (only `POST /admin/routes/test` is
  # the dry run, and it shadows that one method on that one path), which is a
  # claim the document makes twice and the sequence cannot check: it never names
  # the id.
  test "the documented id `test` is creatable and addressable" do
    config = start()

    created = call(config, "post", "/admin/routes", %{"id" => "test", "path" => "/hooks/test"})
    assert created.status == 201
    assert JSON.decode!(created.resp_body)["id"] == "test"
    assert call(config, "get", "/admin/routes/test", nil).status == 200

    replaced = call(config, "put", "/admin/routes/test", %{"path" => "/hooks/test-moved"})
    assert replaced.status == 200
    assert call(config, "patch", "/admin/routes/test", %{"enabled" => false}).status == 200
    assert call(config, "delete", "/admin/routes/test", nil).status == 204
  end

  # The dry run's documented examples are the answer to "why was my webhook
  # rejected", so every one of them has to be a decision the code can actually
  # produce: an example that is unreachable documents a response that does not
  # exist. The matched route's own rules are what separate `matched`,
  # `ipDeniedByRoute`, and `ipDeniedByPinnedRoute`, so each case gets its own
  # table.
  test "every documented dry-run example is a reachable decision", %{document: document} do
    {path, _item, "post", operation} = operation!(document, "testRoute")
    response = documented_response!(document, operation, 200, "POST #{path} → 200")
    examples = get_in(response, ["content", "application/json", "examples"])

    allow_rule = %{"action" => "allow", "cidr" => "203.0.113.0/24"}
    deny_rule = %{"action" => "deny", "cidr" => "203.0.113.0/24"}
    stripe = %{"method" => "POST", "path" => "/webhooks/stripe", "ip" => "203.0.113.7"}

    cases = [
      %{name: "matched", rules: [allow_rule], request: stripe},
      %{name: "matchedWithoutRouteRules", rules: [], request: stripe},
      %{name: "ipDeniedByRoute", rules: [deny_rule], request: stripe},
      %{
        name: "ipDeniedByPinnedRoute",
        rules: [allow_rule],
        request: %{stripe | "ip" => "198.51.100.9"}
      },
      %{
        name: "noRoute",
        rules: [allow_rule],
        request: %{"method" => "POST", "path" => "/nothing/here", "ip" => "203.0.113.7"}
      },
      %{name: "methodMismatch", rules: [allow_rule], request: %{stripe | "method" => "GET"}}
    ]

    for %{name: name, rules: rules, request: request} <- cases do
      config =
        start(seed: [%{"id" => "stripe", "path" => "/webhooks/stripe", "ip_rules" => rules}])

      example = examples[name] || flunk("the dry run documents no example #{inspect(name)}")
      conn = call(config, "post", path, request)

      assert conn.status == 200

      assert JSON.decode!(conn.resp_body) == example["value"],
             "the #{name} example is not the decision this request gets"
    end

    # And every documented example is driven above: a new one cannot be added
    # without saying which decision produces it.
    assert Enum.sort(Map.keys(examples)) == Enum.sort(Enum.map(cases, & &1.name))
  end

  # ── the method matrix ───────────────────────────────────────────────────────

  test "each documented path serves exactly its documented methods", %{document: document} do
    config = start()

    # A route to address with `{id}`: a documented 404 for a *missing route* is
    # not the 404 this test is looking for, so the id has to exist — and it has
    # to be the id the document's `{id}` example names, which is what the
    # substitution below uses.
    assert create_route(config, "stripe", "/webhooks/stripe").status == 201

    # `GET /admin/routes/test` is served by the `{id}` template, so a 404 there
    # would be "no such route" rather than "no such endpoint" — the dry run only
    # shadows `POST`, so create the route and let the template serve the read.
    assert call(config, "put", "/admin/routes/test", %{"path" => "/hooks/test"}).status == 200

    documented_paths =
      for {path, _item, _method, _operation} <- operations(document), do: path

    for path <- Enum.uniq(documented_paths) do
      # A concrete path is served by its own path item *and* by any documented
      # template that matches it: `/admin/routes/test` is also
      # `/admin/routes/{id}`, so PUT/PATCH/DELETE/GET on it are documented, by
      # the template.
      documented_methods = expected_methods(document, path)

      for method <- http_methods() do
        status =
          config
          |> call(method, substitute(path), nil)
          |> Map.fetch!(:status)

        if method in documented_methods do
          refute status == 404,
                 "#{String.upcase(method)} #{path} is documented but answered 404"
        else
          assert status == 404,
                 "#{String.upcase(method)} #{path} is *not* documented but answered #{status}"
        end
      end
    end
  end

  test "paths that no listener serves are 404 for every method", %{document: document} do
    config = start()
    documented = Enum.uniq(for {path, _i, _m, _o} <- operations(document), do: path)

    # Probes are the documented paths' own near-misses — one segment added, one
    # segment dropped — which is where an undocumented endpoint tends to appear,
    # plus a few siblings of the surface as a whole. Not exhaustive: the method
    # matrix above is what covers a method added to a path that already exists,
    # and a wholly unrelated path (`/admin/status`, say) is not caught here.
    #
    # `{id}` is filled in with the documented example's id, because a probe is a
    # request a client could actually send — `/admin/routes/{id}/extra` is not
    # one, and probing it would only re-test the catch-all.
    #
    # A trailing slash is *not* a probe: to Plug, `/admin/routes/` and
    # `/admin/routes` split to the same segments, so it is the documented path
    # spelled differently.
    probes =
      (near_misses(documented) ++ ["/admin/health", "/routes"])
      |> Enum.map(&substitute/1)
      |> Enum.uniq()
      # `/admin/routes/extra` is not an undocumented path: `/admin/routes/{id}`
      # matches it, which is exactly what the matrix above proves. Only paths no
      # documented template serves are probes.
      |> Enum.reject(fn path ->
        Enum.any?(documented, &(&1 == path or matches_template?(&1, path)))
      end)

    # At least one probe per documented path, so this cannot quietly become empty.
    assert length(probes) >= length(documented)

    for path <- probes, method <- http_methods() do
      assert call(config, method, path, nil).status == 404,
             "#{String.upcase(method)} #{path} is not documented but was served"
    end
  end

  # Every documented path with a segment appended, and with each of its segments
  # removed. The documented set itself is filtered out by the caller.
  defp near_misses(documented) do
    Enum.flat_map(documented, fn path ->
      segments = String.split(path, "/", trim: true)

      [path <> "/extra"] ++
        for(
          index <- 0..(length(segments) - 1),
          shorter = segments |> List.delete_at(index) |> Enum.join("/"),
          shorter != "",
          do: "/" <> shorter
        )
    end)
  end

  # ── the document against itself ─────────────────────────────────────────────

  test "every example in the routes tag conforms to the schema it is an example of",
       %{document: document} do
    operations(document)
    |> Enum.each(fn {path, _item, method, operation} ->
      where = "#{String.upcase(method)} #{path}"

      # Response examples.
      for {status, response} <- operation["responses"],
          response = deref_response(document, response, where),
          examples = get_in(response, ["content", "application/json", "examples"]),
          examples != nil,
          {name, %{"value" => value}} <- examples do
        assert_example_conforms(
          document,
          json_schema!(document, response, "#{where} → #{status}"),
          value,
          "#{where} → #{status} example #{inspect(name)}"
        )
      end

      # Parameter examples: an `enabled` example of "maybe" would document a
      # request the implementation rejects.
      for parameter <- operation["parameters"] || [],
          example = parameter["example"],
          example != nil do
        assert_example_conforms(
          document,
          parameter["schema"],
          example,
          "#{where} parameter #{inspect(parameter["name"])} (#{parameter["in"]})"
        )
      end

      # Request-body examples: the request half of the contract, and the half the
      # sequence above sends.
      example = get_in(operation, ["requestBody", "content", "application/json", "example"])

      if example != nil do
        schema = get_in(operation, ["requestBody", "content", "application/json", "schema"])
        assert_example_conforms(document, schema, example, "#{where} request body")
      end
    end)

    # The route-management listener's `/health`, documented as the second arm of
    # the operator `/health` union.
    health_operation = document["paths"]["/health"]["get"]
    health = documented_response!(document, health_operation, 200, "GET /health → 200")
    examples = get_in(health, ["content", "application/json", "examples"])

    for {name, %{"value" => value}} <- examples do
      assert validate(document, health["content"]["application/json"]["schema"], value, []) == [],
             "the `/health` example #{inspect(name)} does not conform to the union"
    end
  end

  # ── the store-unavailable path ──────────────────────────────────────────────

  test "a store that cannot write answers the documented 503", %{document: document} do
    config = start(store: {Ankusa.Routes.RouterOpenAPITest.UnavailableStore, []})

    {path, _item, method, operation} = operation!(document, "createRoute")

    conn = call(config, method, path, %{"path" => "/hooks/any"})
    assert conn.status == 503

    where = "POST #{path} → 503"
    response = documented_response!(document, operation, 503, where)
    body = JSON.decode!(conn.resp_body)

    assert validate(document, json_schema!(document, response, where), body, []) == [],
           "the 503 body does not match the documented schema"

    # And the documented example, not just its schema: the code is the whole
    # answer, so an operator (and an SDK) can match on it.
    values = documented_example_values(response)

    assert values != [], "the 503 response documents no example"
    assert body in values, "the 503 body is not the documented example"
  end

  defmodule UnavailableStore do
    @moduledoc """
    A store whose every write fails, for the documented `503 store_unavailable`.

    Redis being down is the real-world cause; making it a module keeps the test
    deterministic and Redis-free, and it doubles as proof that
    `config.routes.store`'s `{module, opts}` seam accepts a store `ankusa` never
    shipped.

    It still builds and publishes the snapshot it booted from, because that is
    the case the document describes: reads keep working from this node's
    in-memory table while every write is refused. It is also what lets the
    probes reach the write paths that read first — a `PATCH`, which must find
    the route before it can replace it.
    """

    @behaviour Ankusa.Routes.Store

    alias Ankusa.Routes.Snapshot

    # A supervisor child, like any store adapter: `Ankusa.Instance` starts
    # whatever `config.routes.store` names. Not `@impl`: `child_spec/1` is the
    # supervisor's convention, not part of the `Ankusa.Routes.Store` behaviour.
    def child_spec(opts) do
      %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
    end

    @impl true
    def start_link(opts) do
      instance = Keyword.fetch!(opts, :instance)
      %{routes: routes_config} = Ankusa.config(instance)
      {:ok, %{routes: routes, ip_rules: ip_rules}} = Snapshot.initial_table(routes_config)

      Snapshot.publish(%{instance: instance, routes: routes, ip_rules: ip_rules, version: 1})

      Agent.start_link(fn -> nil end, name: Ankusa.via(instance, :routes_store))
    end

    # The version is what the caller validated against; a store that cannot
    # write never gets to use it.
    @impl true
    def insert(_instance, _route, _version), do: {:error, :store_unavailable}

    @impl true
    def replace(_instance, _route, _version), do: {:error, :store_unavailable}

    @impl true
    def delete(_instance, _id), do: {:error, :store_unavailable}

    @impl true
    def put_ip_rules(_instance, _rules), do: {:error, :store_unavailable}
  end

  # ── exercising one operation ────────────────────────────────────────────────

  defp exercise(config, document, operation_id, example_name, state) do
    {path, _item, method, operation} = operation!(document, operation_id)

    assert operation["summary"], "#{operation_id} has no summary"

    documented = documented_request(path, operation)
    conn = call(config, method, documented.path, documented.body)
    [status | _] = operation |> documented_statuses() |> Enum.map(&String.to_integer/1)

    assert conn.status == status,
           "#{operation_id}: #{String.upcase(method)} #{documented.path} answered " <>
             "#{conn.status}, the document says #{status}"

    if conn.status == 204 do
      assert conn.resp_body == ""
      assert example_name == nil
    else
      example = success_example(document, operation, example_name)
      assert_example(document, operation, example, JSON.decode!(conn.resp_body), documented.path)
    end

    apply_effect(operation_id, state)
  end

  # The routes listener's `/health`, whose example lives on the operator tag's
  # `/health` operation as the second arm of the union.
  defp exercise_health(config, document, state, example_name) do
    where = "GET /health → 200"
    health_operation = document["paths"]["/health"]["get"]
    response = documented_response!(document, health_operation, 200, where)
    examples = get_in(response, ["content", "application/json", "examples"])

    %{"value" => value} = examples[example_name]
    schema = json_schema!(document, response, where)
    conn = call(config, "get", "/health", nil)

    assert conn.status == 200
    assert conn.resp_body |> JSON.decode!() |> then(&validate(document, schema, &1, [])) == []

    assert JSON.decode!(conn.resp_body)["routes"] == value["routes"],
           "the routes listener's /health must report the count the example shows"

    state
  end

  defp apply_effect(operation_id, state) do
    case operation_id do
      :createRoute -> Map.put(state, :created, "stripe")
      :putIpRules -> Map.put(state, :ip_rules_pinned, true)
      _other -> state
    end
  end

  # ── building a request from the document ────────────────────────────────────

  # The request an operation's examples describe: the path template filled in
  # from its path parameter's example, the documented query parameters appended,
  # and its JSON body example.
  defp documented_request(path, operation) do
    %{
      path: path |> substitute_parameters(operation) |> substitute() |> append_query(operation),
      body: request_body(operation)
    }
  end

  # The documented query parameters, in document order — `enabled=true`,
  # `limit=100`, `cursor=checkout` for the list operation. A parameter without an
  # example documents that it exists, not what to send.
  defp documented_query(operation) do
    for parameter <- operation["parameters"] || [],
        parameter["in"] == "query",
        not is_nil(parameter["example"]) do
      {parameter["name"], to_string(parameter["example"])}
    end
  end

  defp append_query(path, operation) do
    case documented_query(operation) do
      [] -> path
      pairs -> path <> "?" <> URI.encode_query(pairs)
    end
  end

  defp substitute_parameters(path, operation) do
    Enum.reduce(operation["parameters"] || [], path, fn
      %{"in" => "path", "name" => name, "example" => example}, path ->
        String.replace(path, "{#{name}}", example)

      _parameter, path ->
        path
    end)
  end

  # `{id}` in a *path template* is a parameter; in a documented request path it
  # is the example the sequence created.
  defp substitute(path), do: String.replace(path, "{id}", "stripe")

  # A map, not encoded JSON: `call/4` does the encoding, and double-encoding
  # would send `"{\"id\":...}"` — a JSON *string*, which `read_json/1` correctly
  # rejects as `invalid_body`.
  defp request_body(operation) do
    get_in(operation, ["requestBody", "content", "application/json", "example"])
  end

  defp documented_statuses(operation), do: operation["responses"] |> Map.keys() |> Enum.sort()

  # The example the sequence expects by name: with several documented examples
  # per status (the dry run documents a decision each), picking one by position
  # would be picking by map order.
  defp success_example(document, operation, name) do
    operation_id = operation["operationId"]

    for {status, response} <- operation["responses"],
        String.starts_with?(status, "2"),
        response = deref_response(document, response, "#{operation_id} → #{status}"),
        examples = get_in(response, ["content", "application/json", "examples"]),
        examples != nil,
        example = examples[name] do
      %{
        name: name,
        status: status,
        value: example["value"],
        schema: json_schema!(document, response, "#{operation_id} → #{status}")
      }
    end
    |> List.first() ||
      flunk("#{operation_id} documents no example named #{inspect(name)}")
  end

  # Example and response must agree on every field, except timestamps: a real
  # `inserted_at` can never equal the literal one in the document, so those are
  # checked against the ISO 8601 shape instead. A field the document forgot (or
  # invented) fails here.
  defp assert_example(document, operation, example, actual, request_path) do
    schema = example.schema

    assert validate(document, schema, actual, []) == [],
           "#{operation["operationId"]}: the response to #{request_path} does not match the " <>
             "documented schema:\n" <>
             Enum.map_join(validate(document, schema, actual, []), "\n", &"  #{&1}")

    assert_same_fields(example.value, actual, operation["operationId"])
  end

  defp assert_same_fields(expected, actual, context) when is_map(expected) and is_map(actual) do
    assert Map.keys(expected) |> Enum.sort() == Map.keys(actual) |> Enum.sort(),
           "#{context}: the documented example and the response have different fields:\n" <>
             "  documented: #{inspect(Map.keys(expected) |> Enum.sort())}\n" <>
             "  response:   #{inspect(Map.keys(actual) |> Enum.sort())}"

    for {key, value} <- expected do
      assert_same_fields(value, actual[key], "#{context}.#{key}")
    end
  end

  defp assert_same_fields(expected, actual, context) when is_list(expected) and is_list(actual) do
    assert length(expected) == length(actual),
           "#{context}: documented #{length(expected)} element(s), got #{length(actual)}"

    Enum.zip(expected, actual)
    |> Enum.with_index()
    |> Enum.each(fn {{e, a}, index} -> assert_same_fields(e, a, "#{context}[#{index}]") end)
  end

  defp assert_same_fields(expected, actual, context) when is_binary(expected) do
    if iso8601?(expected) do
      assert iso8601?(actual),
             "#{context}: expected an ISO 8601 timestamp, got #{inspect(actual)}"
    else
      assert expected == actual,
             "#{context}: documented #{inspect(expected)}, got #{inspect(actual)}"
    end
  end

  defp assert_same_fields(expected, actual, context),
    do:
      assert(
        expected == actual,
        "#{context}: documented #{inspect(expected)}, got #{inspect(actual)}"
      )

  defp iso8601?(value) do
    is_binary(value) and Regex.match?(~r/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/, value)
  end

  # ── request helpers ─────────────────────────────────────────────────────────

  defp create_route(config, id, path) do
    call(config, "post", "/admin/routes", %{"id" => id, "path" => path})
  end

  # A probe runs against a fresh instance with `@probe_seed` loaded, so an id it
  # addresses exists (or, for a `404` probe, certainly does not). A `503` needs
  # a store whose writes fail; every other documented error needs a working one.
  defp probe_config(503), do: start(seed: @probe_seed, store: {UnavailableStore, []})
  defp probe_config(_status), do: start(seed: @probe_seed)

  # The probe's request: the documented method and path, with the probe's own
  # query string, concrete path, or body. The path is `substitute/1`d because a
  # probe is a request a client sends — `/admin/routes/{id}` is a template, not
  # a path.
  defp probe_call(config, method, path, request) do
    path =
      case request[:path] do
        nil -> substitute(path) <> query_suffix(request[:query])
        concrete -> concrete
      end

    call(config, method, path, request[:body])
  end

  defp query_suffix(nil), do: ""
  defp query_suffix(query), do: "?" <> query

  defp call(config, method, path, body) do
    conn = Plug.Test.conn(method, path, body && JSON.encode!(body))
    Router.call(conn, Router.init(instance: config.instance))
  end

  defp http_methods, do: ~w(get post put patch delete)

  # The methods a concrete path answers, per the document: its own path item's,
  # plus every documented template that matches it.
  defp expected_methods(document, path) do
    document["paths"]
    |> Enum.filter(fn {other, _item} -> other == path or matches_template?(other, path) end)
    |> Enum.flat_map(fn {_other, item} ->
      for {method, operation} <- item, is_map(operation), method in http_methods(), do: method
    end)
    |> Enum.uniq()
  end

  defp matches_template?(template, path) do
    String.contains?(template, "{") and
      template
      |> String.split("/")
      |> length() == path |> String.split("/") |> length() and
      template
      |> String.split("/")
      |> Enum.zip(String.split(path, "/"))
      |> Enum.all?(fn
        {"{" <> _rest, _concrete} -> true
        {literal, literal} -> true
        {_literal, _concrete} -> false
      end)
  end

  # ── the validator ───────────────────────────────────────────────────────────
  #
  # The subset of JSON Schema the document actually uses: `type` (single or
  # list), `required`, `properties`, `items`, `enum`, `const`, `format`, and
  # `oneOf`. Enough to catch a renamed field, a wrong type, or an example the
  # schemas forbid — and small enough to read.

  defp validate(document, schema, value, path) do
    schema = deref(document, schema, path)

    cond do
      # JSON Schema's literal `true` is "anything goes"; a *missing* schema is a
      # hole in the document, and treating it as a pass is how a response stops
      # being checked at all.
      schema == true ->
        []

      schema == nil ->
        ["#{render(path)}: no schema to validate against"]

      one_of = schema["oneOf"] ->
        matches =
          Enum.filter(one_of, fn candidate ->
            validate(document, candidate, value, path) == []
          end)

        if matches == [] do
          ["#{render(path)}: matches no arm of the union"]
        else
          []
        end

      enum = schema["enum"] ->
        if value in enum,
          do: [],
          else: ["#{render(path)}: #{inspect(value)} is not in #{inspect(enum)}"]

      const = schema["const"] ->
        if value == const, do: [], else: ["#{render(path)}: expected #{inspect(const)}"]

      schema["format"] == "date-time" ->
        if iso8601?(value),
          do: [],
          else: ["#{render(path)}: #{inspect(value)} is not a date-time"]

      types = schema["type"] ->
        validate_typed(document, schema, types, value, path)

      true ->
        []
    end
  end

  defp validate_typed(document, schema, types, value, path) do
    types = List.wrap(types)

    if value == nil and "null" in types do
      []
    else
      expected = Enum.reject(types, &(&1 == "null"))

      if Enum.any?(expected, &type_matches?(&1, value)) do
        validate_bounds(schema, value, path) ++ validate_shape(document, schema, value, path)
      else
        ["#{render(path)}: expected #{Enum.join(expected, " | ")}, got #{inspect(value)}"]
      end
    end
  end

  defp validate_shape(document, schema, value, path) when is_map(value) do
    missing = (schema["required"] || []) -- Map.keys(value)

    missing_errors =
      Enum.map(missing, fn key -> "#{render(path)}: missing required field #{inspect(key)}" end)

    property_errors =
      for {key, property_schema} <- schema["properties"] || %{},
          Map.has_key?(value, key),
          error <- validate(document, property_schema, value[key], path ++ [key]),
          do: error

    if schema["additionalProperties"] == false do
      extra = Map.keys(value) -- Map.keys(schema["properties"] || %{})

      missing_errors ++
        Enum.map(extra, fn key -> "#{render(path)}: undocumented field #{inspect(key)}" end) ++
        property_errors
    else
      missing_errors ++ property_errors
    end
  end

  defp validate_shape(document, schema, value, path) when is_list(value) do
    value
    |> Enum.with_index()
    |> Enum.flat_map(fn {element, index} ->
      validate(document, schema["items"] || true, element, path ++ [index])
    end)
  end

  defp validate_shape(_document, _schema, _value, _path), do: []

  defp validate_bounds(schema, value, path) when is_integer(value) do
    Enum.flat_map([{"minimum", &Kernel.<=/2}, {"maximum", &Kernel.>=/2}], fn {key, ok?} ->
      case schema[key] do
        nil -> []
        bound -> if ok?.(bound, value), do: [], else: ["#{render(path)}: #{key} is #{bound}"]
      end
    end)
  end

  defp validate_bounds(_schema, _value, _path), do: []

  defp assert_example_conforms(document, schema, value, where) do
    errors = validate(document, schema, value, [])

    assert errors == [],
           "#{@spec_file}:\n#{where} does not conform to its own schema:\n" <>
             Enum.map_join(errors, "\n", &"  #{&1}")
  end

  defp type_matches?("string", value), do: is_binary(value)
  defp type_matches?("integer", value), do: is_integer(value)
  defp type_matches?("number", value), do: is_number(value)
  defp type_matches?("boolean", value), do: is_boolean(value)
  defp type_matches?("array", value), do: is_list(value)
  defp type_matches?("object", value), do: is_map(value)
  defp type_matches?(_other, _value), do: true

  defp deref(document, %{"$ref" => "#/" <> pointer}, path) do
    document
    |> get_in(String.split(pointer, "/"))
    |> case do
      nil -> flunk("#{render(path)}: #{pointer} does not resolve in #{@spec_file}")
      resolved -> resolved
    end
  end

  defp deref(_document, schema, _path), do: schema

  # The `value`s of a response's examples, or `[]` when it documents none.
  defp documented_example_values(response) do
    examples = get_in(response, ["content", "application/json", "examples"]) || %{}
    for {_name, %{"value" => value}} <- examples, do: value
  end

  # The documented response for a status, dereferenced: a `$ref` that does not
  # resolve, or a status the document does not carry, fails here rather than
  # quietly reading `nil` out of the wrong shape.
  defp documented_response!(document, operation, status, where) do
    case operation["responses"][to_string(status)] do
      nil -> flunk("#{where}: the document does not document a #{status} response")
      response -> deref_response(document, response, where)
    end
  end

  # A response object is as often a `$ref` (`#/components/responses/InvalidRoute`)
  # as it is inline, and `get_in(response, ["content", ...])` on a `$ref` finds
  # nothing at all — which silently skips every check below it.
  defp deref_response(document, response, where), do: deref(document, response, [where])

  # The schema of a documented JSON response, or a failure: a response the
  # document says is JSON and gives no schema for cannot be checked, and a check
  # that cannot run must not pass.
  defp json_schema!(document, response, where) do
    response = deref_response(document, response, where)

    case get_in(response, ["content", "application/json", "schema"]) do
      nil -> flunk("#{where}: the documented JSON response has no schema in #{@spec_file}")
      schema -> schema
    end
  end

  defp render([]), do: "response"

  defp render(path) do
    Enum.reduce(path, "", fn
      segment, "" -> to_string(segment)
      segment, acc when is_integer(segment) -> "#{acc}[#{segment}]"
      segment, acc -> "#{acc}.#{segment}"
    end)
  end
end
