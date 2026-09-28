defmodule Ankusa.Routes.RouterOpenAPITest do
  @moduledoc """
  The contract test for the `routes` endpoints of
  `priv/openapi/admin.v1.yaml`: the document and `Ankusa.Routes.Router` cannot
  drift apart without failing here.

  Three things are checked, and each one catches a different kind of drift:

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
       to another method fails here.
    3. **Every example is an example of its own schema.** The document is
       validated against itself, so prose-level examples cannot silently rot into
       values the schemas forbid.

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

  # ── the method matrix ───────────────────────────────────────────────────────

  test "each documented path serves exactly its documented methods", %{document: document} do
    config = start()

    # A route to address with `{id}`: a documented 404 for a *missing route* is
    # not the 404 this test is looking for, so the id has to exist — and it has
    # to be the id the document's `{id}` example names, which is what the
    # substitution below uses.
    assert create_route(config, "stripe", "/webhooks/stripe").status == 201

    # `GET /admin/routes/test` is answered by the `{id}` template, so its 404
    # would be "no such route" rather than "no such endpoint" — create the route
    # the dry run's id reserves, so the probe below reads the endpoint, not the
    # resource.
    assert call(config, "put", "/admin/routes/test", %{"path" => "/hooks/test"}).status == 200

    documented_paths =
      for {path, _item, _method, _operation} <- operations(document), do: path

    for path <- Enum.uniq(documented_paths) do
      # A concrete path is served by its own path item *and* by any documented
      # template that matches it: `/admin/routes/test` is also
      # `/admin/routes/{id}`, so PUT/PATCH/DELETE/GET on it are documented, by
      # the template. That is why the dry run reserves the id `test` rather than
      # the other way round.
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
    # A trailing slash is *not* a probe: to Plug, `/admin/routes/` and
    # `/admin/routes` split to the same segments, so it is the documented path
    # spelled differently.
    probes =
      (near_misses(documented) ++ ["/admin/health", "/routes"])
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
          examples = get_in(response, ["content", "application/json", "examples"]),
          examples != nil,
          {name, %{"value" => value}} <- examples do
        schema = get_in(response, ["content", "application/json", "schema"])

        assert_example_conforms(
          document,
          schema,
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
    health = document["paths"]["/health"]["get"]["responses"]["200"]
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

    response = operation["responses"]["503"]
    schema = get_in(response, ["content", "application/json", "schema"])

    assert validate(document, schema, JSON.decode!(conn.resp_body), []) == [],
           "the 503 body does not match the documented schema"
  end

  defmodule UnavailableStore do
    @moduledoc """
    A store whose every write fails, for the documented `503 store_unavailable`.

    Redis being down is the real-world cause; making it a module keeps the test
    deterministic and Redis-free, and it doubles as proof that
    `config.routes.store`'s `{module, opts}` seam accepts a store `ankusa` never
    shipped.
    """

    @behaviour Ankusa.Routes.Store

    # A supervisor child, like any store adapter: `Ankusa.Instance` starts
    # whatever `config.routes.store` names. Not `@impl`: `child_spec/1` is the
    # supervisor's convention, not part of the `Ankusa.Routes.Store` behaviour.
    def child_spec(opts) do
      %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
    end

    @impl true
    def start_link(opts) do
      Agent.start_link(fn -> nil end,
        name: Ankusa.via(Keyword.fetch!(opts, :instance), :routes_store)
      )
    end

    @impl true
    def insert(_instance, _route), do: {:error, :store_unavailable}

    @impl true
    def replace(_instance, _route), do: {:error, :store_unavailable}

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
      example = success_example(operation, example_name)
      assert_example(document, operation, example, JSON.decode!(conn.resp_body), documented.path)
    end

    apply_effect(operation_id, state)
  end

  # The routes listener's `/health`, whose example lives on the operator tag's
  # `/health` operation as the second arm of the union.
  defp exercise_health(config, document, state, example_name) do
    health = document["paths"]["/health"]["get"]
    example = get_in(health, ["responses", "200", "content", "application/json", "examples"])

    %{"value" => value} = example[example_name]
    schema = get_in(health, ["responses", "200", "content", "application/json", "schema"])
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
  # from its parameters' examples, and its JSON body example.
  defp documented_request(path, operation) do
    %{
      path: path |> substitute_parameters(operation) |> substitute(),
      body: request_body(operation)
    }
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
  # per status (the dry run documents four decisions), picking one by position
  # would be picking by map order.
  defp success_example(operation, name) do
    for {status, response} <- operation["responses"],
        String.starts_with?(status, "2"),
        examples = get_in(response, ["content", "application/json", "examples"]),
        examples != nil,
        example = examples[name] do
      %{
        name: name,
        status: status,
        value: example["value"],
        schema: get_in(response, ["content", "application/json", "schema"])
      }
    end
    |> List.first() ||
      flunk("#{operation["operationId"]} documents no example named #{inspect(name)}")
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
      schema == true or schema == nil ->
        []

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

  defp render([]), do: "response"

  defp render(path) do
    Enum.reduce(path, "", fn
      segment, "" -> to_string(segment)
      segment, acc when is_integer(segment) -> "#{acc}[#{segment}]"
      segment, acc -> "#{acc}.#{segment}"
    end)
  end
end
