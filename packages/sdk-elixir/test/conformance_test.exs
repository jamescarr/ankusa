defmodule Ankusa.SDK.ConformanceTest do
  @moduledoc false

  # Runs the language-neutral conformance vectors in `conformance/` against this
  # SDK: one test per vector, named by its `id`. See `conformance/README.md` for
  # the vector format and the runner contract every SDK follows.

  use ExUnit.Case, async: true

  alias Ankusa.SDK.{
    Admin,
    ClaimCheck,
    ClaimRef,
    ConformanceGateway,
    Idempotency,
    Message,
    Recorder,
    Routes,
    Webhook
  }

  # The exact module the vectors name; the runner matches by identity, never by
  # subclass.
  @error_modules %{
    "InvalidClaimRefError" => Ankusa.SDK.InvalidClaimRefError,
    "ClaimNotFoundError" => Ankusa.SDK.ClaimNotFoundError,
    "ClaimRejectedError" => Ankusa.SDK.ClaimRejectedError,
    "ClaimIntegrityError" => Ankusa.SDK.ClaimIntegrityError,
    "ClaimCheckUnavailableError" => Ankusa.SDK.ClaimCheckUnavailableError,
    "MissingHookIdError" => Ankusa.SDK.MissingHookIdError,
    "InvalidRouteIdError" => Ankusa.SDK.InvalidRouteIdError,
    "RouteNotFoundError" => Ankusa.SDK.RouteNotFoundError,
    "RoutesRejectedError" => Ankusa.SDK.RoutesRejectedError,
    "RoutesUnavailableError" => Ankusa.SDK.RoutesUnavailableError,
    "RoleNotEnabledError" => Ankusa.SDK.RoleNotEnabledError,
    "AdminRejectedError" => Ankusa.SDK.AdminRejectedError,
    "AdminUnavailableError" => Ankusa.SDK.AdminUnavailableError,
    "InvalidMessageError" => Ankusa.SDK.InvalidMessageError
  }

  case_paths =
    Path.expand("../../../conformance/cases/*.json", __DIR__)
    |> Path.wildcard()
    |> Enum.sort()

  for path <- case_paths do
    @external_resource path
  end

  cases = Enum.flat_map(case_paths, &JSON.decode!(File.read!(&1))["cases"])

  # `routes.list.query` asserts the query string in input order, which a
  # decoded map loses. A second decode keeps every object as an ordered pair
  # list; only `input.params` is taken from it.
  ordered_decoders = [
    object_start: fn _ -> [] end,
    object_push: fn key, value, acc -> [{key, value} | acc] end,
    object_finish: fn acc, old -> {Enum.reverse(acc), old} end
  ]

  params_by_id =
    case_paths
    |> Enum.flat_map(fn path ->
      {value, _acc, ""} = JSON.decode(File.read!(path), :ok, ordered_decoders)

      value
      |> Enum.flat_map(fn {"cases", file_cases} -> file_cases end)
      |> Enum.flat_map(fn case_pairs ->
        with {"id", id} <- List.keyfind(case_pairs, "id", 0),
             {"input", input} <- List.keyfind(case_pairs, "input", 0),
             {"params", params} <- List.keyfind(input, "params", 0) do
          [{id, params}]
        else
          _other -> []
        end
      end)
    end)
    |> Map.new()

  for test_case <- cases do
    test test_case["id"] do
      run_case(
        unquote(Macro.escape(test_case)),
        unquote(Macro.escape(params_by_id)),
        unquote(Macro.escape(@error_modules))
      )
    end
  end

  defp run_case(test_case, params_by_id, error_modules) do
    input = test_case["input"]
    gateway = input["gateway"]
    recorder = Recorder.new()

    base_url =
      cond do
        is_nil(gateway) -> unreachable_url()
        gateway["unreachable"] -> unreachable_url()
        injected?(input) -> unreachable_url()
        true -> ConformanceGateway.start(gateway, recorder)
      end

    result = execute(test_case, params_by_id, input, base_url, recorder)
    requests = Recorder.requests(recorder)
    expect = test_case["expect"]

    case result do
      {:ok, value} ->
        if Map.has_key?(expect, "ok") do
          assert value == expected_ok(test_case, expect["ok"]),
                 "ok: #{inspect(value)} != #{inspect(expected_ok(test_case, expect["ok"]))}"
        else
          flunk("expected error #{inspect(expect["error"])}, got ok: #{inspect(value)}")
        end

      {:error, error} ->
        if Map.has_key?(expect, "error") do
          assert_error(error, expect["error"], error_modules)
        else
          flunk("expected ok #{inspect(expect["ok"])}, got error: #{inspect(error)}")
        end
    end

    if Map.has_key?(expect, "requests") do
      assert_requests(requests, expect["requests"])
    end
  end

  defp execute(test_case, params_by_id, input, base_url, recorder) do
    {:ok, run(test_case["operation"], test_case, params_by_id, input, base_url, recorder)}
  rescue
    error ->
      if error.__struct__ in Map.values(@error_modules) do
        {:error, error}
      else
        reraise error, __STACKTRACE__
      end
  end

  ## operations

  defp run("parse_claim_ref", _case, _params_by_id, input, _base_url, _recorder) do
    ref = unwrap!(ClaimRef.parse(input["ref"]))
    %{"tenant_id" => ref.tenant_id, "claim_id" => ref.claim_id, "path" => ref.path}
  end

  defp run("parse_headers", _case, _params_by_id, input, _base_url, _recorder) do
    headers = unwrap!(Webhook.parse_headers(input["headers"]))

    %{
      "id" => headers.id,
      "source" => headers.source,
      "tenant" => headers.tenant,
      "content_type" => headers.content_type,
      "dedupe_key" => headers.dedupe_key,
      "replay_id" => headers.replay_id
    }
  end

  defp run("decode_message", _case, _params_by_id, input, _base_url, _recorder) do
    message = unwrap!(Message.decode(input["message"]))

    %{
      "v" => message.v,
      "id" => message.id,
      "source_id" => message.source_id,
      "tenant_id" => message.tenant_id,
      "received_at" => message.received_at,
      "content_type" => message.content_type,
      "size" => message.size,
      "body_base64" => message.body && Base.encode64(message.body),
      "claim" => message.claim,
      "sha256" => message.sha256,
      "dedupe_key" => message.dedupe_key,
      "replay_id" => message.replay_id,
      "headers" => message.headers
    }
  end

  defp run("idempotency_key", _case, _params_by_id, input, _base_url, _recorder) do
    opts = [include_replay: input["include_replay"] == true]

    key =
      case input do
        %{"message" => message} -> Idempotency.key(unwrap!(Message.decode(message)), opts)
        %{"headers" => headers} -> Idempotency.key(unwrap!(Webhook.parse_headers(headers)), opts)
      end

    %{"key" => key}
  end

  defp run("redeem", _case, _params_by_id, input, base_url, recorder) do
    with_client(input, base_url, recorder, fn opts ->
      client = ClaimCheck.new(opts[:base_url], Keyword.drop(opts, [:base_url]))
      body = unwrap!(ClaimCheck.redeem(client, input["ref"], input["sha256"]))
      %{"body" => %{"base64" => Base.encode64(body)}}
    end)
  end

  defp run("health", _case, _params_by_id, input, base_url, recorder) do
    with_client(input, base_url, recorder, fn opts ->
      client = ClaimCheck.new(opts[:base_url], Keyword.drop(opts, [:base_url]))
      unwrap!(ClaimCheck.health(client))
    end)
  end

  defp run("routes_health", _case, _params_by_id, input, base_url, recorder) do
    with_routes(input, base_url, recorder, &unwrap!(Routes.health(&1)))
  end

  defp run("routes_list", test_case, params_by_id, input, base_url, recorder) do
    params = Map.get(params_by_id, test_case["id"], input["params"])

    with_routes(input, base_url, recorder, &unwrap!(Routes.list_routes(&1, params)))
  end

  defp run("routes_create", _case, _params_by_id, input, base_url, recorder) do
    with_routes(input, base_url, recorder, &unwrap!(Routes.create_route(&1, input["input"])))
  end

  defp run("routes_get", _case, _params_by_id, input, base_url, recorder) do
    with_routes(input, base_url, recorder, &unwrap!(Routes.get_route(&1, input["id"])))
  end

  defp run("routes_replace", _case, _params_by_id, input, base_url, recorder) do
    with_routes(input, base_url, recorder, fn client ->
      unwrap!(Routes.replace_route(client, input["id"], input["input"]))
    end)
  end

  defp run("routes_update", _case, _params_by_id, input, base_url, recorder) do
    with_routes(input, base_url, recorder, fn client ->
      unwrap!(Routes.update_route(client, input["id"], input["patch"]))
    end)
  end

  defp run("routes_delete", _case, _params_by_id, input, base_url, recorder) do
    with_routes(input, base_url, recorder, &unwrap!(Routes.delete_route(&1, input["id"])))
  end

  defp run("routes_ip_rules_get", _case, _params_by_id, input, base_url, recorder) do
    with_routes(input, base_url, recorder, &unwrap!(Routes.get_ip_rules(&1)))
  end

  defp run("routes_ip_rules_put", _case, _params_by_id, input, base_url, recorder) do
    with_routes(input, base_url, recorder, &unwrap!(Routes.put_ip_rules(&1, input["rules"])))
  end

  defp run("routes_test", _case, _params_by_id, input, base_url, recorder) do
    with_routes(input, base_url, recorder, &unwrap!(Routes.test_route(&1, input["request"])))
  end

  defp run("admin_health", _case, _params_by_id, input, base_url, recorder) do
    with_admin(input, base_url, recorder, &unwrap!(Admin.health(&1)))
  end

  defp run("admin_metrics", _case, _params_by_id, input, base_url, recorder) do
    with_admin(input, base_url, recorder, fn client ->
      %{"text" => unwrap!(Admin.metrics(client))}
    end)
  end

  defp run("admin_config", _case, _params_by_id, input, base_url, recorder) do
    with_admin(input, base_url, recorder, &unwrap!(Admin.config(&1)))
  end

  defp run("admin_dlq_list", _case, _params_by_id, input, base_url, recorder) do
    params = input["params"]

    with_admin(input, base_url, recorder, &unwrap!(Admin.list_dead_letters(&1, params)))
  end

  defp run("admin_replay_create", _case, _params_by_id, input, base_url, recorder) do
    with_admin(input, base_url, recorder, &unwrap!(Admin.create_replay(&1, input["spec"])))
  end

  defp run("admin_replay_get", _case, _params_by_id, input, base_url, recorder) do
    with_admin(input, base_url, recorder, &unwrap!(Admin.get_replay(&1, input["id"])))
  end

  defp run("admin_replay_list", _case, _params_by_id, input, base_url, recorder) do
    with_admin(input, base_url, recorder, &unwrap!(Admin.list_replays(&1)))
  end

  defp run("admin_replay_update", _case, _params_by_id, input, base_url, recorder) do
    with_admin(input, base_url, recorder, fn client ->
      unwrap!(Admin.update_replay(client, input["id"], input["patch"]))
    end)
  end

  defp run("admin_quarantine", _case, _params_by_id, input, base_url, recorder) do
    params = input["params"]

    with_admin(input, base_url, recorder, &unwrap!(Admin.list_quarantined(&1, params)))
  end

  defp run(operation, _case, _params_by_id, _input, _base_url, _recorder) do
    flunk("unknown conformance operation #{inspect(operation)}")
  end

  ## clients

  defp with_routes(input, base_url, recorder, fun) do
    with_client(input, base_url, recorder, fn opts ->
      fun.(Routes.new(opts[:base_url], Keyword.drop(opts, [:base_url])))
    end)
  end

  defp with_admin(input, base_url, recorder, fun) do
    with_client(input, base_url, recorder, fn opts ->
      fun.(Admin.new(opts[:base_url], Keyword.drop(opts, [:base_url])))
    end)
  end

  defp with_client(input, base_url, recorder, fun) do
    spec = input["client"] || %{}

    opts = [
      headers: spec["headers"],
      timeout_ms: spec["timeout_ms"] || 10_000,
      req_options: req_options(input, recorder)
    ]

    opts =
      if injected?(input) do
        Keyword.put(opts, :base_url, "http://gateway.invalid")
      else
        Keyword.put(opts, :base_url, base_url)
      end

    fun.(opts)
  end

  defp req_options(input, recorder) do
    if injected?(input) do
      [plug: injected_plug(input["gateway"], recorder)]
    else
      []
    end
  end

  # The transport an `"injected"` case replaces the network with: it records the
  # request in the same shape the real gateway mock does, and answers with the
  # case's `gateway` response.
  defp injected_plug(spec, recorder) do
    status = spec["status"]
    headers = spec["headers"] || %{}
    payload = ConformanceGateway.body_bytes(spec["body"])

    fn conn ->
      Recorder.record(recorder, %{
        "method" => conn.method,
        "path" => conn.request_path <> query_suffix(conn.query_string),
        "headers" => Map.new(conn.req_headers),
        "body" => Recorder.decode_body(Req.Test.raw_body(conn))
      })

      conn
      |> then(fn conn ->
        Enum.reduce(headers, conn, fn {name, value}, conn ->
          Plug.Conn.put_resp_header(conn, name, value)
        end)
      end)
      |> Plug.Conn.send_resp(status, payload)
    end
  end

  defp injected?(input), do: get_in(input, ["client", "transport"]) == "injected"

  defp unreachable_url, do: "http://127.0.0.1:1"

  defp query_suffix(""), do: ""
  defp query_suffix(query_string), do: "?" <> query_string

  ## assertions

  defp expected_ok(test_case, expected) do
    # Bytes can't round-trip through JSON: both sides become base64.
    if test_case["operation"] == "redeem" do
      %{"body" => %{"base64" => Base.encode64(ConformanceGateway.body_bytes(expected["body"]))}}
    else
      expected
    end
  end

  defp assert_error(error, expected, error_modules) do
    class = expected["class"]

    assert error.__struct__ == Map.fetch!(error_modules, class),
           "expected #{class}, got #{inspect(error.__struct__)}: #{Exception.message(error)}"

    for {key, value} <- expected, key != "class" do
      field = String.to_existing_atom(key)

      assert Map.has_key?(error, field),
             "#{class} has no field #{inspect(key)} (expected #{inspect(value)})"

      assert Map.fetch!(error, field) == value,
             "#{class}.#{key}: #{inspect(Map.fetch!(error, field))} != #{inspect(value)}"
    end
  end

  defp assert_requests(actual, expected) do
    assert length(actual) == length(expected),
           "expected #{length(expected)} requests, got #{length(actual)}: #{inspect(actual)}"

    actual
    |> Enum.zip(expected)
    |> Enum.each(fn {got, want} ->
      assert got["method"] == want["method"],
             "method: #{inspect(got["method"])} != #{inspect(want["method"])}"

      assert got["path"] == want["path"],
             "path: #{inspect(got["path"])} != #{inspect(want["path"])}"

      for {name, value} <- want["headers"] || %{} do
        assert got["headers"][name] == value,
               "header #{inspect(name)}: #{inspect(got["headers"][name])} != #{inspect(value)}"
      end

      if Map.has_key?(want, "body") do
        assert got["body"] == want["body"],
               "body: #{inspect(got["body"])} != #{inspect(want["body"])}"
      end
    end)
  end

  ## helpers

  defp unwrap!({:ok, value}), do: value
  # `routes_delete` expects `ok: null`: a bare `:ok` carries no value.
  defp unwrap!(:ok), do: nil
  defp unwrap!({:error, error}), do: raise(error)
end
