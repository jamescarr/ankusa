defmodule Ankusa.Gateway.Authz.OpenFGA do
  @moduledoc """
  The `openfga` authorizer: a check against an OpenFGA store. `kind/1` is
  `:remote`, so a decision goes through the gateway's decision cache and this
  authorizer's `Ankusa.Gateway.Caller`; with `batch: true` (the default)
  concurrent checks ride one `batch-check` request.

  Each request becomes one check:

      {"tuple_key": {"user": …, "relation": …, "object": …},
       "contextual_tuples": {"tuple_keys": […]},
       "consistency": "MINIMIZE_LATENCY"}

  plus `authorization_model_id` when set, sent to
  `<url>/stores/<store_id>/check`; the answer is `{"allowed": true | false}`.
  `batch: true` sends `{"checks": [{"tuple_key": …, "contextual_tuples": …,
  "correlation_id": "c<i>"}, …], "consistency": …}` to `batch-check`, each
  check reading its own correlation id's entry: one without it, with an
  `allowed` that is not a boolean, or with a non-empty `error` is unavailable
  for that request only. A `404`, `405` or `501` from `batch-check` memoizes
  "no batching here" for five minutes and the same requests are sent one by
  one.

  ## Mapping a request to a tuple

    * `user` (`"user:{{ principal.id }}"` by default) and `object`
      (`"object:{{ space }}/{{ key }}"`) are `Ankusa.Gateway.Template`
      templates.
    * `resources` maps a space and a key pattern to the object it names:
      `[{space: "docs", match: "teams/{team}/**", object:
      "folder:{{ vars.team }}"}]` — the first entry whose `space` is the
      request's (or `"*"`) and whose `match` matches the request wins, its
      captures are `vars`, and every key that renders one `object` shares one
      decision (`cache_scope/2` is `{relation, object}`). With no entry the
      plain `object` applies.
    * `relations` maps the five action names to OpenFGA relations
      (`%{read: "viewer", write: "editor", delete: "editor", list: "viewer",
      share: "owner"}` by default).
    * `contextual_tuples` (at most 8 entries, `%{user: template, relation:
      string, object: template}`) are expanded with `render_many/2` — one tuple
      per group — and at most 100 tuples ride one check (the rest are dropped
      and a debug log counts them).

  **Identifiers are request data.** Anything OpenFGA reads as a tuple is built
  from the caller's principal, the space and the key, so after rendering a
  value that is `*`, or that contains `#`, whitespace or a control character,
  makes the request `{:ok, :deny, %{reason: :unmappable}}` without a call:
  `user:*` is OpenFGA's public wildcard and `#` starts a userset, and neither
  may come from a caller. A template that cannot render denies the same way.

  ## Options

  | Key | Default | |
  | --- | --- | --- |
  | `url`, `store_id` | required | the OpenFGA API's base URL (literal, https or loopback http) and the store |
  | `authorization_model_id` | `nil` | pin a model |
  | `api_token` | `nil` | sent as `authorization: Bearer <token>`; redacted in `print-config` |
  | `consistency` | `minimize_latency` | `minimize_latency` or `higher_consistency` |
  | `user`, `object` | above | templates |
  | `resources` | `[]` | the space/pattern mapping |
  | `relations` | above | action names to relations |
  | `contextual_tuples` | `[]` | at most 8 entries |
  | `batch` | `true` | use `batch-check` |
  | `headers` | `%{}` | at most 16, sent with every request |
  | `allow_insecure_http` | `false` | accept an `http://` `url` anywhere |
  | `req_options` | `[]` | transport options for `Ankusa.HttpClient` (Elixir config, tests) |

  plus the common `:remote` options (`Ankusa.Gateway.Authz.Remote`). A
  non-`200` status is `{:error, {:unavailable, {:status, code}}}`, a transport
  failure `{:error, {:unavailable, reason}}`, a body that is not the expected
  JSON `{:error, {:unavailable, :bad_response}}`.
  """

  @behaviour Ankusa.Gateway.Authorizer

  alias Ankusa.Gateway.Authz.{Remote, Request}
  alias Ankusa.Gateway.{Options, Pattern, Template}
  alias Ankusa.HttpClient

  require Logger

  @actions [:read, :write, :delete, :list, :share]
  @action_names Enum.map(@actions, &Atom.to_string/1)
  @default_relations %{
    read: "viewer",
    write: "editor",
    delete: "editor",
    list: "viewer",
    share: "owner"
  }
  @consistency %{
    "minimize_latency" => "MINIMIZE_LATENCY",
    "higher_consistency" => "HIGHER_CONSISTENCY"
  }
  @roots ["principal", "claims", "action", "kind", "space", "key", "tenant", "vars"]
  @max_response_bytes 1_048_576
  @max_contextual 8
  @max_tuples 100
  @single_concurrency 8

  @keys [
          :url,
          :store_id,
          :authorization_model_id,
          :api_token,
          :consistency,
          :user,
          :object,
          :resources,
          :relations,
          :contextual_tuples,
          :batch,
          :headers,
          :allow_insecure_http,
          :req_options
        ] ++ Remote.keys()

  @default_user "user:{{ principal.id }}"
  @default_object "object:{{ space }}/{{ key }}"

  @unmappable {:ok, :deny, %{reason: :unmappable}}
  @bad_response {:error, {:unavailable, :bad_response}}

  @impl true
  @spec init!(map(), String.t()) :: map()
  def init!(opts, key) do
    given = Options.map!(opts, key)
    Options.unknown_keys!(given, @keys, key)

    allow_insecure =
      Options.boolean!(Map.get(given, :allow_insecure_http, false), "#{key}.allow_insecure_http")

    url =
      given
      |> Map.get(:url)
      |> Remote.url!(key, allow_insecure_http: allow_insecure)
      |> String.trim_trailing("/")

    %{
      url: url,
      store_id: Options.string!(Map.get(given, :store_id), "#{key}.store_id"),
      model:
        string_or_nil(Map.get(given, :authorization_model_id), "#{key}.authorization_model_id"),
      api_token: string_or_nil(Map.get(given, :api_token), "#{key}.api_token"),
      consistency: consistency!(Map.get(given, :consistency, :minimize_latency), key),
      user: template!(Map.get(given, :user, @default_user), "#{key}.user"),
      object: template!(Map.get(given, :object, @default_object), "#{key}.object"),
      resources: resources!(Map.get(given, :resources), key),
      relations: relations!(Map.get(given, :relations), key),
      contextual: contextual!(Map.get(given, :contextual_tuples), key),
      batch: Options.boolean!(Map.get(given, :batch, true), "#{key}.batch"),
      headers: Remote.headers!(Map.get(given, :headers), key),
      allow_insecure_http: allow_insecure,
      req_options: Remote.req_options!(Map.get(given, :req_options), key),
      remote: Remote.options!(given, key)
    }
  end

  defp string_or_nil(nil, _key), do: nil

  defp string_or_nil(value, key), do: Options.string!(value, key)

  defp template!(value, key), do: Template.compile!(value, key, @roots)

  defp consistency!(value, key) when is_atom(value) and not is_nil(value),
    do: consistency!(Atom.to_string(value), key)

  defp consistency!(value, key) when is_binary(value) do
    case Map.fetch(@consistency, String.downcase(value)) do
      {:ok, consistency} -> consistency
      :error -> unknown_consistency!(value, key)
    end
  end

  defp consistency!(other, key), do: unknown_consistency!(other, key)

  defp unknown_consistency!(value, key) do
    raise ArgumentError,
          "#{key}.consistency must be minimize_latency or higher_consistency, " <>
            "got #{inspect(value)}"
  end

  defp resources!(nil, _key), do: []

  defp resources!(list, key) when is_list(list) do
    list
    |> Enum.with_index()
    |> Enum.map(fn {entry, index} -> resource!(entry, "#{key}.resources[#{index}]") end)
  end

  defp resources!(other, key),
    do: raise(ArgumentError, "#{key}.resources must be a list of entries, got #{inspect(other)}")

  defp resource!(entry, key) do
    entry = Options.map!(entry, key)
    Options.unknown_keys!(entry, [:space, :match, :object], key)

    %{
      space: Options.string!(field(entry, :space, "*"), "#{key}.space"),
      match: Pattern.compile!(field(entry, :match), "#{key}.match"),
      object: template!(field(entry, :object), "#{key}.object")
    }
  end

  defp relations!(nil, _key), do: @default_relations

  defp relations!(given, key) when is_map(given) and not is_struct(given) do
    Map.new(given, fn {name, relation} ->
      {action_name!(name, key), Options.string!(relation, "#{key}.relations.#{name}")}
    end)
    |> then(&Map.merge(@default_relations, &1))
  end

  defp relations!(other, key),
    do:
      raise(
        ArgumentError,
        "#{key}.relations must be a map of action names to relations, got #{inspect(other)}"
      )

  defp action_name!(name, key) when is_atom(name) and not is_nil(name) and not is_boolean(name),
    do: action_name!(Atom.to_string(name), key)

  defp action_name!(name, key) when is_binary(name) do
    case Enum.find(@actions, &(Atom.to_string(&1) == name)) do
      nil ->
        raise ArgumentError,
              "#{key}.relations has unknown action #{inspect(name)}; " <>
                "known: #{Enum.join(@action_names, ", ")}"

      action ->
        action
    end
  end

  defp action_name!(other, key),
    do: raise(ArgumentError, "#{key}.relations must be action names, got #{inspect(other)}")

  defp contextual!(nil, _key), do: []

  defp contextual!(list, key) when is_list(list) and length(list) <= @max_contextual do
    list
    |> Enum.with_index()
    |> Enum.map(fn {entry, index} ->
      contextual_entry!(entry, "#{key}.contextual_tuples[#{index}]")
    end)
  end

  defp contextual!(list, key) when is_list(list),
    do:
      raise(
        ArgumentError,
        "#{key}.contextual_tuples must hold at most #{@max_contextual} entries, " <>
          "got #{length(list)}"
      )

  defp contextual!(other, key),
    do:
      raise(
        ArgumentError,
        "#{key}.contextual_tuples must be a list of entries, got #{inspect(other)}"
      )

  defp contextual_entry!(entry, key) do
    entry = Options.map!(entry, key)
    Options.unknown_keys!(entry, [:user, :relation, :object], key)

    %{
      user: template!(field(entry, :user), "#{key}.user"),
      relation: Options.string!(field(entry, :relation), "#{key}.relation"),
      object: template!(field(entry, :object), "#{key}.object")
    }
  end

  # An option's value from a map with atom keys (Elixir config) or string keys
  # (YAML; the section converts top-level keys, not the ones inside a nested
  # entry).
  defp field(map, name, default \\ nil) do
    case Map.fetch(map, name) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(name), default)
    end
  end

  # ── the behaviour ───────────────────────────────────────────────────────────

  @impl true
  @spec kind(map()) :: :remote
  def kind(_state), do: :remote

  @impl true
  @spec decide(Request.t(), map(), map()) :: Ankusa.Gateway.Authorizer.result()
  def decide(%Request{} = request, state, ctx) do
    with {:ok, tuple} <- tuple(request, state),
         {:ok, tuples} <- contextual(request, state) do
      case check(state, tuple, tuples, ctx) do
        {:ok, allowed} -> {:ok, allowed, %{}}
        {:error, {:unavailable, _}} = error -> error
      end
    else
      :error -> @unmappable
    end
  end

  @impl true
  @spec decide_many([Request.t()], map(), map()) :: [Ankusa.Gateway.Authorizer.result()]
  def decide_many([], _state, _ctx), do: []

  def decide_many(requests, state, ctx) do
    if state.batch and not Remote.batch_unavailable?(ctx.instance, ctx.name),
      do: batch(requests, state, ctx),
      else: each(requests, state, ctx)
  end

  @impl true
  @spec cache_scope(Request.t(), map()) :: term()
  def cache_scope(%Request{} = request, state) do
    with {:ok, relation} <- relation(state.relations, request.action),
         {:ok, object} <- object(request, state) do
      {relation, object}
    else
      :error -> :unmappable
    end
  end

  # ── one check ───────────────────────────────────────────────────────────────

  defp tuple(request, state) do
    bindings = Template.bindings(request)

    with {:ok, user} <- rendered(state.user, bindings),
         {:ok, object} <- object(request, state),
         {:ok, relation} <- relation(state.relations, request.action),
         :ok <- identifiers([user, object]) do
      {:ok, %{"user" => user, "relation" => relation, "object" => object}}
    else
      _error -> :error
    end
  end

  defp object(request, state) do
    case matched_resource(request, state.resources) do
      {:ok, template, vars} ->
        rendered(template, Template.bindings(request, vars))

      :nomatch ->
        rendered(state.object, Template.bindings(request))
    end
  end

  defp matched_resource(_request, []), do: :nomatch

  defp matched_resource(request, [entry | rest]) do
    if entry.space == "*" or entry.space == request.resource.space do
      case Pattern.match(entry.match, request.resource.kind, request.resource.key) do
        {:ok, vars} -> {:ok, entry.object, vars}
        :nomatch -> matched_resource(request, rest)
      end
    else
      matched_resource(request, rest)
    end
  end

  defp relation(relations, action) do
    case Map.fetch(relations, action) do
      {:ok, relation} -> {:ok, relation}
      :error -> :error
    end
  end

  defp rendered(template, bindings) do
    case Template.render(template, bindings) do
      {:ok, text} -> {:ok, text}
      {:error, _reason} -> :error
    end
  end

  # OpenFGA identifiers are built from request data: nothing the caller can
  # put in one may be a wildcard (`*`), start a userset (`#`) or carry
  # whitespace or a control character.
  defp identifiers(values) do
    if Enum.all?(values, &identifier?/1), do: :ok, else: :error
  end

  defp identifier?(value) do
    not Regex.match?(~r/[*#\s]/u, value) and not control?(value)
  end

  defp control?(<<c, _rest::binary>>) when c < 0x20 or c == 0x7F, do: true
  defp control?(<<_c, rest::binary>>), do: control?(rest)
  defp control?(<<>>), do: false

  defp contextual(request, state) do
    bindings = Template.bindings(request)

    state.contextual
    |> Enum.reduce_while({:ok, []}, fn entry, {:ok, acc} ->
      with {:ok, users} <- Template.render_many(entry.user, bindings),
           {:ok, objects} <- Template.render_many(entry.object, bindings),
           :ok <- identifiers(users ++ objects) do
        tuples =
          for user <- users, object <- objects do
            %{"user" => user, "relation" => entry.relation, "object" => object}
          end

        {:cont, {:ok, acc ++ tuples}}
      else
        _error -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, tuples} -> {:ok, truncate(tuples)}
      :error -> :error
    end
  end

  defp truncate(tuples) when length(tuples) <= @max_tuples, do: tuples

  defp truncate(tuples) do
    Logger.debug(
      "[ankusa] openfga: dropping #{length(tuples) - @max_tuples} contextual tuples " <>
        "over the #{@max_tuples}-tuple limit"
    )

    Enum.take(tuples, @max_tuples)
  end

  defp check(state, tuple, tuples, ctx) do
    body =
      %{"tuple_key" => tuple, "consistency" => state.consistency}
      |> with_model(state.model)
      |> with_tuples(tuples)

    url = state.url <> "/stores/" <> path_segment(state.store_id) <> "/check"

    case post(state, url, JSON.encode!(body), ctx.instance, fresh_request_id()) do
      {:ok, 200, _headers, response} -> allowed(response)
      {:ok, status, _headers, _body} -> {:error, {:unavailable, {:status, status}}}
      {:error, reason} -> {:error, {:unavailable, reason}}
    end
  end

  defp allowed(body) do
    case JSON.decode(body) do
      {:ok, %{"allowed" => true}} -> {:ok, :allow}
      {:ok, %{"allowed" => false}} -> {:ok, :deny}
      _other -> @bad_response
    end
  end

  # ── a batch of checks ───────────────────────────────────────────────────────

  defp batch(requests, state, ctx) do
    prepared =
      Enum.map(requests, fn request ->
        with {:ok, tuple} <- tuple(request, state),
             {:ok, tuples} <- contextual(request, state) do
          {:ok, tuple, tuples}
        else
          _error -> :unmappable
        end
      end)

    checks =
      for {:ok, tuple, tuples} <- prepared, do: {tuple, tuples}

    if checks == [] do
      List.duplicate(@unmappable, length(requests))
    else
      body =
        checks
        |> Enum.with_index()
        |> Enum.map(fn {{tuple, tuples}, index} ->
          %{"tuple_key" => tuple, "correlation_id" => "c#{index}"}
          |> with_tuples(tuples)
        end)
        |> then(&%{"checks" => &1, "consistency" => state.consistency})
        |> with_model(state.model)

      url = state.url <> "/stores/" <> path_segment(state.store_id) <> "/batch-check"

      case post(state, url, JSON.encode!(body), ctx.instance, fresh_request_id()) do
        {:ok, 200, _headers, response} ->
          case batch_results(response, length(checks)) do
            {:ok, results} -> merge_batch(prepared, results)
            :bad -> List.duplicate({:error, {:unavailable, :bad_batch}}, length(requests))
          end

        {:ok, status, _headers, _body} ->
          if Remote.no_batch_status?(status) do
            Remote.mark_batch_unavailable(ctx.instance, ctx.name)
            each(requests, state, ctx)
          else
            List.duplicate({:error, {:unavailable, {:status, status}}}, length(requests))
          end

        {:error, reason} ->
          List.duplicate({:error, {:unavailable, reason}}, length(requests))
      end
    end
  end

  defp batch_results(body, count) do
    with {:ok, map} when is_map(map) <- JSON.decode(body),
         %{} = results <- Map.get(map, "results") || Map.get(map, "result") || map do
      {:ok, Enum.map(0..(count - 1), &check_result(Map.get(results, "c#{&1}")))}
    else
      _other -> :bad
    end
  end

  defp check_result(%{"allowed" => allowed} = entry) when is_boolean(allowed) do
    if empty?(Map.get(entry, "error")) do
      if allowed, do: {:ok, :allow, %{}}, else: {:ok, :deny, %{}}
    else
      @bad_response
    end
  end

  defp check_result(_entry), do: @bad_response

  defp empty?(nil), do: true
  defp empty?(""), do: true
  defp empty?(value) when is_map(value), do: map_size(value) == 0
  defp empty?(value) when is_list(value), do: value == []
  defp empty?(_value), do: false

  defp merge_batch(prepared, results) do
    {answers, _remaining} =
      Enum.reduce(prepared, {[], results}, fn
        :unmappable, {acc, pending} -> {[@unmappable | acc], pending}
        {:ok, _tuple, _tuples}, {acc, [answer | pending]} -> {[answer | acc], pending}
      end)

    Enum.reverse(answers)
  end

  # ── the request itself ──────────────────────────────────────────────────────

  defp with_model(body, nil), do: body
  defp with_model(body, model), do: Map.put(body, "authorization_model_id", model)

  defp with_tuples(body, []), do: body

  defp with_tuples(body, tuples),
    do: Map.put(body, "contextual_tuples", %{"tuple_keys" => tuples})

  defp path_segment(value), do: URI.encode(value, &URI.char_unreserved?/1)

  defp post(state, url, body, instance, request_id) do
    headers =
      [{"content-type", "application/json"}, {"x-request-id", request_id}] ++
        authorization(state) ++ Map.to_list(state.headers)

    HttpClient.request_with_headers(
      :post,
      url,
      headers,
      body,
      state.remote.timeout_ms,
      [instance: instance, max_response_bytes: @max_response_bytes] ++ state.req_options
    )
  end

  defp authorization(%{api_token: nil}), do: []
  defp authorization(%{api_token: token}), do: [{"authorization", "Bearer " <> token}]

  defp each(requests, state, ctx) do
    requests
    |> Task.async_stream(&decide(&1, state, ctx),
      max_concurrency: @single_concurrency,
      ordered: true,
      timeout: :infinity
    )
    |> Enum.map(fn {:ok, result} -> result end)
  end

  defp fresh_request_id do
    Ankusa.ULID.encode(
      <<System.system_time(:millisecond)::48, :crypto.strong_rand_bytes(10)::binary>>
    )
  end
end
