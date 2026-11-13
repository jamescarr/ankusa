defmodule Ankusa.Gateway.Authz.Http do
  @moduledoc """
  The `http` authorizer: a policy decision point reached over HTTP, in one of
  two protocols. `kind/1` is `:remote`: a decision goes through the gateway's
  decision cache and this authorizer's `Ankusa.Gateway.Caller`, so concurrent
  checks coalesce into one call per key and, with batching, into one call per
  batch of up to `batch_max`.

  ## `protocol: authzen` (the default)

  The OpenID AuthZEN Access Evaluation API. A check is one
  `POST <url><evaluation_path>` (`/access/v1/evaluation`) with

      {"subject": …, "resource": …, "action": {"name": …}, "context": …}

  and the decision is `{"decision": true | false}`. Only a JSON `true` allows
  and only a JSON `false` denies: a missing, null or otherwise typed verdict is
  `{:error, {:unavailable, :bad_response}}`, never a default. A
  `cache-control: max-age=<s>` on the response becomes the allow's `ttl_ms`
  (the engine caps it at `allow_ttl_max_ms`).

  With `batch: true` (the default) `decide_many/3` sends
  `POST <url><evaluations_path>` (`/access/v1/evaluations`) with
  `{"evaluations": [one full evaluation per request], "options":
  {"evaluations_semantic": "execute_all"}}`. AuthZEN answers in request order,
  so an `evaluations` array of another length makes every request of the batch
  unavailable (`:bad_batch`), and an element without a boolean `decision`, or
  with `context.error`, is unavailable for its own request. A `404`, `405` or
  `501` from the bulk endpoint memoizes "no batching here" for five minutes
  (`Ankusa.Gateway.Authz.Remote.mark_batch_unavailable/2`) and the same
  requests are then sent one by one.

  `subject` and `resource` are JSON shapes whose string leaves are
  `Ankusa.Gateway.Template` templates (roots `principal`, `claims`, `action`,
  `kind`, `space`, `key`, `tenant`). A leaf whose whole value is one
  list-valued variable renders as a JSON array (`principal.groups`,
  `principal.tenants` — `["*"]` for every tenant). Defaults:

      subject:  {"type": "user", "id": "{{ principal.id }}",
                 "properties": {"groups": "{{ principal.groups }}",
                                "tenants": "{{ principal.tenants }}"}}
      resource: {"type": "{{ kind }}", "id": "{{ space }}/{{ key }}",
                 "properties": {"space": "{{ space }}", "key": "{{ key }}"}}

  A template that cannot render (an absent or empty variable, two list-valued
  variables) is `{:ok, :deny, %{reason: :unmappable}}` without a call. The
  default `resource.id` is unique per resource — a claim's key carries its
  tenant — so decisions do not leak between resources; `cache_scope/2` is
  `{rendered resource.type, rendered resource.id}`, and an operator who
  writes a coarser `resource` chooses to share decisions between the keys that
  render alike.

  `actions` maps the five action names to the names the PDP knows
  (`%{read: "read", …}` by default); `context` is static JSON sent with every
  evaluation.

  ## `protocol: template` (a bespoke permissions API)

  `request` is a JSON shape whose string leaves are templates (same roots);
  a check is `POST <url>` with it, and the verdict is the boolean at
  `allow_path` (dotted, an all-digit segment indexes an array) —
  `ttl_path` optionally names integer seconds in the same response.
  `batch: {url: <bulk endpoint>, items_key: "checks", results_path: "results"}`
  sends `{<items_key>: [one rendered request per decision]}` and reads the
  array at `results_path` element by element with `allow_path`/`ttl_path`; a
  length mismatch is unavailable for all, an element whose `allow_path` is not
  a boolean is unavailable for its own request. Without `batch`, decisions are
  sent one by one (concurrently within one caller run). A decision is cached
  under the engine's default scope, `{kind, space, tenant, key}`: every key is
  its own decision, and a bespoke API that answers per folder still costs one
  call per key.

  ## `forward_authorization`

  With `forward_authorization: true` the caller's own `authorization` header is
  sent to the PDP, which may decide on it (finer than the principal the
  gateway authenticated). The header is `request.authorization` (a remote
  authorizer never sees the connection): a request without it is `{:ok, :deny,
  %{reason: :unmappable}}` without a call, and `cache_scope/2` appends the
  header's SHA-256, so two tokens of one subject never share a decision.

  ## Options

  | Key | Default | |
  | --- | --- | --- |
  | `protocol` | `authzen` | `authzen` or `template` |
  | `url` | required | literal; https, or http to a loopback host / with `allow_insecure_http` |
  | `headers` | `%{}` | at most 16, sent with every request |
  | `allow_insecure_http` | `false` | accept an `http://` `url` anywhere |
  | `forward_authorization` | `false` | send the caller's `authorization` header |
  | `evaluation_path`, `evaluations_path` | `/access/v1/evaluation`, `/access/v1/evaluations` | authzen |
  | `subject`, `resource`, `actions`, `context` | above | authzen |
  | `batch` | `true` | authzen: use the evaluations endpoint; template: a bulk endpoint block or `nil` |
  | `request`, `allow_path`, `ttl_path` | required, required, `nil` | template |
  | `req_options` | `[]` | transport options for `Ankusa.HttpClient` (Elixir config, tests) |

  plus the common `:remote` options (`Ankusa.Gateway.Authz.Remote`). Every
  non-`200` status is `{:error, {:unavailable, {:status, code}}}`, a transport
  failure `{:error, {:unavailable, reason}}`; the step's `on_unavailable` then
  decides what the policy does.
  """

  @behaviour Ankusa.Gateway.Authorizer

  alias Ankusa.Gateway.Authz.{Remote, Request}
  alias Ankusa.Gateway.{Options, Template}
  alias Ankusa.HttpClient
  alias Ankusa.Principal

  @actions [:read, :write, :delete, :list, :share]
  @action_names Enum.map(@actions, &Atom.to_string/1)
  @roots ["principal", "claims", "action", "kind", "space", "key", "tenant", "vars"]
  @max_response_bytes 1_048_576
  @path_segment ~r/\A[A-Za-z0-9_-]+\z/
  @single_concurrency 8

  @common_keys [
                 :protocol,
                 :url,
                 :headers,
                 :allow_insecure_http,
                 :forward_authorization,
                 :req_options,
                 :batch
               ] ++ Remote.keys()
  @authzen_keys [:evaluation_path, :evaluations_path, :subject, :resource, :actions, :context] ++
                  @common_keys
  @template_keys [:request, :allow_path, :ttl_path] ++ @common_keys

  @default_actions %{
    read: "read",
    write: "write",
    delete: "delete",
    list: "list",
    share: "share"
  }

  @default_subject %{
    "type" => "user",
    "id" => "{{ principal.id }}",
    "properties" => %{
      "groups" => "{{ principal.groups }}",
      "tenants" => "{{ principal.tenants }}"
    }
  }

  @default_resource %{
    "type" => "{{ kind }}",
    "id" => "{{ space }}/{{ key }}",
    "properties" => %{"space" => "{{ space }}", "key" => "{{ key }}"}
  }

  @unmappable {:ok, :deny, %{reason: :unmappable}}
  @bad_response {:error, {:unavailable, :bad_response}}

  @impl true
  @spec init!(map(), String.t()) :: map()
  def init!(opts, key) do
    given = Options.map!(opts, key)
    protocol = protocol!(Map.get(given, :protocol, :authzen), key)
    Options.unknown_keys!(given, allowed_keys(protocol), key)

    base = base!(given, protocol, key)
    Map.merge(base, own!(protocol, given, base, key))
  end

  defp allowed_keys(:authzen), do: @authzen_keys
  defp allowed_keys(:template), do: @template_keys

  defp protocol!(:authzen, _key), do: :authzen
  defp protocol!(:template, _key), do: :template
  defp protocol!("authzen", _key), do: :authzen
  defp protocol!("template", _key), do: :template

  defp protocol!(other, key),
    do:
      raise(
        ArgumentError,
        "#{key}.protocol must be authzen or template, got #{inspect(other)}"
      )

  defp base!(given, protocol, key) do
    allow_insecure = boolean!(given, :allow_insecure_http, false, key)

    %{
      protocol: protocol,
      url: given |> Map.get(:url) |> Remote.url!(key, allow_insecure_http: allow_insecure),
      allow_insecure_http: allow_insecure,
      forward_authorization: boolean!(given, :forward_authorization, false, key),
      headers: Remote.headers!(Map.get(given, :headers), key),
      req_options: Remote.req_options!(Map.get(given, :req_options), key),
      remote: Remote.options!(given, key)
    }
  end

  defp boolean!(given, name, default, key),
    do: Options.boolean!(Map.get(given, name, default), "#{key}.#{name}")

  # ── the two protocols ───────────────────────────────────────────────────────

  defp own!(:authzen, given, _base, key) do
    %{
      evaluation_path:
        path!(Map.get(given, :evaluation_path, "/access/v1/evaluation"), "#{key}.evaluation_path"),
      evaluations_path:
        path!(
          Map.get(given, :evaluations_path, "/access/v1/evaluations"),
          "#{key}.evaluations_path"
        ),
      subject: json!(Map.get(given, :subject, @default_subject), "#{key}.subject"),
      resource: json!(Map.get(given, :resource, @default_resource), "#{key}.resource"),
      actions: actions!(Map.get(given, :actions), key),
      context: Options.map!(Map.get(given, :context, %{}), "#{key}.context"),
      batch: boolean!(given, :batch, true, key)
    }
  end

  defp own!(:template, given, base, key) do
    %{
      request: json!(Map.get(given, :request), "#{key}.request"),
      allow_path: required_path_segments!(Map.get(given, :allow_path), "#{key}.allow_path"),
      ttl_path: path_segments!(Map.get(given, :ttl_path), "#{key}.ttl_path"),
      batch: template_batch!(Map.get(given, :batch), base, key)
    }
  end

  # ── authzen's shapes ────────────────────────────────────────────────────────

  defp path!(value, key) do
    value = Options.string!(value, key)

    if String.starts_with?(value, "/"),
      do: value,
      else: raise(ArgumentError, "#{key} must start with \"/\", got #{inspect(value)}")
  end

  defp actions!(nil, _key), do: @default_actions

  defp actions!(given, key) when is_map(given) and not is_struct(given) do
    Map.new(given, fn {name, action} ->
      {action_name!(name, key), Options.string!(action, "#{key}.actions.#{name}")}
    end)
    |> then(&Map.merge(@default_actions, &1))
  end

  defp actions!(other, key),
    do:
      raise(
        ArgumentError,
        "#{key}.actions must be a map of action names to PDP action names, got #{inspect(other)}"
      )

  defp action_name!(name, key) when is_atom(name) and not is_nil(name) and not is_boolean(name),
    do: action_name!(Atom.to_string(name), key)

  defp action_name!(name, key) when is_binary(name) do
    case Enum.find(@actions, &(Atom.to_string(&1) == name)) do
      nil ->
        raise ArgumentError,
              "#{key}.actions has unknown action #{inspect(name)}; " <>
                "known: #{Enum.join(@action_names, ", ")}"

      action ->
        action
    end
  end

  defp action_name!(other, key),
    do: raise(ArgumentError, "#{key}.actions must be action names, got #{inspect(other)}")

  # ── templates inside JSON ───────────────────────────────────────────────────

  defp json!(value, key) do
    value = Options.map!(value, key)
    compile_json(value, key)
  end

  defp compile_json(map, path) when is_map(map) and not is_struct(map) do
    Map.new(map, fn {name, value} -> {json_key!(name, path), compile_json(value, path)} end)
  end

  defp compile_json(list, path) when is_list(list) do
    list
    |> Enum.with_index()
    |> Enum.map(fn {value, index} -> compile_json(value, "#{path}[#{index}]") end)
  end

  defp compile_json(value, path) when is_binary(value),
    do: Template.compile!(value, path, @roots)

  defp compile_json(value, _path) when is_number(value) or is_boolean(value) or is_nil(value),
    do: value

  defp compile_json(other, path),
    do: raise(ArgumentError, "#{path} must be a JSON value, got #{inspect(other)}")

  defp json_key!(name, _path) when is_binary(name), do: name
  defp json_key!(name, _path) when is_atom(name) and not is_nil(name), do: Atom.to_string(name)

  defp json_key!(name, path),
    do: raise(ArgumentError, "#{path} has a non-string JSON key #{inspect(name)}")

  # ── template protocol's paths ───────────────────────────────────────────────

  defp required_path_segments!(nil, key),
    do: raise(ArgumentError, "#{key} is required (the dotted path of the boolean answer)")

  defp required_path_segments!(value, key), do: path_segments!(value, key)

  defp path_segments!(nil, _key), do: nil

  defp path_segments!(path, key) do
    path = Options.string!(path, key)

    segments =
      path
      |> String.split(".")
      |> Enum.map(fn segment ->
        cond do
          Regex.match?(~r/\A\d+\z/, segment) -> String.to_integer(segment)
          Regex.match?(@path_segment, segment) -> segment
          true -> invalid_path!(path, key)
        end
      end)

    segments
  end

  defp invalid_path!(path, key) do
    raise ArgumentError,
          "#{key} must be dotted segments of [A-Za-z0-9_-] (an all-digit segment indexes " <>
            "an array), got #{inspect(path)}"
  end

  defp template_batch!(nil, _base, _key), do: nil

  defp template_batch!(given, base, key) when is_map(given) and not is_struct(given) do
    Options.unknown_keys!(given, [:url, :items_key, :results_path], "#{key}.batch")

    %{
      url:
        given
        |> Map.get(:url)
        |> Remote.url!("#{key}.batch",
          allow_insecure_http: base.allow_insecure_http
        ),
      items_key: Options.string!(Map.get(given, :items_key, "checks"), "#{key}.batch.items_key"),
      results_path:
        path_segments!(Map.get(given, :results_path, "results"), "#{key}.batch.results_path")
    }
  end

  defp template_batch!(other, _base, key),
    do:
      raise(
        ArgumentError,
        "#{key}.batch must be a bulk endpoint block (url, items_key, results_path) or null, " <>
          "got #{inspect(other)}"
      )

  # ── the behaviour ───────────────────────────────────────────────────────────

  @impl true
  @spec kind(map()) :: :remote
  def kind(_state), do: :remote

  @impl true
  @spec decide(Request.t(), map(), map()) :: Ankusa.Gateway.Authorizer.result()
  def decide(%Request{} = request, %{protocol: :authzen} = state, ctx),
    do: authzen_decide(request, state, ctx)

  def decide(%Request{} = request, %{protocol: :template} = state, ctx),
    do: template_decide(request, state, ctx)

  @impl true
  @spec decide_many([Request.t()], map(), map()) :: [Ankusa.Gateway.Authorizer.result()]
  def decide_many([], _state, _ctx), do: []

  def decide_many(requests, %{protocol: :authzen} = state, ctx) do
    if state.batch and not Remote.batch_unavailable?(ctx.instance, ctx.name),
      do: authzen_batch(requests, state, ctx),
      else: each(requests, state, ctx)
  end

  def decide_many(requests, %{protocol: :template, batch: nil} = state, ctx),
    do: each(requests, state, ctx)

  def decide_many(requests, %{protocol: :template} = state, ctx),
    do: template_batch(requests, state, ctx)

  @impl true
  @spec cache_scope(Request.t(), map()) :: term()
  def cache_scope(%Request{} = request, state) do
    base =
      case state.protocol do
        :authzen ->
          resource_scope(request, state)

        :template ->
          {request.resource.kind, request.resource.space, request.resource.tenant,
           request.resource.key}
      end

    forward_scope(base, request, state)
  end

  # ── authzen: one decision ───────────────────────────────────────────────────

  defp authzen_decide(request, state, ctx) do
    with {:ok, headers} <- forwarded(request, state),
         {:ok, evaluation} <- evaluation(request, state) do
      # A base URL, whether or not the operator wrote its trailing slash.
      url = String.trim_trailing(state.url, "/") <> state.evaluation_path
      body = JSON.encode!(evaluation)

      case post(state, url, headers, body, ctx.instance, request_id(request)) do
        {:ok, 200, response_headers, response_body} ->
          verdict(response_body, max_age(response_headers))

        {:ok, status, _headers, _body} ->
          {:error, {:unavailable, {:status, status}}}

        {:error, reason} ->
          {:error, {:unavailable, reason}}
      end
    else
      :error -> @unmappable
    end
  end

  defp verdict(body, ttl_ms) do
    case JSON.decode(body) do
      {:ok, %{"decision" => true}} -> {:ok, :allow, ttl_meta(ttl_ms)}
      {:ok, %{"decision" => false}} -> {:ok, :deny, %{}}
      _other -> @bad_response
    end
  end

  defp ttl_meta(nil), do: %{}
  defp ttl_meta(ttl_ms), do: %{ttl_ms: ttl_ms}

  defp max_age(headers) do
    case Map.get(headers, "cache-control") do
      nil ->
        nil

      values ->
        Enum.find_value(values, fn value ->
          case Regex.run(~r/max-age\s*=\s*(\d+)/i, value) do
            [_match, seconds] -> String.to_integer(seconds) * 1_000
            nil -> nil
          end
        end)
    end
  end

  # ── authzen: a batch ────────────────────────────────────────────────────────

  defp authzen_batch(requests, state, ctx) do
    prepared =
      requests
      |> Enum.map(fn request ->
        case evaluation(request, state) do
          {:ok, evaluation} -> {:ok, evaluation}
          :error -> :unmappable
        end
      end)

    sendable = for {:ok, _} = ok <- prepared, do: elem(ok, 1)

    if sendable == [] do
      List.duplicate(@unmappable, length(requests))
    else
      body =
        JSON.encode!(%{
          "evaluations" => sendable,
          "options" => %{"evaluations_semantic" => "execute_all"}
        })

      url = String.trim_trailing(state.url, "/") <> state.evaluations_path
      headers = forwarding_headers(state, requests)

      case post(state, url, headers, body, ctx.instance, request_id(hd(requests))) do
        {:ok, 200, _response_headers, response_body} ->
          case decode_evaluations(response_body, length(sendable)) do
            {:ok, decisions} -> merge_batch(prepared, decisions)
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

  defp decode_evaluations(body, count) do
    with {:ok, %{"evaluations" => decisions}} when is_list(decisions) <- JSON.decode(body),
         true <- length(decisions) == count do
      {:ok, Enum.map(decisions, &element_verdict/1)}
    else
      _other -> :bad
    end
  end

  defp element_verdict(%{"decision" => decision} = element) when is_boolean(decision) do
    if error_in_context?(element) do
      @bad_response
    else
      if decision, do: {:ok, :allow, %{}}, else: {:ok, :deny, %{}}
    end
  end

  defp element_verdict(_element), do: @bad_response

  defp error_in_context?(element) do
    case Map.get(element, "context") do
      %{"error" => error} when not is_nil(error) and error != false -> true
      _other -> false
    end
  end

  defp merge_batch(prepared, decisions) do
    {results, _remaining} =
      prepared
      |> Enum.reduce({[], decisions}, fn
        :unmappable, {acc, pending} -> {[@unmappable | acc], pending}
        {:ok, _evaluation}, {acc, [decision | pending]} -> {[decision | acc], pending}
      end)

    Enum.reverse(results)
  end

  # ── template: one decision ──────────────────────────────────────────────────

  defp template_decide(request, state, ctx) do
    with {:ok, headers} <- forwarded(request, state),
         {:ok, body} <- render_json(state.request, Template.bindings(request)) do
      case post(state, state.url, headers, JSON.encode!(body), ctx.instance, request_id(request)) do
        {:ok, 200, _response_headers, response_body} ->
          template_verdict(response_body, state)

        {:ok, status, _headers, _body} ->
          {:error, {:unavailable, {:status, status}}}

        {:error, reason} ->
          {:error, {:unavailable, reason}}
      end
    else
      :error -> @unmappable
    end
  end

  defp template_verdict(body, state) do
    case JSON.decode(body) do
      {:ok, map} when is_map(map) -> path_verdict(map, state.allow_path, state.ttl_path)
      _other -> @bad_response
    end
  end

  defp path_verdict(map, allow_path, ttl_path) do
    case dig(map, allow_path) do
      true -> {:ok, :allow, template_ttl(dig(map, ttl_path))}
      false -> {:ok, :deny, %{}}
      _other -> @bad_response
    end
  end

  defp template_ttl(seconds) when is_integer(seconds) and seconds >= 0,
    do: %{ttl_ms: seconds * 1_000}

  defp template_ttl(_other), do: %{}

  # ── template: a batch ───────────────────────────────────────────────────────

  defp template_batch(requests, %{batch: batch} = state, ctx) do
    prepared =
      Enum.map(requests, fn request ->
        case render_json(state.request, Template.bindings(request)) do
          {:ok, body} -> {:ok, body}
          :error -> :unmappable
        end
      end)

    sendable = for {:ok, _} = ok <- prepared, do: elem(ok, 1)

    if sendable == [] do
      List.duplicate(@unmappable, length(requests))
    else
      body = JSON.encode!(%{batch.items_key => sendable})
      headers = forwarding_headers(state, requests)

      case post(state, batch.url, headers, body, ctx.instance, request_id(hd(requests))) do
        {:ok, 200, _response_headers, response_body} ->
          case decode_results(response_body, batch.results_path, state, length(sendable)) do
            {:ok, decisions} -> merge_batch(prepared, decisions)
            :bad -> List.duplicate({:error, {:unavailable, :bad_response}}, length(requests))
          end

        {:ok, status, _headers, _body} ->
          List.duplicate({:error, {:unavailable, {:status, status}}}, length(requests))

        {:error, reason} ->
          List.duplicate({:error, {:unavailable, reason}}, length(requests))
      end
    end
  end

  defp decode_results(body, results_path, state, count) do
    with {:ok, map} when is_map(map) <- JSON.decode(body),
         results when is_list(results) <- dig(map, results_path),
         true <- length(results) == count do
      {:ok, Enum.map(results, &path_result(&1, state))}
    else
      _other -> :bad
    end
  end

  defp path_result(element, state) when is_map(element) do
    case dig(element, state.allow_path) do
      true -> {:ok, :allow, template_ttl(dig(element, state.ttl_path))}
      false -> {:ok, :deny, %{}}
      _other -> @bad_response
    end
  end

  defp path_result(_element, _state), do: @bad_response

  # ── JSON rendering ──────────────────────────────────────────────────────────

  # A property whose whole value is one of the principal's own lists renders as
  # a JSON array — and an empty list is the empty array, not a missing value
  # (the whole point of the default subject: a caller with no groups still has
  # `"groups": []`).
  defp render_json(%Template{parts: [{:var, :principal, [field], _name, nil}]}, bindings)
       when field in ["groups", "tenants", "scopes"] do
    case principal_list(field, Map.get(bindings, :principal)) do
      :missing -> :error
      values -> {:ok, Enum.flat_map(values, &scalar_text/1)}
    end
  end

  defp render_json(%Template{} = template, bindings) do
    case Template.render(template, bindings) do
      {:ok, text} ->
        {:ok, text}

      {:error, {:template_list, _variable}} ->
        case Template.render_many(template, bindings) do
          {:ok, texts} -> {:ok, texts}
          {:error, _reason} -> :error
        end

      {:error, _reason} ->
        :error
    end
  end

  defp render_json(map, bindings) when is_map(map) do
    Enum.reduce_while(map, {:ok, %{}}, fn {name, value}, {:ok, acc} ->
      case render_json(value, bindings) do
        {:ok, rendered} -> {:cont, {:ok, Map.put(acc, name, rendered)}}
        :error -> {:halt, :error}
      end
    end)
  end

  defp render_json(list, bindings) when is_list(list) do
    Enum.reduce_while(list, {:ok, []}, fn value, {:ok, acc} ->
      case render_json(value, bindings) do
        {:ok, rendered} -> {:cont, {:ok, [rendered | acc]}}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, rendered} -> {:ok, Enum.reverse(rendered)}
      :error -> :error
    end
  end

  defp render_json(value, _bindings) when is_number(value) or is_boolean(value) or is_nil(value),
    do: {:ok, value}

  defp render_json(_other, _bindings), do: :error

  defp principal_list("minimise", _principal), do: :missing

  defp principal_list("groups", %Principal{groups: groups}), do: groups
  defp principal_list("scopes", %Principal{scopes: scopes}), do: scopes
  defp principal_list("tenants", %Principal{tenants: :all}), do: ["*"]
  defp principal_list("tenants", %Principal{tenants: tenants}), do: tenants
  defp principal_list(_field, _principal), do: :missing

  defp scalar_text(value) do
    case Template.scalar(value) do
      {:ok, text} -> [text]
      :error -> []
    end
  end

  # ── the evaluation an authzen request sends ─────────────────────────────────

  defp evaluation(request, state) do
    bindings = Template.bindings(request)

    with {:ok, subject} <- render_json(state.subject, bindings),
         {:ok, resource} <- render_json(state.resource, bindings),
         {:ok, action} <- action_name(state.actions, request.action) do
      {:ok,
       %{
         "subject" => subject,
         "resource" => resource,
         "action" => %{"name" => action},
         "context" => state.context
       }}
    else
      _error -> :error
    end
  end

  defp action_name(actions, action) do
    case Map.fetch(actions, action) do
      {:ok, name} -> {:ok, name}
      :error -> :error
    end
  end

  # The decision cache's scope for an authzen request: the rendered resource
  # (type and id), so keys that render alike share one decision.
  defp resource_scope(request, state) do
    bindings = Template.bindings(request)

    with {:ok, resource} <- render_json(state.resource, bindings),
         %{"type" => type, "id" => id} <- resource do
      {type, id}
    else
      _other -> :unmappable
    end
  end

  # ── forwarding the caller's credential ──────────────────────────────────────

  defp forwarded(request, %{forward_authorization: true}) do
    case authorization(request) do
      nil -> :error
      value -> {:ok, [{"authorization", value}]}
    end
  end

  defp forwarded(_request, _state), do: {:ok, []}

  # A batch forwards the credential of its first request: the engine batches
  # only decisions one subject's cache key covers, and with forwarding the key
  # carries the credential's hash.
  defp forwarding_headers(%{forward_authorization: true} = state, [request | _rest]) do
    case forwarded(request, state) do
      {:ok, headers} -> headers
      :error -> []
    end
  end

  defp forwarding_headers(_state, _requests), do: []

  defp forward_scope(base, request, %{forward_authorization: true}) do
    case authorization(request) do
      nil -> {base, :none}
      value -> {base, :crypto.hash(:sha256, value)}
    end
  end

  defp forward_scope(base, _request, _state), do: base

  defp authorization(%Request{authorization: value}) when is_binary(value), do: value
  defp authorization(_request), do: nil

  # ── the request itself ──────────────────────────────────────────────────────

  defp post(state, url, extra_headers, body, instance, request_id) do
    headers =
      [{"content-type", "application/json"}, {"x-request-id", request_id}] ++
        extra_headers ++ Map.to_list(state.headers)

    HttpClient.request_with_headers(
      :post,
      url,
      headers,
      body,
      state.remote.timeout_ms,
      [instance: instance, max_response_bytes: @max_response_bytes] ++ state.req_options
    )
  end

  defp request_id(%Request{request_id: id}) when is_binary(id), do: id
  defp request_id(_request), do: fresh_request_id()

  defp fresh_request_id do
    Ankusa.ULID.encode(
      <<System.system_time(:millisecond)::48, :crypto.strong_rand_bytes(10)::binary>>
    )
  end

  defp each(requests, state, ctx) do
    requests
    |> Task.async_stream(&decide(&1, state, ctx),
      max_concurrency: @single_concurrency,
      ordered: true,
      timeout: :infinity
    )
    |> Enum.map(fn {:ok, result} -> result end)
  end

  # A dotted path's value in decoded JSON: an all-digit segment indexes an
  # array.
  defp dig(value, []), do: value
  defp dig(map, [segment | rest]) when is_map(map), do: dig(Map.get(map, segment), rest)

  defp dig(list, [index | rest]) when is_list(list) and is_integer(index),
    do: dig(Enum.at(list, index), rest)

  defp dig(_value, _path), do: nil
end
