defmodule Ankusa.Gateway.Authz.Rules do
  @moduledoc """
  The `rules` authorizer: permissioning that is nothing but the token. Each
  rule says, in order, which requests it speaks about and what their answer
  is; the first rule that matches decides.

      authz: %{
        authorizers: %{
          "team" => {Ankusa.Gateway.Authz.Rules,
                     %{rules: [
                       %{effect: :allow, actions: [:read], spaces: ["docs"],
                         keys: ["teams/{team}/**"], when: %{groups_any: ["team-{{ vars.team }}"]}}
                     ]}}
        }
      }

  ## Options

  `rules` is 1 to 256 entries; every field is validated against
  `<key>.rules[<i>]`:

  | Field | Default | Meaning |
  | --- | --- | --- |
  | `effect` | required | `:allow \| :deny` (or the strings `"allow"`, `"deny"`) |
  | `actions` | required | non-empty list of `read write delete list share`, or `["*"]` |
  | `spaces` | `["*"]` | space names; `"claims"` is the claims API, whose keys are `<tenant>/<claim_id>` |
  | `keys` | `["**"]` | `Ankusa.Gateway.Pattern` patterns; the first matching one's captures are the rule's `vars` |
  | `when` | `%{}` | the predicates below; every present one has to hold |

  `when` takes `groups_any`, `groups_all`, `scopes_any`, `scopes_all`,
  `tenants_any` (non-empty lists of `Ankusa.Gateway.Template` templates, roots
  `vars`, `space`, `tenant` and `action`), `principals` (exact principal ids)
  and `claims` (`%{"dotted.claim" => template | [templates]}`; a claim holds
  when it equals — or, as a list, contains — one of the rendered values). `*`
  as an action or a space means every one; a rule action list that names `*`
  names nothing else.

  `groups_*` compare with `principal.groups`; `scopes_*` with the principal's
  scopes plus the strings of its `claims["scope"]` and `claims["scp"]`
  (space-delimited or a list); `tenants_any` with `principal.tenants` (`:all`
  matches every template); `principals` with `principal.id`.

  A template that cannot render makes its predicate false, so a rule never
  matches on a value the caller did not have. No rule matching is
  `{:ok, :abstain, %{}}`; the policy's other steps then decide.

  Everything is compiled in `init!/2` — patterns, templates, actions — so a
  decision parses nothing. `kind/1` is `:inline`: a rule is a few comparisons,
  cheaper than a cache lookup.
  """

  @behaviour Ankusa.Gateway.Authorizer

  alias Ankusa.Gateway.Authz.Request
  alias Ankusa.Gateway.{Options, Pattern, Template}

  @actions %{
    "read" => :read,
    "write" => :write,
    "delete" => :delete,
    "list" => :list,
    "share" => :share
  }
  @action_names Map.keys(@actions)
  @space ~r/\A[a-z0-9][a-z0-9_-]{0,62}\z/
  @claim_path ~r/\A[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*\z/
  @template_roots ["vars", "space", "tenant", "action"]
  @rule_keys [:effect, :actions, :spaces, :keys, :when]
  @predicate_keys [
    :groups_any,
    :groups_all,
    :scopes_any,
    :scopes_all,
    :tenants_any,
    :principals,
    :claims
  ]
  @max_rules 256

  @impl true
  @spec init!(map(), String.t()) :: map()
  def init!(opts, key) do
    given = Options.map!(opts, key)
    Options.unknown_keys!(given, [:rules], key)

    %{rules: rules!(Map.get(given, :rules), key)}
  end

  @impl true
  @spec kind(map()) :: :inline
  def kind(_state), do: :inline

  @impl true
  @spec decide(Request.t(), map(), map()) :: {:ok, :allow | :deny | :abstain, map()}
  def decide(%Request{} = request, %{rules: rules}, _ctx) do
    case Enum.find_value(rules, &match_rule(&1, request)) do
      nil -> {:ok, :abstain, %{}}
      effect -> {:ok, effect, %{}}
    end
  end

  # ── rules ───────────────────────────────────────────────────────────────────

  defp rules!(rules, key) when is_list(rules) and rules != [] and length(rules) <= @max_rules do
    rules
    |> Enum.with_index()
    |> Enum.map(fn {rule, index} -> rule!(rule, "#{key}.rules[#{index}]") end)
  end

  defp rules!(other, key) do
    raise ArgumentError,
          "#{key}.rules must be a list of 1 to #{@max_rules} rules, got #{inspect(other)}"
  end

  defp rule!(rule, key) do
    given = Options.map!(rule, key)
    Options.unknown_keys!(given, @rule_keys, key)

    %{
      effect: effect!(Map.get(given, :effect), key),
      actions: actions!(Map.get(given, :actions), key),
      spaces: spaces!(Map.get(given, :spaces, ["*"]), key),
      keys: patterns!(Map.get(given, :keys, ["**"]), key),
      predicates: predicates!(Map.get(given, :when, %{}), key)
    }
  end

  defp effect!(:allow, _key), do: :allow
  defp effect!(:deny, _key), do: :deny
  defp effect!("allow", _key), do: :allow
  defp effect!("deny", _key), do: :deny

  defp effect!(other, key),
    do: raise(ArgumentError, "#{key}.effect must be allow or deny, got #{inspect(other)}")

  # ── actions, spaces, patterns ───────────────────────────────────────────────

  defp actions!(actions, key) when is_list(actions) and actions != [] do
    names = Enum.map(actions, &action!(&1, key))

    cond do
      names == [:*] ->
        :all

      :* in names ->
        raise ArgumentError,
              "#{key}.actions: \"*\" cannot be combined with other actions, got #{inspect(actions)}"

      true ->
        Enum.uniq(names)
    end
  end

  defp actions!(other, key) do
    raise ArgumentError,
          "#{key}.actions must be a non-empty list of #{Enum.join(@action_names, ", ")} or [\"*\"], " <>
            "got #{inspect(other)}"
  end

  defp action!("*", _key), do: :*

  defp action!(value, key) when is_atom(value) and not is_nil(value) and not is_boolean(value),
    do: action!(Atom.to_string(value), key)

  defp action!(value, key) when is_binary(value) do
    case Map.fetch(@actions, value) do
      {:ok, action} ->
        action

      :error ->
        raise ArgumentError,
              "#{key}.actions has unknown action #{inspect(value)}; " <>
                "known: #{Enum.join(@action_names, ", ")}, or \"*\""
    end
  end

  defp action!(other, key),
    do: raise(ArgumentError, "#{key}.actions must be action names, got #{inspect(other)}")

  defp spaces!(spaces, key) when is_list(spaces) and spaces != [] do
    names = Enum.map(spaces, &space!(&1, key))

    cond do
      names == ["*"] ->
        :all

      "*" in names ->
        raise ArgumentError,
              "#{key}.spaces: \"*\" cannot be combined with other spaces, got #{inspect(spaces)}"

      true ->
        Enum.uniq(names)
    end
  end

  defp spaces!(other, key) do
    raise ArgumentError,
          "#{key}.spaces must be a non-empty list of space names or [\"*\"], got #{inspect(other)}"
  end

  defp space!(name, key) when is_atom(name) and not is_nil(name) and not is_boolean(name),
    do: space!(Atom.to_string(name), key)

  defp space!("*", _key), do: "*"

  defp space!(name, key) when is_binary(name) do
    if Regex.match?(@space, name),
      do: name,
      else: raise(ArgumentError, "#{key}.spaces has invalid space name #{inspect(name)}")
  end

  defp space!(other, key),
    do: raise(ArgumentError, "#{key}.spaces must be space names, got #{inspect(other)}")

  defp patterns!(patterns, key) when is_list(patterns) and patterns != [] do
    patterns
    |> Enum.with_index()
    |> Enum.map(fn {pattern, index} -> Pattern.compile!(pattern, "#{key}.keys[#{index}]") end)
  end

  defp patterns!(other, key) do
    raise ArgumentError,
          "#{key}.keys must be a non-empty list of patterns, got #{inspect(other)}"
  end

  # ── predicates ──────────────────────────────────────────────────────────────

  defp predicates!(given, key) do
    key = key <> ".when"
    given = Options.map!(given, key)
    Options.unknown_keys!(given, @predicate_keys, key)

    %{
      groups_any: templates!(Map.get(given, :groups_any), key, :groups_any),
      groups_all: templates!(Map.get(given, :groups_all), key, :groups_all),
      scopes_any: templates!(Map.get(given, :scopes_any), key, :scopes_any),
      scopes_all: templates!(Map.get(given, :scopes_all), key, :scopes_all),
      tenants_any: templates!(Map.get(given, :tenants_any), key, :tenants_any),
      principals: principals!(Map.get(given, :principals), key),
      claims: claims!(Map.get(given, :claims), key)
    }
  end

  defp templates!(nil, _key, _field), do: nil

  defp templates!(templates, key, field) when is_list(templates) and templates != [] do
    templates
    |> Enum.with_index()
    |> Enum.map(fn {template, index} ->
      Template.compile!(template, "#{key}.#{field}[#{index}]", @template_roots)
    end)
  end

  defp templates!(other, key, field) do
    raise ArgumentError,
          "#{key}.#{field} must be a non-empty list of templates, got #{inspect(other)}"
  end

  defp principals!(nil, _key), do: nil

  defp principals!(ids, key) when is_list(ids) and ids != [] do
    Options.string_list!(ids, "#{key}.principals")
  end

  defp principals!(other, key) do
    raise ArgumentError,
          "#{key}.principals must be a non-empty list of principal ids, got #{inspect(other)}"
  end

  defp claims!(nil, _key), do: nil

  defp claims!(claims, key)
       when is_map(claims) and map_size(claims) > 0 and not is_struct(claims) do
    Enum.map(claims, fn {path, spec} ->
      path = claim_path!(path, key)
      {path, templates!(spec, key, "claims.#{path}")}
    end)
  end

  defp claims!(other, key) do
    raise ArgumentError,
          "#{key}.claims must be a map of dotted claim paths to templates, got #{inspect(other)}"
  end

  defp claim_path!(path, key) when is_atom(path) and not is_nil(path) and not is_boolean(path),
    do: claim_path!(Atom.to_string(path), key)

  defp claim_path!(path, key) when is_binary(path) do
    if Regex.match?(@claim_path, path),
      do: path,
      else: raise(ArgumentError, "#{key}.claims has invalid claim path #{inspect(path)}")
  end

  defp claim_path!(other, key),
    do: raise(ArgumentError, "#{key}.claims must name claim paths, got #{inspect(other)}")

  # ── matching ────────────────────────────────────────────────────────────────

  defp match_rule(rule, request) do
    if action?(rule.actions, request.action) and space?(rule.spaces, request.resource.space) do
      case key(rule.keys, request) do
        {:ok, vars} -> if predicates?(rule.predicates, request, vars), do: rule.effect
        :nomatch -> nil
      end
    end
  end

  defp action?(:all, _action), do: true
  defp action?(actions, action), do: action in actions

  defp space?(:all, _space), do: true
  defp space?(spaces, space), do: space in spaces

  defp key(patterns, %{resource: %{kind: kind, key: key}}) do
    Enum.find_value(patterns, :nomatch, fn pattern ->
      case Pattern.match(pattern, kind, key) do
        {:ok, captures} -> {:ok, captures}
        :nomatch -> nil
      end
    end)
  end

  defp predicates?(predicates, request, vars) do
    if blank?(predicates) do
      true
    else
      principal = request.principal
      bindings = Template.bindings(request, vars)
      scopes = if predicates.scopes_any || predicates.scopes_all, do: scopes(principal), else: []

      groups_any?(predicates.groups_any, principal.groups, bindings) and
        groups_all?(predicates.groups_all, principal.groups, bindings) and
        any_rendered?(predicates.scopes_any, scopes, bindings) and
        all_rendered?(predicates.scopes_all, scopes, bindings) and
        tenants_any?(predicates.tenants_any, principal.tenants, bindings) and
        principals?(predicates.principals, principal.id) and
        claims?(predicates.claims, principal.claims, bindings)
    end
  end

  defp blank?(predicates), do: Enum.all?(Map.values(predicates), &is_nil/1)

  defp groups_any?(nil, _groups, _bindings), do: true
  defp groups_any?(templates, groups, bindings), do: any_rendered?(templates, groups, bindings)

  defp groups_all?(nil, _groups, _bindings), do: true
  defp groups_all?(templates, groups, bindings), do: all_rendered?(templates, groups, bindings)

  defp any_rendered?(nil, _values, _bindings), do: true

  defp any_rendered?(templates, values, bindings) do
    case render_all(templates, bindings) do
      {:ok, rendered} -> Enum.any?(rendered, &(&1 in values))
      :error -> false
    end
  end

  defp all_rendered?(nil, _values, _bindings), do: true

  defp all_rendered?(templates, values, bindings) do
    case render_all(templates, bindings) do
      {:ok, rendered} -> Enum.all?(rendered, &(&1 in values))
      :error -> false
    end
  end

  defp tenants_any?(nil, _tenants, _bindings), do: true
  defp tenants_any?(_templates, :all, _bindings), do: true
  defp tenants_any?(templates, tenants, bindings), do: any_rendered?(templates, tenants, bindings)

  defp principals?(nil, _id), do: true
  defp principals?(ids, id), do: id in ids

  defp claims?(nil, _claims, _bindings), do: true

  defp claims?(entries, claims, bindings) do
    Enum.all?(entries, fn {path, templates} -> claim_holds?(path, templates, claims, bindings) end)
  end

  defp claim_holds?(path, templates, claims, bindings) do
    with {:ok, rendered} <- render_all(templates, bindings),
         {:ok, value} <- Template.claim(claims, path) do
      matches_claim?(value, rendered)
    else
      _missing -> false
    end
  end

  defp matches_claim?(value, rendered) when is_list(value) do
    Enum.any?(value, fn element ->
      case Template.scalar(element) do
        {:ok, text} -> text in rendered
        :error -> false
      end
    end)
  end

  defp matches_claim?(value, rendered) do
    case Template.scalar(value) do
      {:ok, text} -> text in rendered
      :error -> false
    end
  end

  defp render_all(templates, bindings) do
    Enum.reduce_while(templates, {:ok, []}, fn template, {:ok, acc} ->
      case Template.render(template, bindings) do
        {:ok, text} -> {:cont, {:ok, [text | acc]}}
        {:error, _reason} -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, rendered} -> {:ok, Enum.reverse(rendered)}
      :error -> :error
    end
  end

  # The principal's scopes plus the token's own `scope` and `scp` claims, which
  # is where every OAuth and OIDC issuer puts them.
  defp scopes(principal) do
    Enum.uniq(principal.scopes ++ claim_scopes(principal.claims))
  end

  defp claim_scopes(claims) do
    Enum.flat_map(["scope", "scp"], fn name ->
      case Template.claim(claims, name) do
        {:ok, value} when is_binary(value) -> String.split(value, " ", trim: true)
        {:ok, values} when is_list(values) -> Enum.filter(values, &is_binary/1)
        _other -> []
      end
    end)
  end
end
