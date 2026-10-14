defmodule Ankusa.Scim.Patch do
  @moduledoc """
  SCIM PATCH (RFC 7644 section 3.5.2): applies a `PatchOp` request body to a
  resource.

  `apply/3` returns the patched resource and the changes to the group's
  membership, which are *not* applied to the resource: the router hands them to
  `Ankusa.Directory.add_members/3` and friends, which are atomic, so two
  provisioning runs adding members at once cannot lose each other's.

  ## Paths

    * none: `value` is an object and each of its keys is a path
    * `attr`, `attr.sub` (`name.givenName`)
    * `<extension urn>:attr`, and the core URN prefixed forms
    * `attr[sub eq "v"]` and `attr[sub eq "v"].sub2` (`emails[type eq "work"].value`)
    * on groups, `members` and `members[value eq "id"]`

  ## Operations

    * `add` on a list appends, skipping a duplicate (by `"value"` for objects);
      on an object merges; otherwise sets. With a filter and no matching element
      it appends a new one.
    * `replace` sets. With a filter, replaces the matching elements.
    * `remove` deletes the attribute, or with a filter the matching elements.
      Without a path it is `noTarget`. On `members`, a `value` list removes
      those ids (Microsoft Entra's form); with neither value nor filter it
      removes every member.

  Operation names and attribute names are case-insensitive. An attribute Ankusa
  does not keep is ignored, as it is in a POST body (`Ankusa.Scim.Resource`);
  `id` and `meta` are `mutability` errors. `active` accepts `"False"` and
  `"True"`.

  Errors are `{:error, scim_type, detail}` for `Ankusa.Scim.Errors.send/4`:
  `invalidSyntax` (the envelope), `invalidPath`, `noTarget`, `invalidValue`,
  `mutability`.
  """

  alias Ankusa.Scim.{Filter, Resource}

  @patch_schema "urn:ietf:params:scim:api:messages:2.0:PatchOp"
  @enterprise Resource.enterprise_schema()
  @core_prefixes [
    "urn:ietf:params:scim:schemas:core:2.0:User:",
    "urn:ietf:params:scim:schemas:core:2.0:Group:"
  ]

  @canonical %{
    user: Map.new(Resource.user_keys(), &{String.downcase(&1), &1}),
    group: Map.new(Resource.group_keys(), &{String.downcase(&1), &1})
  }

  @path ~r/\A([A-Za-z][A-Za-z0-9_-]*)(?:\[((?:[^\]"]|"(?:[^"\\]|\\.)*")*)\])?(?:\.([A-Za-z][A-Za-z0-9_-]*))?\z/
  @value_filter ~r/\A\s*([A-Za-z][A-Za-z0-9]*)\s+[Ee][Qq]\s+"((?:[^"\\]|\\.)*)"\s*\z/s

  @type member_change ::
          {:add, [String.t()]} | {:remove, [String.t()]} | {:replace, [String.t()]}
  @type error :: {:error, String.t(), String.t()}

  @doc """
  Apply `body` to `resource` (a `:user` or a `:group`). The membership changes
  come back in the order the operations asked for them.
  """
  @spec apply(:user | :group, map(), term()) :: {:ok, map(), [member_change()]} | error()
  def apply(type, resource, body) when type in [:user, :group] and is_map(resource) do
    with {:ok, ops} <- operations(body),
         {:ok, actions} <- expand(type, ops),
         {:ok, patched, changes} <- run(resource, actions),
         {:ok, patched} <- Resource.check(patched, type) do
      {:ok, patched, changes}
    end
  end

  # ── the envelope ───────────────────────────────────────────────────────────

  defp operations(%{"schemas" => schemas, "Operations" => [_ | _] = ops}) when is_list(schemas) do
    if @patch_schema in schemas,
      do: {:ok, ops},
      else: {:error, "invalidSyntax", "schemas must include #{@patch_schema}"}
  end

  defp operations(_body) do
    {:error, "invalidSyntax",
     "a PatchOp needs schemas (#{@patch_schema}) and a non-empty Operations list"}
  end

  # ── operations to actions: {verb, path, value | :none} ─────────────────────

  defp expand(type, ops) do
    Enum.reduce_while(ops, {:ok, []}, fn op, {:ok, acc} ->
      case expand_op(type, op) do
        {:ok, actions} -> {:cont, {:ok, acc ++ actions}}
        {:error, _, _} = error -> {:halt, error}
      end
    end)
  end

  defp expand_op(type, %{"op" => verb} = op) when is_binary(verb) do
    with {:ok, verb} <- verb(verb) do
      value = Map.get(op, "value", :none)

      case Map.get(op, "path") do
        path when path in [nil, ""] -> pathless(type, verb, value)
        path when is_binary(path) -> single(type, verb, path, value)
        _ -> {:error, "invalidPath", "path must be a string"}
      end
    end
  end

  defp expand_op(_type, _op),
    do: {:error, "invalidSyntax", "every operation needs an \"op\" of add, replace or remove"}

  defp verb(verb) do
    case String.downcase(verb) do
      "add" -> {:ok, :add}
      "replace" -> {:ok, :replace}
      "remove" -> {:ok, :remove}
      _ -> {:error, "invalidSyntax", "unknown op #{inspect(verb)}: use add, replace or remove"}
    end
  end

  defp pathless(_type, :remove, _value),
    do: {:error, "noTarget", "remove needs a path: nothing says what to remove"}

  defp pathless(type, verb, %{} = value) do
    Enum.reduce_while(value, {:ok, []}, fn {key, val}, {:ok, acc} ->
      case single(type, verb, key, val) do
        {:ok, actions} -> {:cont, {:ok, acc ++ actions}}
        {:error, _, _} = error -> {:halt, error}
      end
    end)
  end

  defp pathless(_type, _verb, _value),
    do: {:error, "invalidValue", "an operation without a path needs an object as its value"}

  defp single(type, verb, path, value) do
    case parse_path(type, path) do
      {:ok, parsed} -> {:ok, [{verb, parsed, value}]}
      :ignore -> {:ok, []}
      {:error, _, _} = error -> error
    end
  end

  # ── paths ──────────────────────────────────────────────────────────────────

  # `{:ok, %{keys:, attr:, filter:, sub:}}` for an attribute Ankusa keeps,
  # `:ignore` for one it does not.
  defp parse_path(type, path) do
    {prefix, rest} = split_schema(path)

    cond do
      prefix == [@enterprise] and rest == "" ->
        {:ok, %{keys: [@enterprise], attr: @enterprise, filter: nil, sub: nil}}

      true ->
        with {:ok, name, filter, sub} <- split_path(rest) do
          attribute(type, prefix, name, filter, sub)
        end
    end
  end

  defp split_schema(@enterprise), do: {[@enterprise], ""}
  defp split_schema(@enterprise <> ":" <> rest), do: {[@enterprise], rest}

  defp split_schema(path) do
    case Enum.find(@core_prefixes, &String.starts_with?(path, &1)) do
      nil -> {[], path}
      prefix -> {[], binary_part(path, byte_size(prefix), byte_size(path) - byte_size(prefix))}
    end
  end

  defp split_path(rest) do
    case Regex.run(@path, rest) do
      [_all, name] -> {:ok, name, nil, nil}
      [_all, name, filter] -> filter(name, filter, nil)
      [_all, name, "", sub] -> {:ok, name, nil, sub}
      [_all, name, filter, sub] -> filter(name, filter, sub)
      nil -> {:error, "invalidPath", "cannot parse the path #{inspect(rest)}"}
    end
  end

  defp filter(name, filter, sub) do
    case Regex.run(@value_filter, filter) do
      [_all, fsub, quoted] ->
        case Filter.unescape(quoted) do
          {:ok, value} -> {:ok, name, {fsub, value}, sub}
          {:error, detail} -> {:error, "invalidPath", detail}
        end

      nil ->
        {:error, "invalidPath", ~s(only [attribute eq "value"] filters are supported in a path)}
    end
  end

  defp attribute(_type, [@enterprise], name, filter, sub),
    do: {:ok, %{keys: [@enterprise, name], attr: name, filter: filter, sub: sub}}

  defp attribute(type, [], name, filter, sub) do
    lower = String.downcase(name)

    cond do
      lower in ["id", "meta"] ->
        {:error, "mutability", "#{lower} is read-only"}

      lower == "members" and type == :group ->
        {:ok, %{keys: ["members"], attr: "members", filter: filter, sub: sub}}

      lower == "schemas" ->
        :ignore

      canonical = Map.get(Map.fetch!(@canonical, type), lower) ->
        {:ok, %{keys: [canonical], attr: canonical, filter: filter, sub: sub}}

      true ->
        :ignore
    end
  end

  # ── running the actions ────────────────────────────────────────────────────

  defp run(resource, actions) do
    Enum.reduce_while(actions, {:ok, resource, []}, fn action, {:ok, res, changes} ->
      case act(action, res) do
        {:ok, res, nil} -> {:cont, {:ok, res, changes}}
        {:ok, res, change} -> {:cont, {:ok, res, changes ++ [change]}}
        {:error, _, _} = error -> {:halt, error}
      end
    end)
  end

  defp act({verb, %{attr: "members"} = path, value}, res) do
    with {:ok, change} <- member_change(verb, path, value), do: {:ok, res, change}
  end

  defp act({verb, %{keys: ["active"], filter: nil, sub: nil}, value}, res) when verb != :remove do
    case Resource.coerce_active(value) do
      {:ok, active} -> {:ok, Map.put(res, "active", active), nil}
      :error -> {:error, "invalidValue", "active must be true or false"}
    end
  end

  defp act({verb, %{filter: nil, sub: nil} = path, value}, res) do
    with {:ok, value} <- value(verb, value) do
      {:ok, update(res, path.keys, &whole(verb, &1, value)), nil}
    end
  end

  defp act({verb, %{filter: nil, sub: sub} = path, value}, res) do
    with {:ok, value} <- value(verb, value) do
      {:ok, update(res, path.keys, &sub_of(verb, &1, sub, value)), nil}
    end
  end

  defp act({verb, %{filter: filter} = path, value}, res) do
    with {:ok, value} <- value(verb, value) do
      {:ok, update(res, path.keys, &filtered(verb, &1, filter, path.sub, value)), nil}
    end
  end

  defp value(:remove, _value), do: {:ok, :none}
  defp value(_verb, :none), do: {:error, "invalidValue", "add and replace need a value"}
  defp value(_verb, value), do: {:ok, value}

  # `fun` maps the current value (`:none` when absent) to the new one, or
  # `:delete`. Nested keys build the maps on the way down and prune the ones
  # left empty.
  defp update(map, [key], fun) do
    case fun.(Map.get(map, key, :none)) do
      :delete -> Map.delete(map, key)
      value -> Map.put(map, key, value)
    end
  end

  defp update(map, [key | rest], fun) do
    child =
      case Map.get(map, key) do
        %{} = child -> child
        _ -> %{}
      end

    case update(child, rest, fun) do
      empty when empty == %{} -> Map.delete(map, key)
      updated -> Map.put(map, key, updated)
    end
  end

  # attr
  defp whole(:remove, _old, _value), do: :delete
  defp whole(:replace, _old, value), do: value
  defp whole(:add, :none, value), do: value
  defp whole(:add, old, value) when is_list(old) and is_list(value), do: append_all(old, value)
  defp whole(:add, old, value) when is_list(old), do: append_all(old, [value])
  defp whole(:add, %{} = old, %{} = value), do: Map.merge(old, value)
  defp whole(:add, _old, value), do: value

  # attr.sub: a list's elements each get the sub-attribute
  defp sub_of(verb, old, sub, value) when is_list(old) do
    old
    |> Enum.map(fn
      %{} = element -> set_sub(verb, element, sub, value)
      other -> other
    end)
    |> then(&if(&1 == [], do: :delete, else: &1))
  end

  defp sub_of(verb, old, sub, value) do
    element = if is_map(old), do: old, else: %{}

    case set_sub(verb, element, sub, value) do
      empty when empty == %{} -> :delete
      updated -> updated
    end
  end

  defp set_sub(:remove, element, sub, _value), do: delete_ci(element, sub)
  defp set_sub(_verb, element, sub, value), do: Map.put(delete_ci(element, sub), sub, value)

  # attr[filter] and attr[filter].sub
  defp filtered(verb, old, {fsub, fvalue}, sub, value) do
    elements = if is_list(old), do: old, else: []
    matched? = &match_element?(&1, fsub, fvalue)

    result =
      cond do
        verb == :remove and sub == nil ->
          Enum.reject(elements, matched?)

        verb == :remove ->
          Enum.map(elements, &if(matched?.(&1), do: delete_ci(&1, sub), else: &1))

        not Enum.any?(elements, matched?) ->
          elements ++ [new_element(fsub, fvalue, sub, value)]

        sub == nil ->
          Enum.map(elements, &if(matched?.(&1), do: value, else: &1))

        true ->
          Enum.map(
            elements,
            &if(matched?.(&1), do: Map.put(delete_ci(&1, sub), sub, value), else: &1)
          )
      end

    if result == [], do: :delete, else: result
  end

  defp new_element(fsub, fvalue, nil, %{} = value), do: Map.put_new(value, fsub, fvalue)
  defp new_element(fsub, fvalue, nil, value), do: %{fsub => fvalue, "value" => value}
  defp new_element(fsub, fvalue, sub, value), do: %{fsub => fvalue, sub => value}

  defp match_element?(%{} = element, fsub, fvalue) do
    case get_ci(element, fsub) do
      actual when is_binary(actual) -> String.downcase(actual) == String.downcase(fvalue)
      actual when is_boolean(actual) or is_number(actual) -> to_string(actual) == fvalue
      _ -> false
    end
  end

  defp match_element?(_element, _fsub, _fvalue), do: false

  defp get_ci(map, key) do
    Enum.find_value(map, fn {k, v} ->
      if String.downcase(to_string(k)) == String.downcase(key), do: v
    end)
  end

  defp delete_ci(map, key) do
    Map.reject(map, fn {k, _v} -> String.downcase(to_string(k)) == String.downcase(key) end)
  end

  # Multi-valued attributes dedupe by "value" (emails, phone numbers) when the
  # elements are objects that have one, and by equality otherwise.
  defp append_all(old, new) do
    Enum.reduce(new, old, fn element, acc ->
      if Enum.any?(acc, &same_element?(&1, element)), do: acc, else: acc ++ [element]
    end)
  end

  defp same_element?(%{"value" => a}, %{"value" => b}), do: a == b
  defp same_element?(a, b), do: a == b

  # ── group membership ───────────────────────────────────────────────────────

  defp member_change(_verb, %{sub: sub}, _value) when sub != nil,
    do:
      {:error, "invalidPath",
       "members has no sub-attribute to address; use members[value eq \"id\"]"}

  defp member_change(:remove, %{filter: {fsub, id}}, _value) do
    if String.downcase(fsub) == "value",
      do: {:ok, {:remove, [id]}},
      else:
        {:error, "invalidPath", ~s(members can only be filtered by value: members[value eq "id"])}
  end

  defp member_change(_verb, %{filter: {_fsub, _id}}, _value),
    do: {:error, "invalidPath", "a members filter can only be used with remove"}

  defp member_change(:remove, _path, :none), do: {:ok, {:replace, []}}

  defp member_change(verb, _path, value) do
    with {:ok, ids} <- member_ids(value) do
      case verb do
        :add -> {:ok, {:add, ids}}
        :replace -> {:ok, {:replace, ids}}
        :remove -> {:ok, {:remove, ids}}
      end
    end
  end

  defp member_ids(%{"value" => _} = member), do: member_ids([member])

  defp member_ids(list) when is_list(list) do
    case Resource.members(list) do
      {:ok, ids} -> {:ok, ids}
      {:error, _type, detail} -> {:error, "invalidValue", detail}
    end
  end

  defp member_ids(_value),
    do: {:error, "invalidValue", ~s(members must be a list of {"value": "<user id>"})}
end
