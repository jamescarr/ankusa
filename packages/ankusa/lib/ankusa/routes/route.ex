defmodule Ankusa.Routes.Route do
  @moduledoc """
  One route definition: which requests the edge captures when route management
  is on.

  A route is a **capture gate**, not an identity. It decides method and path
  matching and, optionally, which client addresses may use it;
  `Ankusa.RouteResolver` still maps the accepted request to a `source_id` and
  `tenant_id`. That is why there is no `source_id` field here.

  ## Path patterns

  Segments are `/`-separated and one of three things:

    * a **literal** (`hooks`, `stripe.com`, `2024-01-01`) — matched with `==`;
    * a **param** (`:tenant`, `:source_id`) — exactly one segment, any value;
    * the **wildcard** `*`, legal only as the last segment — one *or more*
      remaining segments, so `/hooks/shopify/*` matches `/hooks/shopify/a/b` and
      not `/hooks/shopify`.

  No regex: it invites ReDoS in a component that runs before any authentication,
  and neither the `:tenant` nor the `*` form needs it.

  ## Validation

  Every rule lives in `from_attrs/2`, so the admin API, the config seed, and a
  Redis-stored definition are all parsed by the same code and produce the same
  error tuples. `{:invalid, field, message}` names the offending field and, where
  the field is a list, the index.
  """

  alias Ankusa.Routes.Matcher
  alias CIDR

  @type ip_rule :: %{action: :allow | :deny, cidr: CIDR.t()}

  @type t :: %__MODULE__{
          id: String.t(),
          path: String.t(),
          methods: [String.t()],
          enabled: boolean(),
          ip_rules: [ip_rule()],
          metadata: map(),
          inserted_at: DateTime.t(),
          updated_at: DateTime.t()
        }

  defstruct [:id, :path, :methods, :enabled, :ip_rules, :metadata, :inserted_at, :updated_at]

  @fields ~w(id path methods enabled ip_rules metadata)
  @id_re ~r/^[a-z0-9](?:[a-z0-9_-]{0,62}[a-z0-9])?$/
  @method_re ~r/^[A-Z]+$/

  @id_message "must be a lowercase slug of at most 64 characters"
  @methods_message "must be a non-empty list of HTTP methods"

  @doc """
  Build a route from client or config attributes.

  `opts[:id]` wins over `attrs["id"]`: on `PUT`/`PATCH` the id in the URL is the
  one being addressed, and a body naming a different one must not silently move
  the route. An id is generated when neither is present.

  Attrs are a map or keyword list with string or atom keys; `inserted_at` and
  `updated_at` are not settable, so an update cannot forge its own history.
  """
  @spec from_attrs(map() | keyword(), keyword()) ::
          {:ok, t()} | {:error, {:invalid, String.t(), String.t()}}
  def from_attrs(attrs, opts \\ []) do
    with {:ok, attrs} <- attrs(attrs),
         {:ok, id} <- id(attrs, opts),
         {:ok, path} <- path(attrs),
         {:ok, methods} <- methods(attrs),
         {:ok, enabled} <- enabled(attrs),
         {:ok, ip_rules} <- ip_rules(attrs),
         {:ok, metadata} <- metadata(attrs) do
      now = new_timestamp()

      {:ok,
       %__MODULE__{
         id: id,
         path: path,
         methods: methods,
         enabled: enabled,
         ip_rules: ip_rules,
         metadata: metadata,
         inserted_at: now,
         updated_at: now
       }}
    end
  end

  @doc "The JSON view of a route: what the admin API returns and Redis stores."
  @spec to_json(t()) :: map()
  def to_json(%__MODULE__{} = route) do
    %{
      "id" => route.id,
      "path" => route.path,
      "methods" => route.methods,
      "enabled" => route.enabled,
      "ip_rules" => Enum.map(route.ip_rules, &rule_json/1),
      "metadata" => route.metadata,
      "inserted_at" => DateTime.to_iso8601(route.inserted_at),
      "updated_at" => DateTime.to_iso8601(route.updated_at)
    }
  end

  @doc "The inverse of `to_json/1`, for a definition read back from a store."
  @spec from_json(map()) :: {:ok, t()} | {:error, {:invalid, String.t(), String.t()}}
  def from_json(json) when is_map(json) do
    with {:ok, inserted_at} <- timestamp(json["inserted_at"], "inserted_at"),
         {:ok, updated_at} <- timestamp(json["updated_at"], "updated_at"),
         {:ok, route} <- from_attrs(Map.drop(json, ["inserted_at", "updated_at"])) do
      {:ok, %{route | inserted_at: inserted_at, updated_at: updated_at}}
    end
  end

  def from_json(_other), do: {:error, {:invalid, "route", "must be a JSON object"}}

  @doc """
  Parse an ordered rule list into `t:ip_rule/0`s, naming the index of the first
  bad rule.

  Shared by a route's own `ip_rules`, `config.routes.ip_rules.rules`, and the
  admin API's `PUT /admin/ip-rules` body, so a rule that is accepted in one place
  is accepted in all three and the message is the same.
  """
  @spec parse_rules(term()) :: {:ok, [ip_rule()]} | {:error, String.t()}
  def parse_rules(rules) when is_list(rules) do
    rules
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {rule, index}, {:ok, acc} ->
      case parse_rule(rule) do
        {:ok, parsed} -> {:cont, {:ok, [parsed | acc]}}
        {:error, message} -> {:halt, {:error, "rule #{index}: #{message}"}}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, message} -> {:error, message}
    end
  end

  def parse_rules(_other), do: {:error, "must be a list of rules"}

  @doc "A UTC timestamp truncated to the second: the granularity these ids keep."
  @spec new_timestamp() :: DateTime.t()
  def new_timestamp, do: DateTime.utc_now() |> DateTime.truncate(:second)

  # ── field validation ────────────────────────────────────────────────────────

  defp attrs(attrs) when is_map(attrs) or is_list(attrs) do
    if is_list(attrs) and not Keyword.keyword?(attrs) do
      {:error, {:invalid, "route", "must be a map or keyword list"}}
    else
      Enum.reduce_while(Map.new(attrs), {:ok, %{}}, fn {key, value}, {:ok, acc} ->
        case field(key) do
          {:ok, name} -> {:cont, {:ok, Map.put(acc, name, value)}}
          :error -> {:halt, {:error, {:invalid, to_string(key), "unknown field"}}}
        end
      end)
    end
  end

  defp attrs(_other), do: {:error, {:invalid, "route", "must be a map or keyword list"}}

  # String keys (JSON, YAML) and atom keys (a keyword list in config) are the
  # same field; anything else — including `inserted_at`/`updated_at` — is not a
  # field at all.
  defp field(key) when is_binary(key), do: if(key in @fields, do: {:ok, key}, else: :error)

  defp field(key) when is_atom(key) and not is_nil(key) and not is_boolean(key) do
    field(Atom.to_string(key))
  end

  defp field(_key), do: :error

  defp id(attrs, opts) do
    case Keyword.get(opts, :id) || attrs["id"] do
      value when is_binary(value) ->
        if Regex.match?(@id_re, value), do: {:ok, value}, else: {:error, invalid("id")}

      _missing ->
        {:ok, generate_id()}
    end
  end

  defp path(attrs) do
    case attrs["path"] do
      path when is_binary(path) -> validate_path(path)
      nil -> {:error, {:invalid, "path", "is required"}}
      _other -> {:error, {:invalid, "path", "must be a string"}}
    end
  end

  defp validate_path(path) do
    cond do
      not String.starts_with?(path, "/") ->
        {:error, {:invalid, "path", "must start with \"/\""}}

      String.contains?(path, ["?", "#"]) ->
        {:error, {:invalid, "path", "must not contain \"?\" or \"#\""}}

      Regex.match?(~r/\s/, path) ->
        {:error, {:invalid, "path", "must not contain whitespace"}}

      true ->
        case Matcher.segments(path) do
          {:ok, _segments} -> {:ok, path}
          {:error, message} -> {:error, {:invalid, "path", message}}
        end
    end
  end

  defp methods(attrs) do
    case attrs["methods"] || ["POST"] do
      [_ | _] = list ->
        if Enum.all?(list, &(is_binary(&1) and Regex.match?(@method_re, &1))) do
          {:ok, Enum.uniq(list)}
        else
          {:error, {:invalid, "methods", @methods_message}}
        end

      _other ->
        {:error, {:invalid, "methods", @methods_message}}
    end
  end

  defp enabled(attrs) do
    case attrs["enabled"] do
      nil -> {:ok, true}
      value when is_boolean(value) -> {:ok, value}
      _other -> {:error, {:invalid, "enabled", "must be a boolean"}}
    end
  end

  defp ip_rules(attrs) do
    case parse_rules(attrs["ip_rules"] || []) do
      {:ok, rules} -> {:ok, rules}
      {:error, message} -> {:error, {:invalid, "ip_rules", message}}
    end
  end

  defp parse_rule(rule) when is_map(rule) or is_list(rule) do
    if is_list(rule) and not Keyword.keyword?(rule) do
      {:error, "must be a map"}
    else
      rule = Map.new(rule, fn {key, value} -> {rule_key(key), value} end)

      with :ok <- rule_fields(rule),
           {:ok, action} <- rule_action(rule),
           {:ok, cidr} <- rule_cidr(rule) do
        {:ok, %{action: action, cidr: cidr}}
      end
    end
  end

  defp parse_rule(_other), do: {:error, "must be a map"}

  defp rule_key(key) when is_binary(key), do: key

  defp rule_key(key) when is_atom(key) and not is_nil(key) and not is_boolean(key),
    do: Atom.to_string(key)

  defp rule_key(key), do: key

  defp rule_fields(rule) do
    case Enum.find(Map.keys(rule), &(&1 not in ["action", "cidr"])) do
      nil -> :ok
      key -> {:error, "unknown field #{inspect(key)}"}
    end
  end

  defp rule_action(%{"action" => action}) when action in [:allow, "allow"], do: {:ok, :allow}
  defp rule_action(%{"action" => action}) when action in [:deny, "deny"], do: {:ok, :deny}

  defp rule_action(%{"action" => action}), do: {:error, "invalid action #{inspect(action)}"}
  defp rule_action(_rule), do: {:error, "action is required"}

  defp rule_cidr(%{"cidr" => cidr}) do
    case CIDR.parse(cidr) do
      %CIDR{} = parsed -> {:ok, parsed}
      {:error, _} -> {:error, "invalid cidr #{inspect(cidr)}"}
    end
  end

  defp rule_cidr(_rule), do: {:error, "cidr is required"}

  defp metadata(attrs) do
    case attrs["metadata"] do
      nil -> {:ok, %{}}
      value when is_map(value) -> encode_metadata(value)
      _other -> {:error, {:invalid, "metadata", "must be a JSON object"}}
    end
  end

  # A metadata value only has to be something a JSON body can carry; keys are
  # normalized to strings because that is what JSON makes of them anyway.
  defp encode_metadata(value) do
    if Enum.all?(value, fn {key, _v} -> is_binary(key) or is_atom(key) end) do
      normalized = Map.new(value, fn {key, v} -> {to_string(key), v} end)

      # `JSON.encode!/1` is the only encoder in the standard library, so
      # "encodable" is decided by encoding it and discarding the result; a
      # function, pid, or tuple in a metadata value raises here and is reported
      # as a field error.
      try do
        _json = JSON.encode!(normalized)
        {:ok, normalized}
      rescue
        _error -> {:error, {:invalid, "metadata", "must be a JSON object"}}
      end
    else
      {:error, {:invalid, "metadata", "must be a JSON object"}}
    end
  end

  @doc "The JSON view of one IP rule, for the admin API."
  @spec rule_json(ip_rule()) :: map()
  def rule_json(%{action: action, cidr: cidr}) do
    %{"action" => Atom.to_string(action), "cidr" => to_string(cidr)}
  end

  @doc """
  The JSON view of a whole global rule list (`%{default: ..., rules: [...]}`).

  The admin API's `/admin/ip-rules` body and the Redis store's `ip_rules` value
  are this shape, so a rule renders identically wherever it is read from.
  """
  @spec rules_json(%{default: :allow | :deny, rules: [ip_rule()]}) :: map()
  def rules_json(%{default: default, rules: rules}) do
    %{"default" => Atom.to_string(default), "rules" => Enum.map(rules, &rule_json/1)}
  end

  @doc """
  Parse `rules_json/1`'s output back into the store form. `{:error, message}` is
  a stored value that no longer parses — corruption, or a hand-edited value.
  """
  @spec parse_rules_json(term()) ::
          {:ok, %{default: :allow | :deny, rules: [ip_rule()]}} | {:error, String.t()}
  def parse_rules_json(%{"rules" => rules} = json) do
    with {:ok, default} <- rules_default(json["default"]),
         {:ok, parsed} <- parse_rules(rules) do
      {:ok, %{default: default, rules: parsed}}
    end
  end

  def parse_rules_json(_other), do: {:error, "must be a JSON object"}

  defp rules_default(nil), do: {:ok, :allow}
  defp rules_default("allow"), do: {:ok, :allow}
  defp rules_default("deny"), do: {:ok, :deny}
  defp rules_default(other), do: {:error, "invalid default #{inspect(other)}"}

  defp timestamp(value, field) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      {:error, _reason} -> {:error, {:invalid, field, "must be an ISO 8601 timestamp"}}
    end
  end

  defp timestamp(_other, field), do: {:error, {:invalid, field, "must be an ISO 8601 timestamp"}}

  defp invalid("id"), do: {:invalid, "id", @id_message}

  # A lowercase ULID: the id has to match the same slug rule a client-supplied
  # one does, and time-ordered ids keep an operator's listing readable.
  defp generate_id do
    Ankusa.ULID.encode(
      <<System.system_time(:millisecond)::48, :crypto.strong_rand_bytes(10)::binary>>
    )
    |> String.downcase()
  end
end
