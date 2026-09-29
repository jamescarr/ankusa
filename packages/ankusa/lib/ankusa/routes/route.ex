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

  alias Ankusa.Net
  alias Ankusa.Routes.Matcher
  alias CIDR

  @type ip_rule :: %{action: :allow | :deny, cidr: %CIDR{}}

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
  @id_re ~r/\A[a-z0-9](?:[a-z0-9_-]{0,62}[a-z0-9])?\z/
  @method_re ~r/\A[A-Z]+\z/

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
         {:ok, _id} <- stored_id(json),
         {:ok, route} <- from_attrs(Map.drop(json, ["inserted_at", "updated_at"])) do
      {:ok, %{route | inserted_at: inserted_at, updated_at: updated_at}}
    end
  end

  def from_json(_other), do: {:error, {:invalid, "route", "must be a JSON object"}}

  # A stored definition always carries its id. One without it is corrupt, and
  # minting a fresh id on every load would hand each node a different one.
  defp stored_id(%{"id" => id}) when is_binary(id), do: {:ok, id}
  defp stored_id(_json), do: {:error, {:invalid, "id", "is required"}}

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

  @doc """
  Coerce a map or keyword list into a string-keyed map. `field` names the value
  in error tuples: a non-keyword list is `"must be a map"` (the stricter
  `ip_rules` message), anything else non-map is `"must be a map or keyword
  list"`.
  """
  @spec stringify(term(), String.t()) ::
          {:ok, %{String.t() => term()}} | {:error, {:invalid, String.t(), String.t()}}
  def stringify(value, _field) when is_map(value),
    do: {:ok, Map.new(value, fn {k, v} -> {to_string(k), v} end)}

  def stringify(value, field) when is_list(value) do
    if Keyword.keyword?(value) do
      {:ok, Map.new(value, fn {k, v} -> {to_string(k), v} end)}
    else
      {:error, {:invalid, field, "must be a map"}}
    end
  end

  def stringify(_value, field), do: {:error, {:invalid, field, "must be a map or keyword list"}}

  # ── field validation ────────────────────────────────────────────────────────

  defp attrs(attrs) do
    with {:ok, attrs} <- stringify(attrs, "route") do
      Enum.reduce_while(attrs, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
        case field(key) do
          {:ok, name} -> {:cont, {:ok, Map.put(acc, name, value)}}
          :error -> {:halt, {:error, {:invalid, to_string(key), "unknown field"}}}
        end
      end)
    end
  end

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
      nil ->
        {:ok, generate_id()}

      value when is_binary(value) ->
        if Regex.match?(@id_re, value), do: {:ok, value}, else: {:error, invalid("id")}

      # A number or a map is not "no id": quietly replacing it with a generated
      # one would answer 201 for a route the caller did not name.
      _other ->
        {:error, invalid("id")}
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

  defp parse_rule(rule) do
    case stringify(rule, "rule") do
      {:ok, rule} ->
        with :ok <- rule_fields(rule),
             {:ok, action} <- rule_action(rule),
             {:ok, cidr} <- rule_cidr(rule) do
          {:ok, %{action: action, cidr: cidr}}
        end

      {:error, {:invalid, _field, message}} ->
        {:error, message}
    end
  end

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

  # An ordinary failure keeps its plain message. A mapped range parses fine, so
  # nothing about it looks wrong to the operator: say why it was refused, in the
  # message they actually read (an API 400, a boot error).
  defp rule_cidr(%{"cidr" => cidr}) do
    case Net.parse_cidr(cidr) do
      {:ok, parsed} ->
        {:ok, parsed}

      {:error, :mapped_range} ->
        {:error, "invalid cidr #{inspect(cidr)}: #{Net.mapped_range_hint()}"}

      {:error, _message} ->
        {:error, "invalid cidr #{inspect(cidr)}"}
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
  Parse a global rule list — the admin API's `PUT /admin/ip-rules` body and the
  Redis store's `ip_rules` value are both this shape — into the store form.

  `default` and `rules` are both required. This replaces a security control
  wholesale, so nothing is filled in on the caller's behalf: an omitted `default`
  used to mean `allow`, which quietly turned a deny-by-default list into an open
  one.
  """
  @spec parse_ip_rules(term()) ::
          {:ok, %{default: :allow | :deny, rules: [ip_rule()]}}
          | {:error, {:invalid, String.t(), String.t()}}
  def parse_ip_rules(attrs) do
    with {:ok, attrs} <- stringify(attrs, "ip_rules"),
         {:ok, default} <- ip_rules_default(attrs["default"]),
         {:ok, rules} <- ip_rules_list(attrs["rules"]) do
      {:ok, %{default: default, rules: rules}}
    end
  end

  defp ip_rules_default(nil), do: {:error, {:invalid, "default", "is required"}}
  defp ip_rules_default(action) when action in [:allow, "allow"], do: {:ok, :allow}
  defp ip_rules_default(action) when action in [:deny, "deny"], do: {:ok, :deny}

  defp ip_rules_default(action),
    do: {:error, {:invalid, "default", "must be \"allow\" or \"deny\", got #{inspect(action)}"}}

  defp ip_rules_list(nil), do: {:error, {:invalid, "rules", "is required"}}

  defp ip_rules_list(rules) do
    case parse_rules(rules) do
      {:ok, parsed} -> {:ok, parsed}
      {:error, message} -> {:error, {:invalid, "rules", message}}
    end
  end

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
