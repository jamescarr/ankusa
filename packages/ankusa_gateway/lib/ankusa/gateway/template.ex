defmodule Ankusa.Gateway.Template do
  @moduledoc """
  `{{ var }}` templates for the strings the gateway derives from a request: a
  `rules` authorizer's `when` values, a PDP's subject and resource, a table
  authorizer's identities, an enricher's user id. Same surface syntax as
  `Ankusa.Sink.Template`, which is bound to an envelope and cannot be reused.

      "team-{{ vars.team }}"
      "tenant/{{ claims.tid ?? default }}"

  ## Syntax

  `{{ var }}` and `{{ var ?? fallback }}`, whitespace inside the braces
  optional. `var` is one of

    * `principal.id`, `principal.groups`, `principal.tenants`,
      `principal.scopes` (`principal.tenants` of `:all` renders as `["*"]`);
    * `claims.<name>(.<name>)*` — a claim, walked through nested maps;
    * `action`, `kind`, `space`, `key`, `tenant` — the request's resource;
    * `vars.<capture>` — a `{name}` capture of the `Ankusa.Gateway.Pattern`
      the request matched.

  `compile!/3` takes the roots a template may use (`"principal"`, `"claims"`,
  `"action"`, `"kind"`, `"space"`, `"key"`, `"tenant"`, `"vars"`), so a
  `rules` template cannot reach `principal.groups` that the rule's operator did
  not permit. An unknown root, a variable of the wrong shape for its root, an
  unclosed `{{` and an empty `{{ }}` raise `ArgumentError`; every message
  starts with the caller's `label`.

  `fallback` is literal text of `[A-Za-z0-9._:/-]+`, used when the variable has
  no value.

  ## Values

  A value is a string (non-empty, without a control character), an integer or
  a boolean, rendered with `to_string/1`; anything else — an absent binding, a
  `nil`, an empty string, a value with a line break, a map — is *missing*. A
  list-valued variable (`principal.groups`, `principal.scopes`,
  `principal.tenants`, or a list claim) renders as `{:error, {:template_list,
  var}}` from `render/2`: it has no single string. `render_many/2` is how a
  list becomes several strings — one per element, at most one list-valued
  variable per template.

  ## URLs

  `compile_url!/3` is the only way a URL is templated. It refuses a `{{` in
  the scheme, host or port (`"<label>: the scheme, host and port of a URL
  cannot be templated"`), and rendering percent-encodes every substituted value
  (`URI.encode(value, &URI.char_unreserved?/1)`), so request data never picks
  the origin, adds a path segment or adds a query parameter.
  """

  alias Ankusa.Gateway.Authz.Request
  alias Ankusa.Principal

  defstruct parts: [], url?: false

  @type root :: :principal | :claims | :action | :kind | :space | :key | :tenant | :vars

  @typedoc """
  `{:var, root, path, name, fallback}`: `name` is the dotted variable as
  written (`"claims.team"`), for errors; `path` is what the root is walked by.
  """
  @type var_part :: {:var, root(), [String.t()], String.t(), String.t() | nil}

  @typedoc "A compiled template: literal text and variables, in order."
  @type t :: %__MODULE__{parts: [binary() | var_part()], url?: boolean()}

  @type bindings_map :: %{
          optional(:principal) => Principal.t(),
          optional(:claims) => map(),
          optional(:action) => atom() | String.t(),
          optional(:kind) => atom() | String.t(),
          optional(:space) => String.t(),
          optional(:key) => String.t(),
          optional(:tenant) => String.t() | nil,
          optional(:vars) => %{optional(String.t()) => String.t()}
        }

  @roots [:principal, :claims, :action, :kind, :space, :key, :tenant, :vars]
  @root_names Enum.map(@roots, &Atom.to_string/1)
  @principal_paths ~w(id groups tenants scopes)
  @plain_roots [:action, :kind, :space, :key, :tenant]
  @claim_segment ~r/\A[A-Za-z0-9_-]+\z/
  @fallback ~r/\A[A-Za-z0-9._:\/-]+\z/
  @origin_message "the scheme, host and port of a URL cannot be templated"

  @doc """
  Compile `string`, whose variables must use one of `allowed_roots`; raises
  `ArgumentError` prefixed with `label`.
  """
  @spec compile!(String.t(), String.t(), [String.t() | atom()]) :: t()
  def compile!(string, label, allowed_roots) do
    compile(string, label, allowed_roots, false)
  end

  @doc """
  `compile!/3` for a URL: the scheme, host and port stay literal, and every
  substituted value is percent-encoded.
  """
  @spec compile_url!(String.t(), String.t(), [String.t() | atom()]) :: t()
  def compile_url!(string, label, allowed_roots) do
    url!(string, label)
    compile(string, label, allowed_roots, true)
  end

  @doc """
  The values of `template` for `bindings`: `{:ok, string}`, `{:error,
  {:template_missing, var}}` for the first missing value with no fallback, or
  `{:error, {:template_list, var}}` for a list-valued variable.
  """
  @spec render(t(), bindings_map()) ::
          {:ok, String.t()}
          | {:error, {:template_missing, String.t()} | {:template_list, String.t()}}
  def render(%__MODULE__{parts: parts, url?: url?}, bindings) do
    Enum.reduce_while(parts, {:ok, []}, fn part, {:ok, acc} ->
      case resolve(part, bindings, url?) do
        {:scalar, text} -> {:cont, {:ok, [text | acc]}}
        {:list, name, _values} -> {:halt, {:error, {:template_list, name}}}
        {:missing, name} -> {:halt, {:error, {:template_missing, name}}}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, acc |> Enum.reverse() |> IO.iodata_to_binary()}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  `render/2` for a template with one list-valued variable: one string per
  element, in order. A template with no list renders one string; one with two
  or more lists is `{:error, {:template_lists, vars}}`.
  """
  @spec render_many(t(), bindings_map()) :: {:ok, [String.t()]} | {:error, term()}
  def render_many(%__MODULE__{parts: parts, url?: url?}, bindings) do
    resolved = Enum.map(parts, &resolve(&1, bindings, url?))

    case {missing(resolved), lists(resolved)} do
      {[name | _], _lists} -> {:error, {:template_missing, name}}
      {[], []} -> {:ok, [join(resolved)]}
      {[], [_one]} -> {:ok, expand(resolved)}
      {[], names} -> {:error, {:template_lists, Enum.uniq(names)}}
    end
  end

  @doc """
  The bindings for `request`: `principal`, `claims`, `action`, `kind`, `space`,
  `key`, `tenant` and the `vars` the request's key pattern captured.
  """
  @spec bindings(Request.t(), map()) :: bindings_map()
  def bindings(%Request{} = request, vars \\ %{}) do
    %{
      principal: request.principal,
      claims: request.principal.claims,
      action: request.action,
      kind: request.resource.kind,
      space: request.resource.space,
      key: request.resource.key,
      tenant: request.resource.tenant,
      vars: vars
    }
  end

  @doc """
  The claim at a dotted path (`"team"`, `"org.team"`) or a list of segments:
  `{:ok, value}` or `:error` when the path is not there.
  """
  @spec claim(map(), String.t() | [String.t()]) :: {:ok, term()} | :error
  def claim(claims, path) when is_map(claims) and is_binary(path),
    do: claim(claims, String.split(path, "."))

  def claim(claims, path) when is_map(claims) and is_list(path) do
    case dig(claims, path) do
      {:ok, value} -> {:ok, value}
      :missing -> :error
    end
  end

  @doc """
  The string a template substitutes for `value`: `{:ok, text}` for a scalar
  with a value, `:error` for anything else (the same rule `render/2` applies).
  """
  @spec scalar(term()) :: {:ok, String.t()} | :error
  def scalar(value) do
    case format(value) do
      {:scalar, text} -> {:ok, text}
      _other -> :error
    end
  end

  # ── compiling ───────────────────────────────────────────────────────────────

  defp compile(string, label, allowed_roots, url?) when is_binary(string) do
    roots = roots!(allowed_roots, label)
    %__MODULE__{parts: parse!(string, label, roots, []), url?: url?}
  end

  defp compile(string, label, _allowed_roots, _url?),
    do: raise(ArgumentError, "#{label}: a template must be a string, got #{inspect(string)}")

  defp roots!(allowed_roots, label) when is_list(allowed_roots) do
    Enum.map(allowed_roots, fn root ->
      name = if is_atom(root), do: Atom.to_string(root), else: root

      case Enum.find(@roots, &(Atom.to_string(&1) == name)) do
        nil ->
          raise ArgumentError,
                "#{label}: unknown template variable root #{inspect(root)} " <>
                  "(allowed roots: #{Enum.join(@root_names, ", ")})"

        root ->
          root
      end
    end)
  end

  defp roots!(other, label),
    do:
      raise(
        ArgumentError,
        "#{label}: allowed template roots must be a list, got #{inspect(other)}"
      )

  # A URL's origin is operator configuration: a `{{` before the first `/` after
  # `scheme://` (or a template without a `scheme://` at all) is refused.
  defp url!(string, label) when is_binary(string) do
    if :binary.match(string, "{{") == :nomatch, do: :ok, else: url_origin!(string, label)
  end

  defp url!(string, label),
    do: raise(ArgumentError, "#{label}: a template must be a string, got #{inspect(string)}")

  defp url_origin!(string, label) do
    case :binary.match(string, "://") do
      {at, 3} ->
        if brace_before?(string, authority_end(string, at + 3)),
          do: raise(ArgumentError, "#{label}: #{@origin_message}"),
          else: :ok

      :nomatch ->
        raise ArgumentError, "#{label}: #{@origin_message}"
    end
  end

  defp brace_before?(string, limit) do
    case :binary.match(string, "{{") do
      {at, 2} -> at < limit
      :nomatch -> false
    end
  end

  defp authority_end(string, from) do
    rest = binary_part(string, from, byte_size(string) - from)

    case :binary.match(rest, ["/", "?", "#"]) do
      {at, 1} -> from + at
      :nomatch -> byte_size(string)
    end
  end

  defp parse!(string, label, roots, acc) do
    case :binary.match(string, "{{") do
      :nomatch ->
        Enum.reverse(finish(string, acc))

      {at, 2} ->
        literal = binary_part(string, 0, at)
        rest = binary_part(string, at + 2, byte_size(string) - at - 2)

        case :binary.match(rest, "}}") do
          :nomatch ->
            raise ArgumentError, "#{label}: unclosed {{ in the template"

          {close, 2} ->
            inner = binary_part(rest, 0, close)
            tail = binary_part(rest, close + 2, byte_size(rest) - close - 2)
            var = variable!(inner, label, roots)
            parse!(tail, label, roots, [var | finish(literal, acc)])
        end
    end
  end

  defp finish("", acc), do: acc
  defp finish(literal, acc), do: [literal | acc]

  defp variable!(inner, label, roots) do
    trimmed = String.trim(inner)

    if trimmed == "",
      do: raise(ArgumentError, "#{label}: empty {{ }} in the template"),
      else: with_fallback(trimmed, label, roots)
  end

  defp with_fallback(inner, label, roots) do
    case String.split(inner, "??") do
      [one] ->
        var!(String.trim(one), nil, label, roots)

      [one, fallback] ->
        var!(String.trim(one), fallback!(fallback, label), label, roots)

      _more ->
        raise ArgumentError, "#{label}: more than one ?? in {{ #{inner} }}"
    end
  end

  defp fallback!(fallback, label) do
    fallback = String.trim(fallback)

    if Regex.match?(@fallback, fallback),
      do: fallback,
      else:
        raise(
          ArgumentError,
          "#{label}: a fallback must be literal text ([A-Za-z0-9._:/-]+), got #{inspect(fallback)}"
        )
  end

  defp var!(name, fallback, label, roots) do
    [root | path] = String.split(name, ".")
    root = root!(root, name, label, roots)
    {:var, root, path!(root, path, name, label), name, fallback}
  end

  defp root!(root, name, label, roots) do
    case Enum.find(@roots, &(Atom.to_string(&1) == root)) do
      nil ->
        raise ArgumentError,
              "#{label}: unknown template variable root #{inspect(root)} in #{inspect(name)} " <>
                "(allowed roots: #{Enum.join(@root_names, ", ")})"

      root ->
        if root in roots,
          do: root,
          else:
            raise(
              ArgumentError,
              "#{label}: #{inspect(name)} is not allowed here " <>
                "(allowed roots: #{Enum.map_join(roots, ", ", &Atom.to_string/1)})"
            )
    end
  end

  defp path!(:principal, [field], _name, _label) when field in @principal_paths, do: [field]

  defp path!(:claims, [_ | _] = path, name, label) do
    Enum.each(path, fn segment ->
      unless Regex.match?(@claim_segment, segment), do: unknown!(name, label)
    end)

    path
  end

  defp path!(:vars, [capture], name, label) do
    unless Regex.match?(@claim_segment, capture), do: unknown!(name, label)
    [capture]
  end

  defp path!(root, [], _name, _label) when root in @plain_roots, do: []
  defp path!(_root, _path, name, label), do: unknown!(name, label)

  defp unknown!(name, label),
    do: raise(ArgumentError, "#{label}: unknown template variable #{inspect(name)}")

  # ── rendering ───────────────────────────────────────────────────────────────

  # `{:scalar, text} | {:list, name, [text]} | {:missing, name}`; in a URL
  # template every substituted value is percent-encoded, the literals are not.
  defp resolve(part, _bindings, _url?) when is_binary(part), do: {:scalar, part}

  defp resolve({:var, root, path, name, fallback}, bindings, url?) do
    with {:ok, value} <- lookup(root, path, bindings),
         formatted when formatted != :missing <- format(value) do
      case formatted do
        {:list, texts} -> {:list, name, Enum.map(texts, &encode(&1, url?))}
        {:scalar, text} -> {:scalar, encode(text, url?)}
      end
    else
      _missing -> fallback(fallback, name, url?)
    end
  end

  defp fallback(nil, name, _url?), do: {:missing, name}
  defp fallback(text, _name, url?), do: {:scalar, encode(text, url?)}

  defp lookup(:principal, path, bindings), do: principal(path, Map.get(bindings, :principal))
  defp lookup(:claims, path, bindings), do: dig(Map.get(bindings, :claims), path)
  defp lookup(:vars, [capture], bindings), do: capture(Map.get(bindings, :vars), capture)
  defp lookup(:action, [], bindings), do: present(Map.get(bindings, :action))
  defp lookup(:kind, [], bindings), do: present(Map.get(bindings, :kind))
  defp lookup(:space, [], bindings), do: present(Map.get(bindings, :space))
  defp lookup(:key, [], bindings), do: present(Map.get(bindings, :key))
  defp lookup(:tenant, [], bindings), do: present(Map.get(bindings, :tenant))
  defp lookup(_root, _path, _bindings), do: :missing

  defp principal(["id"], %Principal{id: id}), do: present(id)
  defp principal(["groups"], %Principal{groups: groups}), do: present(groups)
  defp principal(["tenants"], %Principal{tenants: :all}), do: {:ok, ["*"]}
  defp principal(["tenants"], %Principal{tenants: tenants}), do: present(tenants)
  defp principal(["scopes"], %Principal{scopes: scopes}), do: present(scopes)
  defp principal(_path, _principal), do: :missing

  defp capture(%{} = vars, name), do: present(Map.get(vars, name))
  defp capture(_vars, _name), do: :missing

  defp present(nil), do: :missing
  defp present(value), do: {:ok, value}

  defp dig(value, []), do: present(value)

  defp dig(%{} = map, [segment | rest]) do
    case Map.fetch(map, segment) do
      {:ok, value} -> dig(value, rest)
      :error -> :missing
    end
  end

  defp dig(_value, _path), do: :missing

  defp format(value) when is_binary(value),
    do: if(usable?(value), do: {:scalar, value}, else: :missing)

  defp format(value) when is_integer(value), do: {:scalar, Integer.to_string(value)}

  defp format(value) when is_atom(value) and not is_nil(value) do
    text = Atom.to_string(value)
    if usable?(text), do: {:scalar, text}, else: :missing
  end

  defp format(values) when is_list(values), do: format_list(values)
  defp format(_other), do: :missing

  defp format_list([]), do: :missing

  defp format_list(values) do
    values
    |> Enum.reduce_while([], fn value, acc ->
      case format(value) do
        {:scalar, text} -> {:cont, [text | acc]}
        _other -> {:halt, :error}
      end
    end)
    |> case do
      :error -> :missing
      texts -> {:list, Enum.reverse(texts)}
    end
  end

  defp usable?(""), do: false
  defp usable?(value), do: not control?(value)

  defp control?(<<c, _rest::binary>>) when c < 0x20 or c == 0x7F, do: true
  defp control?(<<_c, rest::binary>>), do: control?(rest)
  defp control?(<<>>), do: false

  # ── many ────────────────────────────────────────────────────────────────────

  defp missing(resolved), do: for({:missing, name} <- resolved, do: name)
  defp lists(resolved), do: for({:list, name, _values} <- resolved, do: name)

  # One string from parts without a list (join/1 is only called then).
  defp join(resolved) do
    resolved
    |> Enum.map(fn {:scalar, text} -> text end)
    |> IO.iodata_to_binary()
  end

  defp expand(resolved) do
    {:list, _name, values} = Enum.find(resolved, &match?({:list, _, _}, &1))

    Enum.map(values, fn value ->
      resolved
      |> Enum.map(fn
        {:scalar, text} -> text
        {:list, _name, _values} -> value
      end)
      |> IO.iodata_to_binary()
    end)
  end

  defp encode(text, false), do: text
  defp encode(text, true), do: URI.encode(text, &URI.char_unreserved?/1)
end
