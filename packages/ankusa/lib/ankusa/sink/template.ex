defmodule Ankusa.Sink.Template do
  @moduledoc """
  `{{ var }}` templates for the per-hook address options of a sink: data a
  YAML file or the admin API can state, where the Elixir API takes a function
  (`t:Ankusa.Sink.Messaging.per_hook/1`, `Ankusa.Sink.Http`'s computed
  options).

      topic: "events.{{ event_type ?? other }}"
      url: "https://hooks.example.com/t/{{ tenant }}"

  ## Syntax

  `{{ var }}` and `{{ var ?? fallback }}`, whitespace inside the braces
  optional. `var` is one of

    * `tenant` — `env.tenant_id`, `"default"` without one
    * `source` — `env.source_id`
    * `id` — `env.id`
    * `event_type` — `env.event_type` (`Ankusa.EventType`)
    * `header.<name>` — the first value of that request header (case-insensitive),
      read from the stored header list: a header the source's `store_headers`
      policy drops or hashes is refused by `check/2`

  `fallback` is a literal of `[A-Za-z0-9._:/-]+`, used when the variable has no
  value. A variable with no value and no fallback is
  `{:error, {:template_missing, var}}` from `render/2`; dispatch dead-letters
  such a delivery as `{:permanent, {:template_missing, var}}` (see
  `Ankusa.Sink.safe_deliver/4`), replayable once the config is fixed. An empty
  value counts as no value, and so does one with a control character: a value
  that comes from the hook never reaches a header or a path with a line break
  in it.

  A string on a templatable key that contains `{{` must parse entirely: an
  unknown variable, an unclosed `{{`, an empty `{{ }}`, a fallback with other
  characters and an empty `header.` name are errors. `}}` outside a template is
  literal. A literal `{{` cannot be written on a templatable key.

  Rendering never sanitises otherwise: a value the broker refuses as a name is
  the broker's error.

  ## Where

  The keys that take a template, per sink module (`keys/1`): `Sink.Kafka`
  `topic` and `key`, `Sink.RabbitMQ` `routing_key`, `Sink.NATS` `subject`,
  `Sink.Redis` `channel`, `Sink.SQS` `message_group_id`, `Sink.GooglePubSub`
  `ordering_key` (an unordered sink's own key) and `Sink.Http` `url` and every
  value of `headers`. A `{{` in any other string option of one of those sinks
  (`queue_url`, `project`, `exchange`, `brokers`, `servers`, a broker `url`,
  `secret`, …) is an error: those are a connection's or a batcher's identity.
  Credentials (`password`, `token`, …) are never read for templates.

  `normalize_sinks!/2` compiles the strings once, when `Ankusa.Source.new/2`
  builds the source. A compiled template is the data
  `{:template, [binary | {:var, name, fallback | nil}]}`, never a closure, so
  the stored spec keeps the string the operator wrote.
  """

  alias Ankusa.Envelope

  defmodule MissingError do
    @moduledoc """
    Raised by `Ankusa.Sink.Messaging.resolve/2` for a template whose variable
    has no value and no fallback. `Ankusa.Sink.safe_deliver/4` turns it into
    `{:error, {:permanent, {:template_missing, var}}}`.
    """
    defexception [:var]

    @impl true
    def message(%{var: var}), do: "template variable #{inspect(var)} has no value"
  end

  @typedoc "A compiled template: literal text and variables, in order."
  @type t :: {:template, [binary() | {:var, String.t(), String.t() | nil}]}

  @keys %{
    Ankusa.Sink.Kafka => [:topic, :key],
    Ankusa.Sink.RabbitMQ => [:routing_key],
    Ankusa.Sink.NATS => [:subject],
    Ankusa.Sink.Redis => [:channel],
    Ankusa.Sink.SQS => [:message_group_id],
    Ankusa.Sink.GooglePubSub => [:ordering_key],
    Ankusa.Sink.Http => [:url, :headers]
  }

  # Options that hold a secret a person chose: never judged for `{{`.
  @credential_keys [
    :username,
    :password,
    :token,
    :nkey_seed,
    :jwt,
    :access_key_id,
    :secret_access_key,
    :session_token
  ]

  @max_name_bytes 128
  @header_name ~r/\A[A-Za-z0-9!#$%&'*+.^_`|~-]+\z/
  @fallback ~r/\A[A-Za-z0-9._:\/-]+\z/

  # ── the template ──────────────────────────────────────────────────────────

  @doc "Does `string` contain a `{{`, so that it is meant as a template?"
  @spec template?(term()) :: boolean()
  def template?(string) when is_binary(string), do: :binary.match(string, "{{") != :nomatch
  def template?(_other), do: false

  @doc "Is `term` a compiled template?"
  @spec compiled?(term()) :: boolean()
  def compiled?({:template, parts}) when is_list(parts), do: true
  def compiled?(_other), do: false

  @doc "Compile `string`; `{:error, reason}` names what is wrong with it."
  @spec compile(binary()) :: {:ok, t()} | {:error, String.t()}
  def compile(string) when is_binary(string) do
    with {:ok, reversed} <- parse(string, []), do: {:ok, {:template, Enum.reverse(reversed)}}
  end

  @doc "`compile/1`, raising `ArgumentError` prefixed with `label`."
  @spec compile!(String.t(), binary()) :: t()
  def compile!(label, string) do
    case compile(string) do
      {:ok, template} -> template
      {:error, reason} -> raise ArgumentError, "#{label}: #{reason}"
    end
  end

  @doc """
  The value of `template` for `env`: `{:ok, binary}`, or `{:error,
  {:template_missing, var}}` for the first variable with no value and no
  fallback.
  """
  @spec render(t(), Envelope.t()) :: {:ok, binary()} | {:error, {:template_missing, String.t()}}
  def render({:template, parts}, %Envelope{} = env), do: render(parts, env, [])

  defp render([], _env, acc), do: {:ok, acc |> Enum.reverse() |> IO.iodata_to_binary()}
  defp render([part | rest], env, acc) when is_binary(part), do: render(rest, env, [part | acc])

  defp render([{:var, name, fallback} | rest], env, acc) do
    case value(name, env) || fallback do
      nil -> {:error, {:template_missing, name}}
      text -> render(rest, env, [text | acc])
    end
  end

  @doc "The one address a template has for every hook: none."
  @spec static(t()) :: nil
  def static({:template, _parts}), do: nil

  # A value is a non-empty string with no control character.
  defp value("tenant", env), do: usable(env.tenant_id || "default")
  defp value("source", env), do: usable(env.source_id)
  defp value("id", env), do: usable(env.id)
  defp value("event_type", env), do: usable(env.event_type)

  defp value("header." <> name, env), do: usable(Envelope.header_downcased(env, name))

  defp usable(value) when is_binary(value) and value != "" do
    if control?(value), do: nil, else: value
  end

  defp usable(_other), do: nil

  defp control?(<<c, _rest::binary>>) when c < 0x20 or c == 0x7F, do: true
  defp control?(<<_c, rest::binary>>), do: control?(rest)
  defp control?(<<>>), do: false

  # ── parsing ───────────────────────────────────────────────────────────────

  defp parse(string, acc) do
    case :binary.match(string, "{{") do
      :nomatch ->
        {:ok, finish(string, acc)}

      {at, 2} ->
        literal = binary_part(string, 0, at)
        rest = binary_part(string, at + 2, byte_size(string) - at - 2)

        case :binary.match(rest, "}}") do
          :nomatch ->
            {:error, "unclosed {{ in the template"}

          {close, 2} ->
            inner = binary_part(rest, 0, close)
            tail = binary_part(rest, close + 2, byte_size(rest) - close - 2)

            with {:ok, var} <- variable(inner), do: parse(tail, [var | finish(literal, acc)])
        end
    end
  end

  defp finish("", acc), do: acc
  defp finish(literal, acc), do: [literal | acc]

  defp variable(inner) do
    case inner |> String.split("??") |> Enum.map(&String.trim/1) do
      [""] -> {:error, "empty {{ }} in the template"}
      [name] -> with {:ok, name} <- name(name), do: {:ok, {:var, name, nil}}
      [name, fallback] -> with_fallback(name, fallback)
      _more -> {:error, "more than one ?? in {{ #{String.trim(inner)} }}"}
    end
  end

  defp with_fallback(name, fallback) do
    with {:ok, name} <- name(name) do
      if Regex.match?(@fallback, fallback),
        do: {:ok, {:var, name, fallback}},
        else:
          {:error,
           "the fallback of {{ #{name} }} must be one or more of A-Z a-z 0-9 . _ : / -, " <>
             "got: #{inspect(fallback)}"}
    end
  end

  defp name(name) when name in ["tenant", "source", "id", "event_type"], do: {:ok, name}

  defp name("header." <> header) do
    header = String.downcase(header, :ascii)

    cond do
      header == "" ->
        {:error, "header. needs a header name"}

      byte_size(header) > @max_name_bytes or not Regex.match?(@header_name, header) ->
        {:error, "header name #{inspect(header)} is not a valid header name (at most 128 bytes)"}

      true ->
        {:ok, "header." <> header}
    end
  end

  defp name(other) do
    {:error,
     "unknown variable #{inspect(other)}: use tenant, source, id, event_type or header.<name>"}
  end

  # ── a source's sinks ──────────────────────────────────────────────────────

  @doc "The option keys of sink module `mod` that take a template."
  @spec keys(module()) :: [atom()]
  def keys(mod), do: Map.get(@keys, mod, [])

  @doc """
  Compile the templates of every sink entry of `source_id`; an entry that is
  not a `{module, keyword}` is left as it is. Raises `ArgumentError` naming the
  source, the sink and the key. `Ankusa.Source.new/2` calls it.
  """
  @spec normalize_sinks!(String.t(), [{module(), keyword()}]) :: [{module(), keyword()}]
  def normalize_sinks!(source_id, sinks) when is_list(sinks) do
    sinks
    |> Enum.with_index()
    |> Enum.map(fn {sink, index} ->
      normalize_sink!("source #{inspect(source_id)}: sinks[#{index}]", sink)
    end)
  end

  def normalize_sinks!(_source_id, sinks), do: sinks

  @doc """
  `normalize_sinks!/2` for one entry. `label` names it in an error: `label.key:
  reason` for a bad template and `label: templates are not supported on key`
  for a `{{` on a key that takes none. The loader passes the entry's YAML path.
  """
  @spec normalize_sink!(String.t(), {module(), keyword()} | term()) ::
          {module(), keyword()} | term()
  def normalize_sink!(label, {mod, opts} = sink) when is_atom(mod) and is_list(opts) do
    if Map.has_key?(@keys, mod) and Keyword.keyword?(opts) do
      templatable = keys(mod)
      {mod, Enum.map(opts, fn {key, value} -> {key, option!(label, templatable, key, value)} end)}
    else
      sink
    end
  end

  def normalize_sink!(_label, sink), do: sink

  defp option!(label, templatable, :headers, headers) when is_list(headers) do
    cond do
      :headers in templatable -> Enum.map(headers, &header!(label, &1))
      Enum.any?(headers, &template_header?/1) -> refuse!(label, :headers)
      true -> headers
    end
  end

  defp option!(label, templatable, key, value) when is_binary(value) do
    cond do
      not template?(value) -> value
      key in templatable -> compile!("#{label}.#{key}", value)
      key in @credential_keys -> value
      true -> refuse!(label, key)
    end
  end

  defp option!(label, _templatable, key, [_ | _] = list) do
    if key not in @credential_keys and Enum.any?(list, &template?/1),
      do: refuse!(label, key),
      else: list
  end

  defp option!(_label, _templatable, _key, value), do: value

  defp header!(label, {name, value}) do
    cond do
      template?(name) ->
        raise ArgumentError, "#{label}: templates are not supported on header names"

      template?(value) ->
        {name, compile!("#{label}.headers.#{name}", value)}

      true ->
        {name, value}
    end
  end

  defp header!(_label, other), do: other

  defp refuse!(label, key),
    do: raise(ArgumentError, "#{label}: templates are not supported on #{key}")

  defp template_header?({name, value}), do: template?(name) or template?(value)
  defp template_header?(_other), do: false

  @doc """
  Is any templatable option of `sinks` written as a template? The loader asks it
  of `lifecycle.sinks`, which have no source to read a variable from.
  """
  @spec templated?([{module(), keyword()}]) :: boolean()
  def templated?(sinks) when is_list(sinks) do
    Enum.any?(sinks, fn
      {mod, opts} when is_atom(mod) and is_list(opts) ->
        templatable = keys(mod)

        Enum.any?(opts, fn
          {:headers, headers} when is_list(headers) -> Enum.any?(headers, &template_header?/1)
          {key, value} -> key in templatable and template?(value)
          _other -> false
        end)

      _other ->
        false
    end)
  end

  # ── what the instance's config says about it ──────────────────────────────

  @doc """
  The check a source needs the instance's config for: a `header.<name>`
  variable must name a header the source's `store_headers` policy keeps and does
  not hash, because a replay renders from the stored hook.
  """
  @spec check(Ankusa.Config.t(), Ankusa.Source.t()) :: :ok | {:error, String.t()}
  def check(%Ankusa.Config{} = config, %Ankusa.Source{sinks: sinks} = source) do
    case header_variables(sinks) do
      [] ->
        :ok

      names ->
        policy = Ankusa.Ordering.stored_policy(config, source)

        case Enum.find(names, &(&1 in policy.drop or &1 in policy.hash)) do
          nil ->
            :ok

          name ->
            {:error,
             "source #{inspect(source.id)}: a template reads header #{inspect(name)}, which " <>
               "this source's store_headers policy drops or hashes; a replay could not " <>
               "render it from the stored hook"}
        end
    end
  end

  defp header_variables(sinks) do
    sinks
    |> Enum.flat_map(fn
      {_mod, opts} when is_list(opts) -> opts |> Keyword.values() |> Enum.flat_map(&compiled/1)
      _other -> []
    end)
    |> Enum.flat_map(fn
      {:var, "header." <> name, _fallback} -> [name]
      _other -> []
    end)
    |> Enum.uniq()
  end

  defp compiled({:template, parts}) when is_list(parts), do: parts

  defp compiled(headers) when is_list(headers) do
    Enum.flat_map(headers, fn
      {_name, value} -> compiled(value)
      _other -> []
    end)
  end

  defp compiled(_other), do: []

  @doc "Boot check: `check/2` for every statically configured source. Raises `ArgumentError`."
  @spec validate_config!(Ankusa.Config.t()) :: :ok
  def validate_config!(%Ankusa.Config{} = config) do
    config
    |> Ankusa.Queue.static_sources()
    |> Enum.each(fn {_id, source} ->
      with {:error, message} <- check(config, source), do: raise(ArgumentError, message)
    end)
  end
end
