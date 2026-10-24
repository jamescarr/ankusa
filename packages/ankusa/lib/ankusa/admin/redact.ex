defmodule Ankusa.Admin.Redact do
  @moduledoc """
  Turn a `%Ankusa.Config{}` into a JSON-encodable map safe to hand to anyone
  who can reach the admin API.

  Adapter options are an open vocabulary (every adapter, and every third-party
  adapter, names its own credentials), so redaction is an allowlist, not a
  blocklist. There are two modes.

  **Open** covers `%Ankusa.Config{}`'s own fields and a source entry's own
  fields (`tenant`, `name`, `source_id`, `ingest_path`, `on_verify_failure`,
  `trust_url_tenant`):
  none of them holds a secret, so values are converted as they are. Maps and
  keyword lists become string-keyed maps, atoms become strings (module atoms
  via `inspect/1`), numbers, booleans, `nil` and binaries stay, functions
  become `"#Function"`, and any other term goes through `inspect/1`. A value
  shaped like a `{module, opts}` pair (an atom head, an Erlang module included,
  with list or map opts), or a list of them, is never converted as it is: its
  opts switch to closed mode.

  **Closed** covers everything inside a `{module, opts}` pair (sinks,
  verifiers, source store, blob store, codec, retry policy, route resolver,
  routes store, lifecycle sinks) and a source's stored JSON spec. For each
  key/value, the first matching rule wins:

    1. under `headers`, names stay and values become `"[REDACTED]"`;
    2. a `{module, opts}` pair (or a list of them) becomes
       `%{"module" => "Ankusa.Sink.Log", "opts" => %{}}`, its opts closed;
    3. a `{module, fun, args}` callback becomes `"Module.fun/arity"`, never its
       args (a static GCS token is a bare arg);
    4. numbers, booleans, `nil` and atoms stay; functions become `"#Function"`;
    5. structs, maps and non-empty keyword lists become closed maps;
    6. a list made only of such maps, keyword lists and pairs is converted
       element by element;
    7. under `url`, `endpoint` or `resource` a binary is shown with the
       password of `user:pass@` userinfo hidden, a lone `token@` userinfo
       hidden, and every query value hidden (names stay);
    8. a value under a key on the shown-key allowlist (ids, names, regions,
       topics and other non-secret identifiers) is shown;
    9. anything else, strings and charlists included, is `"[REDACTED]"`.

  A new adapter option is therefore hidden until its key is added to
  `@shown_keys`. Path and host of a URL are shown, so a capability URL whose
  secret sits in the path is not protected.
  """

  @redacted "[REDACTED]"

  @url_keys ~w(url endpoint resource)

  @shown_keys ~w(tenant_id id forward_headers prefix header json preset from
                 signature_header sig_prefix sig_key version signed timestamp timestamp_header
                 bucket region namespace container account_name client_id tenancy_ocid user_ocid
                 key_fingerprint exchange routing_key topic brokers servers subject channel
                 queue_url message_group_id client_name inbox_prefix username mechanism
                 type method exchange_type parse hash encoding secret_decode on_verify_failure
                 algorithm location param fingerprints subjects
                 project ordering_key)

  @doc "A redacted, JSON-encodable view of the whole config."
  @spec config(Ankusa.Config.t()) :: map()
  def config(%Ankusa.Config{} = config) do
    fields =
      config
      |> Map.from_struct()
      |> redact_auth()

    # `secrets` holds the key-encryption keys: closed mode shows `kek` and
    # `kek_previous` as `[REDACTED]` and a provider callback as `Mod.fun/arity`.
    # `intake` is open for its own scalar keys, but its adapter blocks
    # (`kafka`, `jetstream`, `memory`: brokers, SASL, TLS key material, credentials)
    # are an adapter's vocabulary, so they go through closed mode.
    {adapters, plain_intake} = Map.split(fields.intake, [:kafka, :jetstream, :memory])

    intake =
      plain_intake
      |> open_map()
      |> Map.merge(
        Map.new(adapters, fn {k, opts} -> {to_string(k), closed(to_string(k), opts)} end)
      )

    fields
    |> Map.drop([:secrets, :intake])
    |> open_map()
    |> Map.put("secrets", closed("secrets", fields.secrets))
    |> Map.put("intake", intake)
  end

  # The control planes' `auth` blocks (`Ankusa.Admin.Auth`): a token entry keeps
  # its id and scopes but not its hash (a low-entropy token is brute-forceable
  # from it), a JWKS URL goes through the URL rules, and static keys are not
  # shown at all. Mode, claim names and the subject map are shown as they are.
  # The claim gateway's ref signing keys and the backup manifest's signing keys
  # are lists of plain strings, which open mode would show: each is hidden, the
  # count stays.
  defp redact_auth(config) do
    config
    |> update_in([:admin, :auth], &auth_view/1)
    |> update_in([:routes, :admin, :auth], &auth_view/1)
    |> update_in([:claim_check, :auth], &auth_view/1)
    |> update_in([:claim_check, :ref_signing_keys], &Enum.map(&1, fn _key -> @redacted end))
    |> update_in([:backup, :signing_key], &Enum.map(&1, fn _key -> @redacted end))
  end

  defp auth_view(%{tokens: tokens, jwt: jwt} = auth) do
    jwks_url = jwt.jwks_url && redact_url(jwt.jwks_url)

    %{
      auth
      | tokens: Enum.map(tokens, &%{&1 | sha256: @redacted}),
        jwt: %{jwt | jwks_url: jwks_url, keys: Enum.map(jwt.keys, fn _key -> @redacted end)}
    }
  end

  # A bare `%Ankusa.Config{}` that never went through `new/1` has only a mode.
  defp auth_view(auth), do: auth

  @doc """
  A redacted, JSON-encodable view of one tenant-scoped source, built from the
  `Ankusa.SourceStore.stored()` map: the shape the admin API's source routes
  return and the `data` of a source lifecycle event.

  The stored spec is plain JSON (string keys, plus strings, numbers, maps, and
  lists), so its `verify` and `sinks` take the closed-mode rules of the module
  doc.
  """
  @spec source_entry(Ankusa.SourceStore.stored()) :: map()
  def source_entry(%{tenant: tenant, name: name, source_id: source_id, spec: spec} = stored) do
    entry = %{
      "tenant" => open(tenant),
      "name" => open(name),
      "source_id" => open(source_id),
      "ingest_path" => "/webhooks/#{source_id}",
      "verify" => closed("verify", Map.get(spec, "verify") || %{"type" => "none"}),
      "on_verify_failure" => open(Map.get(spec, "on_verify_failure")),
      # A boolean, shown as stored; anything else in a stored spec reads as the
      # default, `false`, which the decoder would have refused anyway.
      "trust_url_tenant" => Map.get(spec, "trust_url_tenant", false) == true,
      "sinks" => closed("sinks", Map.get(spec, "sinks", []))
    }

    # The per-source retry options: numbers and a boolean, no secret in them.
    entry =
      case Map.get(spec, "retry") do
        %{} = retry -> Map.put(entry, "retry", retry)
        _ -> entry
      end

    # The source's change counter, for a store that keeps one.
    case Map.get(stored, :revision) do
      revision when is_integer(revision) -> Map.put(entry, "revision", revision)
      _ -> entry
    end
  end

  @doc """
  Walk a stored source spec (string keys, plain JSON values) and hand every
  string the closed-mode rules would hide to `fun`, threading an accumulator.
  Everything else comes back as it was.

  This is the inverse of the allowlist above, and it is what decides which
  fields of a spec are encrypted at rest (`Ankusa.Crypto.Envelope.seal/2`):
  header values, every string under a key that is not on the shown-key
  allowlist (`secret` included, in a list too), and a `url`/`endpoint`/
  `resource` that carries credentials. A new adapter option is hidden, and so
  encrypted, until its key is added to `@shown_keys`.
  """
  @spec reduce_hidden(map(), acc, (String.t(), acc -> {String.t(), acc})) :: {map(), acc}
        when acc: term()
  def reduce_hidden(spec, acc, fun) when is_map(spec) and is_function(fun, 2),
    do: hide_map(spec, acc, fun)

  @doc "`reduce_hidden/3` without an accumulator: replace every hidden string by `fun.(string)`."
  @spec map_hidden(map(), (String.t() -> String.t())) :: map()
  def map_hidden(spec, fun) when is_map(spec) and is_function(fun, 1) do
    {mapped, :ok} = reduce_hidden(spec, :ok, fn string, :ok -> {fun.(string), :ok} end)
    mapped
  end

  defp hide_map(map, acc, fun) do
    {pairs, acc} =
      Enum.map_reduce(map, acc, fn {key, value}, acc ->
        {value, acc} = hide_entry(key_string(key), value, acc, fun)
        {{key, value}, acc}
      end)

    {Map.new(pairs), acc}
  end

  # Header names stay, so a spec still says which headers a sink sends.
  defp hide_entry("headers", headers, acc, fun) when is_map(headers) do
    {pairs, acc} =
      Enum.map_reduce(headers, acc, fn
        {name, value}, acc when is_binary(value) ->
          {value, acc} = fun.(value, acc)
          {{name, value}, acc}

        pair, acc ->
          {pair, acc}
      end)

    {Map.new(pairs), acc}
  end

  defp hide_entry(key, value, acc, fun) when is_binary(value) do
    cond do
      key in @url_keys -> if redact_url(value) == value, do: {value, acc}, else: fun.(value, acc)
      key in @shown_keys -> {value, acc}
      true -> fun.(value, acc)
    end
  end

  defp hide_entry(_key, value, acc, fun) when is_map(value), do: hide_map(value, acc, fun)

  defp hide_entry(key, list, acc, fun) when is_list(list) do
    Enum.map_reduce(list, acc, fn item, acc -> hide_entry(key, item, acc, fun) end)
  end

  defp hide_entry(_key, value, acc, _fun), do: {value, acc}

  defp key_string(key) when is_binary(key) or is_atom(key), do: to_string(key)
  defp key_string(key), do: inspect(key)

  # -- open mode ------------------------------------------------------------

  defp open_map(map), do: Map.new(map, fn {k, v} -> {key_string(k), open(v)} end)

  # A `{module, opts}` pair is where an adapter's credentials live, so anything
  # shaped like one is walked in closed mode, an Erlang module (`:my_sink`)
  # included: `inspect/1` would print its opts as written.
  defp open({module, opts} = pair) when is_atom(module) do
    if module_atom?(module) or is_list(opts) or is_map(opts),
      do: closed_pair(pair),
      else: inspect(pair)
  end

  defp open({module, fun, args}) when is_atom(module) and is_atom(fun) and is_list(args) do
    Exception.format_mfa(module, fun, length(args))
  end

  defp open(%_{} = struct), do: struct |> Map.from_struct() |> open_map()
  defp open(map) when is_map(map), do: open_map(map)

  defp open(list) when is_list(list) do
    cond do
      module_pairs?(list) or pairs?(list) -> Enum.map(list, &open/1)
      list != [] and Keyword.keyword?(list) -> list |> Map.new() |> open_map()
      true -> Enum.map(list, &open/1)
    end
  end

  defp open(binary) when is_binary(binary), do: binary

  # Before the atom clause: `true`/`false`/`nil` are atoms, and stringifying
  # them would turn `enabled: true` into `"true"`.
  defp open(other) when is_number(other) or is_boolean(other) or is_nil(other), do: other

  defp open(atom) when is_atom(atom) do
    case Atom.to_string(atom) do
      "Elixir." <> _ -> inspect(atom)
      _ -> Atom.to_string(atom)
    end
  end

  defp open(fun) when is_function(fun), do: "#Function"
  defp open(other), do: inspect(other)

  # -- closed mode ----------------------------------------------------------

  defp closed_map(map), do: Map.new(map, fn {k, v} -> {key_string(k), closed_entry(k, v)} end)

  defp closed_entry(key, value), do: closed(key_string(key), value)

  defp closed("headers", value), do: redact_headers(value)

  defp closed(key, value) do
    case structural(value) do
      {:ok, converted} -> converted
      :error -> leaf(key, value)
    end
  end

  # Rules 2-6: shapes whose contents are walked in closed mode.
  defp structural({module, _opts} = pair) when is_atom(module) do
    if module_atom?(module), do: {:ok, closed_pair(pair)}, else: :error
  end

  defp structural({module, fun, args}) when is_atom(module) and is_atom(fun) and is_list(args) do
    {:ok, Exception.format_mfa(module, fun, length(args))}
  end

  defp structural(value)
       when is_number(value) or is_boolean(value) or is_nil(value) or is_atom(value) or
              is_function(value),
       do: {:ok, open(value)}

  defp structural(%_{} = struct), do: {:ok, struct |> Map.from_struct() |> closed_map()}
  defp structural(map) when is_map(map), do: {:ok, closed_map(map)}

  defp structural(list) when is_list(list) do
    cond do
      module_pairs?(list) ->
        {:ok, Enum.map(list, &closed_pair/1)}

      list != [] and Keyword.keyword?(list) ->
        {:ok, list |> Map.new() |> closed_map()}

      Enum.all?(list, &structured?/1) ->
        {:ok, Enum.map(list, &closed("", &1))}

      true ->
        :error
    end
  end

  defp structural(_other), do: :error

  defp structured?(%_{}), do: true
  defp structured?(map) when is_map(map), do: true
  defp structured?(list) when is_list(list), do: list != [] and Keyword.keyword?(list)

  defp structured?({module, _opts}) when is_atom(module), do: module_atom?(module)

  defp structured?(_other), do: false

  # Rules 7-9.
  defp leaf(key, value) when key in @url_keys and is_binary(value), do: redact_url(value)
  defp leaf(key, value) when key in @shown_keys, do: open(value)
  defp leaf(_key, _value), do: @redacted

  defp closed_pair({module, opts}) do
    %{"module" => inspect(module), "opts" => closed_opts(opts)}
  end

  defp closed_opts([]), do: %{}
  defp closed_opts(opts) when is_list(opts), do: closed("opts", opts)
  defp closed_opts(opts) when is_map(opts), do: closed_map(opts)
  defp closed_opts(opts), do: closed("opts", opts)

  # Header names are kept, so an operator can see a sink sends `authorization`
  # without seeing the credential. Anything that isn't name/value pairs is
  # hidden whole.
  defp redact_headers(headers) when is_list(headers) or is_map(headers) do
    if Enum.all?(headers, &match?({_name, _value}, &1)) do
      Map.new(headers, fn {name, _value} -> {key_string(name), @redacted} end)
    else
      @redacted
    end
  end

  defp redact_headers(_headers), do: @redacted

  # A list of `{module, opts}` pairs — `sinks: [{Ankusa.Sink.Log, []}]` — is
  # also a valid keyword list (a module *is* an atom), so it has to be
  # recognized before the keyword-list branch would fold it into a map.
  defp module_pairs?([{module, _opts} | _] = list) when is_atom(module) do
    module_atom?(module) and
      Enum.all?(list, fn
        {mod, _opts} when is_atom(mod) -> module_atom?(mod)
        _ -> false
      end)
  end

  defp module_pairs?(_list), do: false

  # A list whose every element is an `{atom, list | map}` pair is a list of
  # adapters, not a keyword list of settings, whatever the atoms are called.
  defp pairs?([_ | _] = list) do
    Enum.all?(list, fn
      {module, opts} when is_atom(module) -> is_list(opts) or is_map(opts)
      _ -> false
    end)
  end

  defp pairs?(_list), do: false

  defp module_atom?(atom) do
    case Atom.to_string(atom) do
      "Elixir." <> _ -> true
      _ -> false
    end
  end

  # Userinfo and query values are credentials in practice (`user:pass@`,
  # `https://token@host`, `?token=...&sig=...`). Done by string replacement of
  # the parsed parts, so the rest of the URL is shown exactly as written.
  defp redact_url(binary) do
    %URI{userinfo: userinfo, query: query} = URI.parse(binary)

    binary
    |> redact_query(query)
    |> redact_userinfo(userinfo)
  end

  defp redact_query(binary, query) when is_binary(query) and query != "" do
    redacted =
      query
      |> String.split("&")
      |> Enum.map_join("&", fn part ->
        case String.split(part, "=", parts: 2) do
          [name, _value] -> name <> "=" <> @redacted
          [_value] -> @redacted
        end
      end)

    String.replace(binary, "?" <> query, "?" <> redacted, global: false)
  end

  defp redact_query(binary, _query), do: binary

  defp redact_userinfo(binary, userinfo) when is_binary(userinfo) do
    replacement =
      case String.split(userinfo, ":", parts: 2) do
        [user, _password] -> user <> ":" <> @redacted
        [_token] -> @redacted
      end

    String.replace(binary, userinfo <> "@", replacement <> "@", global: false)
  end

  defp redact_userinfo(binary, _userinfo), do: binary
end
