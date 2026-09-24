defmodule Ankusa.Admin.Redact do
  @moduledoc """
  Turn a `%Ankusa.Config{}` into a JSON-encodable map safe to hand to anyone
  who can reach the admin API.

  Redaction is by key name, because Ankusa does not know which of a source's
  opts are secret — a deployment does. Any key named `secret`, `password`,
  `secret_access_key`, `token`, `api_tokens`, or `sasl` becomes `"[REDACTED]"`,
  and any string that parses as a URI with `user:pass` userinfo keeps the user
  and loses the password. A botched hook could take a deployment down over a
  leaked Stripe secret or a Postgres password, so the default for these names
  is "never serialized", and a new secret-shaped opt is covered by adding its
  key here.

  The shape matches the config's own nesting, so no reading logic is needed to
  find a field:

    * `{module, opts}` becomes `%{"module" => "Ankusa.Sink.Log", "opts" => %{}}`
      (module names as `inspect/1` strings, so no `Elixir.` prefix).
    * Keyword lists become maps; other lists stay lists.
    * Atoms become strings, functions become `"#Function"`.

  Sources need no special case: `config.source_store` is
  `{Ankusa.SourceStore.Static, sources: %{...}}`, so they appear under that
  module's `"opts"` — exactly where the operator declared them.
  """

  @redacted "[REDACTED]"

  @secret_keys ~w(secret password secret_access_key token api_tokens sasl)

  @doc "A redacted, JSON-encodable view of the whole config."
  @spec config(Ankusa.Config.t()) :: map()
  def config(%Ankusa.Config{} = config) do
    config
    |> Map.from_struct()
    |> redact_map()
  end

  defp redact_map(map), do: Map.new(map, fn {k, v} -> {to_string(k), redact(to_string(k), v)} end)

  defp redact(key, value) do
    if key in @secret_keys, do: @redacted, else: convert(value)
  end

  # The framework's `{module, opts}` pair, everywhere it appears: the WAL,
  # route resolver, source store, verifiers, dedup keys, and sinks.
  defp convert({module, opts}) when is_atom(module) do
    %{"module" => inspect(module), "opts" => convert_opts(opts)}
  end

  defp convert(%_{} = struct), do: struct |> Map.from_struct() |> redact_map()

  defp convert(map) when is_map(map), do: redact_map(map)

  defp convert(list) when is_list(list) do
    cond do
      module_pairs?(list) -> Enum.map(list, &convert/1)
      list != [] and Keyword.keyword?(list) -> redact_map(Map.new(list))
      true -> Enum.map(list, &convert/1)
    end
  end

  defp convert(binary) when is_binary(binary), do: redact_userinfo(binary)

  # Before the atom clause: `true`/`false`/`nil` are atoms, and stringifying
  # them would turn `enabled: true` into `"true"`.
  defp convert(other) when is_number(other) or is_boolean(other) or is_nil(other), do: other

  defp convert(atom) when is_atom(atom) do
    case Atom.to_string(atom) do
      "Elixir." <> _ -> inspect(atom)
      _ -> Atom.to_string(atom)
    end
  end

  defp convert(fun) when is_function(fun), do: "#Function"

  defp convert(other), do: inspect(other)

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

  defp module_atom?(atom) do
    case Atom.to_string(atom) do
      "Elixir." <> _ -> true
      _ -> false
    end
  end

  # Keyword-list opts keep their own key-based redaction; a `{mod, opts}` pair
  # always carries a keyword list (or a map, from a hand-built config).
  defp convert_opts(opts) when is_list(opts) do
    if opts == [], do: %{}, else: redact_map(Map.new(opts))
  end

  defp convert_opts(opts) when is_map(opts), do: redact_map(opts)
  defp convert_opts(opts), do: convert(opts)

  defp redact_userinfo(binary) do
    case URI.parse(binary) do
      %URI{userinfo: userinfo} when is_binary(userinfo) ->
        case String.split(userinfo, ":", parts: 2) do
          [user, _password] ->
            String.replace(binary, userinfo <> "@", user <> ":" <> @redacted <> "@",
              global: false
            )

          _no_password ->
            binary
        end

      _ ->
        binary
    end
  end
end
