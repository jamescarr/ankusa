defmodule AnkusaGatewayServer.Document do
  @moduledoc """
  The config file as a document, before it is validated: locate and parse
  it, migrate its `config_version`, substitute `${…}` references, and apply
  the environment's overrides. Both images (`AnkusaServer.Config`,
  `AnkusaGatewayServer.Config`) run their file through these steps in this
  order; each brings its own paths, migration map and override table.
  """

  import AnkusaGatewayServer.Schema, only: [render_path: 1, type_error: 3]

  alias AnkusaGatewayServer.ConfigError

  # `${NAME}`, `${NAME:-default}`, `${file:/path}`, `${sm:provider:ref}`. A file
  # path or a reference holds no `$` or `}`: `${file:${DIR}/key}` is spelled
  # `secret_file: ${DIR}/key`.
  @var_re ~r/\$\{(?:file:([^}$]+)|sm:([a-z0-9_-]+):([^}$]+)|([A-Z0-9_]+)(?::-([^}]*))?)\}/

  # The `claim_check` keys `config_version: 2` moved to `gateway:`, and where.
  @claim_check_moved %{
    "port" => "gateway.port",
    "ip" => "gateway.ip",
    "tls" => "gateway.tls",
    "auth" => "gateway.auth",
    "max_concurrent_reads" => "gateway.max_concurrent_requests",
    "integrity_check" => "gateway.claims.integrity_check"
  }

  @auth_modes ~w(none token jwt mtls)

  # ── locate + parse ──────────────────────────────────────────────────────────

  @doc """
  The YAML document at `path_override`, else `$ANKUSA_CONFIG`, else
  `opts[:default_path]`, else `opts[:fallback_path]`; with none of them on
  disk, `opts[:on_missing].()`. A file named explicitly that is not there is an
  error.
  """
  @spec read!(String.t() | nil, map(), keyword()) :: map()
  def read!(path_override, env, opts) do
    case path_override || env["ANKUSA_CONFIG"] do
      nil -> read_any!(env, opts)
      path -> read_yaml!(path, env)
    end
  end

  defp read_any!(env, opts) do
    default = Keyword.fetch!(opts, :default_path)
    fallback = Keyword.fetch!(opts, :fallback_path)

    cond do
      File.exists?(default) -> read_yaml!(default, env)
      File.exists?(fallback) -> read_yaml!(fallback, env)
      true -> Keyword.fetch!(opts, :on_missing).()
    end
  end

  defp read_yaml!(path, env) do
    case YamlElixir.read_from_file(path) do
      {:ok, nil} ->
        %{}

      {:ok, doc} when is_map(doc) ->
        doc

      {:ok, other} ->
        raise ConfigError,
          message: "#{path}: expected a YAML mapping at the top level, got #{inspect(other)}"

      {:error, %YamlElixir.FileNotFoundError{}} ->
        raise ConfigError, message: "config file #{path} not found#{configured_by(env, path)}"

      {:error, error} ->
        raise ConfigError, message: "#{path}: #{Exception.message(error)}"
    end
  end

  defp configured_by(env, path) do
    if env["ANKUSA_CONFIG"] == path, do: " (ANKUSA_CONFIG)", else: ""
  end

  # ── migrate ─────────────────────────────────────────────────────────────────

  @doc """
  The document at `current`: `config_version` (1 when absent) and every step
  of `migrations` (`from => fun`, a document of version `from` into one of
  `from + 1`) from there up. A version the caller does not read, in either
  direction, is an error: an older image must not guess at a newer file.
  """
  @spec migrate!(map(), pos_integer(), %{pos_integer() => (map() -> map())}) :: map()
  def migrate!(doc, current, migrations) do
    path = ["config_version"]

    version =
      case Map.get(doc, "config_version") do
        nil -> 1
        version when is_integer(version) and version >= 1 -> version
        other -> raise ConfigError, message: type_error(path, "a positive integer", other)
      end

    if version > current do
      raise ConfigError,
        message:
          "config_version: #{version} is newer than this image reads (up to #{current}); " <>
            "run an image that reads it, or lower config_version"
    end

    doc =
      Enum.reduce(version..current//1, doc, fn
        ^current, doc ->
          doc

        from, doc ->
          case migrations do
            %{^from => step} ->
              step.(doc)

            _ ->
              raise ConfigError,
                message: "config_version: no migration from version #{from} to #{from + 1}"
          end
      end)

    Map.put(doc, "config_version", current)
  end

  @doc false
  # The `claim_check` keys `config_version: 2` moved to `gateway:`, and where:
  # what a version-2 file that still names one is told.
  @spec moved_keys() :: %{String.t() => String.t()}
  def moved_keys, do: @claim_check_moved

  @doc false
  # Version 1 -> 2 (ADR 0005): the gateway left core, and its keys left
  # `claim_check:` for `gateway:`. `claim_check.port|ip|tls` become
  # `gateway.port|ip|tls`, `max_concurrent_reads` becomes
  # `gateway.max_concurrent_requests`, `integrity_check` becomes
  # `gateway.claims.integrity_check`, and the `auth` block (one `mode` and its
  # block) becomes `gateway.auth` with one authenticator of that type (none for
  # `mode: none`). A target the file already sets is an error, never a silent
  # winner; so is a `${…}` mode, which cannot be read before substitution.
  @spec migrate_gateway_keys(map()) :: map()
  def migrate_gateway_keys(%{"claim_check" => %{} = claim_check} = doc) do
    moved = Map.take(claim_check, Map.keys(@claim_check_moved))

    if moved == %{} do
      doc
    else
      gateway =
        case Map.get(doc, "gateway") do
          nil -> %{}
          %{} = gateway -> gateway
          other -> raise ConfigError, message: type_error(["gateway"], "a mapping", other)
        end

      gateway =
        Enum.reduce(moved, gateway, fn {key, value}, gateway ->
          target = gateway_target(key)

          if get_in(gateway, target) != nil do
            raise ConfigError,
              message: "claim_check.#{key} and gateway.#{Enum.join(target, ".")} are both set"
          end

          put_path(gateway, target, migrated_value(key, value))
        end)

      doc
      |> Map.put("claim_check", Map.drop(claim_check, Map.keys(moved)))
      |> Map.put("gateway", gateway)
    end
  end

  def migrate_gateway_keys(doc), do: doc

  defp gateway_target("max_concurrent_reads"), do: ["max_concurrent_requests"]

  defp gateway_target("integrity_check"), do: ["claims", "integrity_check"]

  defp gateway_target(key), do: [key]

  defp migrated_value("auth", %{} = auth) do
    mode = Map.get(auth, "mode")

    if is_binary(mode) and String.contains?(mode, "${") do
      raise ConfigError,
        message:
          "claim_check.auth.mode: a templated mode cannot be migrated; write gateway.auth " <>
            "and set config_version: 2"
    end

    authenticators =
      case mode do
        none when none in [nil, "none"] -> []
        "token" -> [%{"type" => "token", "tokens" => Map.get(auth, "tokens")}]
        "jwt" -> [Map.put(Map.get(auth, "jwt") || %{}, "type", "jwt")]
        "mtls" -> [Map.put(Map.get(auth, "mtls") || %{}, "type", "mtls")]
        other -> raise ConfigError, message: "claim_check.auth.mode: " <> unknown(other)
      end

    case Map.fetch(auth, "allow_insecure") do
      {:ok, allow} -> %{"allow_insecure" => allow, "authenticators" => authenticators}
      :error -> %{"authenticators" => authenticators}
    end
  end

  defp migrated_value(_key, value), do: value

  defp unknown(value),
    do: "unknown value #{inspect(value)}; expected one of #{Enum.join(@auth_modes, ", ")}"

  # ── interpolate ─────────────────────────────────────────────────────────────

  @doc """
  Every string of `value` with `${NAME}` / `${NAME:-default}` substituted from
  `env`; `${file:…}` and `${sm:…}` references are left as templates for
  `AnkusaGatewayServer.Secrets`. `opts[:secret_file_site?]` (a predicate on a
  path, default none) says which maps may carry `secret_file:`, which becomes
  `secret:` read from that file.
  """
  @spec interpolate(term(), map(), [term()], keyword()) :: term()
  def interpolate(value, env, path, opts \\ []) do
    walk(value, env, path, Keyword.get(opts, :secret_file_site?, fn _path -> false end))
  end

  # ── 2. interpolate ──────────────────────────────────────────────────────────

  defp walk(value, env, path, site?) when is_map(value) do
    value = Map.new(value, fn {k, v} -> {k, walk(v, env, path ++ [k], site?)} end)
    if site?.(path), do: expand_secret_file(value, path), else: value
  end

  defp walk(value, env, path, site?) when is_list(value) do
    value
    |> Enum.with_index()
    |> Enum.map(fn {v, i} -> walk(v, env, path ++ [i], site?) end)
  end

  defp walk(value, env, path, _site?) when is_binary(value) do
    # Whatever in the file still reads `${` once the references are taken out
    # did not match the grammar (`${stripe_secret}`, `${A-B}`): left alone it
    # would become a literal secret or URL, so it is an error. Only the template
    # is checked: a substituted value may contain `${` itself.
    if value |> then(&Regex.replace(@var_re, &1, "")) |> String.contains?("${") do
      raise ConfigError,
        message:
          "#{render_path(path)}: unresolved ${…} (use ${NAME}, ${NAME:-default}, " <>
            "${file:/abs/path} or ${sm:<provider>:<ref>}; variable names are [A-Z0-9_]+; " <>
            "use ${NAME:-} for an empty default)"
    end

    value
    |> parts()
    |> Enum.map(&substitute(&1, env, path))
    |> AnkusaGatewayServer.Secrets.template(path)
  end

  defp walk(value, _env, _path, _site?), do: value

  # The value as literal text and references, in order. A variable is
  # substituted here; a file or secret manager reference stays for
  # `AnkusaGatewayServer.Secrets`, so a file that cannot be read fails with the
  # same dotted key as an unset variable.
  defp parts(value) do
    {parts, position} =
      @var_re
      |> Regex.scan(value, return: :index)
      |> Enum.reduce({[], 0}, fn [{start, length} | groups], {parts, position} ->
        literal = binary_part(value, position, start - position)
        {[reference(value, groups) | [literal | parts]], start + length}
      end)

    tail = binary_part(value, position, byte_size(value) - position)
    Enum.reverse([tail | parts])
  end

  defp reference(value, groups) do
    case Enum.map(groups, &group(value, &1)) do
      [file | _] when is_binary(file) -> {:file, file}
      [_file, name, ref | _] when is_binary(name) -> {:sm, name, ref}
      [_file, _name, _ref, var, default] -> {:var, var, default}
      [_file, _name, _ref, var] -> {:var, var, nil}
    end
  end

  # An optional group that did not take part is `{-1, 0}` (or is cut off the end).
  defp group(_value, {-1, _length}), do: nil

  defp group(value, {start, length}), do: binary_part(value, start, length)

  defp substitute({:var, name, default}, env, path) do
    case Map.fetch(env, name) do
      {:ok, replacement} ->
        replacement

      # An empty default is still a default: `${NAME:-}` is how a file asks for empty.
      :error when is_binary(default) ->
        default

      :error ->
        raise ConfigError, message: "#{render_path(path)}: ${#{name}} is not set"
    end
  end

  defp substitute(part, _env, _path), do: part

  defp expand_secret_file(%{"secret_file" => files} = map, path) do
    if Map.has_key?(map, "secret") do
      raise ConfigError,
        message: "#{render_path(path)}: secret and secret_file are mutually exclusive"
    end

    key_path = path ++ ["secret_file"]

    secret =
      case files do
        [_ | _] ->
          files
          |> Enum.with_index()
          |> Enum.map(fn {file, i} -> file_ref!(file, key_path ++ [i]) end)

        file ->
          file_ref!(file, key_path)
      end

    map |> Map.delete("secret_file") |> Map.put("secret", secret)
  end

  defp expand_secret_file(map, _path), do: map

  defp file_ref!(file, path) when is_binary(file) and file != "",
    do: AnkusaGatewayServer.Secrets.file_template(file, path)

  defp file_ref!(other, path),
    do:
      raise(ConfigError, message: type_error(path, "a file path or a list of file paths", other))

  # ── environment overrides ───────────────────────────────────────────────────

  @doc """
  `doc` with each `{var, path}` of `overrides` whose `var` is set in `env`
  written at `path`.
  """
  @spec apply_overrides(map(), map(), [{String.t(), [String.t()]}]) :: map()
  def apply_overrides(doc, env, overrides) do
    Enum.reduce(overrides, doc, fn {var, path}, doc ->
      case Map.fetch(env, var) do
        {:ok, value} -> put_path(doc, path, value)
        :error -> doc
      end
    end)
  end

  @doc """
  `ANKUSA_CLAIM_CHECK_TOKEN`: the gateway's authenticator list becomes one
  `token` authenticator holding that token (`claims:read`, `tenant:*`), and
  `gateway.auth`'s other keys (`allow_insecure`) are kept as the file has them.
  Set but empty is an error, not a way to switch authentication off.
  """
  @spec claim_check_token(map(), map()) :: map()
  def claim_check_token(doc, env) do
    case Map.fetch(env, "ANKUSA_CLAIM_CHECK_TOKEN") do
      {:ok, ""} ->
        raise ConfigError, message: "ANKUSA_CLAIM_CHECK_TOKEN is set but empty"

      {:ok, token} ->
        authenticators = [
          %{
            "type" => "token",
            "tokens" => [
              %{
                "id" => "ANKUSA_CLAIM_CHECK_TOKEN",
                "token" => token,
                "scopes" => ["claims:read", "tenant:*"]
              }
            ]
          }
        ]

        put_path(doc, ["gateway", "auth", "authenticators"], authenticators)

      :error ->
        doc
    end
  end

  @doc """
  `doc` with `value` at `path`. A section that is present but empty (`http:`
  with every child commented out) parses as nil and takes the value like a
  missing one. Any other non-map is left alone for the walker to reject with
  its type error.
  """
  @spec put_path(term(), [term()], term()) :: term()
  def put_path(nil, path, value), do: put_path(%{}, path, value)
  def put_path(doc, [key], value) when is_map(doc), do: Map.put(doc, key, value)

  def put_path(doc, [key | rest], value) when is_map(doc),
    do: Map.put(doc, key, put_path(doc[key], rest, value))

  def put_path(doc, _path, _value), do: doc
end
