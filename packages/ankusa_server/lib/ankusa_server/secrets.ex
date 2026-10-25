defmodule AnkusaServer.Secrets do
  @moduledoc """
  Where the config file's secrets come from, and how they are read again.

  The loader (`AnkusaServer.Config`) leaves two kinds of reference in a string
  value for this module to resolve, after the environment overrides and before
  the file is validated:

    * `${file:/abs/path}`: the file's contents, with one trailing newline
      removed (what a Kubernetes secret volume or a Docker secret holds). The
      path is absolute and the file at most #{65_536} bytes. A source's
      `secret_file:` is the same thing under `secret:`.
    * `${sm:<provider>:<ref>}`: a secret manager, through the provider the file
      names under `secrets.providers.<provider>` (`type: aws` for AWS Secrets
      Manager, `type: vault` for HashiCorp Vault KV v2). The provider
      implements `Ankusa.SecretProvider`.

  Anything that is not a reference is left alone. `${NAME}` and
  `${NAME:-default}` never get here: the loader fills them from the environment
  first.

  ## Outage rule

  At boot, a reference that cannot be resolved is a `AnkusaServer.ConfigError`
  naming the dotted key and the reference (never a value). On a reload
  (`AnkusaServer.Reload`), a reference that fails keeps the value it had at the last good
  load and is reported in `:kept` and logged as a warning: a secret manager
  that is briefly down must not take verification down with it. A reference
  with no previous value (a new one) is still an error.

  ## Cache

  `secrets.cache_ttl_ms` (default `0`) is how long a `${sm:}` value fetched at
  one load is reused by the next. `0` reads the manager on every reload;
  raise it to protect a manager's API quota from a reload loop. A reload's
  `refresh: true` ignores it. Files are always read again. Within one load a
  reference is fetched once, however many keys use it.

  ## What reload does

  `AnkusaServer.Reload` loads the file again with the `t:t/0` of the last load
  (`AnkusaServer.Config.load!/1`'s `:secrets` option), so every reference is
  resolved again and an outage keeps the old value. Nothing here is applied to a
  running instance: that is the reload's job.
  """

  require Logger

  alias AnkusaServer.{Config, ConfigError}
  alias AnkusaServer.SecretProvider.{AwsSecretsManager, Vault}

  @max_file_bytes 65_536
  @default_timeout_ms 5_000
  @max_timeout_ms 60_000
  @max_ttl_ms 86_400_000
  @provider_name ~r/\A[a-z0-9_-]+\z/
  @loopback_hosts ["localhost", "127.0.0.1", "::1", "[::1]"]

  @provider_types %{"aws" => AwsSecretsManager, "vault" => Vault}
  @provider_keys %{
    "aws" => ~w(type region endpoint access_key_id secret_access_key session_token timeout_ms),
    "vault" => ~w(type address token namespace mount timeout_ms)
  }

  defmodule Template do
    @moduledoc false
    # A string value with references left in it: literal parts and
    # `{:file, path}` / `{:sm, provider, ref}` parts, in order. Inspecting it
    # shows only where it sits in the file: the literal parts may be secret.
    @derive {Inspect, only: [:path]}
    @enforce_keys [:parts, :path]
    defstruct [:parts, :path]

    @type t :: %__MODULE__{
            parts: [binary() | {:file, String.t()} | {:sm, String.t(), String.t()}],
            path: [term()]
          }
  end

  @typedoc "One reference: a file, or a secret in a manager."
  @type ref :: {:file, String.t()} | {:sm, String.t(), String.t()}

  @typedoc "A reference that failed on a reload and kept its previous value."
  @type kept :: %{ref: String.t(), path: String.t(), reason: atom()}

  @typedoc """
  What a load resolved: every reference's value and when it was fetched, and
  the references that kept an old value. Opaque to callers; `AnkusaServer.Reload` hands it
  back to the loader. Inspecting it shows only `:kept`.
  """
  @type t :: %__MODULE__{
          values: %{ref() => {binary(), integer()}},
          kept: [kept()]
        }

  @derive {Inspect, only: [:kept]}
  defstruct values: %{}, kept: []

  # ── building references (the loader's side) ─────────────────────────────────

  @doc false
  # Parts of one string value after the environment was substituted: a plain
  # binary when no reference is left, else a `Template`.
  @spec template([binary() | ref()], [term()]) :: binary() | Template.t()
  def template(parts, path) do
    Enum.each(parts, &check_ref!(&1, path))

    case merge(parts) do
      [] -> ""
      [literal] when is_binary(literal) -> literal
      parts -> %Template{parts: parts, path: path}
    end
  end

  @doc false
  @spec file_template(String.t(), [term()]) :: Template.t()
  def file_template(file, path),
    do: %Template{parts: [check_ref!({:file, file}, path)], path: path}

  defp check_ref!({:file, file} = ref, path) do
    if Path.type(file) != :absolute do
      raise ConfigError,
        message: "#{Config.render_path(path)}: a file secret needs an absolute path, got #{file}"
    end

    ref
  end

  defp check_ref!(part, _path), do: part

  defp merge(parts) do
    parts
    |> Enum.reduce([], fn
      "", acc -> acc
      part, [last | rest] when is_binary(part) and is_binary(last) -> [last <> part | rest]
      part, acc -> [part | acc]
    end)
    |> Enum.reverse()
  end

  # ── resolving ───────────────────────────────────────────────────────────────

  @doc false
  # Replace every `Template` in the interpolated document with its value.
  # `previous` is the `t()` of the load this one follows (nil at boot): its
  # values satisfy the cache and stand in for a reference that fails.
  @spec resolve(map(), t() | nil, keyword()) :: {map(), t()}
  def resolve(doc, previous, opts \\ []) do
    state = %{
      prev: if(previous, do: previous.values, else: %{}),
      reloading?: previous != nil,
      refresh: Keyword.get(opts, :refresh, false),
      now: System.monotonic_time(:millisecond),
      values: %{},
      kept: [],
      providers: :none,
      ttl: 0
    }

    {doc, state} = configure_providers(doc, state)
    {doc, state} = walk(doc, state)

    {doc, %__MODULE__{values: state.values, kept: Enum.reverse(state.kept)}}
  end

  # `secrets.providers` is resolved first, with no provider available: a
  # provider's own credentials come from the environment or a file, never from
  # another manager (nothing could resolve the first one).
  defp configure_providers(%{"secrets" => %{} = secrets} = doc, state) do
    {providers, state} = walk(Map.get(secrets, "providers"), state)

    state = %{
      state
      | providers: parse_providers(providers),
        ttl: parse_ttl(Map.get(secrets, "cache_ttl_ms"))
    }

    doc =
      if Map.has_key?(secrets, "providers"),
        do: put_in(doc, ["secrets", "providers"], providers),
        else: doc

    {doc, state}
  end

  defp configure_providers(doc, state), do: {doc, %{state | providers: %{}}}

  defp walk(%Template{} = template, state), do: resolve_template(template, state)

  defp walk(map, state) when is_map(map) do
    Enum.reduce(map, {%{}, state}, fn {key, value}, {acc, state} ->
      {value, state} = walk(value, state)
      {Map.put(acc, key, value), state}
    end)
  end

  defp walk(list, state) when is_list(list) do
    {values, state} =
      Enum.reduce(list, {[], state}, fn value, {acc, state} ->
        {value, state} = walk(value, state)
        {[value | acc], state}
      end)

    {Enum.reverse(values), state}
  end

  defp walk(other, state), do: {other, state}

  defp resolve_template(%Template{parts: parts, path: path}, state) do
    {iodata, state} =
      Enum.map_reduce(parts, state, fn
        part, state when is_binary(part) -> {part, state}
        ref, state -> fetch(ref, path, state)
      end)

    {IO.iodata_to_binary(iodata), state}
  end

  defp fetch(ref, path, state) do
    case state.values do
      %{^ref => {value, _at}} ->
        {value, state}

      _not_yet ->
        case obtain(ref, path, state) do
          {:ok, value, at} -> {value, %{state | values: Map.put(state.values, ref, {value, at})}}
          {:error, reason} -> failed(ref, reason, path, state)
        end
    end
  end

  defp obtain({:file, file}, _path, state) do
    with {:ok, contents} <- read_file(file), do: {:ok, contents, state.now}
  end

  defp obtain({:sm, name, id} = ref, path, state) do
    case cached(ref, state) do
      {:ok, _value, _at} = hit -> hit
      :miss -> ask(name, id, path, state)
    end
  end

  defp cached(ref, %{refresh: false, prev: prev, now: now, ttl: ttl}) do
    case prev do
      %{^ref => {value, at}} when now - at < ttl -> {:ok, value, at}
      _stale -> :miss
    end
  end

  defp cached(_ref, %{refresh: true}), do: :miss

  defp ask(name, _id, path, %{providers: :none}) do
    raise ConfigError,
      message:
        "#{Config.render_path(path)}: ${sm:#{name}:…} cannot configure a provider " <>
          "(use ${VAR} or ${file:/path} for a provider's own credentials)"
  end

  defp ask(name, id, path, state) do
    case state.providers do
      %{^name => {module, opts}} ->
        with {:ok, value} <- call(module, id, opts), do: {:ok, value, state.now}

      providers ->
        configured = providers |> Map.keys() |> Enum.sort() |> Enum.join(", ")

        raise ConfigError,
          message:
            "#{Config.render_path(path)}: ${sm:#{name}:…} names no provider " <>
              "(secrets.providers has: #{if configured == "", do: "none", else: configured})"
    end
  end

  # A provider is code that talks to the network: whatever it raises is its
  # failure to answer, reported without its arguments (a token).
  defp call(module, id, opts) do
    case module.fetch(id, opts) do
      {:ok, value} when is_binary(value) -> {:ok, value}
      {:error, reason} when reason in [:not_found, :unavailable] -> {:error, reason}
      _other -> {:error, :unavailable}
    end
  rescue
    _error -> {:error, :unavailable}
  catch
    _kind, _reason -> {:error, :unavailable}
  end

  defp failed(ref, reason, path, %{reloading?: true, prev: %{} = prev} = state)
       when is_map_key(prev, ref) do
    {value, at} = Map.fetch!(prev, ref)

    kept = %{ref: describe(ref), path: Config.render_path(path), reason: reason}

    {value, %{state | values: Map.put(state.values, ref, {value, at}), kept: [kept | state.kept]}}
  end

  defp failed(ref, reason, path, _state) do
    raise ConfigError,
      message: "#{Config.render_path(path)}: #{describe(ref)}: #{reason_text(reason)}"
  end

  defp describe({:file, file}), do: "${file:#{file}}"
  defp describe({:sm, name, id}), do: "${sm:#{name}:#{id}}"

  defp reason_text(:enoent), do: "file not found"
  defp reason_text(:eacces), do: "file is not readable"
  defp reason_text(:eisdir), do: "is a directory, not a file"
  defp reason_text(:too_large), do: "file is larger than #{@max_file_bytes} bytes"

  defp reason_text(:not_found),
    do: "secret not found (or not readable with the provider's credentials)"

  defp reason_text(:unavailable), do: "secret provider unavailable"
  defp reason_text(reason), do: "could not be read (#{inspect(reason)})"

  # One trailing newline comes off: an editor or `echo` adds it, and a secret
  # that ends in one is not something an operator writes on purpose.
  #
  # The path is the operator's own config file naming a file of this host (an
  # absolute path, checked when the reference is parsed), never request input:
  # a spec an API client submits does not go through the loader's `${file:}`.
  # sobelow_skip ["Traversal.FileModule"]
  defp read_file(file) do
    case File.open(file, [:read, :binary]) do
      {:ok, io} ->
        try do
          case IO.binread(io, @max_file_bytes + 1) do
            :eof -> {:ok, ""}
            {:error, reason} -> {:error, reason}
            data when byte_size(data) > @max_file_bytes -> {:error, :too_large}
            data -> {:ok, trim_newline(data)}
          end
        after
          File.close(io)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp trim_newline(data) do
    cond do
      String.ends_with?(data, "\r\n") -> binary_part(data, 0, byte_size(data) - 2)
      String.ends_with?(data, "\n") -> binary_part(data, 0, byte_size(data) - 1)
      true -> data
    end
  end

  # ── providers ───────────────────────────────────────────────────────────────

  defp parse_ttl(nil), do: 0
  defp parse_ttl(ttl) when is_integer(ttl) and ttl in 0..@max_ttl_ms, do: ttl

  defp parse_ttl(ttl) when is_binary(ttl) do
    case Integer.parse(ttl) do
      {int, ""} -> parse_ttl(int)
      _ -> bad_ttl(ttl)
    end
  end

  defp parse_ttl(ttl), do: bad_ttl(ttl)

  defp bad_ttl(ttl) do
    raise ConfigError,
      message: "secrets.cache_ttl_ms must be an integer in 0..#{@max_ttl_ms}, got #{inspect(ttl)}"
  end

  defp parse_providers(nil), do: %{}

  defp parse_providers(providers) when is_map(providers) do
    Map.new(providers, fn {name, spec} ->
      path = ["secrets", "providers", name]

      unless is_binary(name) and Regex.match?(@provider_name, name) do
        raise ConfigError,
          message:
            "#{Config.render_path(path)}: a provider name is [a-z0-9_-]+ (it appears in ${sm:<name>:…})"
      end

      {name, parse_provider(spec, path)}
    end)
  end

  defp parse_providers(other) do
    raise ConfigError,
      message:
        "secrets.providers: expected a mapping of provider name to provider, got #{inspect(other)}"
  end

  defp parse_provider(%{} = spec, path) do
    type = field!(spec, "type", path, required: true)
    module = Map.get(@provider_types, type)

    unless module do
      raise ConfigError,
        message:
          "#{Config.render_path(path ++ ["type"])} must be one of " <>
            "#{@provider_types |> Map.keys() |> Enum.sort() |> Enum.join(", ")}, got #{inspect(type)}"
    end

    Enum.each(spec, fn {key, _value} ->
      unless key in Map.fetch!(@provider_keys, type) do
        raise ConfigError, message: "#{Config.render_path(path)}: unknown key \"#{key}\""
      end
    end)

    {module, provider_opts(type, spec, path)}
  end

  defp parse_provider(other, path) do
    raise ConfigError,
      message: "#{Config.render_path(path)}: expected a mapping, got #{inspect(other)}"
  end

  defp provider_opts("aws", spec, path) do
    [
      region: field!(spec, "region", path, required: true),
      endpoint: spec |> field!("endpoint", path) |> url!(path ++ ["endpoint"]),
      access_key_id: field!(spec, "access_key_id", path),
      secret_access_key: field!(spec, "secret_access_key", path),
      session_token: field!(spec, "session_token", path),
      timeout_ms: timeout!(spec, path)
    ]
    |> compact()
  end

  defp provider_opts("vault", spec, path) do
    [
      address: spec |> field!("address", path, required: true) |> url!(path ++ ["address"]),
      token: field!(spec, "token", path, required: true),
      namespace: field!(spec, "namespace", path),
      mount: field!(spec, "mount", path),
      timeout_ms: timeout!(spec, path)
    ]
    |> compact()
  end

  defp compact(opts), do: Enum.reject(opts, fn {_key, value} -> is_nil(value) end)

  defp field!(spec, key, path, opts \\ []) do
    case Map.get(spec, key) do
      nil ->
        if opts[:required] do
          raise ConfigError,
            message: "#{Config.render_path(path)}: missing required key \"#{key}\""
        end

      "" ->
        raise ConfigError, message: "#{Config.render_path(path ++ [key])} must not be empty"

      value when is_binary(value) ->
        value

      other ->
        raise ConfigError,
          message:
            "#{Config.render_path(path ++ [key])}: expected a string, got #{inspect(other)}"
    end
  end

  defp timeout!(spec, path) do
    case Map.get(spec, "timeout_ms", @default_timeout_ms) do
      ms when is_integer(ms) and ms in 1..@max_timeout_ms ->
        ms

      other ->
        raise ConfigError,
          message:
            "#{Config.render_path(path ++ ["timeout_ms"])} must be an integer in " <>
              "1..#{@max_timeout_ms}, got #{inspect(other)}"
    end
  end

  defp url!(nil, _path), do: nil

  defp url!(url, path) do
    case URI.parse(url) do
      %URI{scheme: "https", host: host} when is_binary(host) and host != "" ->
        String.trim_trailing(url, "/")

      %URI{scheme: "http", host: host} when host in @loopback_hosts ->
        String.trim_trailing(url, "/")

      _other ->
        raise ConfigError,
          message:
            "#{Config.render_path(path)} must be an https:// URL " <>
              "(http:// is accepted only for a loopback address)"
    end
  end

  # ── reloading ───────────────────────────────────────────────────────────────

  @doc """
  Warn about each reference of `kept` (`t:kept/0`, what a reload could not
  resolve and kept the previous value of): the reference's place in the file and
  why, never a value.
  """
  @spec log_kept([kept()]) :: :ok
  def log_kept(kept), do: Enum.each(kept, &warn_kept/1)

  defp warn_kept(%{ref: ref, path: path, reason: reason}) do
    Logger.warning(
      "[ankusa] secret reload: #{ref} (#{path}) #{reason_text(reason)}; keeping the previous value"
    )
  end
end
