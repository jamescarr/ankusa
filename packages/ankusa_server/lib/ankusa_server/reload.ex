defmodule AnkusaServer.Reload do
  @moduledoc """
  Config reload: re-read the file while the node runs and apply what may change.

  Triggered by `SIGHUP` (`AnkusaServer.Reload.Signal`) or `POST /v1/config/reload`
  (`Ankusa.Admin.Router`, scope `config:write` + `tenant:*`). Either way:

    1. the file is loaded again (`AnkusaServer.Config.load!/1`): the environment
       is substituted, `config_version` migrated, every `${file:}` / `${sm:}`
       reference resolved again (a reference that fails keeps the value of the
       last good load, see `AnkusaServer.Secrets`) and the whole file validated;
    2. the document is compared, key by key, with the one the running config was
       built from. Each key that differs is *reloadable* or not;
    3. a change to any non-reloadable key refuses the reload
       (`{:error, {:not_reloadable, keys}}`, `409` over HTTP) and nothing is
       applied, so the file and the node never half-agree;
    4. otherwise `Ankusa.Reload.apply_config/3` applies the reloadable fields
       atomically and writes the `config.reload` audit row, then `log.level` /
       `log.format` are applied if the file changed them.

  ## Reloadable keys

  | Key | Notes |
  | --- | --- |
  | `log` | `log.level` and `log.format`, only when the file's value changed: a level set at runtime (`PUT /v1/log-level`) survives a reload that does not touch `log` |
  | `rate_limits` | everything but `rate_limits.scope` |
  | `sources` | with `source_store.type: static`: rotating a secret, adding or removing a source. For any other store the file's sources are seeds, and a change is logged and not applied |
  | `routes.seed` | the difference is applied through the routes API; every seed route needs an `id` |
  | `dispatch.concurrency`, `dispatch.sink_concurrency`, `dispatch.tenants` | the next scheduling step |
  | `metrics.detail` | rebuilds the metric definitions: counters restart from zero |
  | `admin.gauge_interval_ms` | restarts the gauge poller |

  `config_version` itself is not compared: the loader has already migrated it.
  Every other key needs a restart, and the `409` names each one that changed.

  Reloads are serialized per instance: a second trigger waits for the first.
  """

  require Logger

  alias Ankusa.Audit.Context
  alias AnkusaServer.{Config, ConfigError, Secrets}

  # A changed key is reloadable when its path starts with one of these…
  @reloadable [
    ["log"],
    ["rate_limits"],
    ["sources"],
    ["routes", "seed"],
    ["dispatch", "concurrency"],
    ["dispatch", "sink_concurrency"],
    ["dispatch", "tenants"],
    ["metrics", "detail"],
    ["admin", "gauge_interval_ms"]
  ]

  # …unless it starts with one of these: inside a reloadable section, but the
  # instance's shape depends on it.
  @pinned [["rate_limits", "scope"]]

  @typedoc "What the last good load looked like: one digest per key, never a value."
  @type state :: %{digests: %{[term()] => binary()}, secrets: Secrets.t()}

  @doc """
  Remember what `loaded` (the boot's `AnkusaServer.Config.load!/1`) built
  `instance` from, for `reload/2` to compare the next load with.
  """
  @spec remember(atom(), Config.loaded()) :: :ok
  def remember(instance, %{doc: doc, secrets: %Secrets{} = secrets}),
    do: :persistent_term.put({__MODULE__, instance}, %{digests: digests(doc), secrets: secrets})

  @doc """
  Reload `instance` from its config file.

  Options: `:path` and `:env` as for `AnkusaServer.Config.load!/1`, `refresh:
  true` to bypass `secrets.cache_ttl_ms`, and `:audit`, the
  `Ankusa.Audit.Context` of whoever asked (default: the system).

  Returns `{:ok, summary}` (`t:Ankusa.Reload.summary/0`: the dotted keys that
  changed, the source ids whose spec changed, the references that kept their
  previous value) or `{:error, error}` (`t:Ankusa.Reload.error/0`) with nothing
  applied.
  """
  @spec reload(atom(), keyword()) ::
          {:ok, Ankusa.Reload.summary()} | {:error, Ankusa.Reload.error()}
  def reload(instance \\ :default, opts \\ []) do
    :global.trans(
      {{__MODULE__, instance}, self()},
      fn -> locked(instance, opts) end,
      [node()],
      :infinity
    )
  end

  defp locked(instance, opts) do
    with {:ok, state} <- recall(instance),
         {:ok, loaded} <- load(state, opts),
         {:ok, paths} <- reloadable(changed(state.digests, digests(loaded.doc))),
         {:ok, %{sources: sources}} <-
           Ankusa.Reload.apply_config(instance, loaded.config,
             audit: Keyword.get(opts, :audit) || Context.system(),
             keys: Enum.map(paths, &Config.render_path/1)
           ) do
      applied(instance, loaded, paths, sources)
    else
      {:error, reason} = error ->
        Logger.warning("[ankusa] config reload refused: #{describe(reason)}")
        error
    end
  end

  defp recall(instance) do
    with {:ok, _config} <- Ankusa.fetch_config(instance),
         %{} = state <- :persistent_term.get({__MODULE__, instance}, nil) do
      {:ok, state}
    else
      _ -> {:error, {:invalid_config, "instance #{inspect(instance)} is not running"}}
    end
  end

  defp load(state, opts) do
    {:ok,
     Config.load!(
       Keyword.put(Keyword.take(opts, [:path, :env, :refresh]), :secrets, state.secrets)
     )}
  rescue
    error in [ConfigError, ArgumentError] -> {:error, {:invalid_config, error.message}}
  end

  # ── what changed ────────────────────────────────────────────────────────────

  # One digest per leaf of the document, by the path to it: two loads can be
  # compared without keeping a secret in memory a second time.
  defp digests(doc), do: flatten(doc, [], %{})

  defp flatten(map, path, acc) when is_map(map) and not is_struct(map) and map_size(map) > 0,
    do: Enum.reduce(map, acc, fn {key, value}, acc -> flatten(value, [key | path], acc) end)

  defp flatten([_ | _] = list, path, acc) do
    list
    |> Enum.with_index()
    |> Enum.reduce(acc, fn {value, index}, acc -> flatten(value, [index | path], acc) end)
  end

  defp flatten(leaf, path, acc),
    do: Map.put(acc, Enum.reverse(path), :crypto.hash(:sha256, :erlang.term_to_binary(leaf)))

  defp changed(old, new) do
    (Map.keys(old) ++ Map.keys(new))
    |> Enum.uniq()
    |> Enum.filter(&(Map.get(old, &1) != Map.get(new, &1)))
    |> Enum.reject(&(&1 == ["config_version"]))
    |> Enum.sort()
  end

  defp reloadable(paths) do
    case Enum.reject(paths, &reloadable?/1) do
      [] -> {:ok, paths}
      refused -> {:error, {:not_reloadable, Enum.map(refused, &Config.render_path/1)}}
    end
  end

  defp reloadable?(path) do
    Enum.any?(@reloadable, &List.starts_with?(path, &1)) and
      not Enum.any?(@pinned, &List.starts_with?(path, &1))
  end

  # ── applied ─────────────────────────────────────────────────────────────────

  defp applied(instance, loaded, paths, sources) do
    apply_log(instance, loaded.log, paths)
    Secrets.log_kept(loaded.secrets.kept)
    warn_unpublished(instance, paths)
    remember(instance, loaded)
    keys = Enum.map(paths, &Config.render_path/1)

    Logger.info(
      "[ankusa] config reloaded: #{length(keys)} key(s) changed, #{length(sources)} source(s) changed, " <>
        "#{length(loaded.secrets.kept)} secret(s) kept"
    )

    {:ok,
     %{
       keys: keys,
       sources: sources,
       kept: Enum.map(loaded.secrets.kept, &%{path: &1.path, reason: reason(&1.reason)})
     }}
  end

  # Only a key the file changed: a level an operator set at runtime is theirs
  # until the file says otherwise.
  defp apply_log(instance, log, paths) do
    if Enum.any?(paths, &List.starts_with?(&1, ["log", "level"])),
      do: {:ok, _previous} = Ankusa.LogLevel.set(log.level)

    if Enum.any?(paths, &List.starts_with?(&1, ["log", "format"])) do
      with {:error, reason} <- AnkusaServer.LogFormatter.install(log.format, instance) do
        Logger.error("[ankusa] log.format #{log.format} did not start (#{inspect(reason)})")
      end
    end

    :ok
  end

  # `sources` of a store that keeps its sources elsewhere are seeds.
  defp warn_unpublished(instance, paths) do
    with true <- Enum.any?(paths, &List.starts_with?(&1, ["sources"])),
         {:ok, %Ankusa.Config{source_store: {mod, _}}} when mod != Ankusa.SourceStore.Static <-
           Ankusa.fetch_config(instance) do
      Logger.warning(
        "[ankusa] instance #{inspect(instance)} does not run source_store.type: static; " <>
          "the sources in the file were not re-published"
      )
    end

    :ok
  end

  defp reason(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp reason(reason), do: inspect(reason)

  defp describe({:not_reloadable, keys}),
    do: "these keys changed and cannot be reloaded: " <> Enum.join(keys, ", ")

  defp describe({:invalid_config, message}), do: message
  defp describe(other), do: inspect(other)
end
