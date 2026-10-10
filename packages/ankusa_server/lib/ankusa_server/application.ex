defmodule AnkusaServer.Application do
  @moduledoc """
  Boot: read the operator's config file, then start exactly one
  `Ankusa.Instance`.

  The instance is owned here rather than by `config :ankusa, autostart: true`
  so there is one place that decides what runs: the YAML file. Core's
  `Ankusa.Application` still starts `Ankusa.Registry` (this application depends
  on core, so it is started first), and `Ankusa.Instance` registers every
  component under this instance's name.

  A bad config exits 78 (`EX_CONFIG` from `sysexits.h`) after printing the
  message — a config error is an operator mistake to be read and fixed, not a
  BEAM crash dump to be decoded.
  """

  use Application

  require Logger

  @impl true
  def start(_type, _args) do
    if Application.get_env(:ankusa_server, :autostart, true) do
      start_instance()
    else
      # `mix test` (and any embedder that wants the supervision tree without a
      # running instance): read no config, bind no port. Otherwise the suite
      # would fight a developer's own Ankusa for 4000/4002 and write into ./data.
      Supervisor.start_link([], strategy: :one_for_one, name: AnkusaServer.Supervisor)
    end
  end

  defp start_instance do
    loaded = AnkusaServer.Config.load_or_halt!()

    Logger.configure(level: loaded.log_level)
    config = loaded.config

    Logger.info(
      "[ankusa] ankusa_server #{version()} roles=#{inspect(config.roles)} http=#{config.port} " <>
        "admin=#{admin(config)} routes=#{routes(config)} wal=#{Ankusa.Queue.label(config.wal)} " <>
        "sources=#{sources(config)}"
    )

    # The GCS token cache first: the instance's blob store asks it for tokens.
    Supervisor.start_link([AnkusaServer.GcsToken, {Ankusa.Instance, config}],
      strategy: :one_for_one,
      name: AnkusaServer.Supervisor
    )
  end

  @doc "This image's version. Independently versioned from the Hex packages."
  @spec version() :: String.t()
  def version, do: app_version(:ankusa_server)

  @doc "The version of `ankusa` core this image was built against."
  @spec core_version() :: String.t()
  def core_version, do: app_version(:ankusa)

  @doc """
  The configured source ids, sorted and comma-joined, for the boot banner and
  `check-config`.
  """
  @spec sources(Ankusa.Config.t()) :: String.t()
  def sources(%Ankusa.Config{source_store: {Ankusa.SourceStore.Static, opts}}) do
    opts
    |> Keyword.get(:sources, %{})
    |> Map.keys()
    |> Enum.sort()
    |> Enum.join(",")
  end

  def sources(_config), do: "?"

  defp admin(%{admin: %{enabled: true, port: port}}), do: to_string(port)
  defp admin(_config), do: "off"

  # Off, or on and which store the definitions live in: the one thing about
  # routes an operator needs from the banner is whether an edge is enforcing
  # anything and where its definitions come from. Core starts the store, the
  # guard and the management listener only when routes are enabled *and* this
  # node has the `:edge` role, so a dispatch-only node that set
  # `routes.enabled: true` enforces nothing and must not claim otherwise.
  #
  # Only the store's module name is printed: `store.url` is a connection
  # string, and it may carry a password.
  defp routes(%{routes: %{enabled: true, store: {mod, _opts}}} = config) do
    if Ankusa.Config.role?(config, :edge) do
      "on:#{mod |> Module.split() |> List.last()}"
    else
      "n/a (no edge role)"
    end
  end

  defp routes(%{routes: %{enabled: false}}), do: "off"

  defp routes(_config), do: "?"

  defp app_version(app) do
    # `eval` runs without the applications started, so the spec has to be
    # loaded explicitly. `:already_loaded` is the normal case in a release.
    _ = Application.load(app)
    app |> Application.spec(:vsn) |> List.to_string()
  end
end
