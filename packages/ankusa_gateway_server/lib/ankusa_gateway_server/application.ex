defmodule AnkusaGatewayServer.Application do
  @moduledoc """
  The gateway image's boot: read the config file (`AnkusaGatewayServer.Config`),
  set the log level and format, start tracing when asked, then start a core
  instance with no role and the gateway beside it.

  A bad config exits 78 after printing the message. On SIGTERM it drains the
  instance first (`prep_stop/1`, `Ankusa.Instance.Drain`): `/ready` flips to
  `503` before the tree stops.

  Nothing starts unless `config :ankusa_gateway_server, autostart: true` (the
  image's own config, outside `:test`): the full image depends on this
  application for its config machinery, and must not get a second node.
  """

  use Application

  require Logger

  alias AnkusaGatewayServer.{Config, LogFormatter, Tracing}

  @impl true
  def start(_type, _args) do
    if Application.get_env(:ankusa_gateway_server, :autostart, false) do
      start_gateway()
    else
      with {:ok, pid} <-
             Supervisor.start_link([],
               strategy: :one_for_one,
               name: AnkusaGatewayServer.Supervisor
             ),
           do: {:ok, pid, nil}
    end
  end

  @impl true
  def prep_stop(nil), do: nil

  def prep_stop(instance) do
    Ankusa.Instance.Drain.run(instance)
    instance
  end

  defp start_gateway do
    %{config: config, gateway: gateway, log: log, tracing: tracing} = Config.load_or_halt!()
    start_logging(log, config.instance)
    start_tracing(tracing)

    Logger.info(
      "[ankusa] ankusa_gateway_server #{version()} port=#{gateway.port} " <>
        "claims=#{if gateway.claims.enabled, do: "on", else: "off"} " <>
        "authenticators=#{authenticators(gateway)} metrics=#{metrics(config)}"
    )

    # The GCP token cache first: the blob stores ask it for tokens.
    with {:ok, pid} <-
           Supervisor.start_link(
             [
               AnkusaGatewayServer.GcpToken,
               {Ankusa.Instance, config},
               {Ankusa.Gateway, gateway}
             ],
             strategy: :one_for_one,
             name: AnkusaGatewayServer.Supervisor
           ),
         do: {:ok, pid, config.instance}
  end

  defp start_logging(%{level: level, format: format}, instance) do
    {:ok, _previous} = Ankusa.LogLevel.set(level)

    with {:error, reason} <- LogFormatter.install(format, instance) do
      Logger.error(
        "[ankusa] log.format #{format} did not start (#{inspect(reason)}); logging as text"
      )
    end
  end

  defp start_tracing(%{enabled: false}), do: :ok

  defp start_tracing(tracing) do
    case Tracing.start(tracing, %{name: "ankusa-gateway", version: version()}) do
      :ok ->
        Logger.info(
          "[ankusa] tracing on: OTLP to #{tracing.otlp_endpoint}, " <>
            "sample_ratio=#{tracing.sample_ratio}"
        )

      {:error, reason} ->
        Logger.error("[ankusa] tracing did not start (#{inspect(reason)}); running untraced")
    end
  end

  defp authenticators(%{auth: %{authenticators: []}}), do: "none"

  defp authenticators(%{auth: %{authenticators: list}}),
    do: Enum.map_join(list, ",", &type_name(&1.type))

  defp type_name(type) when type in [:token, :jwt, :mtls], do: Atom.to_string(type)
  defp type_name(module), do: inspect(module)

  defp metrics(%{metrics: %{enabled: true, port: port}}), do: to_string(port)
  defp metrics(_config), do: "off"

  @doc "The version of an application of this image (`:ankusa_gateway_server` by default)."
  @spec version(atom()) :: String.t()
  def version(app \\ :ankusa_gateway_server) do
    # `eval` runs without the applications started, so the spec has to be
    # loaded explicitly. `:already_loaded` is the normal case in a release.
    _ = Application.load(app)
    app |> Application.spec(:vsn) |> List.to_string()
  end
end
