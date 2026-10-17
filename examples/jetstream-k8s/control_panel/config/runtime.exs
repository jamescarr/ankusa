import Config

# config/runtime.exs is executed for all environments, including
# during releases. It is executed after compilation and before the
# system starts, so it is typically used to load production configuration
# and secrets from environment variables or elsewhere.

# The release's `bin/server` script sets PHX_SERVER; `mix phx.server` serves on
# its own.
if System.get_env("PHX_SERVER") do
  config :control_panel, ControlPanelWeb.Endpoint, server: true
end

config :control_panel, ControlPanelWeb.Endpoint,
  http: [port: String.to_integer(System.get_env("PORT", "4000"))]

# Where the panel's dependencies live. In the cluster the chart sets all four;
# the defaults are for a panel run next to a published ingest and NATS.
namespace_file = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

k8s_namespace =
  System.get_env("K8S_NAMESPACE") ||
    case File.read(namespace_file) do
      {:ok, ns} -> String.trim(ns)
      {:error, _} -> "ankusa-jetstream"
    end

config :control_panel,
  # The load generator POSTs /webhooks/demo here: the ingress gateway Service.
  gateway_url: System.get_env("GATEWAY_URL", "http://localhost:8080"),
  # A body over the ingest's inline limit is checked in to floci (S3), which
  # holds it in memory: the generator spends at most `claim_bytes_per_sec` of
  # such bodies a second, so a slider setting cannot fill it.
  claim_threshold_bytes: String.to_integer(System.get_env("CLAIM_THRESHOLD_BYTES", "65536")),
  claim_bytes_per_sec: String.to_integer(System.get_env("CLAIM_BYTES_PER_SEC", "8388608")),
  nats_host: System.get_env("NATS_HOST", "localhost"),
  nats_port: String.to_integer(System.get_env("NATS_PORT", "4222")),
  k8s_namespace: k8s_namespace

if config_env() == :dev do
  # Reload browser tabs when matching files change.
  config :control_panel, ControlPanelWeb.Endpoint,
    live_reload: [
      web_console_logger: true,
      patterns: [
        # Static assets, except user uploads
        ~r"priv/static/(?!uploads/).*\.(js|css|png|jpeg|jpg|gif|svg)$"E,
        # Router, Controllers, LiveViews and LiveComponents
        ~r"lib/control_panel_web/router\.ex$"E,
        ~r"lib/control_panel_web/(controllers|live|components)/.*\.(ex|heex)$"E
      ]
    ]
end

if config_env() == :prod do
  # The secret key base signs the session cookie and the LiveView tokens. The
  # chart passes a fixed demo value: nothing here is worth protecting.
  secret_key_base =
    System.get_env("SECRET_KEY_BASE") ||
      raise """
      environment variable SECRET_KEY_BASE is missing.
      You can generate one by calling: mix phx.gen.secret
      """

  host = System.get_env("PHX_HOST") || "localhost"

  config :control_panel, ControlPanelWeb.Endpoint,
    url: [host: host, port: 8080, scheme: "http"],
    # The browser reaches the panel as localhost:8080 through the Envoy gateway,
    # not as the pod's own host, so the websocket's Origin never matches.
    check_origin: false,
    http: [
      # IPv4 on every interface: a pod has no IPv6 address to bind to.
      ip: {0, 0, 0, 0}
    ],
    secret_key_base: secret_key_base
end
