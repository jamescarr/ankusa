import Config

# The release logs to stdout in the container-friendly shape: no metadata, one
# line per event (`log.format: text`, the default). `log.format: json` replaces
# this formatter at boot with AnkusaGatewayServer.LogFormatter. The level
# itself comes from the operator's config file (`log.level`), applied by
# AnkusaGatewayServer.Application at boot.
config :logger, :console, format: "$time [$level] $message\n", metadata: []

# Does starting this application read the config file and bind ports? Yes
# everywhere but `mix test`, and never when it is a dependency of the full
# image (`ankusa_server` sets it to false: its own application owns the node).
config :ankusa_gateway_server, autostart: config_env() != :test
