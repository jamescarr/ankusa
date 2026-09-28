import Config

# The release logs to stdout in the container-friendly shape: no metadata, one
# line per event. The level itself comes from the operator's config file
# (`log.level`), applied by AnkusaServer.Application at boot.
config :logger, :console, format: "$time [$level] $message\n", metadata: []

# Does starting this application bind ports and read the config file? Yes
# everywhere but `mix test`: the suite is about the loader, not about a running
# server, and booting one would take 4000/4002 and write into ./data.
config :ankusa_server, autostart: config_env() != :test

# Deliberately no `config :ankusa, autostart: true`. Core defaults to false and
# AnkusaServer.Application owns the instance: it builds the config from YAML and
# starts exactly one instance, so nothing ever binds a port behind its back.
