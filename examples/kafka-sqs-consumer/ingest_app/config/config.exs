import Config

# AnkusaExample.Ingest.Application configures and starts the instance itself
# (from env vars) — don't let `:ankusa`'s own Application also boot its
# built-in default instance, or the two collide on the same port/registry
# names.
config :ankusa, autostart: false

config :logger, :console, format: "$time [$level] $message\n"
