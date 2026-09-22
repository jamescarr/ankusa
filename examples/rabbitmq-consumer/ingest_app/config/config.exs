import Config

# HookExample.Ingest.Application configures and starts the instance itself
# (from env vars) — don't let `:hook`'s own Application also boot its
# built-in default instance, or the two collide on the same port/registry
# names.
config :hook, autostart: false

config :logger, :console, format: "$time [$level] $message\n"
