import Config

# Don't boot the demo `:hook` app-level instance (port 4000, DiskLog WAL) as a
# side effect of `:hook` being a transitive OTP application dependency here —
# this package only needs `Hook.Registry` running, which `Hook.Application`
# starts unconditionally.
config :hook, autostart: false
