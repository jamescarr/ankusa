import Config

# Don't boot the demo `:ankusa` app-level instance (port 4000, DiskLog WAL) as a
# side effect of `:ankusa` being a transitive OTP application dependency here —
# this package only needs `Ankusa.Registry` running, which `Ankusa.Application`
# starts unconditionally.
config :ankusa, autostart: false
