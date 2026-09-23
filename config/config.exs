import Config

# The default instance's HTTP port.
config :ankusa, port: 4000

# Boot the default instance? Disabled in tests so each test starts isolated
# instances with their own temp data directories.
config :ankusa, autostart: config_env() != :test

# Zero-config demo source so the quick-start endpoint works before you have any
# provider credentials. `POST /webhooks/demo` accepts anything and logs it.
config :ankusa,
  sources: %{
    "demo" => [
      verifier: {Ankusa.Verifier.None, []},
      dedup: {Ankusa.DedupKey.Rules, []},
      on_verify_failure: :accept_flag,
      sinks: [{Ankusa.Sink.Log, []}]
    ]
  }

config :logger, :console, format: "$time [$level] $message\n"
