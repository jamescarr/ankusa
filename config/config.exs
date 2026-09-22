import Config

# The default instance's HTTP port.
config :hook, port: 4000

# Boot the default instance? Disabled in tests so each test starts isolated
# instances with their own temp data directories.
config :hook, autostart: config_env() != :test

# Zero-config demo source so the quick-start endpoint works before you have any
# provider credentials. `POST /hooks/demo` accepts anything and logs it.
config :hook,
  sources: %{
    "demo" => [
      verifier: {Hook.Verifier.None, []},
      dedup: {Hook.DedupKey.Rules, []},
      on_verify_failure: :accept_flag,
      sinks: [{Hook.Sink.Log, []}]
    ]
  }

config :logger, :console, format: "$time [$level] $message\n"
