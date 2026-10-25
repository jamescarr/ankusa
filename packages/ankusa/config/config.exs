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
      on_verify_failure: :accept_flag,
      sinks: [{Ankusa.Sink.Log, []}]
    ]
  }

# The keys core sets as `Logger` metadata (the correlation of a hook, see
# `Ankusa.Edge.Ingest` and `Ankusa.Sink.attempt/5`). The console line stays as it
# was: the format has no `$metadata`. Listing them is what lets credo check
# every `Logger.metadata/1` and `Logger.warning/2` call against one set.
config :logger, :console,
  format: "$time [$level] $message\n",
  metadata: [:instance, :source_id, :tenant_id, :hook_id, :sink, :attempt, :trace_id, :replay_id]
