# `:integration` tests need
# `docker compose -f docker-compose.integration.yml up -d` (floci/floci-gcp
# object store emulators); run them explicitly with
# `mix test --only integration` (or `mise run test:integration`).
ExUnit.start(exclude: [:integration])

# `Ankusa.CrashHarness`: the child-BEAM harness that `loss_test.exs` and the
# container drills in `scripts/drills/` share.
Code.require_file("support/crash_harness.exs", __DIR__)

# `Ankusa.AuditHarness`: the child-BEAM harness behind `audit_test.exs`'s
# "audit row survives kill".
Code.require_file("support/audit_harness.exs", __DIR__)

# Runs per property in `test/ankusa/property/` (StreamData reads this key when
# a `check all` starts). 5 000 is the floor the hardening plan asks of CI;
# `PROPERTY_RUNS=50000 mix test test/ankusa/property` is the long soak.
property_runs =
  case Integer.parse(System.get_env("PROPERTY_RUNS", "5000")) do
    {runs, ""} when runs > 0 -> runs
    _ -> raise "PROPERTY_RUNS must be a positive integer"
  end

Application.put_env(:stream_data, :max_runs, property_runs)
