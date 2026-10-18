# `:integration` tests need
# `docker compose -f docker-compose.integration.yml up -d` (floci/floci-gcp
# object store emulators); run them explicitly with
# `mix test --only integration` (or `mise run test:integration`).
ExUnit.start(exclude: [:integration])

# `Ankusa.CrashHarness`: the child-BEAM harness that `loss_test.exs` and the
# container drills in `scripts/drills/` share.
Code.require_file("support/crash_harness.exs", __DIR__)
