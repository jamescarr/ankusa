# `:integration` tests need
# `docker compose -f docker-compose.integration.yml up -d` (floci/floci-gcp
# object store emulators); run them explicitly with
# `mix test --only integration` (or `mise run test:integration`).
ExUnit.start(exclude: [:integration])
