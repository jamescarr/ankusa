# `:integration` tests need `docker compose up -d` (floci/floci-gcp object
# store emulators); run them explicitly with `mix test --include integration`.
ExUnit.start(exclude: [:integration])
