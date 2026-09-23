# Build context is `examples/oban-consumer/consumer_app/` itself (unlike the
# ingest_app images, which build from the repo root) — this app has zero
# Ankusa path deps, so it stands alone exactly like a real Oban worker fleet
# consuming a plain HTTP webhook handoff would.
FROM elixir:1.20.4-alpine AS build

RUN mix local.hex --force && mix local.rebar --force

ENV MIX_ENV=prod
WORKDIR /app

COPY mix.exs mix.lock* ./
COPY config ./config
COPY lib ./lib
COPY priv ./priv

RUN mix deps.get --only prod && mix release consumer

FROM elixir:1.20.4-alpine

COPY --from=build /app/_build/prod/rel/consumer /app

ENV RELEASE_DISTRIBUTION=none
EXPOSE 4200

CMD ["/app/bin/consumer", "start"]
