# Build context is the repo root (see docker-compose.yml: `context: ../..`),
# so the mono-repo's relative path deps resolve identically to local dev:
# this app depends on `../../..` (hook core) and
# `../../../hook_rabbitmq` (the sink), unmodified.
#
# Dev-mode image (mix run, not a release) — right-sized for an example; a
# real deployment would build a proper `mix release` in a multi-stage image.
FROM elixir:1.20.4-alpine

RUN mix local.hex --force && mix local.rebar --force

WORKDIR /repo
COPY mix.exs mix.lock ./
COPY lib ./lib
COPY config ./config
COPY hook_rabbitmq ./hook_rabbitmq
COPY examples/rabbitmq-consumer/ingest_app ./examples/rabbitmq-consumer/ingest_app

WORKDIR /repo/examples/rabbitmq-consumer/ingest_app
RUN mix deps.get && mix compile

EXPOSE 4000
CMD ["mix", "run", "--no-halt"]
