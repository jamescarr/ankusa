# Build context is the repo root (see docker-compose.yml: `context: ../..`),
# so the mono-repo's relative path deps resolve identically to local dev:
# this app depends on `../../../packages/ankusa` (ankusa core) and
# `../../../packages/ankusa_rabbitmq` (the sink), unmodified.
#
# Dev-mode image (mix run, not a release) — right-sized for an example; a
# real deployment would build a proper `mix release` in a multi-stage image.
# build-base + cmake + linux-headers + openssl-dev + zstd-dev: the rocksdb NIF
# (ankusa core) builds from source; the last three are its system needs.
FROM elixir:1.20.4-alpine

RUN apk add --no-cache build-base cmake linux-headers openssl-dev zstd-dev && mix local.hex --force && mix local.rebar --force

WORKDIR /repo
COPY packages/ankusa ./packages/ankusa
COPY packages/async_api_spex ./packages/async_api_spex
COPY packages/ankusa_rabbitmq ./packages/ankusa_rabbitmq
COPY examples/rabbitmq-consumer/ingest_app ./examples/rabbitmq-consumer/ingest_app

WORKDIR /repo/examples/rabbitmq-consumer/ingest_app
RUN mix deps.get && mix compile

EXPOSE 4000
CMD ["mix", "run", "--no-halt"]
