# Build context is the repo root (see run.sh: `docker build -f
# examples/oban-consumer/ingest_app/Dockerfile ...`), so the mono-repo's
# relative path deps resolve identically to local dev: this app depends on
# `../../../packages/ankusa` (ankusa core), unmodified.
#
# Multi-stage image building a real `mix release` — unlike the dev-mode
# single-stage images used by the other examples, this one is deployed into
# a real Kubernetes cluster and needs a proper release artifact.
FROM elixir:1.20.4-alpine AS build

RUN apk add --no-cache build-base cmake linux-headers openssl-dev zstd-dev && mix local.hex --force && mix local.rebar --force
ENV MIX_ENV=prod

WORKDIR /repo
COPY packages/ankusa ./packages/ankusa
COPY packages/async_api_spex ./packages/async_api_spex
COPY examples/oban-consumer/ingest_app ./examples/oban-consumer/ingest_app

WORKDIR /repo/examples/oban-consumer/ingest_app
RUN mix deps.get --only prod && mix release ingest

FROM elixir:1.20.4-alpine

# The release's RocksDB NIF links the system zstd, which this base image lacks.
RUN apk add --no-cache zstd-libs

COPY --from=build /repo/examples/oban-consumer/ingest_app/_build/prod/rel/ingest /app

ENV RELEASE_DISTRIBUTION=none

EXPOSE 4000
CMD ["/app/bin/ingest", "start"]
