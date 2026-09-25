# Build context is the repo root (see run.sh: `docker build -f
# examples/oban-consumer/ingest_app/Dockerfile ...`), so the mono-repo's
# relative path deps resolve identically to local dev: this app depends on
# `../../..` (ankusa core) and `../../../ankusa_postgres` (the WAL), unmodified.
#
# Multi-stage image building a real `mix release` — unlike the dev-mode
# single-stage images used by the other examples, this one is deployed into
# a real Kubernetes cluster and needs a proper release artifact.
FROM elixir:1.20.4-alpine AS build

RUN mix local.hex --force && mix local.rebar --force
ENV MIX_ENV=prod

WORKDIR /repo
COPY mix.exs mix.lock ./
COPY lib ./lib
COPY config ./config
COPY priv ./priv
COPY ankusa_postgres ./ankusa_postgres
COPY ankusa_ra ./ankusa_ra
COPY examples/oban-consumer/ingest_app ./examples/oban-consumer/ingest_app

WORKDIR /repo/examples/oban-consumer/ingest_app
RUN mix deps.get --only prod && mix release ingest

FROM elixir:1.20.4-alpine

COPY --from=build /repo/examples/oban-consumer/ingest_app/_build/prod/rel/ingest /app

# Distribution is off by default (a Postgres-WAL fleet needs no Erlang
# distribution at all). The Ra fleet turns it on per pod with the standard Mix
# release variables: `RELEASE_DISTRIBUTION=name`, `RELEASE_NODE=ankusa@<pod>.<svc>`
# and `RELEASE_COOKIE` (from a Secret). Nothing here hardcodes a node name, so
# the same image serves both shapes.
ENV RELEASE_DISTRIBUTION=none

EXPOSE 4000
CMD ["/app/bin/ingest", "start"]
