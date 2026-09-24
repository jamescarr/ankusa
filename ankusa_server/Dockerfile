# syntax=docker/dockerfile:1.7
#
# Ankusa as a server: the whole framework — core plus the Postgres WAL, RabbitMQ,
# and Kafka adapters — in one image, configured by a mounted YAML file.
#
# The build context is the repo root, not ankusa_server/, for the same reason as
# examples/oban-consumer/ingest_app: the path deps (`..`, `../ankusa_postgres`,
# …) have to resolve to this checkout, or `mix release` would pull the published
# Hex packages instead of the code being built.
FROM elixir:1.20.4-alpine AS build

# build-base/cmake/git are for the C++ NIFs: `crc32cer` (via brod, the Kafka
# client) compiles on install in :prod and has no prebuilt artifact.
RUN apk add --no-cache build-base cmake git && mix local.hex --force && mix local.rebar --force
ENV MIX_ENV=prod
WORKDIR /repo

COPY mix.exs mix.lock ./
COPY lib ./lib
COPY config ./config
COPY priv ./priv
COPY ankusa_postgres ./ankusa_postgres
COPY ankusa_rabbitmq ./ankusa_rabbitmq
COPY ankusa_kafka ./ankusa_kafka
COPY ankusa_server ./ankusa_server

WORKDIR /repo/ankusa_server
RUN mix deps.get --only prod && mix release ankusa

# Alpine 3.24 to match the elixir:1.20.4-alpine build stage (3.24.2). The release
# is a set of BEAM files plus ERTS, so the runtime needs little beyond the C
# libraries the NIFs link against: curl is for the healthcheck, tini reaps the
# BEAM's children as PID 1. liblksctp is not for SCTP sockets (nothing here uses
# one) — ERTS dlopens it at boot and prints an alarming stderr warning without
# it on every `eval` and `start`.
FROM alpine:3.24

RUN apk add --no-cache libstdc++ libgcc ncurses-libs openssl ca-certificates tini curl liblksctp \
 && addgroup -S ankusa && adduser -S -G ankusa -h /var/lib/ankusa ankusa \
 && mkdir -p /var/lib/ankusa /etc/ankusa && chown ankusa:ankusa /var/lib/ankusa

COPY --from=build --chown=ankusa:ankusa /repo/ankusa_server/_build/prod/rel/ankusa /opt/ankusa
COPY ankusa_server/rel/ankusa.yml /etc/ankusa/ankusa.yml
COPY --chmod=0755 ankusa_server/rel/docker-entrypoint.sh /usr/local/bin/docker-entrypoint

# Never root: the WAL, quarantine log, and DLQ are all written under the data
# volume, and a container compromise should not own the host.
USER ankusa

ENV ANKUSA_CONFIG=/etc/ankusa/ankusa.yml ANKUSA_DATA_DIR=/var/lib/ankusa \
    RELEASE_DISTRIBUTION=none LANG=C.UTF-8 HOME=/var/lib/ankusa

VOLUME /var/lib/ankusa

# 4000 ingest, 4001 claim check (:claim_check role), 4002 admin/metrics.
# Publishing more than 4000 is a deliberate act: 4001 and 4002 are
# unauthenticated (see README).
EXPOSE 4000 4001 4002

HEALTHCHECK --interval=10s --timeout=3s --start-period=15s \
  CMD curl -fsS "http://127.0.0.1:${ANKUSA_ADMIN_PORT:-4002}/health" || exit 1

ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/docker-entrypoint"]
CMD ["start"]
