# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
#
# The Ankusa gateway (jamescarr/ankusa-gateway): core and the gateway library,
# a core instance with no role and `Ankusa.Gateway` beside it, configured by a
# mounted YAML file (`/etc/ankusa/gateway.yml`).
#
# The build context is the repo root, not packages/ankusa_gateway_server/, so
# the path deps (`../ankusa`, `../ankusa_gateway`) resolve to this checkout.
#
# Every image reference here is pinned by the digest of its multi-arch index
# (`mise run lint:pins` fails a bare tag), the same pins as
# packages/ankusa_server/Dockerfile. To bump one, change tag and digest
# together in both files, in a PR:
#   docker buildx imagetools inspect elixir:1.20.4-alpine --format '{{json .Manifest.Digest}}'
FROM elixir:1.20.4-alpine@sha256:f1a8c06b332cacc7239381a25331a807fb63962f8221dff545ea4d8f78b0f50c AS build

# rocksdb (core's local store NIF, compiled in and never opened by a node with
# no role) needs build-base, cmake, linux-headers, openssl-dev and zstd-dev.
RUN apk add --no-cache build-base cmake git linux-headers openssl-dev zstd-dev && mix local.hex --force && mix local.rebar --force
ENV MIX_ENV=prod
WORKDIR /repo

# The deps layer: mix.exs and mix.lock alone let `deps.get` resolve, and the
# RocksDB build is cached until one of these lockfiles changes.
COPY packages/ankusa/mix.exs packages/ankusa/mix.lock ./packages/ankusa/
COPY packages/async_api_spex/mix.exs ./packages/async_api_spex/
COPY packages/ankusa_gateway/mix.exs packages/ankusa_gateway/mix.lock ./packages/ankusa_gateway/
COPY packages/ankusa_gateway_postgres/mix.exs packages/ankusa_gateway_postgres/mix.lock ./packages/ankusa_gateway_postgres/
COPY packages/ankusa_gateway_server/mix.exs packages/ankusa_gateway_server/mix.lock ./packages/ankusa_gateway_server/

WORKDIR /repo/packages/ankusa_gateway_server
RUN mix deps.get --only prod && mix deps.compile rocksdb

WORKDIR /repo
COPY packages ./packages

WORKDIR /repo/packages/ankusa_gateway_server
RUN mix deps.get --only prod && mix release ankusa_gateway

# Alpine 3.24 to match the build stage. curl is for the healthcheck, tini reaps
# the BEAM's children as PID 1, liblksctp silences ERTS's dlopen warning, and
# coreutils gives `:disksup` a GNU `df`.
FROM alpine:3.24@sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6

RUN apk add --no-cache libstdc++ libgcc zstd-libs ncurses-libs openssl ca-certificates tini curl liblksctp coreutils \
 && addgroup -S ankusa && adduser -S -G ankusa -h /var/lib/ankusa ankusa \
 && mkdir -p /var/lib/ankusa /etc/ankusa && chown ankusa:ankusa /var/lib/ankusa

COPY --from=build --chown=ankusa:ankusa /repo/packages/ankusa_gateway_server/_build/prod/rel/ankusa_gateway /opt/ankusa-gateway
COPY packages/ankusa_gateway_server/rel/gateway.yml /etc/ankusa/gateway.yml
COPY --chmod=0755 packages/ankusa_gateway_server/rel/docker-entrypoint.sh /usr/local/bin/docker-entrypoint

# Never root.
USER ankusa

# Distribution is on, bound to loopback only (`rel/env.sh.eex`,
# `rel/vm.args.eex`), so `docker exec … docker-entrypoint remote|rpc` works and
# nothing outside the container can reach it.
ENV ANKUSA_CONFIG=/etc/ankusa/gateway.yml ANKUSA_DATA_DIR=/var/lib/ankusa \
    LANG=C.UTF-8 HOME=/var/lib/ankusa

VOLUME /var/lib/ankusa

# 4001 the gateway, 4004 metrics (`metrics.enabled`). The gateway authenticates
# its callers off loopback.
EXPOSE 4001 4004

HEALTHCHECK --interval=10s --timeout=3s --start-period=15s \
  CMD /usr/local/bin/docker-entrypoint healthcheck || exit 1

ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/docker-entrypoint"]
CMD ["start"]
