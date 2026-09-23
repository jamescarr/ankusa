# Build context is the repo root (see docker-compose.yml: `context: ../..`),
# so the mono-repo's relative path deps resolve identically to local dev.
#
# Alpine image requires CMake for brod's crc32cer NIF.

FROM elixir:1.20.4-alpine

RUN apk add --no-cache build-base cmake git && \
    mix local.hex --force && \
    mix local.rebar --force

WORKDIR /repo

# Copy the entire mono-repo (ankusa core + ankusa_kafka + this example)
COPY mix.exs mix.lock ./
COPY lib ./lib
COPY config ./config
COPY ankusa_kafka ./ankusa_kafka
COPY examples/kafka-sqs-consumer/ingest_app ./examples/kafka-sqs-consumer/ingest_app

WORKDIR /repo/examples/kafka-sqs-consumer/ingest_app

RUN mix deps.get && MIX_ENV=prod mix compile

EXPOSE 4000 4001

CMD ["mix", "run", "--no-halt"]
