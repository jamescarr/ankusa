# Build context is the repo root (see docker-compose.yml: `context: ../..`),
# so the mono-repo's relative path deps resolve identically to local dev.
#
# Dev-mode image (mix run, not a release), like examples/rabbitmq-consumer.
# build-base + cmake: brod's crc32cer NIF compiles from source.
FROM elixir:1.20.4-alpine

RUN apk add --no-cache build-base cmake && mix local.hex --force && mix local.rebar --force

WORKDIR /repo
COPY mix.exs mix.lock ./
COPY lib ./lib
COPY config ./config
COPY ankusa_kafka ./ankusa_kafka
COPY examples/kafka-sqs-consumer/ingest_app ./examples/kafka-sqs-consumer/ingest_app

WORKDIR /repo/examples/kafka-sqs-consumer/ingest_app
RUN mix deps.get && mix compile

EXPOSE 4000
CMD ["mix", "run", "--no-halt"]
