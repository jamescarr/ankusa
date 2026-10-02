# Build context is the repo root (see docker-compose.yml: `context: ../..`),
# so the mono-repo's relative path deps resolve identically to local dev.
#
# Dev-mode image (mix run, not a release), like examples/rabbitmq-consumer.
# build-base + cmake: brod's crc32cer NIF and rocksdb compile from source;
# linux-headers, openssl-dev and zstd-dev are the rocksdb build's system needs.
FROM elixir:1.20.4-alpine

RUN apk add --no-cache build-base cmake linux-headers openssl-dev zstd-dev && mix local.hex --force && mix local.rebar --force

WORKDIR /repo
COPY packages/ankusa ./packages/ankusa
COPY packages/async_api_spex ./packages/async_api_spex
COPY packages/ankusa_kafka ./packages/ankusa_kafka
COPY examples/kafka-sqs-consumer/ingest_app ./examples/kafka-sqs-consumer/ingest_app

WORKDIR /repo/examples/kafka-sqs-consumer/ingest_app
RUN mix deps.get && mix compile

EXPOSE 4000
CMD ["mix", "run", "--no-halt"]
