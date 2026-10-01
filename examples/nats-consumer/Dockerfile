# Builds the worker against the published `ankusa` crate, so the build
# context is just this directory.
FROM rust:1.98.1-alpine3.24 AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM alpine:3.24
COPY --from=build /src/target/release/ankusa-nats-consumer /usr/local/bin/worker
CMD ["worker"]
