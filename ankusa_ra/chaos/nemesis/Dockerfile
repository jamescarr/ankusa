# The nemesis: the tools a fault needs, and nothing else. It talks to the other
# containers over the compose network (iptables/tc) and to the Docker socket
# (killing containers outright), which is why it needs NET_ADMIN and the socket
# mounted.
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      iproute2 \
      iptables \
      iputils-ping \
      netcat-openbsd \
      curl \
      ca-certificates \
      postgresql-client \
      faketime \
      procps \
      jq \
      python3 \
      docker.io \
    && rm -rf /var/lib/apt/lists/*

# Compose v2, for the scenarios that have to *recreate* a container: a fresh
# volume for `replace-member`, a changed clock offset for `clock-skew`, a new
# image for `rolling-upgrade`. The `docker.io` package ships the CLI only, and
# libfaketime is no substitute — it cannot reach a running BEAM.
ARG COMPOSE_VERSION=v5.5.1
# `uname -m` gives the names the compose releases use (aarch64, x86_64);
# Debian's `arm64` is not one of them.
RUN mkdir -p /usr/libexec/docker/cli-plugins /usr/lib/docker/cli-plugins \
    && curl -fsSL "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-$(uname -m)" \
         -o /usr/libexec/docker/cli-plugins/docker-compose \
    && chmod +x /usr/libexec/docker/cli-plugins/docker-compose \
    && docker compose version

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /scenarios
ENTRYPOINT ["/bin/bash"]
CMD ["-c", "sleep infinity"]
