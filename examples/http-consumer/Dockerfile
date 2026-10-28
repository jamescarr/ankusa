# Build context is the repo root, not this directory: worker.py's
# pyproject.toml path-depends on ../../packages/sdk-python (see "Building an
# app against the path deps" in docs/packaging.md for why the adapter
# packages do the same thing), so the build needs both directories in one
# context.
#   docker compose build worker   (context: ../.. in docker-compose.yml)
FROM python:3.15-alpine
COPY --from=ghcr.io/astral-sh/uv:0.13.1 /uv /uvx /usr/local/bin/

WORKDIR /repo
COPY packages/sdk-python ./packages/sdk-python
COPY examples/http-consumer/pyproject.toml examples/http-consumer/uv.lock ./examples/http-consumer/
WORKDIR /repo/examples/http-consumer
RUN uv sync --locked --no-dev

COPY examples/http-consumer/worker.py ./

CMD ["uv", "run", "--no-sync", "python", "worker.py"]
