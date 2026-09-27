# Build context is the repo root, not this directory: worker.py's
# pyproject.toml path-depends on ../../sdks/python (see "Building an app
# against the path deps" in docs/packaging.md for why the adapter packages
# do the same thing), so the build needs both directories in one context.
#   docker compose build worker   (context: ../.. in docker-compose.yml)
FROM python:3.13-alpine
COPY --from=ghcr.io/astral-sh/uv:0.5.29 /uv /uvx /usr/local/bin/

WORKDIR /repo
COPY sdks/python ./sdks/python
COPY examples/quickstart/pyproject.toml examples/quickstart/uv.lock ./examples/quickstart/
WORKDIR /repo/examples/quickstart
RUN uv sync --locked --no-dev

COPY examples/quickstart/worker.py ./

CMD ["uv", "run", "--no-sync", "python", "worker.py"]
