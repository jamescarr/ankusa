# Build context is the repo root, not this directory: package.json's `ankusa`
# dependency is a file: link to ../../../packages/sdk-typescript (see "Building
# an app against the path deps" in docs/packaging.md), so the build needs both
# directories in one context, at the same relative paths.
#   docker compose build consumer   (context: ../.. in docker-compose.yml)
FROM node:24-alpine

# The link imports the SDK's build output, so build it first.
WORKDIR /repo/packages/sdk-typescript
COPY packages/sdk-typescript/package.json packages/sdk-typescript/package-lock.json packages/sdk-typescript/tsconfig.json ./
RUN npm ci
COPY packages/sdk-typescript/src ./src
RUN npm run build

WORKDIR /repo/examples/fleet-e2e/consumer
COPY examples/fleet-e2e/consumer/package.json examples/fleet-e2e/consumer/package-lock.json ./
RUN npm ci
COPY examples/fleet-e2e/consumer/tsconfig.json ./
COPY examples/fleet-e2e/consumer/src ./src

CMD ["npx", "tsx", "src/worker.ts"]
