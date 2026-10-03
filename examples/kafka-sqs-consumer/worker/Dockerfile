# Build context is the repo root, not this directory: package.json's `ankusa`
# dependency is a file: link to ../../../packages/sdk-typescript (see "Building
# an app against the path deps" in docs/packaging.md), so the build needs both
# directories in one context, at the same relative paths.
#   docker compose build worker   (context: ../.. in docker-compose.yml)
FROM node:22-alpine

# The link imports the SDK's build output, so build it first.
WORKDIR /repo/packages/sdk-typescript
COPY packages/sdk-typescript/package.json packages/sdk-typescript/package-lock.json packages/sdk-typescript/tsconfig.json ./
RUN npm ci
COPY packages/sdk-typescript/src ./src
RUN npm run build

WORKDIR /repo/examples/kafka-sqs-consumer/worker
COPY examples/kafka-sqs-consumer/worker/package.json examples/kafka-sqs-consumer/worker/package-lock.json ./
RUN npm ci
COPY examples/kafka-sqs-consumer/worker/tsconfig.json ./
COPY examples/kafka-sqs-consumer/worker/src ./src

CMD ["npx", "tsx", "src/worker.ts"]
