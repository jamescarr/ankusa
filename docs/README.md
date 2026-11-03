# Ankusa documentation

Ankusa is a webhook receiver. It writes every hook to durable local storage
before answering the provider's `2xx`, then hands it to the sinks you
configured: Kafka, RabbitMQ, NATS JetStream, SQS, Google Pub/Sub, Redis
pub/sub, or a signed HTTP POST to your own service. These pages cover running
that receiver, configuring it, and receiving what it delivers.

## Start here

1. [quickstart.md](quickstart.md) runs Ankusa, a broker, and a worker on your
   laptop, sends a hook, then kills the worker and the broker in turn so you
   can see what survives. Requires Docker with Compose v2 and `curl`.
2. [architecture.md](architecture.md) is the page to read if you read only one:
   the guarantee, the request path, and the failure modes every other page
   assumes.
3. [configuration.md](configuration.md) has every YAML key, its default, and
   the Elixir equivalent.

## Running it

| Doc | Read it when |
| --- | --- |
| [deployment.md](deployment.md) | you are choosing roles or a topology, or running the container and the Helm chart |
| [storage.md](storage.md) | you need to know where an acked hook lives: the node's store, the segment archive, backup, restore |
| [delivery.md](delivery.md) | you are picking a sink, tuning retries, working the dead-letter queue, or draining a quarantine pen |
| [claim-check.md](claim-check.md) | bodies are too large for a queue message and your workers should not hold storage credentials |
| [multi-tenancy.md](multi-tenancy.md) | one instance serves many customers, and you need per-tenant catch URLs, quotas, or erasure |
| [runbooks/](runbooks/README.md) | an alert fired and you want the symptom, the metric, the action, and the check that it worked |

## Getting hooks into your code

| Doc | Read it when |
| --- | --- |
| [integrations.md](integrations.md) | you are handing deliveries to Oban, Celery, or a queue your workers already consume, and need to verify the signature |
| [elixir.md](elixir.md) | you are embedding the library in an Elixir app instead of running the container |
| [asyncapi.md](asyncapi.md) | you want the AsyncAPI document an instance serves, or the lifecycle events it publishes |

## Working on Ankusa

| Doc | Read it when |
| --- | --- |
| [testing.md](testing.md) | you are running a suite and need to know what it covers and which infrastructure it needs |
| [packaging.md](packaging.md) | you are adding an adapter, or wondering why one lives outside core |
| [releasing.md](releasing.md) | you are publishing core, an adapter, an SDK, or the server image |
| [critical-review.md](critical-review.md) | you want the 2026-10 end-to-end review: findings by root cause, and which are fixed |
| [adr/](adr/README.md) | you are about to change the ack path, the store key layout, a wire format, the isolation model, or the durability mode, and need the record that explains why they are as they are |
| [CONTRIBUTING.md](https://github.com/jamescarr/ankusa/blob/main/CONTRIBUTING.md) | you are opening a PR |
| [AGENTS.md](https://github.com/jamescarr/ankusa/blob/main/AGENTS.md) | you want the repo map, the command list, and the rules a maintainer or an agent works under |

Callback signatures, options, and module docs are on
[HexDocs](https://hexdocs.pm/ankusa). Each package keeps its changelog beside
the code, in `packages/*/CHANGELOG.md`.
