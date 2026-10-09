# Ankusa documentation

New here? Start with the [quickstart](quickstart.md).

## Start

| Doc | Read it when |
| --- | --- |
| [`quickstart.md`](quickstart.md) | run it with a worker, break the worker, replay |
| [`configuration.md`](configuration.md) | every YAML key, and the Elixir config |
| [`deployment.md`](deployment.md) | roles, the container, fleets |

## How it works

| Doc | Read it when |
| --- | --- |
| [`architecture.md`](architecture.md) | the guarantees |
| [`delivery.md`](delivery.md) | sinks, retries, DLQ, quarantine |
| [`storage.md`](storage.md) | the store and object stores |
| [`claim-check.md`](claim-check.md) | large payloads to queue workers |
| [`multi-tenancy.md`](multi-tenancy.md) | catch URLs per customer |

## Integrate

| Doc | Read it when |
| --- | --- |
| [`integrations.md`](integrations.md) | Oban, Celery, queues |
| [`asyncapi.md`](asyncapi.md) | the AsyncAPI document of an instance's channels, and lifecycle events |
| [`elixir.md`](elixir.md) | embed the library |

## Contributing

| Doc | Read it when |
| --- | --- |
| [`testing.md`](testing.md) | running the suites, local infra, what each one covers |
| [`packaging.md`](packaging.md) | why adapters are separate packages, and how to add one |
| [`critical-review.md`](critical-review.md) | the 2026-10 end-to-end review: findings by root cause, and which are fixed |
| [`releasing.md`](releasing.md) | releasing any package: Hex (core, adapters, and the Elixir SDK), the server image, npm, PyPI, crates.io, RubyGems, Go, Packagist, or Maven Central |
| [`../CONTRIBUTING.md`](../CONTRIBUTING.md) | before opening a PR |

Exact callback signatures and options: the module docs on
[HexDocs](https://hexdocs.pm/ankusa).
