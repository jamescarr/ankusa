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
| [`storage.md`](storage.md) | the log and object stores |
| [`claim-check.md`](claim-check.md) | large payloads to queue workers |
| [`multi-tenancy.md`](multi-tenancy.md) | catch URLs per customer |

## Integrate

| Doc | Read it when |
| --- | --- |
| [`integrations.md`](integrations.md) | Oban, Celery, queues |
| [`elixir.md`](elixir.md) | embed the library |

## Contributing

| Doc | Read it when |
| --- | --- |
| [`testing.md`](testing.md) | running the suites, local infra, what each one covers |
| [`packaging.md`](packaging.md) | why adapters are separate packages, and how to add one |
| [`releasing.md`](releasing.md) | tagging a Hex package or the server image |
| [`../CONTRIBUTING.md`](../CONTRIBUTING.md) | before opening a PR |

Exact callback signatures and options: the module docs on
[HexDocs](https://hexdocs.pm/ankusa).
