# Ankusa documentation

If you read one page, read [architecture.md](architecture.md): it states the
guarantee, the request path, and the failure modes every other page assumes.
If you want to run it first, start with [quickstart.md](quickstart.md).

## Using it

| Doc | What it answers |
| --- | --- |
| [quickstart.md](quickstart.md) | How do I run it with a queue worker, break the worker and the broker, and replay? |
| [configuration.md](configuration.md) | What does every YAML key (and the Elixir config) mean, and what is its default? |
| [deployment.md](deployment.md) | Which roles and topologies exist, and how do I run the container? |
| [architecture.md](architecture.md) | What is the guarantee, and what happens on the request path when a component fails? |
| [delivery.md](delivery.md) | Which sinks exist, how do retries and dead letters work, and how is delivery verified? |
| [storage.md](storage.md) | Where does a hook live — the node store, the archive, backup, and restore? |
| [claim-check.md](claim-check.md) | How do large payloads reach queue workers without giving them storage credentials? |
| [multi-tenancy.md](multi-tenancy.md) | How do I serve many customers from one instance, and isolate or erase one? |
| [integrations.md](integrations.md) | How do I hand deliveries to Oban, Celery, or a queue consumer, and verify signatures? |
| [asyncapi.md](asyncapi.md) | What AsyncAPI document and lifecycle events does an instance serve? |
| [elixir.md](elixir.md) | How do I embed Ankusa in an Elixir application? |
| [runbooks/](runbooks/README.md) | What do I do when an alert fires: symptom, metric, action, verification? |

## Working on Ankusa

| Doc | What it answers |
| --- | --- |
| [testing.md](testing.md) | Which suites exist, what does each cover, and what local infrastructure do they need? |
| [packaging.md](packaging.md) | Why are adapters separate packages, and how do I add one? |
| [releasing.md](releasing.md) | How do I release a package, an SDK, or the server image? |
| [critical-review.md](critical-review.md) | What did the 2026-10 end-to-end review find, by root cause, and what is fixed? |
| [adr/](adr/README.md) | Why is the tenant key layout, or the dual-commit intake log, the way it is? |
| [CONTRIBUTING.md](https://github.com/jamescarr/ankusa/blob/main/CONTRIBUTING.md) | What must a PR include? |
| [AGENTS.md](https://github.com/jamescarr/ankusa/blob/main/AGENTS.md) | Repo map, commands, and the rules an agent or maintainer works under. |

Module docs, callback signatures, and options are on
[HexDocs](https://hexdocs.pm/ankusa). Package changelogs are beside the code:
`packages/*/CHANGELOG.md`.
