# Ankusa documentation

Start with the root [`README.md`](../README.md) for the pitch and a two-minute
quickstart. These pages go deeper, one concern at a time.

| Doc | Covers |
| --- | --- |
| [`architecture.md`](architecture.md) | The core invariant, the ingest pipeline end to end, per-component durability guarantees, the instance/supervision model |
| [`quickstart.md`](quickstart.md) | Install, run, ingest a hook, inspect state, point a real provider at it |
| [`configuration.md`](configuration.md) | Full `%Ankusa.Config{}` reference, source configuration, every behaviour's options |
| [`multi-tenancy.md`](multi-tenancy.md) | Catch-URL routing (`Ankusa.RouteResolver`), tenant scoping, building your own catch-URL scheme |
| [`storage.md`](storage.md) | WAL (`DiskLog`, `Postgres`), segment compaction, `BlobStore` (`LocalFS`, `S3`, `GCS`), replay by id |
| [`delivery.md`](delivery.md) | Dispatch pipeline, `Sink` (`Log`, `Http`, `RabbitMQ`), retry/backoff, DLQ + replay, quarantine |
| [`integrations.md`](integrations.md) | Using Ankusa with a job framework (Oban, Celery) without coupling to one |
| [`packaging.md`](packaging.md) | Mono-repo layout, why adapters live in separate packages, the rule for when to split, how to add one |
| [`deployment.md`](deployment.md) | Roles and topologies, Docker, scaling an ingest fleet, releasing to Hex, the worked example |
| [`testing.md`](testing.md) | How the test suites are organized across packages, integration tags, local dev infra |

## Reading order

New to the codebase and want the full picture in order: `architecture.md` →
`quickstart.md` → `configuration.md` → `storage.md` → `delivery.md`.

Already know the shape and want one thing: `multi-tenancy.md` for
catch-URL/tenant design, `packaging.md` for "why is this a separate
package", `deployment.md` for "how do I actually run this".

## Source of truth

These docs describe behavior; the modules' own `@moduledoc`s are the
canonical reference for exact callback signatures and options — every
behaviour and adapter in the framework is documented in place. Run
`mix docs` (or read `lib/ankusa/**/*.ex` directly) when you need the precise
contract, not the narrative.
