# Agent notes

## Run the checks before calling work done

Format, warnings-as-errors, and tests — for **every** package the change
touches, not just the one you edited. All three adapter packages depend on
`ankusa` core, so a core change isn't finished until they pass too.

| Package | Run from its directory |
| --- | --- |
| `ankusa` (core) | `mix format --check-formatted && mix compile --warnings-as-errors && mix test` |
| `ankusa_postgres` | the same three, in `ankusa_postgres/` (after `docker compose up -d --wait`) |
| `ankusa_rabbitmq` | the same three, in `ankusa_rabbitmq/` (after `docker compose up -d --wait`) |
| `ankusa_kafka` | the same three, in `ankusa_kafka/` (after `docker compose up -d --wait`) |
| `examples/*/ingest_app` | `mix compile --warnings-as-errors` |
| `examples/*/worker` | `npx tsc --noEmit` |

`mix format --check-formatted` is what CI fails on first: run `mix format`
before pushing, not after CI tells you.

Core's suite excludes `:integration` tests (S3/GCS against a floci
emulator); add `--include integration` with the emulator running. What each
suite covers: [`docs/testing.md`](docs/testing.md).

`ankusa_kafka` compiles a C++ NIF (`crc32cer`, via `brod`), which needs
CMake ≥ 3.16 and a C++ compiler. Where those aren't installed, run that
package's checks in a container instead:

```sh
cd ankusa_kafka
docker compose up -d --wait
docker run --rm --network ankusa_kafka_default -v "$PWD":/repo \
  -e KAFKA_BROKERS=redpanda:9092 -w /repo/ankusa_kafka \
  elixir:1.20.4-alpine \
  sh -c 'apk add --no-cache -q build-base cmake git && mix deps.get && mix test'
```

Working on something that needs a browser or a running system? Exercise the
real thing (or a throwaway script against it) — see the verification
requirements in the project brief.

Two rules that follow from all of the above:

- **Never report a check as passing unless it ran in this session.** "It
  should work", "the code looks right", and a hand-written log line are not
  evidence.
- **Never mark a task done while its own acceptance criteria fail** — a
  step that crashed, or a drill that was never run, is not done. Say what
  failed and what's still open.
