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

Changing core's dependencies touches every package that path-depends on it, so
after adding or removing one, run `mix deps.get` in each of the packages above
and commit their `mix.lock` files. CI runs `mix deps.get --check-locked` and
`mix deps.unlock --check-unused` everywhere — including the examples — so a
`deps.get` that rewrites a stale lock, or a lock entry for a dependency nobody
declares, fails the build instead of passing quietly.

Core's suite excludes `:integration` tests (S3/GCS against a floci
emulator); add `--include integration` with the emulator running. What each
suite covers: [`docs/testing.md`](docs/testing.md).

`ankusa_kafka` compiles a C++ NIF (`crc32cer`, via `brod`), which needs
CMake ≥ 3.16 and a C++ compiler. Where those aren't installed, run that
package's checks in a container instead:

```sh
cd ankusa_kafka
docker compose up -d --wait
# The repo *root* is the mount, not `ankusa_kafka/`: in :dev/:test this package
# path-depends on `..`, so mounting only the package leaves `..` with no
# `mix.exs` and Mix fails before it compiles anything.
# MIX_BUILD_PATH keeps the container's Linux artifacts out of your `_build` —
# a Linux-built `crc32cer` NIF will not load on macOS, and its CMake cache
# records container paths that break a later local build.
docker run --rm --network ankusa_kafka_default -v "$PWD/..":/repo \
  -e KAFKA_BROKERS=redpanda:9092 -e MIX_BUILD_PATH=/tmp/build \
  -w /repo/ankusa_kafka elixir:1.20.4-alpine \
  sh -c 'mix local.hex --force >/dev/null && apk add --no-cache -q build-base cmake git >/dev/null \
         && mix format --check-formatted && mix compile --warnings-as-errors && mix test'
```

The same applies to `examples/kafka-sqs-consumer/ingest_app`, which path-depends
on both core and `ankusa_kafka`.

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

## Dependencies

**Take the dependency when it is the right tool.** Judge it on merit: is the
library more correct, better tested, and less surface for us to own and be wrong
about than the code it replaces? Then use it. "Zero dependencies" is not a goal,
and refusing a good library to keep a count at zero is not a principle — it is
how you end up maintaining hand-rolled SigV4 signing.

The rule that *does* hold is about placement: a dependency only some deployments
need goes in its own adapter package, so nobody else's `mix deps.get` compiles
it. A dependency every user benefits from belongs in `ankusa` core. Full decision
rule, and the worked examples: [`docs/packaging.md`](docs/packaging.md).
