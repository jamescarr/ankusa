# Contributing

## Before you call a change done

`mise run check` runs every check CI runs, across every package and example.
Format, warnings-as-errors, and tests for **every** package the change touches —
all three adapter packages depend on `ankusa` core, so a core change isn't
finished until they pass too.

The per-package matrix is in [`AGENTS.md`](AGENTS.md) (which also has the
container recipe for `ankusa_kafka`, whose `crc32cer` NIF needs CMake and a C++
compiler).

Image change? `mise run docker:smoke` builds `jamescarr/ankusa:dev` and runs
`ankusa_server/scripts/smoke.sh` against it.

## Where things are documented

- Test layout, integration tags, local infra: [`docs/testing.md`](docs/testing.md)
- Why a package gets split, and how to add one: [`docs/packaging.md`](docs/packaging.md)
- Releasing a Hex package or the server image: [`docs/releasing.md`](docs/releasing.md)
- What the framework guarantees, and how to run it: [`docs/README.md`](docs/README.md)
