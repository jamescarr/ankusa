# Contributing

## Before you call a change done

`mise run check` runs every check CI runs, across every package, example, and
tool (`mise install` first for the pinned toolchain). Format,
warnings-as-errors, and tests for **every** package the change touches: every
adapter package depends on `ankusa` core, so a core change isn't finished
until they pass too. `mise run check:package <pkg>` runs one package; the
per-change matrix is in [`AGENTS.md`](AGENTS.md).

Image change? `mise run docker:smoke` builds `jamescarr/ankusa:dev` and runs
`packages/ankusa_server/scripts/smoke.sh` against it.

New sink adapter? `mise run new:adapter <name>` scaffolds
`packages/ankusa_<name>/`. `mise run status` shows every package's version,
last tag, and whether it's published.

## Where things are documented

- Test layout, integration tags, local infra: [`docs/testing.md`](docs/testing.md)
- Why a package gets split, and how to add one: [`docs/packaging.md`](docs/packaging.md)
- Releasing any package (Hex, the server image, npm): [`docs/releasing.md`](docs/releasing.md)
- What the framework guarantees, and how to run it: [`docs/README.md`](docs/README.md)
