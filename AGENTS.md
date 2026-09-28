# Agent notes

## Run the checks before calling work done

Format, warnings-as-errors, and tests — for **every** package the change
touches, not just the one you edited. Every adapter package and
`ankusa_server` path-depend on `ankusa` core, so a core change isn't finished
until they pass too. Each check is a mise task (`mise tasks ls`); CI runs the
same ones.

| What changed | Run |
| --- | --- |
| a package under `packages/` | `mise run check:package <pkg>` for every touched package (all of them for a core change); it starts and stops the package's own `docker-compose.yml` |
| `examples/*` | `mise run check:examples` |
| `tools/*` | `mise run check:tools` |
| core's object-store adapters | `mise run test:integration` (the `:integration` suite against the floci emulators) |
| anything, before tagging a release | `mise run e2e` — the kind + Oban end-to-end gate (needs `docker`; `kind`/`kubectl` come from `.mise.toml`) |
| everything | `mise run check` |

`mix format --check-formatted` is what CI fails on first: run `mise run format`
before pushing, not after CI tells you.

Changing core's dependencies touches every package that path-depends on it, so
after adding or removing one, run `mise run deps` and commit every `mix.lock`
it changes. CI runs `mix deps.get --check-locked` and
`mix deps.unlock --check-unused` everywhere — including the examples — so a
`deps.get` that rewrites a stale lock, or a lock entry for a dependency nobody
declares, fails the build instead of passing quietly.

What each suite covers: [`docs/testing.md`](docs/testing.md).

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
