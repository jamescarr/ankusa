# Credo config for every Mix package in this repo (`mise run check:package`
# runs `mix credo --strict` in each; Credo finds this file by walking up from the
# package directory).
#
# The gate is the default check set under --strict, with three adjustments that
# record how this codebase is written rather than hide findings:
#
#   * Design.AliasUsage is off: modules are called by their full
#     `Ankusa.Queue.Writer`-style names on purpose, so a call site reads the same
#     without scrolling to the alias block.
#   * The two complexity checks keep a ceiling at the worst function that
#     exists today (nesting 5, cyclomatic 20) instead of the stock 2 and 9, so
#     new code cannot go past what is already there. Lowering a ceiling is a
#     refactor of that function, not a config change.
%{
  configs: [
    %{
      name: "default",
      strict: true,
      checks: %{
        extra: [
          {Credo.Check.Refactor.Nesting, [max_nesting: 5]},
          {Credo.Check.Refactor.CyclomaticComplexity, [max_complexity: 20]}
        ],
        disabled: [
          {Credo.Check.Design.AliasUsage, []}
        ]
      }
    }
  ]
}
