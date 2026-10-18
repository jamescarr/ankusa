# Used by "mix format"
[
  # stream_data is a `only: :test` dependency, so `import_deps: [:stream_data]`
  # would fail `mix format` in the dev environment; these are its exported
  # `locals_without_parens` (`check all x <- gen do`, `property "..." do`).
  locals_without_parens: [all: :*, check: 1, check: 2, property: 1, property: 2, property: 3],
  inputs: ["{mix,.formatter}.exs", "{config,lib,test,bench}/**/*.{ex,exs}"]
]
