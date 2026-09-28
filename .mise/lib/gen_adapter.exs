# Renders .mise/templates/adapter/ into packages/ankusa_<name>/ for
# `mise run new:adapter`. Plain `elixir`, run from the repo root.
#
#   elixir .mise/lib/gen_adapter.exs NAME MODULE CORE_VERSION
#
# Every `*.eex` template renders to the same relative path with `.eex`
# stripped and `__name__` in path segments replaced by NAME. Assigns: name,
# module, app (ankusa_NAME), camel (Macro.camelize(NAME)), core_req (~> M.m of
# the core version).
[name, module, core_version] =
  case System.argv() do
    [_, _, _] = args ->
      args

    _ ->
      IO.puts(:stderr, "usage: elixir .mise/lib/gen_adapter.exs NAME MODULE CORE_VERSION")
      System.halt(1)
  end

templates = Path.expand(".mise/templates/adapter")
app = "ankusa_#{name}"
out = Path.join("packages", app)

if File.exists?(out) do
  IO.puts(:stderr, "#{out} already exists")
  System.halt(1)
end

%Version{major: major, minor: minor} = Version.parse!(core_version)

assigns = [
  name: name,
  module: module,
  app: app,
  camel: Macro.camelize(name),
  core_req: "~> #{major}.#{minor}"
]

for path <- Path.wildcard(Path.join(templates, "**/*.eex"), match_dot: true) do
  target =
    path
    |> Path.relative_to(templates)
    |> String.replace_suffix(".eex", "")
    |> String.replace("__name__", name)

  dest = Path.join(out, target)
  File.mkdir_p!(Path.dirname(dest))
  File.write!(dest, EEx.eval_file(path, assigns: assigns))
  IO.puts("  create #{dest}")
end

File.cp!("LICENSE", Path.join(out, "LICENSE"))
IO.puts("  create #{Path.join(out, "LICENSE")}")
