# Version and CHANGELOG edits for `mise run release:prepare`. Plain `elixir`,
# no Mix project; any error exits 1 with a message on stderr.
#
#   elixir .mise/lib/release.exs plan  DIR BUMP
#     Prints the next version. BUMP is patch|minor|major or an explicit
#     version greater than the current one. Fails when CHANGELOG.md has
#     nothing under [Unreleased].
#
#   elixir .mise/lib/release.exs apply DIR NEW TAG_PREFIX PREV_TAG DATE
#     Sets @version in mix.exs, `version` in Cargo.toml for a Cargo package,
#     VERSION in lib/**/version.rb for a gem, `const Version` in version.go
#     for a Go module, `VERSION` in src/Version.php for the PHP SDK, or
#     `version :=` in build.sbt for an sbt package, `(def version` in build.clj
#     for a Clojure (deps.edn) package (npm
#     versions are `npm version`'s job; pyproject.toml versions are
#     `uv version`'s job), opens
#     `## [NEW] - DATE` under [Unreleased] in CHANGELOG.md, and points the
#     footer compare links at the new tag. PREV_TAG may be "" (first release).
defmodule Release do
  @repo "https://github.com/jamescarr/ankusa"
  @version_re ~r/^  @version "([^"]+)"$/m
  @toml_version_re ~r/^version = "([^"]+)"$/m
  # [ \t]*, not \s*: in multiline mode \s would swallow preceding newlines.
  @gem_version_re ~r/^([ \t]*)VERSION = "([^"]+)"$/m
  @go_version_re ~r/^const Version = "([^"]+)"$/m
  @php_version_re ~r/^    public const string VERSION = '([^']+)';$/m
  @sbt_version_re ~r/^version := "([^"]+)"$/m
  @clj_version_re ~r/^\(def version "([^"]+)"\)$/m

  def main(["plan", dir, bump]) do
    current = current_version(dir)
    ensure_unreleased_entries!(dir)
    IO.puts(next_version(current, bump))
  end

  def main(["apply", dir, new, prefix, prev_tag, date]) do
    Version.parse(new) == :error && die("#{new} is not a version")

    cargo_toml = Path.join(dir, "Cargo.toml")

    cond do
      File.exists?(Path.join(dir, "composer.json")) ->
        php = Path.join(dir, "src/Version.php")
        source = File.read!(php)
        _ = php_version!(source, php)

        File.write!(
          php,
          Regex.replace(@php_version_re, source, "    public const string VERSION = '#{new}';")
        )

      # Their own tools bump these: `npm version` and `uv version`.
      File.exists?(Path.join(dir, "package.json")) or
          File.exists?(Path.join(dir, "pyproject.toml")) ->
        :ok

      File.exists?(cargo_toml) ->
        source = File.read!(cargo_toml)
        _ = toml_version!(source, cargo_toml)
        File.write!(cargo_toml, Regex.replace(@toml_version_re, source, ~s(version = "#{new}")))

      File.exists?(Path.join(dir, "mix.exs")) ->
        mix = Path.join(dir, "mix.exs")
        source = File.read!(mix)
        _ = mix_version!(source, mix)
        File.write!(mix, Regex.replace(@version_re, source, ~s(  @version "#{new}")))

      Path.wildcard(Path.join(dir, "*.gemspec")) != [] ->
        file = gem_version_file!(dir)
        source = File.read!(file)
        _ = gem_version!(source, file)
        File.write!(file, Regex.replace(@gem_version_re, source, ~s(\\1VERSION = "#{new}")))

      File.exists?(Path.join(dir, "version.go")) ->
        file = Path.join(dir, "version.go")
        source = File.read!(file)
        _ = go_version!(source, file)
        File.write!(file, Regex.replace(@go_version_re, source, ~s(const Version = "#{new}")))

      File.exists?(Path.join(dir, "build.sbt")) ->
        file = Path.join(dir, "build.sbt")
        source = File.read!(file)
        _ = sbt_version!(source, file)
        File.write!(file, Regex.replace(@sbt_version_re, source, ~s(version := "#{new}")))

      File.exists?(Path.join(dir, "deps.edn")) ->
        file = Path.join(dir, "build.clj")
        source = File.read!(file)
        _ = clj_version!(source, file)
        File.write!(file, Regex.replace(@clj_version_re, source, ~s[(def version "#{new}")]))

      true ->
        die("#{dir} has none of package.json, pyproject.toml, Cargo.toml, composer.json, mix.exs, *.gemspec, version.go, build.sbt, or deps.edn")
    end

    changelog = Path.join(dir, "CHANGELOG.md")

    changelog
    |> File.read!()
    |> String.split("\n")
    |> open_section(new, date, changelog)
    |> link_footer(new, prefix, prev_tag)
    |> Enum.join("\n")
    |> then(&File.write!(changelog, &1))
  end

  def main(_) do
    die("""
    usage: elixir .mise/lib/release.exs plan DIR BUMP
           elixir .mise/lib/release.exs apply DIR NEW TAG_PREFIX PREV_TAG DATE\
    """)
  end

  defp current_version(dir) do
    package_json = Path.join(dir, "package.json")
    pyproject = Path.join(dir, "pyproject.toml")
    cargo_toml = Path.join(dir, "Cargo.toml")
    composer = Path.join(dir, "composer.json")
    mix = Path.join(dir, "mix.exs")
    version_go = Path.join(dir, "version.go")
    build_sbt = Path.join(dir, "build.sbt")
    deps_edn = Path.join(dir, "deps.edn")

    cond do
      File.exists?(package_json) ->
        case package_json |> File.read!() |> JSON.decode!() do
          %{"version" => v} when is_binary(v) -> v
          _ -> die("#{package_json} has no \"version\"")
        end

      File.exists?(pyproject) ->
        pyproject |> File.read!() |> toml_version!(pyproject)

      File.exists?(cargo_toml) ->
        cargo_toml |> File.read!() |> toml_version!(cargo_toml)

      File.exists?(composer) ->
        php = Path.join(dir, "src/Version.php")
        php |> File.read!() |> php_version!(php)

      File.exists?(mix) ->
        mix |> File.read!() |> mix_version!(mix)

      Path.wildcard(Path.join(dir, "*.gemspec")) != [] ->
        file = gem_version_file!(dir)
        file |> File.read!() |> gem_version!(file)

      File.exists?(version_go) ->
        version_go |> File.read!() |> go_version!(version_go)

      File.exists?(build_sbt) ->
        build_sbt |> File.read!() |> sbt_version!(build_sbt)

      File.exists?(deps_edn) ->
        build_clj = Path.join(dir, "build.clj")
        build_clj |> File.read!() |> clj_version!(build_clj)

      true ->
        die("#{dir} has none of package.json, pyproject.toml, Cargo.toml, composer.json, mix.exs, *.gemspec, version.go, build.sbt, or deps.edn")
    end
  end

  # The one lib/**/version.rb a gem package ships.
  defp gem_version_file!(dir) do
    case Path.wildcard(Path.join(dir, "lib/**/version.rb")) do
      [file] -> file
      [] -> die("#{dir} has no lib/**/version.rb")
      _ -> die("#{dir} has more than one lib/**/version.rb")
    end
  end

  defp gem_version!(source, path) do
    case Regex.scan(@gem_version_re, source) do
      [[_, _, v]] -> v
      [] -> die(~s(#{path} has no `VERSION = "..."` line))
      _ -> die(~s(#{path} has more than one `VERSION = "..."` line))
    end
  end

  defp go_version!(source, path) do
    case Regex.scan(@go_version_re, source) do
      [[_, v]] -> v
      [] -> die(~s(#{path} has no `const Version = "..."` line))
      _ -> die(~s(#{path} has more than one `const Version = "..."` line))
    end
  end

  defp sbt_version!(source, path) do
    case Regex.scan(@sbt_version_re, source) do
      [[_, v]] -> v
      [] -> die(~s(#{path} has no `version := "..."` line))
      _ -> die(~s(#{path} has more than one `version := "..."` line))
    end
  end

  defp clj_version!(source, path) do
    case Regex.scan(@clj_version_re, source) do
      [[_, v]] -> v
      [] -> die(~s[#{path} has no `(def version "...")` line])
      _ -> die(~s[#{path} has more than one `(def version "...")` line])
    end
  end

  # `[package] version = "..."` and PEP 621's `version = "..."` are the same
  # line, so one reader serves both.
  defp toml_version!(source, path) do
    case Regex.scan(@toml_version_re, source) do
      [[_, v]] -> v
      [] -> die(~s(#{path} has no `version = "..."` line))
      _ -> die(~s(#{path} has more than one `version = "..."` line))
    end
  end

  defp mix_version!(source, path) do
    case Regex.scan(@version_re, source) do
      [[_, v]] -> v
      [] -> die(~s(#{path} has no `  @version "..."` line))
      _ -> die(~s(#{path} has more than one `  @version "..."` line))
    end
  end

  defp php_version!(source, path) do
    case Regex.scan(@php_version_re, source) do
      [[_, v]] -> v
      [] -> die(~s(#{path} has no `    public const string VERSION = '...';` line))
      _ -> die(~s(#{path} has more than one `    public const string VERSION = '...';` line))
    end
  end

  defp next_version(current, bump) when bump in ["patch", "minor", "major"] do
    %Version{major: ma, minor: mi, patch: pa} = parse!(current)

    next =
      case bump do
        "major" -> %Version{major: ma + 1, minor: 0, patch: 0}
        "minor" -> %Version{major: ma, minor: mi + 1, patch: 0}
        "patch" -> %Version{major: ma, minor: mi, patch: pa + 1}
      end

    Version.to_string(next)
  end

  defp next_version(current, explicit) do
    case Version.parse(explicit) do
      {:ok, next} ->
        Version.compare(next, parse!(current)) == :gt ||
          die("#{explicit} is not greater than the current version #{current}")

        Version.to_string(next)

      :error ->
        die("bump must be patch, minor, major, or a version; got #{inspect(explicit)}")
    end
  end

  defp parse!(v) do
    case Version.parse(v) do
      {:ok, version} -> version
      :error -> die("current version #{inspect(v)} is not SemVer")
    end
  end

  defp ensure_unreleased_entries!(dir) do
    lines = dir |> Path.join("CHANGELOG.md") |> File.read!() |> String.split("\n")

    entries =
      case Enum.find_index(lines, &unreleased?/1) do
        nil ->
          []

        i ->
          lines
          |> Enum.drop(i + 1)
          |> Enum.take_while(&(not String.starts_with?(&1, "## ")))
          |> Enum.reject(&(String.trim(&1) == ""))
      end

    entries == [] && die("#{dir}/CHANGELOG.md has no [Unreleased] entries")
  end

  defp unreleased?(line), do: String.starts_with?(line, "## [Unreleased]")

  defp open_section(lines, new, date, path) do
    case Enum.find_index(lines, &unreleased?/1) do
      nil -> die("#{path} has no ## [Unreleased] heading")
      i -> List.insert_at(lines, i + 1, ["", "## [#{new}] - #{date}"]) |> List.flatten()
    end
  end

  # Keep-a-Changelog reference links: [Unreleased] compares the new tag to
  # HEAD, [NEW] compares the previous tag to the new one (or links the tag
  # itself on a first release).
  defp link_footer(lines, new, prefix, prev_tag) do
    tag = "#{prefix}-v#{new}"
    unreleased = "[Unreleased]: #{@repo}/compare/#{tag}...HEAD"

    release =
      case prev_tag do
        "" -> "[#{new}]: #{@repo}/releases/tag/#{tag}"
        prev -> "[#{new}]: #{@repo}/compare/#{prev}...#{tag}"
      end

    case Enum.find_index(lines, &String.starts_with?(&1, "[Unreleased]: ")) do
      nil ->
        body = lines |> Enum.reverse() |> Enum.drop_while(&(&1 == "")) |> Enum.reverse()
        body ++ ["", unreleased, release, ""]

      i ->
        lines |> List.replace_at(i, unreleased) |> List.insert_at(i + 1, release)
    end
  end

  defp die(message) do
    IO.puts(:stderr, message)
    System.halt(1)
  end
end

Release.main(System.argv())
