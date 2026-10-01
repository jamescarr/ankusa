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
#     VERSION in lib/**/version.rb for a gem, or `const Version` in version.go
#     for a Go module (npm versions are `npm version`'s job; pyproject.toml
#     versions are `uv version`'s job), opens
#     `## [NEW] - DATE` under [Unreleased] in CHANGELOG.md, and points the
#     footer compare links at the new tag. PREV_TAG may be "" (first release).
defmodule Release do
  @repo "https://github.com/jamescarr/ankusa"
  @version_re ~r/^  @version "([^"]+)"$/m
  @toml_version_re ~r/^version = "([^"]+)"$/m
  # [ \t]*, not \s*: in multiline mode \s would swallow preceding newlines.
  @gem_version_re ~r/^([ \t]*)VERSION = "([^"]+)"$/m
  @go_version_re ~r/^const Version = "([^"]+)"$/m

  def main(["plan", dir, bump]) do
    current = current_version(dir)
    ensure_unreleased_entries!(dir)
    IO.puts(next_version(current, bump))
  end

  def main(["apply", dir, new, prefix, prev_tag, date]) do
    Version.parse(new) == :error && die("#{new} is not a version")

    cargo_toml = Path.join(dir, "Cargo.toml")

    cond do
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

      true ->
        die("#{dir} has none of package.json, pyproject.toml, Cargo.toml, mix.exs, *.gemspec, or version.go")
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
    mix = Path.join(dir, "mix.exs")
    version_go = Path.join(dir, "version.go")

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

      File.exists?(mix) ->
        mix |> File.read!() |> mix_version!(mix)

      Path.wildcard(Path.join(dir, "*.gemspec")) != [] ->
        file = gem_version_file!(dir)
        file |> File.read!() |> gem_version!(file)

      File.exists?(version_go) ->
        version_go |> File.read!() |> go_version!(version_go)

      true ->
        die("#{dir} has none of package.json, pyproject.toml, Cargo.toml, mix.exs, *.gemspec, or version.go")
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
