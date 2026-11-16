defmodule Ankusa.Gateway.PatternTest do
  @moduledoc """
  The key-pattern grammar and its matching: literals, `{name}` captures, `*`,
  a trailing `**`, the three resource kinds, and the boot-time errors — with
  StreamData properties for the grammar's three shapes.
  """

  use ExUnit.Case, async: true
  use ExUnitProperties

  import StreamData

  alias Ankusa.Gateway.Pattern

  @label "gateway.authz.authorizers.team.rules[0].keys[0]"

  defp compile!(pattern), do: Pattern.compile!(pattern, @label)

  defp segment, do: string(:alphanumeric, min_length: 1, max_length: 6)

  describe "a literal pattern" do
    test "matches only the key it spells" do
      pattern = compile!("teams/eng/q3.pdf")

      assert Pattern.match(pattern, :object, "teams/eng/q3.pdf") == {:ok, %{}}
      assert Pattern.match(pattern, :object, "teams/eng/q4.pdf") == :nomatch
      assert Pattern.match(pattern, :object, "teams/eng") == :nomatch
      assert Pattern.match(pattern, :object, "teams/eng/q3.pdf/x") == :nomatch
    end

    test "matches a claim's <tenant>/<claim_id> key" do
      pattern = compile!("acme/{claim}")

      assert Pattern.match(pattern, :claim, "acme/01M39VMD8RA3C5HR4RBV67Y002") ==
               {:ok, %{"claim" => "01M39VMD8RA3C5HR4RBV67Y002"}}

      assert Pattern.match(pattern, :claim, "other/01M39VMD8RA3C5HR4RBV67Y002") == :nomatch
    end
  end

  describe "captures and wildcards" do
    test "{name} captures exactly one segment" do
      pattern = compile!("teams/{team}/members/{member}")

      assert Pattern.match(pattern, :object, "teams/eng/members/alice") ==
               {:ok, %{"team" => "eng", "member" => "alice"}}

      assert Pattern.match(pattern, :object, "teams/eng/members") == :nomatch
      assert Pattern.match(pattern, :object, "teams/eng/members/alice/x") == :nomatch
    end

    test "* matches one segment and captures nothing" do
      pattern = compile!("reports/*/{name}")

      assert Pattern.match(pattern, :object, "reports/q3/name") == {:ok, %{"name" => "name"}}
      assert Pattern.match(pattern, :object, "reports/2025/q3/name") == :nomatch
      assert Pattern.match(pattern, :object, "reports/name") == :nomatch
    end

    test "** matches every remaining segment, including none" do
      pattern = compile!("teams/{team}/**")

      assert Pattern.match(pattern, :object, "teams/eng") == {:ok, %{"team" => "eng"}}

      assert Pattern.match(pattern, :object, "teams/eng/a/b/c.txt") ==
               {:ok, %{"team" => "eng"}}

      assert Pattern.match(pattern, :object, "teams") == :nomatch
    end

    test "** alone matches every key, including the empty prefix" do
      pattern = compile!("**")

      assert Pattern.match(pattern, :object, "a/b") == {:ok, %{}}
      assert Pattern.match(pattern, :prefix, "") == {:ok, %{}}
      assert Pattern.match(pattern, :object, "") == {:ok, %{}}
    end
  end

  describe "the prefix kind" do
    test "is matched with its trailing slash removed" do
      pattern = compile!("a/b")

      assert Pattern.match(pattern, :prefix, "a/b/") == {:ok, %{}}
      assert Pattern.match(pattern, :prefix, "a/b") == {:ok, %{}}
      assert Pattern.match(pattern, :prefix, "a/b/c") == :nomatch
      assert Pattern.match(pattern, :object, "a/b/") == :nomatch
    end

    test "the empty prefix is zero segments" do
      assert Pattern.match(compile!("**"), :prefix, "") == {:ok, %{}}
      assert Pattern.match(compile!("*"), :prefix, "") == :nomatch
      assert Pattern.match(compile!("*"), :prefix, "a/") == {:ok, %{}}
    end

    test "a deeper prefix still needs the pattern's segments" do
      assert Pattern.match(compile!("a/*"), :prefix, "a/b/") == {:ok, %{}}
      assert Pattern.match(compile!("a/*"), :prefix, "a/") == :nomatch
    end
  end

  describe "compile!/2" do
    test "raises with the caller's label" do
      assert_raise ArgumentError, ~r/\A#{Regex.escape(@label)}: /, fn ->
        Pattern.compile!("**/a", @label)
      end
    end

    test "refuses a ** that is not the last segment" do
      assert_raise ArgumentError, ~r/\*\* must be the last segment/, fn -> compile!("a/**/b") end
      assert_raise ArgumentError, ~r/\*\* must be the last segment/, fn -> compile!("**/**") end
    end

    test "refuses an empty segment" do
      for pattern <- ["", "/a", "a/", "a//b"] do
        assert_raise ArgumentError, ~r/no empty segment/, fn -> compile!(pattern) end
      end
    end

    test "refuses a malformed capture" do
      for pattern <- ["{Team}", "{1x}", "{team", "team}", "{a}{b}", "{a b}"] do
        assert_raise ArgumentError, ~r/#{Regex.escape(@label)}: /, fn -> compile!(pattern) end
      end
    end

    test "refuses a stray brace or star in a literal" do
      for pattern <- ["a{b}", "a}b", "a*b"] do
        assert_raise ArgumentError, ~r/invalid segment/, fn -> compile!(pattern) end
      end
    end

    test "refuses a capture name used twice" do
      assert_raise ArgumentError, ~r/\{name\} appears twice/, fn -> compile!("{name}/{name}") end
    end

    test "allows a capture name used twice in different patterns" do
      assert %Pattern{} = compile!("{name}")
      assert %Pattern{} = compile!("{name}/{other}")
    end

    test "counts the segments" do
      assert %Pattern{} = compile!(Enum.join(List.duplicate("a", 32), "/"))

      assert_raise ArgumentError, ~r/at most 32 segments/, fn ->
        compile!(Enum.join(List.duplicate("a", 33), "/"))
      end
    end

    test "counts the bytes" do
      assert %Pattern{} = compile!(String.duplicate("a", 512))

      assert_raise ArgumentError, ~r/at most 512 bytes/, fn ->
        compile!(String.duplicate("a", 513))
      end
    end

    test "refuses a pattern that is not a string" do
      assert_raise ArgumentError, ~r/#{Regex.escape(@label)}: a pattern must be a string/, fn ->
        Pattern.compile!(:nope, @label)
      end
    end
  end

  describe "properties" do
    property "a literal pattern matches only its own key" do
      check all segments <- list_of(segment(), min_length: 1, max_length: 6) do
        key = Enum.join(segments, "/")
        pattern = compile!(key)

        assert Pattern.match(pattern, :object, key) == {:ok, %{}}
        assert Pattern.match(pattern, :object, key <> "x") == :nomatch
      end
    end

    property "a {name} pattern captures exactly one segment" do
      check all before_segments <- list_of(segment(), max_length: 3),
                captured <- segment(),
                after_segments <- list_of(segment(), max_length: 3) do
        pattern = compile!(Enum.join(before_segments ++ ["{value}"] ++ after_segments, "/"))
        key = Enum.join(before_segments ++ [captured] ++ after_segments, "/")

        assert Pattern.match(pattern, :object, key) == {:ok, %{"value" => captured}}

        shorter = Enum.join(before_segments ++ after_segments, "/")
        assert Pattern.match(pattern, :object, shorter) == :nomatch
      end
    end

    property "** matches any suffix, including none" do
      check all prefix <- list_of(segment(), min_length: 1, max_length: 3),
                suffix <- list_of(segment(), max_length: 4) do
        pattern = compile!(Enum.join(prefix ++ ["**"], "/"))
        key = Enum.join(prefix ++ suffix, "/")

        assert Pattern.match(pattern, :object, key) == {:ok, %{}}
        assert Pattern.match(pattern, :object, Enum.join(["zz-other" | suffix], "/")) == :nomatch
      end
    end
  end
end
