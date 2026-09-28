defmodule Ankusa.Routes.MatcherTest do
  @moduledoc """
  The path-pattern grammar and matching. Every case here is a decision about
  which requests get captured, so the negative cases matter as much as the
  positive ones.
  """

  use ExUnit.Case, async: true

  alias Ankusa.Routes.{Matcher, Route}

  defp compile(path), do: Matcher.compile(%Route{path: path})

  defp matches?(pattern, path), do: Matcher.match?(compile(pattern), path)

  describe "segments/1" do
    test "compiles literals, params, and a trailing wildcard" do
      assert Matcher.segments("/hooks/:tenant/github") ==
               {:ok, [{:literal, "hooks"}, :param, {:literal, "github"}]}

      assert Matcher.segments("/hooks/shopify/*") ==
               {:ok, [{:literal, "hooks"}, {:literal, "shopify"}, :wildcard]}
    end

    test "collapses duplicate and trailing slashes" do
      assert Matcher.segments("/hooks//stripe/") ==
               {:ok, [{:literal, "hooks"}, {:literal, "stripe"}]}
    end

    test "rejects a wildcard that is not last" do
      assert Matcher.segments("/hooks/*/x") == {:error, "invalid segment \"*\""}
    end

    test "rejects a colon inside a literal segment" do
      assert Matcher.segments("/hooks/a:b") == {:error, "invalid segment \"a:b\""}

      assert Matcher.segments("/hooks/:1") == {:error, "invalid segment \":1\""}
    end

    test "rejects a percent-encoded byte, a dot segment, and reserved characters" do
      assert Matcher.segments("/hooks/a%20b") == {:error, "invalid segment \"a%20b\""}
      assert Matcher.segments("/hooks/..") == {:error, "invalid segment \"..\""}
      assert Matcher.segments("/hooks/a?b") == {:error, "invalid segment \"a?b\""}
    end

    test "rejects a pattern with no segments" do
      assert Matcher.segments("/") == {:error, "must contain at least one segment"}
      assert Matcher.segments("//") == {:error, "must contain at least one segment"}
    end
  end

  describe "compile/1" do
    test "raises for a path that never passed validation" do
      assert_raise ArgumentError, ~r/invalid path/, fn ->
        Matcher.compile(%Route{path: "/hooks/*/x"})
      end
    end
  end

  describe "match?/2" do
    test "an exact pattern matches only itself" do
      assert matches?("/hooks/stripe", ["hooks", "stripe"])
      refute matches?("/hooks/stripe", ["hooks", "stripe", "extra"])
      refute matches?("/hooks/stripe", ["hooks"])
      refute matches?("/hooks/stripe", ["hooks", "shopify"])
    end

    test "a param consumes exactly one segment" do
      assert matches?("/hooks/:tenant/github", ["hooks", "acme", "github"])
      refute matches?("/hooks/:tenant/github", ["hooks", "acme", "b", "github"])
      refute matches?("/hooks/:tenant/github", ["hooks", "github"])
    end

    test "a wildcard needs one or more remaining segments" do
      assert matches?("/hooks/shopify/*", ["hooks", "shopify", "a"])
      assert matches?("/hooks/shopify/*", ["hooks", "shopify", "a", "b"])
      refute matches?("/hooks/shopify/*", ["hooks", "shopify"])
      refute matches?("/hooks/shopify/*", ["hooks"])
    end

    test "a wildcard-only pattern matches any path" do
      assert matches?("/*", ["anything", "at", "all"])
      refute matches?("/*", [])
    end
  end

  describe "normalize/2" do
    test "drops empty segments, which is how a duplicate slash and a trailing slash go away" do
      assert Matcher.normalize(["hooks", "", "stripe", ""], "/hooks//stripe/") ==
               {:ok, ["hooks", "stripe"]}
    end

    test "rejects an encoded slash rather than treating it as a segment boundary" do
      assert Matcher.normalize(["hooks", "a%2Fb"], "/hooks/a%2Fb") == :error
      assert Matcher.normalize(["hooks", "a%2Fb"], "/hooks/a%2fb") == :error
    end

    test "rejects dot segments rather than resolving them" do
      assert Matcher.normalize(["hooks", "a", "..", "b"], "/hooks/a/../b") == :error
      assert Matcher.normalize(["hooks", "."], "/hooks/.") == :error
    end

    test "rejects a path with nothing left" do
      assert Matcher.normalize([], "/") == :error
      assert Matcher.normalize([""], "/") == :error
    end
  end

  describe "specificity/1 and segments_to_path/1" do
    test "counts literal segments and reports a wildcard" do
      assert Matcher.specificity(compile("/hooks/stripe")) == {2, false}
      assert Matcher.specificity(compile("/hooks/:tenant")) == {1, false}
      assert Matcher.specificity(compile("/hooks/*")) == {1, true}
    end

    test "renders segments back to a path" do
      assert Matcher.segments_to_path(compile("/hooks/:tenant/*")) == "/hooks/:param/*"
      assert Matcher.segments_to_path(["hooks", "stripe"]) == "/hooks/stripe"
    end
  end

  describe "route path validation" do
    test "accepts the three segment forms" do
      for path <- ["/hooks/stripe", "/hooks/:tenant/github", "/hooks/shopify/*", "/a"] do
        assert {:ok, %Route{path: ^path}} = Route.from_attrs(%{"path" => path})
      end
    end

    test "names the offending segment" do
      assert Route.from_attrs(%{"path" => "/hooks/*/x"}) ==
               {:error, {:invalid, "path", "invalid segment \"*\""}}

      assert Route.from_attrs(%{"path" => "/hooks/a:b"}) ==
               {:error, {:invalid, "path", "invalid segment \"a:b\""}}
    end

    test "rejects a path that is not rooted, carries a query, or is empty" do
      assert Route.from_attrs(%{"path" => "hooks/x"}) ==
               {:error, {:invalid, "path", "must start with \"/\""}}

      assert Route.from_attrs(%{"path" => "/hooks/x?y=1"}) ==
               {:error, {:invalid, "path", "must not contain \"?\" or \"#\""}}

      assert Route.from_attrs(%{"path" => "/"}) ==
               {:error, {:invalid, "path", "must contain at least one segment"}}
    end
  end
end
