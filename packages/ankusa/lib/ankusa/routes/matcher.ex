defmodule Ankusa.Routes.Matcher do
  @moduledoc """
  Path patterns: the grammar (`segments/1`), the compiled form
  (`compile/1`), matching (`match?/2`), and request-path normalization
  (`normalize/2`).

  A compiled pattern is a list of `t:segment/0`s — no regex, no backtracking,
  no way for a crafted request path to cost more than its own length. Literals
  compare with `==`; `:param` consumes exactly one segment; `:wildcard` (always
  last) requires **one or more** remaining segments, so `/hooks/shopify/*` is
  `/hooks/shopify/<something>` and a bare `/hooks/shopify` needs its own route.

  ## Normalization

  `normalize/2` is the only place a request path is turned into matchable
  segments, so both the guard and the dry run agree on what "the same path"
  means:

    * a percent-encoded slash (`%2F`, any case) is a rejection, not a segment
      boundary — otherwise `/hooks/a%2Fb` would authorize as `/hooks/a/b`;
    * `.` and `..` segments are rejections, not resolved (`/hooks/a/../b` must
      not become `/hooks/b`);
    * empty segments from `//` or a trailing `/` are dropped, and a path with
      nothing left is a rejection.

  Plug does **not** percent-decode `path_info`: `Plug.Conn.Adapter` only splits
  the path on `/`, so the segments the guard sees are the raw, still-encoded
  ones and a `%2F` never turns into a segment boundary here. It is rejected all
  the same — an encoded slash is never a legitimate route path, and a later stage
  that did decode it would see a different path than the one that was authorized
  — and it is detected on the raw `request_path`.

  Because segments are matched raw, a literal in a route pattern is limited to
  characters that never need percent-encoding (see `segments/1`), so a pattern
  that no real request could match is refused when the route is written rather
  than accepted and silently dead. An encoded request segment matches only a
  `:param` or the wildcard, never a literal: `/hooks/%73tripe` is not
  `/hooks/stripe`. Encoded dots (`%2e%2e`) are ordinary opaque segments, not
  `..`. Nothing in Ankusa decodes a path, so nothing in Ankusa resolves one —
  but the request path is recorded verbatim in the envelope and reaches whatever
  consumes it, so a consumer that decodes it owns that step. The guard's job is
  only that the route it matched is the one the sender addressed.
  """

  alias Ankusa.Routes.Route

  # `Matcher.match?/2` is the pattern matcher; `Kernel.match?/2` (the pattern
  # assertion) is not used in this module, so the local definition takes the
  # name outright.
  import Kernel, except: [match?: 2]

  @type segment :: {:literal, String.t()} | :param | :wildcard

  @wildcard "*"

  # A literal segment is URL-path-safe ASCII with no reserved meaning. `%` is
  # excluded: request segments are matched still-encoded, so a literal that needed
  # percent-encoding would be ambiguous between its spellings. Characters that are
  # their own encoding leave nothing to disagree about.
  @literal_re ~r/\A[A-Za-z0-9._~-]+\z/
  @param_re ~r/\A:[A-Za-z_][A-Za-z0-9_]*\z/

  @doc """
  Validate a path pattern and compile it to segments.

  `{:error, message}` is the message that goes straight into a route
  validation error, so it is phrased as a field complaint.
  """
  @spec segments(String.t()) :: {:ok, [segment()]} | {:error, String.t()}
  def segments(pattern) when is_binary(pattern) do
    pattern
    |> String.split("/")
    |> Enum.reject(&(&1 == ""))
    |> compile([])
  end

  @doc """
  Compile a route's path pattern. Raises if the pattern is invalid, which can
  only happen for a `%Route{}` that was not built through
  `Ankusa.Routes.Route.from_attrs/2`.
  """
  @spec compile(Route.t()) :: [segment()]
  def compile(%Route{path: path}) do
    case segments(path) do
      {:ok, segments} ->
        segments

      {:error, message} ->
        raise ArgumentError, "route #{inspect(path)} has an invalid path: #{message}"
    end
  end

  @doc """
  Does a compiled pattern match these request segments?

  Both sides are non-empty-segment lists (`normalize/2` and `segments/1` drop
  empties), so a `:param` never matches an empty string.
  """
  @spec match?([segment()], [String.t()]) :: boolean()
  def match?([], []), do: true
  def match?([:wildcard], [_ | _]), do: true
  def match?([:wildcard], []), do: false
  def match?([{:literal, literal} | rest], [literal | path]), do: match?(rest, path)
  def match?([{:literal, _literal} | _rest], _path), do: false
  def match?([:param | rest], [_segment | path]), do: match?(rest, path)
  def match?([:param | _rest], []), do: false
  def match?([], [_segment | _path]), do: false

  @doc """
  Normalize request-path input into matchable segments, or `:error` for a path
  no route may match.
  """
  @spec normalize([String.t()], String.t()) :: {:ok, [String.t()]} | :error
  def normalize(path_info, request_path) when is_list(path_info) do
    if encoded_slash?(request_path) or Enum.any?(path_info, &(&1 in [".", ".."])) do
      :error
    else
      case Enum.reject(path_info, &(&1 == "")) do
        [] -> :error
        segments -> {:ok, segments}
      end
    end
  end

  @doc """
  The sort key for candidate ordering: more literal segments first, then
  patterns without a wildcard. Ties are broken by route id, which
  `Ankusa.Routes.Snapshot` does.
  """
  @spec specificity([segment()]) :: {non_neg_integer(), boolean()}
  def specificity(segments) do
    literals =
      Enum.count(segments, fn
        {:literal, _literal} -> true
        _segment -> false
      end)

    {literals, :wildcard in segments}
  end

  # ── internals ───────────────────────────────────────────────────────────────

  defp compile([], []), do: {:error, "must contain at least one segment"}
  defp compile([], acc), do: {:ok, Enum.reverse(acc)}
  defp compile([@wildcard], acc), do: {:ok, Enum.reverse([:wildcard | acc])}

  defp compile([@wildcard | _rest], _acc), do: {:error, invalid_segment(@wildcard)}

  defp compile([segment | rest], acc) do
    cond do
      segment in [".", ".."] -> {:error, invalid_segment(segment)}
      Regex.match?(@param_re, segment) -> compile(rest, [:param | acc])
      Regex.match?(@literal_re, segment) -> compile(rest, [{:literal, segment} | acc])
      true -> {:error, invalid_segment(segment)}
    end
  end

  defp invalid_segment(segment), do: "invalid segment #{inspect(segment)}"

  defp encoded_slash?(request_path) when is_binary(request_path) do
    # Only two spellings exist (`2` is a digit), so no lowercased copy of an
    # attacker-sized path is needed to find one.
    String.contains?(request_path, ["%2F", "%2f"])
  end

  defp encoded_slash?(_request_path), do: false
end
