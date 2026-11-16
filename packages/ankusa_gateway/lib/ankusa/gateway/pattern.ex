defmodule Ankusa.Gateway.Pattern do
  @moduledoc """
  A key pattern: which resources a `rules` authorizer's `keys` entry speaks
  about, compiled once at boot so a decision never parses a string.

      "teams/{team}/**"    "teams/eng/q3.pdf"   -> %{"team" => "eng"}
      "reports/*"          "reports/q3.pdf"     -> %{}
      "a/{b}/c"            "a/x/c"              -> %{"b" => "x"}

  ## Grammar

  A pattern is split on `/`; a segment is

    * a literal — any text without `{`, `}` or `*`;
    * `{name}` — exactly one segment, captured under `name`
      (`~r/\\A[a-z][a-z0-9_]*\\z/`, at most once per pattern);
    * `*` — exactly one segment, not captured;
    * `**` — zero or more segments, only as the last segment, so matching never
      backtracks: it is one pass over the pattern and the key.

  A pattern has 1 to 32 segments, is at most 512 bytes, and no segment is
  empty. `compile!/2` raises `ArgumentError` and every message starts with the
  caller's `label` (the dotted config key), as every gateway option error does.

  ## Matching

  `match/3` splits the key the same way and walks both lists once. The
  `:prefix` kind is the one that differs: its key is matched with the trailing
  `/` removed, and `""` is zero segments (so `**` matches the empty prefix and
  `*` does not). `:object` and `:claim` keys are matched as they are; a claim's
  key is `"<tenant>/<claim_id>"`.
  """

  defstruct segments: []

  @type segment :: {:literal, String.t()} | {:capture, String.t()} | :star | :globstar
  @type t :: %__MODULE__{segments: [segment()]}

  @max_segments 32
  @max_bytes 512
  @capture ~r/\A[a-z][a-z0-9_]*\z/

  @doc "Compile `pattern`; raises `ArgumentError` prefixed with `label`."
  @spec compile!(String.t(), String.t()) :: t()
  def compile!(pattern, label) when is_binary(pattern) and is_binary(label) do
    if byte_size(pattern) > @max_bytes do
      raise ArgumentError,
            "#{label}: a pattern may be at most #{@max_bytes} bytes, got #{byte_size(pattern)}"
    end

    segments = String.split(pattern, "/")
    count = length(segments)

    if count > @max_segments do
      raise ArgumentError,
            "#{label}: a pattern has at most #{@max_segments} segments, got #{count}"
    end

    compiled =
      segments
      |> Enum.with_index(1)
      |> Enum.map(fn {segment, index} -> segment!(segment, index == count, label) end)

    duplicate = compiled |> captures() |> duplicates()

    if duplicate do
      raise ArgumentError, "#{label}: the capture {#{duplicate}} appears twice in the pattern"
    end

    %__MODULE__{segments: compiled}
  end

  def compile!(pattern, label) do
    raise ArgumentError, "#{label}: a pattern must be a string, got #{inspect(pattern)}"
  end

  @doc """
  Match `key` against `pattern` for a resource of `kind`: `{:ok, captures}` for
  every `{name}` segment, or `:nomatch`.
  """
  @spec match(t(), :object | :prefix | :claim, String.t()) ::
          {:ok, %{optional(String.t()) => String.t()}} | :nomatch
  def match(%__MODULE__{segments: pattern}, kind, key)
      when kind in [:object, :prefix, :claim] and is_binary(key) do
    match_segments(pattern, key_segments(key, kind), %{})
  end

  # ── segments ────────────────────────────────────────────────────────────────

  defp segment!("", _last?, label),
    do: raise(ArgumentError, "#{label}: a pattern has no empty segment")

  defp segment!("**", true, _label), do: :globstar

  defp segment!("**", false, label),
    do: raise(ArgumentError, "#{label}: ** must be the last segment of a pattern")

  defp segment!("*", _last?, _label), do: :star

  defp segment!("{" <> rest, _last?, label) do
    case String.split(rest, "}") do
      [name, ""] ->
        if Regex.match?(@capture, name),
          do: {:capture, name},
          else: raise(ArgumentError, "#{label}: {#{name}} is not a capture (expected {name})")

      _ ->
        raise ArgumentError, "#{label}: a {capture} must be {name} with no other braces"
    end
  end

  defp segment!(segment, _last?, label) do
    if String.contains?(segment, ["{", "}", "*"]),
      do: raise(ArgumentError, "#{label}: invalid segment #{inspect(segment)} in a pattern"),
      else: {:literal, segment}
  end

  defp captures(segments), do: for({:capture, name} <- segments, do: name)

  defp duplicates(names) do
    names
    |> Enum.reduce({MapSet.new(), nil}, fn name, {seen, found} ->
      if found || MapSet.member?(seen, name),
        do: {seen, found || name},
        else: {MapSet.put(seen, name), nil}
    end)
    |> elem(1)
  end

  defp key_segments("", _kind), do: []

  defp key_segments(key, :prefix) do
    case String.trim_trailing(key, "/") do
      "" -> []
      key -> String.split(key, "/")
    end
  end

  defp key_segments(key, _kind), do: String.split(key, "/")

  # ── matching ────────────────────────────────────────────────────────────────

  defp match_segments([], [], captures), do: {:ok, captures}
  defp match_segments([:globstar | _rest], _key, captures), do: {:ok, captures}
  defp match_segments([_segment | _rest], [], _captures), do: :nomatch

  defp match_segments([{:literal, value} | rest], [value | key], captures),
    do: match_segments(rest, key, captures)

  defp match_segments([:star | rest], [_segment | key], captures),
    do: match_segments(rest, key, captures)

  defp match_segments([{:capture, name} | rest], [value | key], captures),
    do: match_segments(rest, key, Map.put(captures, name, value))

  defp match_segments(_pattern, _key, _captures), do: :nomatch
end
