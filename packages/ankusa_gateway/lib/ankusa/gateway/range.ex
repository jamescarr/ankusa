defmodule Ankusa.Gateway.Range do
  @moduledoc """
  The one `Range` form the gateway serves: a single `bytes` range, in two steps
  so a caller can parse before it knows the length.

  `parse/1` reads the request: `bytes=a-b` is `{:span, a, b}`, `bytes=a-` is
  `{:from, a}`, `bytes=-n` is `{:suffix, n}`. Several ranges, another unit, a
  malformed spec, or any `If-Range` are `:all` (RFC 9110 §14.2 lets a server
  ignore them, and claims carry no validator an `If-Range` could be compared
  with).

  `resolve/2` places a parsed range in a representation of `length` bytes:
  `{first, last}` (inclusive, `last` clamped to the end), `:all`, or
  `:unsatisfiable` (a range that starts past the end, a zero-length
  representation, or `bytes=-0`).
  """

  @type parsed ::
          :all
          | {:span, non_neg_integer(), non_neg_integer()}
          | {:from, non_neg_integer()}
          | {:suffix, non_neg_integer()}

  # One range-spec of the `bytes` unit: `first-last`, `first-`, or `-suffix`.
  @range_spec ~r/\A(\d*)-(\d*)\z/

  @spec parse(Plug.Conn.t()) :: parsed()
  def parse(conn) do
    with [] <- Plug.Conn.get_req_header(conn, "if-range"),
         ["bytes=" <> spec] <- Plug.Conn.get_req_header(conn, "range") do
      spec |> String.trim() |> parse_spec()
    else
      _ -> :all
    end
  end

  defp parse_spec(spec) do
    case Regex.run(@range_spec, spec, capture: :all_but_first) do
      ["", ""] -> :all
      ["", suffix] -> {:suffix, String.to_integer(suffix)}
      [first, ""] -> {:from, String.to_integer(first)}
      [first, last] -> span(String.to_integer(first), String.to_integer(last))
      nil -> :all
    end
  end

  defp span(first, last) when last >= first, do: {:span, first, last}
  defp span(_first, _last), do: :all

  @spec resolve(parsed(), non_neg_integer()) ::
          :all | {non_neg_integer(), non_neg_integer()} | :unsatisfiable
  def resolve(:all, _length), do: :all
  def resolve(_parsed, 0), do: :unsatisfiable
  def resolve({:suffix, 0}, _length), do: :unsatisfiable
  def resolve({:suffix, n}, length), do: {max(length - n, 0), length - 1}
  def resolve({:from, first}, length) when first < length, do: {first, length - 1}

  def resolve({:span, first, last}, length) when first < length,
    do: {first, min(last, length - 1)}

  def resolve(_parsed, _length), do: :unsatisfiable
end
