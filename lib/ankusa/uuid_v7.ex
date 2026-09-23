defmodule Ankusa.UUIDv7 do
  @moduledoc """
  UUIDv7 generation (RFC 9562): 48-bit Unix millisecond timestamp, then random.

  Time-ordered ids keep WAL and segment keys naturally sortable by arrival.
  """

  @doc "Generate a UUIDv7 as a lowercase, hyphenated string."
  @spec generate() :: String.t()
  def generate, do: generate(System.system_time(:millisecond))

  @doc "Generate a UUIDv7 for a specific Unix millisecond timestamp."
  @spec generate(integer()) :: String.t()
  def generate(ms) when is_integer(ms) do
    <<rand_a::12, rand_b::62, _::6>> = :crypto.strong_rand_bytes(10)

    <<ms::48, 7::4, rand_a::12, 2::2, rand_b::62>>
    |> encode()
  end

  @doc """
  Extract the 48-bit Unix millisecond timestamp embedded in a UUIDv7 string.

  `:error` for anything that isn't a well-formed UUIDv7 (wrong length, wrong
  version/variant nibbles, non-hex characters) — never raises on untrusted
  input.
  """
  @spec timestamp_ms(String.t()) :: {:ok, non_neg_integer()} | :error
  def timestamp_ms(
        <<a::binary-8, "-", b::binary-4, "-7", c::binary-3, "-", v, d::binary-3, "-",
          e::binary-12>>
      )
      when v in ~c"89ab" do
    with {:ok, <<ms::48, _rest::binary>>} <-
           Base.decode16(a <> b <> "7" <> c <> <<v>> <> d <> e, case: :lower) do
      {:ok, ms}
    else
      _ -> :error
    end
  end

  def timestamp_ms(_), do: :error

  defp encode(<<a::32, b::16, c::16, d::16, e::48>>) do
    [
      Base.encode16(<<a::32>>, case: :lower),
      Base.encode16(<<b::16>>, case: :lower),
      Base.encode16(<<c::16>>, case: :lower),
      Base.encode16(<<d::16>>, case: :lower),
      Base.encode16(<<e::48>>, case: :lower)
    ]
    |> Enum.join("-")
  end
end
