defmodule Ankusa.ULID do
  @moduledoc """
  The ULID text form of 128 bits: 26 characters of Crockford base32
  (`0-9`, `A-Z` minus `I`, `L`, `O`, `U`), most significant bits first, so
  the first 48 bits (a Unix millisecond timestamp) make the string sort by
  time.

  Only the canonical, uppercase form is accepted. The ULID spec lets a
  decoder fold case and read `I`/`L` as `1` and `O` as `0`; this module
  doesn't, so every id has exactly one spelling in a URN, a URL path, and a
  storage key.
  """

  @alphabet ~c"0123456789ABCDEFGHJKMNPQRSTVWXYZ"

  # The largest timestamp `DateTime` can represent (9999-12-31T23:59:59.999Z).
  # A ULID holds 48 bits of milliseconds, which reaches year 10889.
  @max_ms 253_402_300_799_999

  @doc "Encode 128 bits as a canonical ULID."
  @spec encode(<<_::128>>) :: String.t()
  def encode(<<bits::128>>) do
    for <<(value::5 <- <<0::2, bits::128>>)>>, into: <<>>, do: <<char(value)>>
  end

  @doc """
  Decode a canonical ULID to its 128 bits. `:error` for anything else,
  including a timestamp past what `DateTime` can represent; never raises on
  untrusted input.
  """
  @spec decode(term()) :: {:ok, <<_::128>>} | :error
  def decode(<<_::binary-26>> = string), do: decode(string, <<>>)
  def decode(_), do: :error

  @doc "The 48-bit Unix millisecond timestamp of 128 ULID bits."
  @spec timestamp_ms(<<_::128>>) :: non_neg_integer()
  def timestamp_ms(<<ms::48, _::80>>), do: ms

  defp decode(<<>>, <<0::2, ms::48, rest::80>>) when ms <= @max_ms,
    do: {:ok, <<ms::48, rest::80>>}

  defp decode(<<>>, _bits), do: :error

  defp decode(<<c, rest::binary>>, acc) do
    case value(c) do
      nil -> :error
      v -> decode(rest, <<acc::bitstring, v::5>>)
    end
  end

  for {char, value} <- Enum.with_index(@alphabet) do
    defp char(unquote(value)), do: unquote(char)
    defp value(unquote(char)), do: unquote(value)
  end

  defp value(_), do: nil
end
