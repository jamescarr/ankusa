defmodule Hook.UUIDv7 do
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
