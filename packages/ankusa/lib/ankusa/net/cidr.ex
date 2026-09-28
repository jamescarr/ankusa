defmodule Ankusa.Net.CIDR do
  @moduledoc """
  An IPv4 or IPv6 network, parsed once at config/definition time into the bit
  form `contains?/2` needs: `network` is the address masked to `mask`, so a
  membership test is one `&&&` and one comparison.

  Hand-rolled rather than pulled from Hex: the two operations wanted here are
  "parse a prefix notation" and "compare the top N bits", the whole module is
  bit operators, and core would otherwise carry a parsing dependency for it.

  Families never cross. `bits` is part of the comparison, so `0.0.0.0/0` (a
  `32`-bit network) does not contain an IPv6 address and `::/0` does not contain
  an IPv4 one. A `::ffff:1.2.3.4` client is normalized to `{32, _}` by
  `Ankusa.Net.normalize/1` before it gets here, which is what makes an IPv4 rule
  match it.

  ## Prefix notation

    * `"10.0.0.0/8"` — the ordinary form.
    * `"10.1.2.3"` — a bare address is a full-length prefix (`/32`, `/128`).
    * `"0.0.0.0/0"` / `"::/0"` — the match-everything network of each family.

  Anything else (`"/8"`, `"10.0.0.0/33"`, `"10.0.0.0/"`, `"10.0.0.0/8/8"`,
  `"nonsense"`) is `{:error, :invalid_cidr}` — the single error this module
  returns, because the caller always knows which config key it was parsing and
  the alternate spellings are not worth distinguishing.
  """

  import Bitwise

  @enforce_keys [:bits, :network, :mask]
  defstruct [:bits, :network, :mask]

  @typedoc """
  `bits` is the family width (32 or 128), `network` the address already ANDed
  with `mask`, and `mask` the contiguous prefix mask.
  """
  @type t :: %__MODULE__{
          bits: 32 | 128,
          network: non_neg_integer(),
          mask: non_neg_integer()
        }

  @doc """
  Parse prefix notation into a `t:t/0`.
  """
  @spec parse(binary()) :: {:ok, t()} | {:error, :invalid_cidr}
  def parse(binary) when is_binary(binary) do
    with {:ok, address, prefix_bits} <- split(binary),
         {:ok, {bits, value}} <- Ankusa.Net.parse(address),
         {:ok, prefix_bits} <- parse_prefix(prefix_bits, bits) do
      mask = mask(prefix_bits, bits)
      {:ok, %__MODULE__{bits: bits, network: value &&& mask, mask: mask}}
    else
      _ -> {:error, :invalid_cidr}
    end
  end

  def parse(_other), do: {:error, :invalid_cidr}

  @doc """
  `parse/1` for callers that would rather crash: config validated at boot, a
  seed entry, a route body.
  """
  @spec parse!(binary()) :: t()
  def parse!(binary) do
    case parse(binary) do
      {:ok, cidr} -> cidr
      {:error, :invalid_cidr} -> raise ArgumentError, "invalid CIDR #{inspect(binary)}"
    end
  end

  @doc """
  Is `ip` inside this network? `ip` is a normalized `t:Ankusa.Net.ip/0`; an
  address of the other family is never contained.
  """
  @spec contains?(t(), Ankusa.Net.ip()) :: boolean()
  def contains?(%__MODULE__{bits: bits, network: network, mask: mask}, {bits, value}) do
    (value &&& mask) == network
  end

  def contains?(%__MODULE__{}, _other), do: false

  @doc """
  Render as prefix notation. Derived from the mask, not stored: a `/32` and a
  full-length IPv4 mask are the same network, so they must print the same.
  """
  @spec to_string(t()) :: String.t()
  def to_string(%__MODULE__{bits: bits, network: network, mask: mask}) do
    "#{Ankusa.Net.to_string({bits, network})}/#{render_prefix(mask, bits)}"
  end

  # ── parsing ─────────────────────────────────────────────────────────────────

  # At most one "/": a bare address carries its own full-length prefix.
  defp split(binary) do
    case String.split(binary, "/") do
      [address] -> {:ok, address, nil}
      [address, prefix] -> {:ok, address, prefix}
      _more -> :error
    end
  end

  defp parse_prefix(nil, bits), do: {:ok, bits}

  defp parse_prefix(binary, bits) do
    with {value, ""} <- Integer.parse(binary),
         true <- value >= 0 and value <= bits do
      {:ok, value}
    else
      _ -> :error
    end
  end

  # Contiguous ones from the most significant bit: /0 is all zeroes, and a
  # full-length prefix shifts by zero.
  defp mask(0, _bits), do: 0
  defp mask(prefix, bits), do: ((1 <<< prefix) - 1) <<< (bits - prefix)

  # A mask is contiguous by construction, so counting its host bits recovers the
  # prefix length.
  defp render_prefix(0, _bits), do: 0
  defp render_prefix(mask, bits), do: bits - host_bits(mask, 0)

  defp host_bits(0, count), do: count
  defp host_bits(mask, count) when band(mask, 1) == 0, do: host_bits(mask >>> 1, count + 1)
  defp host_bits(_mask, count), do: count
end
