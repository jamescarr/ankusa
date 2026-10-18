defmodule Ankusa.Property.IngestHeadersTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  @moduletag timeout: 3_600_000

  alias Ankusa.Edge.Ingest
  alias Ankusa.Generators, as: G

  property "check_headers/1 never raises, and names the first header no sink can carry" do
    check all headers <- G.headers(12) do
      assert Ingest.check_headers(headers) == expected(headers)
    end
  end

  property "check_headers/1 on raw bytes agrees with the byte-class definition" do
    check all headers <- list_of(G.raw_header(), max_length: 12) do
      assert Ingest.check_headers(headers) == expected(headers)
    end
  end

  property "a list of visible-ASCII names with visible-ASCII/space/tab values is always accepted" do
    check all headers <-
                list_of(
                  tuple({G.token(), string([0x20..0x7E, ?\t], max_length: 64)}),
                  max_length: 16
                ) do
      assert Ingest.check_headers(headers) == :ok
    end
  end

  property "one bad byte anywhere in a good list is refused, and the name is given only when the name is clean" do
    check all headers <- list_of(tuple({G.token(), G.token()}), min_length: 1, max_length: 8),
              index <- integer(0..7),
              bad <- member_of([0x00, 0x0A, 0x0D, 0x7F, 0x80, 0xFF]),
              in_name? <- boolean() do
      index = rem(index, length(headers))

      poisoned =
        List.update_at(headers, index, fn {name, value} ->
          if in_name?, do: {name <> <<bad>>, value}, else: {name, value <> <<bad>>}
        end)

      {name, _} = Enum.at(headers, index)

      assert Ingest.check_headers(poisoned) ==
               {:invalid_header, if(in_name?, do: nil, else: name)}
    end
  end

  # RFC 9110 field-name without the space rule: visible ASCII. field-value:
  # visible ASCII, space and tab; no obs-text.
  defp expected(headers) do
    Enum.find_value(headers, :ok, fn {name, value} ->
      cond do
        not Enum.all?(:binary.bin_to_list(name), &(&1 in 0x21..0x7E)) ->
          {:invalid_header, nil}

        not Enum.all?(:binary.bin_to_list(value), &(&1 in 0x20..0x7E or &1 == ?\t)) ->
          {:invalid_header, name}

        true ->
          nil
      end
    end)
  end
end
