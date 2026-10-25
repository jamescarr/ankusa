defmodule Ankusa.Store.Migrations.OrderingLanes do
  @moduledoc false

  # Store format 7 -> 8 (DEL-1.b, ADR 0004, section 5): the ordering lanes. No lane
  # exists in a format 7 store, so nothing is rewritten: the step stamps format 8
  # and nothing else. What the stamp does is make a format 7 image refuse a store
  # that may now hold due rows of kind 2 and 3 and `?O`/`?H`/`?P` keys
  # (`{:error, {:store_format_newer, 8, 7}}`). A store that never enables
  # `ordered` is bit-identical to format 7 apart from the stamp.

  @behaviour Ankusa.Store.Migration

  @impl true
  def describe, do: "stamp the ordering lanes' layout (rows `?O`, `?H`, `?P`; due kinds 2 and 3)"

  @impl true
  def step(_instance, _cursor), do: {:ok, [], :done}
end
