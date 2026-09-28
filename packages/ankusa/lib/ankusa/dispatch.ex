defmodule Ankusa.Dispatch do
  @moduledoc """
  Dispatch-facing helpers. The running pipeline lives in
  `Ankusa.Dispatch.Pipeline`; this module hosts operator affordances such as
  replaying dead-lettered hooks.
  """

  alias Ankusa.SourceStore
  alias Ankusa.Dispatch.DLQ

  @doc """
  Re-deliver dead-lettered hooks through their source's sinks.

  `filter` (map or keyword) may narrow by `:source_id`, `:id`, and/or `:since`
  (a unix-ms lower bound on when the entry was dead-lettered). Returns the
  number of entries replayed.
  """
  @spec replay(atom(), map() | keyword()) :: non_neg_integer()
  def replay(instance, filter \\ %{}) do
    filter = Map.new(filter)
    config = Ankusa.config(instance)

    config
    |> DLQ.entries()
    |> Enum.filter(&matches?(&1, filter))
    |> Enum.reduce(0, fn %{envelope: env}, count ->
      sinks =
        case SourceStore.fetch(instance, env.source_id) do
          {:ok, source} -> source.sinks
          :error -> []
        end

      ctx = %{instance: instance, source_id: env.source_id, tenant_id: env.tenant_id, attempt: 1}
      Enum.each(sinks, fn {mod, opts} -> mod.deliver(env, ctx, opts) end)
      count + 1
    end)
  end

  defp matches?(%{envelope: env, at: at}, filter) do
    keep?(filter, :source_id, env.source_id) and
      keep?(filter, :id, env.id) and
      since_ok?(filter, at)
  end

  defp keep?(filter, key, actual) do
    case Map.get(filter, key) do
      nil -> true
      expected -> actual == expected
    end
  end

  defp since_ok?(filter, at) do
    case Map.get(filter, :since) do
      nil -> true
      since -> at >= since
    end
  end
end
