defmodule Hook.DedupKey.Rules do
  @moduledoc """
  Config-driven idempotency key extraction.

  Options (tried in order, first non-nil binary wins):

    * `:header` — a request header name, read via `Hook.Envelope.header/2`.
    * `:json` — a list of keys naming a path into the decoded JSON body,
      e.g. `["id"]` or `["data", "id"]`.

  With neither option set, defaults to header `webhook-id`, then JSON `["id"]`.
  """

  @behaviour Hook.DedupKey

  alias Hook.Envelope

  @impl true
  @spec extract(Envelope.t(), keyword()) :: {:ok, String.t()} | :none
  def extract(%Envelope{} = env, opts) do
    rules = rules(opts)

    Enum.find_value(rules, :none, fn rule ->
      case apply_rule(rule, env) do
        key when is_binary(key) and key != "" -> {:ok, key}
        _ -> nil
      end
    end)
  end

  defp rules(opts) do
    header = Keyword.get(opts, :header)
    json = Keyword.get(opts, :json)

    cond do
      header || json ->
        Enum.reject([{:header, header}, {:json, json}], fn {_k, v} -> is_nil(v) end)

      true ->
        [{:header, "webhook-id"}, {:json, ["id"]}]
    end
  end

  defp apply_rule({:header, name}, env), do: Envelope.header(env, name)

  defp apply_rule({:json, path}, env) do
    case JSON.decode(env.body) do
      {:ok, decoded} -> walk(decoded, path)
      _ -> nil
    end
  end

  defp walk(value, []), do: value

  defp walk(map, [key | rest]) when is_map(map) do
    case Map.fetch(map, key) do
      {:ok, next} -> walk(next, rest)
      :error -> nil
    end
  end

  defp walk(_value, _path), do: nil
end
