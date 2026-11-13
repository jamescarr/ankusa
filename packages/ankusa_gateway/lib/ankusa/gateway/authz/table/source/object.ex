defmodule Ankusa.Gateway.Authz.Table.Source.Object do
  @moduledoc """
  A `table` source in an object store: permission rows as NDJSON (`type:
  object`).

      source:
        type: object
        store: {type: s3, s3: {bucket: perms, region: eu-west-1}}
        key: gateway/permissions.ndjson

  Exactly one of `key` (one NDJSON object) and `prefix` (every object under it,
  sorted, at most 1000) is required; a blank line is skipped and a line that
  does not decode to a JSON object is a bad row. `max_object_bytes`
  (`67108864`) caps each object's size.

  The same `store` shape as `claim_check.store` and `gateway.spaces.*.store`
  (both images' YAML reaches `local` with a `root`, `s3` and `gcs`; Azure and
  OCI are Elixir config only).

  `version/2` is the list of `{key, version}` of the objects read, so an
  unchanged file is not loaded again.

  Permission files must stay out of reach of the objects API:
  `Ankusa.Gateway.Config.new/2` refuses a source whose store and prefix
  overlap a space's.
  """

  @behaviour Ankusa.Gateway.Authz.Table.Source

  alias Ankusa.Gateway.{Options, Spaces}

  @keys [:store, :key, :prefix, :max_object_bytes]
  @max_prefix_bytes 512
  @max_objects 1_000

  @impl true
  @spec init!(map(), String.t()) :: map()
  def init!(opts, key) do
    given = Options.map!(opts, key)
    Options.unknown_keys!(given, @keys, key)

    store = Spaces.store!(Map.get(given, :store), "#{key}.store")

    %{
      store: store,
      key: object_key!(Map.get(given, :key), "#{key}.key"),
      prefix: prefix!(Map.get(given, :prefix), "#{key}.prefix"),
      max_object_bytes:
        Options.int_in!(
          Map.get(given, :max_object_bytes, 67_108_864),
          1_048_576..17_179_869_184,
          "#{key}.max_object_bytes"
        )
    }
    |> one_of!(key)
  end

  # Exactly one of :key and :prefix.
  defp one_of!(state, key) do
    case {state.key, state.prefix} do
      {nil, nil} ->
        raise ArgumentError, "#{key}: one of key and prefix is required"

      {key_string, prefix} when is_binary(key_string) and prefix != nil ->
        raise ArgumentError, "#{key}: only one of key and prefix may be set"

      _ ->
        state
    end
  end

  defp object_key!(nil, _key), do: nil

  defp object_key!(key, path) when is_binary(key) do
    if Spaces.validate_key(key) == :ok,
      do: key,
      else: raise(ArgumentError, "#{path} must be a valid object key, got #{inspect(key)}")
  end

  defp object_key!(other, path),
    do: raise(ArgumentError, "#{path} must be a string, got #{inspect(other)}")

  defp prefix!(nil, _key), do: nil
  defp prefix!("", _key), do: ""

  defp prefix!(prefix, key) when is_binary(prefix) do
    cond do
      byte_size(prefix) > @max_prefix_bytes ->
        raise ArgumentError, "#{key} is at most #{@max_prefix_bytes} bytes"

      Spaces.validate_prefix(prefix) == :ok ->
        prefix

      true ->
        raise ArgumentError, "#{key} must be empty, end with /, and hold valid key segments"
    end
  end

  defp prefix!(other, key),
    do: raise(ArgumentError, "#{key} must be a string, got #{inspect(other)}")

  @doc false
  # `{store, prefix}` for `Ankusa.Gateway.Config.new/2`'s overlap check with
  # `gateway.spaces`: a single object is reached at its key.
  @spec reach(map()) :: %{store: {module(), keyword()}, prefix: String.t()} | nil
  def reach(opts) when is_map(opts) do
    case Map.get(opts, :store) do
      {module, _opts} = store when is_atom(module) ->
        %{store: store, prefix: Map.get(opts, :key) || Map.get(opts, :prefix) || ""}

      _no_store ->
        nil
    end
  end

  @impl true
  @spec version(map(), map()) :: {:ok, term()} | {:error, term()}
  def version(state, ctx) do
    with {:ok, keys} <- keys(state, ctx), do: heads(keys, state, ctx)
  end

  @impl true
  @spec load(map(), map(), acc, (map(), acc -> {:cont, acc} | {:halt, acc})) ::
          {:ok, acc} | {:error, term()}
        when acc: term()
  def load(state, ctx, acc, fun) do
    with {:ok, keys} <- keys(state, ctx) do
      read_all(keys, state, ctx, acc, fun)
    end
  end

  defp keys(%{key: key}, _ctx) when is_binary(key), do: {:ok, [key]}

  defp keys(%{prefix: prefix, store: {module, opts}}, ctx) do
    case module.list_page(ctx.instance, prefix, opts ++ [limit: @max_objects]) do
      {:ok, %{objects: objects}} ->
        {:ok, objects |> Enum.map(& &1.key) |> Enum.sort()}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp heads(keys, state, ctx) do
    keys
    |> Enum.reduce_while({:ok, []}, fn key, {:ok, acc} ->
      case head(state, ctx, key) do
        {:ok, meta} -> {:cont, {:ok, [{key, meta.version} | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, versions} -> {:ok, Enum.reverse(versions)}
      error -> error
    end
  end

  defp head(%{store: {module, opts}}, ctx, key),
    do: module.head_object(ctx.instance, key, opts)

  defp read_all([], _state, _ctx, acc, _fun), do: {:ok, acc}

  defp read_all([key | rest], state, ctx, acc, fun) do
    case read(state, ctx, key) do
      {:error, reason} ->
        {:error, reason}

      {:ok, body} ->
        case lines(body, acc, fun) do
          {:halt, acc} -> {:ok, acc}
          {:cont, acc} -> read_all(rest, state, ctx, acc, fun)
        end
    end
  end

  defp read(%{store: {module, opts}, max_object_bytes: max_bytes}, ctx, key) do
    case module.get_object(ctx.instance, key, opts ++ [max_bytes: max_bytes]) do
      {:ok, _meta, body} -> {:ok, body}
      {:error, reason} -> {:error, reason}
    end
  end

  # An empty line is not a row; a line that is not a JSON object is one the
  # snapshot counts as bad (the reducer decides what a valid row is).
  defp lines(body, acc, fun) do
    body
    |> String.split("\n")
    |> Enum.reduce_while({:cont, acc}, fn line, {:cont, acc} ->
      case String.trim(line) do
        "" ->
          {:cont, {:cont, acc}}

        text ->
          row =
            case JSON.decode(text) do
              {:ok, %{} = row} -> row
              _other -> %{}
            end

          case fun.(row, acc) do
            {:cont, acc} -> {:cont, {:cont, acc}}
            {:halt, acc} -> {:halt, {:halt, acc}}
          end
      end
    end)
  end
end
