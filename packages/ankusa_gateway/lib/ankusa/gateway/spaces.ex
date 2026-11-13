defmodule Ankusa.Gateway.Spaces do
  @moduledoc """
  The objects API's spaces (`gateway.spaces`): a name, an object store, a
  prefix in it, the policy every request is decided by, and size bounds.

  | Key | Default | |
  | --- | --- | --- |
  | `store` | required | `{module, opts}`; the module exports `Ankusa.BlobStore`'s object operations (`head_object/3`, `get_object/3`, `put_object/4`, `delete_object/3`, `list_page/3`) |
  | `prefix` | `""` | `""` or ending in `/`, at most 512 bytes; its segments obey the key rules |
  | `policy` | required | a policy of `gateway.authz.policies` |
  | `max_object_bytes` | `67108864` | 1..4294967295: the largest object written or read whole |
  | `read_first_bytes` | `1048576` | 1024..`max_object_bytes`: the first store read of a `GET` |
  | `read_only` | `false` | refuse `PUT` and `DELETE` (`405`) |

  At most 256 spaces; a name matches `~r/\\A[a-z0-9][a-z0-9_-]{0,62}\\z/`, and
  `claims` is the claims API's. Two spaces on one store (equal `store` tuples)
  whose prefixes overlap are refused: neither may see the other's objects or
  its ACL documents. Two stores that differ in options but name one bucket are
  the operator's to keep apart.

  ## Keys

  One canonical key, however it arrives: `key_from_segments/1` for a path
  (each raw segment decoded exactly once; a decoded `/` or `\\` is refused, so
  `%2F` never becomes a boundary), `validate_key/1` for a query parameter or a
  JSON body. A key is valid UTF-8, 1..1024 bytes, at most 32 segments, none
  empty, `.` or `..`, none starting with `.ankusa` (the ACL and audit trees
  and LocalFS's sidecars), no `\\`, no control byte.
  """

  @max_spaces 256
  @name ~r/\A[a-z0-9][a-z0-9_-]{0,62}\z/
  @reserved ["claims"]
  @keys [:store, :prefix, :policy, :max_object_bytes, :read_first_bytes, :read_only]
  @object_operations [
    head_object: 3,
    get_object: 3,
    put_object: 4,
    delete_object: 3,
    list_page: 3
  ]
  @max_key_bytes 1024
  @max_segments 32
  @max_prefix_bytes 512
  # A `%` not followed by two hex digits: `URI.decode/1` would keep it as is.
  @bad_escape ~r/%(?![0-9A-Fa-f]{2})/

  @type space :: %{
          name: String.t(),
          store: {module(), keyword()},
          prefix: String.t(),
          policy: String.t(),
          max_object_bytes: pos_integer(),
          read_first_bytes: pos_integer(),
          read_only: boolean()
        }

  alias Ankusa.Gateway.Options

  @doc """
  Validate `gateway.spaces` against the defined `policies`. Raises
  `ArgumentError` naming the key.
  """
  @spec config!(term(), map()) :: %{String.t() => space()}
  def config!(spaces, policies) do
    given = Options.map!(spaces, "gateway.spaces")

    if map_size(given) > @max_spaces do
      raise ArgumentError,
            "gateway.spaces has #{map_size(given)} entries; at most #{@max_spaces}"
    end

    spaces =
      Map.new(given, fn {name, space} ->
        name = name!(name)
        {name, space!(name, space, policies)}
      end)

    disjoint!(spaces)
    spaces
  end

  defp name!(name) when is_atom(name) and name not in [nil, true, false],
    do: name!(Atom.to_string(name))

  defp name!(name) when is_binary(name) do
    cond do
      name in @reserved ->
        raise ArgumentError, "gateway.spaces.#{name}: the name #{name} is the claims API's"

      Regex.match?(@name, name) ->
        name

      true ->
        raise ArgumentError,
              "gateway.spaces: a space name matches [a-z0-9][a-z0-9_-]{0,62}, got #{inspect(name)}"
    end
  end

  defp name!(other),
    do:
      raise(ArgumentError, "gateway.spaces: a space name must be a string, got #{inspect(other)}")

  defp space!(name, space, policies) do
    key = "gateway.spaces.#{name}"
    given = Options.map!(space, key)
    Options.unknown_keys!(given, @keys, key)

    max_object_bytes =
      Options.int_in!(
        Map.get(given, :max_object_bytes, 67_108_864),
        1..4_294_967_295,
        "#{key}.max_object_bytes"
      )

    %{
      name: name,
      store: store!(Map.get(given, :store), "#{key}.store"),
      prefix: prefix!(Map.get(given, :prefix, ""), "#{key}.prefix"),
      policy: policy!(Map.get(given, :policy), policies, "#{key}.policy"),
      max_object_bytes: max_object_bytes,
      read_first_bytes:
        Options.int_in!(
          Map.get(given, :read_first_bytes, min(1_048_576, max(max_object_bytes, 1024))),
          1024..max(max_object_bytes, 1024),
          "#{key}.read_first_bytes"
        ),
      read_only: Options.boolean!(Map.get(given, :read_only, false), "#{key}.read_only")
    }
  end

  @doc false
  # `{module, opts}` validated for object operations; shared with an object
  # `table` source, whose store is a space's shape.
  @spec store!(term(), String.t()) :: {module(), keyword()}
  def store!({module, opts}, key) when is_atom(module) and is_list(opts) do
    unless Options.implements?(module, @object_operations) do
      raise ArgumentError,
            "#{key}: #{inspect(module)} does not implement the object operations " <>
              "(head_object/3, get_object/3, put_object/4, delete_object/3, list_page/3)"
    end

    if module == Ankusa.BlobStore.LocalFS and not is_binary(opts[:root]) do
      raise ArgumentError, "#{key}: Ankusa.BlobStore.LocalFS needs :root"
    end

    :ok = Ankusa.BlobStore.Options.validate!(module, opts, key)
    :ok = Ankusa.BlobStore.SSE.validate!(module, opts, key)
    {module, opts}
  end

  def store!(other, key),
    do: raise(ArgumentError, "#{key} must be {module, opts}, got #{inspect(other)}")

  defp prefix!("", _key), do: ""

  defp prefix!(prefix, key) when is_binary(prefix) do
    cond do
      byte_size(prefix) > @max_prefix_bytes ->
        raise ArgumentError, "#{key} is at most #{@max_prefix_bytes} bytes"

      not String.ends_with?(prefix, "/") ->
        raise ArgumentError, "#{key} must be empty or end with /, got #{inspect(prefix)}"

      validate_prefix(prefix) != :ok ->
        raise ArgumentError,
              "#{key}: #{inspect(prefix)} is not a valid key prefix (no empty, . or .. " <>
                "segment, none starting with .ankusa)"

      true ->
        prefix
    end
  end

  defp prefix!(other, key),
    do: raise(ArgumentError, "#{key} must be a string, got #{inspect(other)}")

  defp policy!(name, policies, key) do
    name =
      if is_atom(name) and name not in [nil, true, false], do: Atom.to_string(name), else: name

    if is_binary(name) and Map.has_key?(policies, name) do
      name
    else
      defined = policies |> Map.keys() |> Enum.sort() |> Enum.join(", ")
      raise ArgumentError, "#{key} must name a policy (defined: #{defined}), got #{inspect(name)}"
    end
  end

  # No two spaces on one store with overlapping prefixes (equal ones included).
  defp disjoint!(spaces) do
    sorted = spaces |> Map.values() |> Enum.sort_by(& &1.name)

    for a <- sorted,
        b <- sorted,
        a.name < b.name,
        a.store == b.store,
        String.starts_with?(a.prefix, b.prefix) or String.starts_with?(b.prefix, a.prefix) do
      raise ArgumentError,
            "gateway.spaces.#{a.name} and gateway.spaces.#{b.name} share a store and their " <>
              "prefixes overlap"
    end

    :ok
  end

  @doc "The space named `name`."
  @spec fetch(Ankusa.Gateway.Config.t(), String.t()) :: {:ok, space()} | :error
  def fetch(%{spaces: spaces}, name), do: Map.fetch(spaces, name)

  @doc "The store key of `key` in `space`."
  @spec object_key(space(), String.t()) :: String.t()
  def object_key(%{prefix: prefix}, key), do: prefix <> key

  @doc """
  The key of a path's raw segments (those after `/v1/objects/<space>`): a
  malformed escape refused, each decoded exactly once, a decoded `/` or `\\`
  refused, joined with `/`, then `validate_key/1`.
  """
  @spec key_from_segments([String.t()]) :: {:ok, String.t()} | :error
  def key_from_segments([]), do: :error

  def key_from_segments(segments) do
    if Enum.any?(segments, &Regex.match?(@bad_escape, &1)) do
      :error
    else
      decoded_key(Enum.map(segments, &URI.decode/1))
    end
  end

  defp decoded_key(decoded) do
    if Enum.any?(decoded, &String.contains?(&1, ["/", "\\"])) do
      :error
    else
      key = Enum.join(decoded, "/")
      if validate_key(key) == :ok, do: {:ok, key}, else: :error
    end
  end

  @doc "`:ok` when `key` is a valid object key."
  @spec validate_key(term()) :: :ok | :error
  def validate_key(key)
      when is_binary(key) and byte_size(key) >= 1 and byte_size(key) <= @max_key_bytes do
    with true <- String.valid?(key),
         false <- String.contains?(key, "\\"),
         false <- control?(key),
         segments = String.split(key, "/"),
         true <- length(segments) <= @max_segments,
         true <- Enum.all?(segments, &segment?/1) do
      :ok
    else
      _ -> :error
    end
  end

  def validate_key(_key), do: :error

  @doc "`:ok` when `prefix` is a valid listing prefix: `\"\"`, or a key with at most one trailing `/`."
  @spec validate_prefix(term()) :: :ok | :error
  def validate_prefix(""), do: :ok

  def validate_prefix(prefix) when is_binary(prefix) do
    trimmed =
      if String.ends_with?(prefix, "/"),
        do: binary_part(prefix, 0, byte_size(prefix) - 1),
        else: prefix

    validate_key(trimmed)
  end

  def validate_prefix(_prefix), do: :error

  defp segment?(""), do: false
  defp segment?("."), do: false
  defp segment?(".."), do: false
  defp segment?(segment), do: not String.starts_with?(segment, ".ankusa")

  defp control?(key),
    do: :binary.match(key, Enum.map(Enum.to_list(0..31) ++ [127], &<<&1>>)) != :nomatch
end
