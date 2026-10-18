defmodule Ankusa.Edge.StoredHeaders do
  @moduledoc """
  Which provider request headers a hook is *stored* with.

  Every header a provider sends used to be written verbatim into the hook, then
  archived into segments and claim packs and carried to every sink. Credentials
  (`Authorization`, `Cookie`, an API key) have no business in any of those, so a
  policy decides, per header name, what survives:

    * `drop` — the header is removed;
    * `hash` — the header is kept, its value replaced by `"sha256:<hex>"` of the
      original value (a consumer can still tell two requests carried the same
      value, and nobody can read it).

  A name in both lists is dropped. Names are matched case-insensitively.

  The policy is applied by `Ankusa.Edge.Ingest` **after** verification: the
  verifier reads the request's complete header list, so a scheme that signs a
  header (or a source that dedupes on one) is unaffected. Only what is stored,
  archived, claim-checked and forwarded loses the header. A quarantined hook is
  stored with the policy applied too, so a quarantine replay re-verifies against
  the stored list: do not drop a header a verifier of that source needs.

  The instance policy is `Ankusa.Config`'s `store_headers`; a source's own
  `store_headers` overrides it key by key (an absent `drop` or `hash` inherits
  the instance's list).
  """

  @type policy :: %{drop: [String.t()], hash: [String.t()]}
  @type override :: %{drop: [String.t()] | nil, hash: [String.t()] | nil}

  @default_drop ~w(authorization proxy-authorization cookie set-cookie x-api-key x-auth-token)

  @doc "The instance default: the credential-bearing headers dropped, nothing hashed."
  @spec default() :: policy()
  def default, do: %{drop: @default_drop, hash: []}

  @doc """
  Validate and normalise an instance policy (`key` names it in the error): a map
  or keyword list with optional `:drop` and `:hash` lists of non-empty header
  names, lowercased. An absent key keeps the default's list.
  """
  @spec normalize!(term(), String.t()) :: policy()
  def normalize!(value, key) do
    %{drop: drop, hash: hash} = parse!(value, key)
    default = default()
    %{drop: drop || default.drop, hash: hash || default.hash}
  end

  @doc """
  Validate and normalise a source's override: `nil` stays `nil`; otherwise
  `normalize!/2`'s shape, with an absent key left `nil` (inherit).
  """
  @spec normalize_override!(term(), String.t()) :: override() | nil
  def normalize_override!(nil, _key), do: nil
  def normalize_override!(value, key), do: parse!(value, key)

  @doc "The policy a source runs under: its override laid over the instance's."
  @spec resolve(override() | nil, policy()) :: policy()
  def resolve(nil, policy), do: policy

  def resolve(%{drop: drop, hash: hash}, policy),
    do: %{drop: drop || policy.drop, hash: hash || policy.hash}

  @doc """
  Apply `policy` to a header list: `{headers, dropped}`, `dropped` counting the
  headers removed. A list nothing matched is returned as it came, with no
  allocation: the common request carries none of the named headers.
  """
  @spec strip([{String.t(), String.t()}], policy()) ::
          {[{String.t(), String.t()}], non_neg_integer()}
  def strip(headers, %{drop: drop, hash: hash}) do
    if touched?(headers, drop, hash), do: rewrite(headers, drop, hash, [], 0), else: {headers, 0}
  end

  defp touched?([], _drop, _hash), do: false

  defp touched?([{name, _value} | rest], drop, hash) do
    name = lower(name)
    name in drop or name in hash or touched?(rest, drop, hash)
  end

  defp rewrite([], _drop, _hash, acc, dropped), do: {:lists.reverse(acc), dropped}

  defp rewrite([{name, value} = header | rest], drop, hash, acc, dropped) do
    lowered = lower(name)

    cond do
      lowered in drop -> rewrite(rest, drop, hash, acc, dropped + 1)
      lowered in hash -> rewrite(rest, drop, hash, [{name, digest(value)} | acc], dropped)
      true -> rewrite(rest, drop, hash, [header | acc], dropped)
    end
  end

  defp digest(value), do: "sha256:" <> Base.encode16(:crypto.hash(:sha256, value), case: :lower)

  # Bandit lowercases request header names, so the common name is returned
  # as it is; only a name with an upper-case letter is copied.
  defp lower(name), do: if(lowercase?(name), do: name, else: String.downcase(name, :ascii))

  defp lowercase?(<<>>), do: true
  defp lowercase?(<<c, _rest::binary>>) when c in ?A..?Z, do: false
  defp lowercase?(<<_c, rest::binary>>), do: lowercase?(rest)

  defp parse!(value, key) do
    map =
      cond do
        is_map(value) -> value
        is_list(value) and Keyword.keyword?(value) -> Map.new(value)
        true -> bad!(key, value)
      end

    case Map.keys(map) -- [:drop, :hash] do
      [] -> :ok
      [unknown | _] -> raise ArgumentError, "unknown Ankusa.Config key: #{key}.#{unknown}"
    end

    %{drop: names!(map, :drop, key), hash: names!(map, :hash, key)}
  end

  defp names!(map, field, key) do
    case Map.get(map, field) do
      nil ->
        nil

      names when is_list(names) ->
        if Enum.all?(names, &(is_binary(&1) and &1 != "")),
          do: Enum.map(names, &String.downcase(&1, :ascii)),
          else: bad_names!("#{key}.#{field}", names)

      other ->
        bad_names!("#{key}.#{field}", other)
    end
  end

  defp bad_names!(key, value) do
    raise ArgumentError, "#{key} must be a list of header names, got #{inspect(value)}"
  end

  defp bad!(key, value) do
    raise ArgumentError,
          "#{key} must be a map with :drop and :hash lists of header names, got #{inspect(value)}"
  end
end
