defmodule Ankusa.Gateway.Authz.Grants do
  @moduledoc """
  The `grants` authorizer (`kind: :inline`), defined while
  `gateway.sharing.enabled`: native grants, and object ownership.

  For a request on an object or a prefix of a space it reads the object's head
  (object requests: its `ankusa-owner` and `ankusa-incarnation`), the object's
  ACL document, and the document of every ancestor prefix (`"a/b/"`, `"a/"`,
  `""`: at most 33). Each comes from the `:acl` cache in the request process
  first; only the misses are read, concurrently (at most 8 at a time, within
  the policy's deadline), so a warm decision spawns nothing. Any read that
  fails is `{:error, {:unavailable, :acl}}`: never a guess.

    1. The object's owner, unless the request came by share link or
       anonymously: allow, any action on that object.
    2. A live grant carrying the action whose subject is the principal
       (`Ankusa.Gateway.ACL.matching_grants/3`) on the object (of its current
       incarnation only) or on an ancestor prefix: allow.
    3. Otherwise abstain. A claim request always abstains.
  """

  @behaviour Ankusa.Gateway.Authorizer

  alias Ankusa.Gateway.{ACL, Spaces}
  alias Ankusa.Gateway.ACL.Store
  alias Ankusa.Gateway.Authz.Request

  @max_concurrency 8

  @impl true
  def init!(opts, key) do
    Ankusa.Gateway.Options.unknown_keys!(Ankusa.Gateway.Options.map!(opts, key), [], key)
    %{}
  end

  @impl true
  def kind(_state), do: :inline

  @impl true
  def decide(%Request{resource: %{kind: :claim}}, _state, _ctx), do: {:ok, :abstain, %{}}

  def decide(%Request{} = request, _state, %{instance: instance, deadline: deadline}) do
    with {:ok, space} <- Spaces.fetch(Ankusa.Gateway.config(instance), request.resource.space),
         {:ok, found} <- lookups(instance, space, request, deadline) do
      {:ok, verdict(request, found), %{}}
    else
      :error -> {:ok, :abstain, %{}}
      {:error, {:unavailable, _}} -> {:error, {:unavailable, :acl}}
    end
  end

  defp verdict(%Request{resource: %{kind: :object}} = request, found) do
    head = Map.get(found, :head)

    owner? =
      head != nil and head.owner == request.principal.id and
        request.via not in [:link, :anonymous]

    docs = List.wrap(live_object_doc(found.object, head)) ++ found.prefixes

    cond do
      owner? -> :allow
      granted?(request, docs) -> :allow
      true -> :abstain
    end
  end

  defp verdict(request, found),
    do: if(granted?(request, found.prefixes), do: :allow, else: :abstain)

  # An object's document counts only for the object it was made on.
  defp live_object_doc(nil, _head), do: nil
  defp live_object_doc(_doc, nil), do: nil
  defp live_object_doc(%{incarnation: incarnation} = doc, %{incarnation: incarnation}), do: doc
  defp live_object_doc(_doc, _head), do: nil

  defp granted?(request, docs) do
    now = System.os_time(:second)
    Enum.any?(docs, &(ACL.matching_grants(&1, request, now) != []))
  end

  # Every lookup the request needs, from the cache first; the misses read
  # concurrently. `%{head: head | nil, object: doc | nil, prefixes: [doc]}`.
  defp lookups(instance, space, %Request{resource: %{kind: kind, key: key}}, deadline) do
    items =
      Enum.map(ancestors(kind, key), &{:doc, space, :prefix, &1}) ++
        if(kind == :object, do: [{:head, space, key}, {:doc, space, :object, key}], else: [])

    {hits, misses} =
      Enum.reduce(items, {%{}, []}, fn item, {hits, misses} ->
        case Store.peek(instance, item) do
          {:ok, value} -> {Map.put(hits, item, value), misses}
          :miss -> {hits, [item | misses]}
        end
      end)

    with {:ok, fetched} <- fetch(instance, misses, deadline) do
      found = Map.merge(hits, fetched)

      {:ok,
       %{
         head: Map.get(found, {:head, space, key}),
         object: Map.get(found, {:doc, space, :object, key}),
         prefixes: for({{:doc, _, :prefix, _}, doc} <- found, doc != nil, do: doc)
       }}
    end
  end

  defp fetch(_instance, [], _deadline), do: {:ok, %{}}

  defp fetch(instance, misses, deadline) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    misses
    |> Task.async_stream(&{&1, read(instance, &1, deadline)},
      max_concurrency: @max_concurrency,
      timeout: timeout,
      on_timeout: :kill_task,
      ordered: false
    )
    |> Enum.reduce_while({:ok, %{}}, fn
      {:ok, {item, {:ok, value}}}, {:ok, acc} -> {:cont, {:ok, Map.put(acc, item, value)}}
      {:ok, {_item, {:error, _} = error}}, _acc -> {:halt, error}
      {:exit, _reason}, _acc -> {:halt, {:error, {:unavailable, :deadline}}}
    end)
  end

  defp read(instance, {:head, space, key}, deadline),
    do: Store.head(instance, space, key, deadline)

  defp read(instance, {:doc, space, kind, key}, deadline),
    do: Store.fetch(instance, space, kind, key, deadline)

  @doc """
  The directory prefixes a request's grants may sit on: `""`, then each
  directory of the key (`"a/"`, `"a/b/"`). A prefix request on `"a/b/"`
  includes its own prefix; one on `"a/b"` (a partial name) does not.
  """
  @spec ancestors(:object | :prefix, String.t()) :: [String.t()]
  def ancestors(_kind, key) do
    {_path, dirs} =
      key
      |> String.split("/")
      |> Enum.drop(-1)
      |> Enum.reduce({"", []}, fn segment, {path, acc} ->
        path = path <> segment <> "/"
        {path, [path | acc]}
      end)

    ["" | Enum.reverse(dirs)]
  end
end
