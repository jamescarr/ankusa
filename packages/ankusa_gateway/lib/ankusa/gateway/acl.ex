defmodule Ankusa.Gateway.ACL do
  @moduledoc """
  The ACL document v1: the native grants on one object or one prefix of a
  space, stored in the space's own store at
  `<space.prefix>.ankusa/acl/v1/<o|p>/<lowercase hex sha256 of the key>`
  (`o` an object, `p` a prefix; a prefix is `""` or ends in `/`):

      {"v": 1, "space": "reports", "resource": {"kind": "object", "key": "teams/eng/q3.pdf"},
       "incarnation": "01JD0Z2B7M3N4P5Q6R7S8T9V0W",
       "grants": [{"id": "o<64 hex>.01JD0Z3K9Q4W8X2Y7V6T5S4R3P", "subject": "group:eng",
                   "actions": ["read"], "created_by": "alice", "created_at": 1790000000,
                   "expires_at": null}],
       "updated_at": 1790000000}

  A grant's `subject` is `user:<principal id>`, `group:<name>`,
  `tenant:<id>`, `anyone` (any authenticated principal) or `link` (one share
  link: the principal `link:<grant id>`). `incarnation` is, for an object, the
  `ankusa-incarnation` metadata of the object the grants were made on: a
  document whose incarnation is not the object's current one is dead, so the
  grants on a deleted object never land on a later object of the same key.
  A document has no owner; `created_by` records who made a grant and grants
  nothing.

  A grant id is `<o|p><64 hex resource hash>.<ULID>`: never trusted alone,
  every route that takes one also takes the resource and checks the hash.
  """

  alias Ankusa.Gateway.Authz.Request
  alias Ankusa.Principal

  @actions [:read, :write, :delete, :list, :share]
  @action_names Map.new(@actions, &{Atom.to_string(&1), &1})

  @type kind :: :object | :prefix
  @type grant :: %{
          id: String.t(),
          subject: String.t(),
          actions: [Request.action()],
          created_by: String.t() | nil,
          created_at: integer(),
          expires_at: integer() | nil
        }
  @type doc :: %{
          v: 1,
          space: String.t(),
          resource: %{kind: kind(), key: String.t()},
          incarnation: String.t() | nil,
          grants: [grant()],
          updated_at: integer()
        }

  @doc "The actions a grant may carry."
  @spec actions() :: [Request.action()]
  def actions, do: @actions

  @doc "An action of its name, or `:error`."
  @spec action(term()) :: {:ok, Request.action()} | :error
  def action(name) when is_binary(name), do: Map.fetch(@action_names, name)
  def action(name) when name in @actions, do: {:ok, name}
  def action(_name), do: :error

  @doc "The store key of the document of `{kind, key}` in `space`."
  @spec doc_key(%{:prefix => String.t(), optional(atom()) => term()}, {kind(), String.t()}) ::
          String.t()
  def doc_key(%{prefix: prefix}, {kind, key}),
    do: prefix <> ".ankusa/acl/v1/" <> letter(kind) <> "/" <> hash(key)

  @doc "The lowercase hex sha256 of a key or prefix."
  @spec hash(String.t()) :: String.t()
  def hash(key), do: :sha256 |> :crypto.hash(key) |> Base.encode16(case: :lower)

  defp letter(:object), do: "o"
  defp letter(:prefix), do: "p"

  @doc "A new grant id on `{kind, key}`."
  @spec grant_id({kind(), String.t()}) :: String.t()
  def grant_id({kind, key}), do: letter(kind) <> hash(key) <> "." <> ulid()

  @doc "A fresh ULID (an incarnation, a grant's suffix, an audit record's id)."
  @spec ulid() :: String.t()
  def ulid,
    do:
      Ankusa.ULID.encode(
        <<System.system_time(:millisecond)::48, :crypto.strong_rand_bytes(10)::binary>>
      )

  @doc """
  The kind and resource hash a grant id names: `{:ok, kind, hash}`, or
  `:error` when it is not an id.
  """
  @spec parse_grant_id(term()) :: {:ok, kind(), String.t()} | :error
  def parse_grant_id(<<kind, hash::binary-size(64), ".", ulid::binary-size(26)>>)
      when kind in [?o, ?p] do
    if hex?(hash) and ulid?(ulid),
      do: {:ok, if(kind == ?o, do: :object, else: :prefix), hash},
      else: :error
  end

  def parse_grant_id(_other), do: :error

  defp hex?(value), do: value =~ ~r/\A[0-9a-f]{64}\z/
  defp ulid?(value), do: value =~ ~r/\A[0-9A-HJKMNP-TV-Z]{26}\z/

  @doc "A fresh document on `{kind, key}` in `space`, with no grant."
  @spec new(String.t(), {kind(), String.t()}, String.t() | nil) :: doc()
  def new(space, {kind, key}, incarnation) do
    %{
      v: 1,
      space: space,
      resource: %{kind: kind, key: key},
      incarnation: incarnation,
      grants: [],
      updated_at: System.os_time(:second)
    }
  end

  # ── wire format ─────────────────────────────────────────────────────────────

  @doc "The document as stored."
  @spec encode(doc()) :: binary()
  def encode(doc) do
    JSON.encode!(%{
      "v" => 1,
      "space" => doc.space,
      "resource" => %{"kind" => Atom.to_string(doc.resource.kind), "key" => doc.resource.key},
      "incarnation" => doc.incarnation,
      "grants" => Enum.map(doc.grants, &encode_grant/1),
      "updated_at" => doc.updated_at
    })
  end

  @doc "A grant as the sharing API shows it and the document stores it."
  @spec encode_grant(grant()) :: map()
  def encode_grant(grant) do
    %{
      "id" => grant.id,
      "subject" => grant.subject,
      "actions" => Enum.map(grant.actions, &Atom.to_string/1),
      "created_by" => grant.created_by,
      "created_at" => grant.created_at,
      "expires_at" => grant.expires_at
    }
  end

  @doc """
  A stored document: `{:error, :invalid_acl}` for malformed JSON, another
  version, or a field out of shape (the authorizer treats that as
  unavailable: it fails closed, never open).
  """
  @spec decode(binary()) :: {:ok, doc()} | {:error, :invalid_acl}
  def decode(binary) do
    with {:ok, %{"v" => 1} = map} <- JSON.decode(binary),
         {:ok, kind} <- kind(get_in(map, ["resource", "kind"])),
         key when is_binary(key) <- get_in(map, ["resource", "key"]),
         space when is_binary(space) <- map["space"],
         incarnation when is_binary(incarnation) or is_nil(incarnation) <- map["incarnation"],
         grants when is_list(grants) <- map["grants"],
         {:ok, grants} <- decode_grants(grants),
         updated_at when is_integer(updated_at) <- map["updated_at"] do
      {:ok,
       %{
         v: 1,
         space: space,
         resource: %{kind: kind, key: key},
         incarnation: incarnation,
         grants: grants,
         updated_at: updated_at
       }}
    else
      _ -> {:error, :invalid_acl}
    end
  end

  defp kind("object"), do: {:ok, :object}
  defp kind("prefix"), do: {:ok, :prefix}
  defp kind(_other), do: :error

  defp decode_grants(grants) do
    Enum.reduce_while(grants, {:ok, []}, fn grant, {:ok, acc} ->
      case decode_grant(grant) do
        {:ok, grant} -> {:cont, {:ok, [grant | acc]}}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, grants} -> {:ok, Enum.reverse(grants)}
      :error -> :error
    end
  end

  defp decode_grant(%{"id" => id, "subject" => subject, "actions" => actions} = grant)
       when is_binary(id) and is_binary(subject) and is_list(actions) do
    with {:ok, actions} <- decode_actions(actions),
         created_at when is_integer(created_at) <- grant["created_at"],
         expires_at when is_integer(expires_at) or is_nil(expires_at) <- grant["expires_at"],
         created_by when is_binary(created_by) or is_nil(created_by) <- grant["created_by"] do
      {:ok,
       %{
         id: id,
         subject: subject,
         actions: actions,
         created_by: created_by,
         created_at: created_at,
         expires_at: expires_at
       }}
    else
      _ -> :error
    end
  end

  defp decode_grant(_other), do: :error

  defp decode_actions(names) do
    actions = Enum.map(names, &action/1)

    if Enum.all?(actions, &match?({:ok, _}, &1)),
      do: {:ok, Enum.map(actions, fn {:ok, action} -> action end)},
      else: :error
  end

  # ── matching ────────────────────────────────────────────────────────────────

  @doc """
  The grants of `doc` that give `request` its action now (`now` in Unix
  seconds): unexpired, carrying the action, and naming the principal:
  `user:<id>`; `group:<g>` for each of its groups; `tenant:<t>` for each of
  its tenants (any tenant when it holds them all); `anyone` for any principal
  that is not a share link; `link` only for the share link `link:<that
  grant's id>`. An anonymous request (an empty chain) matches no grant.
  """
  @spec matching_grants(doc() | nil, Request.t(), integer()) :: [grant()]
  def matching_grants(nil, _request, _now), do: []

  def matching_grants(%{grants: grants}, %Request{} = request, now) do
    Enum.filter(grants, fn grant ->
      live?(grant, now) and request.action in grant.actions and
        subject?(grant, request.principal, request.via)
    end)
  end

  @doc "Whether a grant has not expired at `now` (Unix seconds)."
  @spec live?(grant(), integer()) :: boolean()
  def live?(%{expires_at: nil}, _now), do: true
  def live?(%{expires_at: expires_at}, now), do: expires_at > now

  defp subject?(%{subject: "link", id: id}, %Principal{id: principal}, :link),
    do: principal == "link:" <> id

  defp subject?(_grant, _principal, :link), do: false
  defp subject?(%{subject: "link"}, _principal, _via), do: false
  # The anonymous principal (an empty chain) is the operator, `anonymous`
  # with every tenant: it must not inherit a `tenant:` or `user:anonymous`
  # grant meant for someone who authenticated.
  defp subject?(_grant, _principal, :anonymous), do: false
  defp subject?(%{subject: "anyone"}, _principal, _via), do: true
  defp subject?(%{subject: "user:" <> id}, %Principal{id: id}, _via), do: true

  defp subject?(%{subject: "group:" <> group}, %Principal{groups: groups}, _via),
    do: group in groups

  defp subject?(%{subject: "tenant:" <> tenant}, %Principal{} = principal, _via),
    do: Principal.tenant?(principal, tenant)

  defp subject?(_grant, _principal, _via), do: false
end
