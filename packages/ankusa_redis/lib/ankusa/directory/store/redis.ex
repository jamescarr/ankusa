defmodule Ankusa.Directory.Store.Redis do
  @moduledoc """
  The SCIM directory (`Ankusa.Directory`) in Redis, shared by every node.

  `Ankusa.Directory.Store.Persistent` keeps users and groups in the node's own
  store, so SCIM provisioning only reaches the node it was sent to. This store
  keeps them in Redis: every node configured with the same `namespace` sees the
  same users and groups, so a user the identity provider deactivates is refused
  by every admin node at once.

  ## Options

  `config.scim.directory = {Ankusa.Directory.Store.Redis, opts}`:

    * `:url` (required): the Redis URL
    * `:namespace`: key prefix, default `"ankusa:directory:<instance>"`. Nodes
      share a directory **only if they use the same namespace**. On Redis
      Cluster put a hash tag in it (`"{ankusa}:directory"`): the write scripts
      touch several keys derived from the namespace, which must live in one slot.

  ## No mirror

  Unlike `Ankusa.SourceStore.Redis`, nothing is cached: authenticating an admin
  request reads the directory, so a stale mirror would keep a deactivated user
  in. Every call goes to Redis, bounded by a 5 s timeout; a Redis error or
  timeout is `{:error, :unavailable}`, which the guard answers `503` (never a
  `403`, so a client does not discard a good token) and SCIM answers `503`
  with `Retry-After`. With `scim.enabled` and this store, an admin API request
  costs one or two round trips (the user, then their groups).

  ## Key layout

  | Key | Type | Contents |
  | --- | --- | --- |
  | `<ns>:user:<id>` | string | the user, as JSON |
  | `<ns>:users` | sorted set | user ids, all score 0, so in lexicographic (id) order |
  | `<ns>:username` | hash | case-folded `userName` -> user id |
  | `<ns>:userext` | hash | `externalId` -> user id |
  | `<ns>:group:<id>`, `<ns>:groups`, `<ns>:groupname`, `<ns>:groupext` | | the same for groups |
  | `<ns>:members:<group id>` | set | the group's member user ids |
  | `<ns>:usergroups:<user id>` | set | the groups a user is in |

  ## Atomicity

  Every mutation is one Lua script (as in `Ankusa.SourceStore.Redis`): the
  uniqueness check, the version check, the index maintenance and both
  membership sets change together or not at all, whatever the nodes are doing.
  `add_members/3` and `remove_members/3` never read-modify-write the group, so
  concurrent provisioning runs cannot lose each other's members.

  An update (`replace_*`, `delete_*`) reads the resource, builds the new one
  here, and sends the version it read to the script, which refuses it
  (`stale`) when the stored version has moved. A caller that passed no
  `expected_version` simply re-reads and tries again (up to 5 times); one that
  did gets `{:error, :version_conflict}`.
  """

  use Supervisor

  @behaviour Ankusa.Directory.Store

  require Logger

  @timeout 5_000
  @chunk 500
  @max_attempts 5

  @kinds %{
    user: %{type: "User", drop: ~w(groups password members id meta), name: "userName"},
    group: %{type: "Group", drop: ~w(members groups password id meta), name: "displayName"}
  }

  # KEYS: resource, name hash, ext hash, index. ARGV: id, doc, name key, external id.
  @create """
  if redis.call('HEXISTS', KEYS[2], ARGV[3]) == 1 then return {'conflict'} end
  redis.call('SET', KEYS[1], ARGV[2])
  redis.call('ZADD', KEYS[4], 0, ARGV[1])
  redis.call('HSET', KEYS[2], ARGV[3], ARGV[1])
  if ARGV[4] ~= '' then redis.call('HSET', KEYS[3], ARGV[4], ARGV[1]) end
  return {'ok'}
  """

  # KEYS: resource, name hash, ext hash, index, members set.
  # ARGV: id, doc, name key, external id, namespace, then the member user ids.
  @create_group """
  if redis.call('HEXISTS', KEYS[2], ARGV[3]) == 1 then return {'conflict'} end
  local missing = {}
  for i = 6, #ARGV do
    if redis.call('EXISTS', ARGV[5] .. ':user:' .. ARGV[i]) == 0 then
      table.insert(missing, ARGV[i])
    end
  end
  if #missing > 0 then return {'unknown', unpack(missing)} end
  redis.call('SET', KEYS[1], ARGV[2])
  redis.call('ZADD', KEYS[4], 0, ARGV[1])
  redis.call('HSET', KEYS[2], ARGV[3], ARGV[1])
  if ARGV[4] ~= '' then redis.call('HSET', KEYS[3], ARGV[4], ARGV[1]) end
  for i = 6, #ARGV do
    redis.call('SADD', KEYS[5], ARGV[i])
    redis.call('SADD', ARGV[5] .. ':usergroups:' .. ARGV[i], ARGV[1])
  end
  return {'ok'}
  """

  # KEYS: resource, name hash, ext hash, members set (groups; unused for users).
  # ARGV: id, new doc, version the caller read, old name key, new name key,
  # old external id, new external id, namespace, members mode ('none', 'keep'
  # or 'set'), then, for 'set', the wanted member user ids.
  @replace """
  local id, doc, version = ARGV[1], ARGV[2], tonumber(ARGV[3])
  local old_name, new_name, old_ext, new_ext = ARGV[4], ARGV[5], ARGV[6], ARGV[7]
  local ns, mode = ARGV[8], ARGV[9]
  local stored = redis.call('GET', KEYS[1])
  if not stored then return {'not_found'} end
  if tonumber(cjson.decode(stored).meta.version) ~= version then return {'stale'} end
  if new_name ~= old_name then
    local holder = redis.call('HGET', KEYS[2], new_name)
    if holder and holder ~= id then return {'conflict'} end
  end
  local have, want, added, removed = {}, {}, {}, {}
  if mode == 'set' then
    for _, member in ipairs(redis.call('SMEMBERS', KEYS[4])) do have[member] = true end
    local missing = {}
    for i = 10, #ARGV do
      local member = ARGV[i]
      want[member] = true
      if not have[member] then
        table.insert(added, member)
        if redis.call('EXISTS', ns .. ':user:' .. member) == 0 then table.insert(missing, member) end
      end
    end
    if #missing > 0 then return {'unknown', unpack(missing)} end
    for member, _ in pairs(have) do
      if not want[member] then table.insert(removed, member) end
    end
  end
  redis.call('SET', KEYS[1], doc)
  if new_name ~= old_name then
    redis.call('HDEL', KEYS[2], old_name)
    redis.call('HSET', KEYS[2], new_name, id)
  end
  if old_ext ~= new_ext then
    if old_ext ~= '' and redis.call('HGET', KEYS[3], old_ext) == id then
      redis.call('HDEL', KEYS[3], old_ext)
    end
    if new_ext ~= '' then redis.call('HSET', KEYS[3], new_ext, id) end
  end
  for _, member in ipairs(removed) do
    redis.call('SREM', KEYS[4], member)
    redis.call('SREM', ns .. ':usergroups:' .. member, id)
  end
  for _, member in ipairs(added) do
    redis.call('SADD', KEYS[4], member)
    redis.call('SADD', ns .. ':usergroups:' .. member, id)
  end
  return {'ok'}
  """

  # KEYS: resource, name hash, ext hash, index, relation set (a user's
  # `usergroups`, a group's `members`). ARGV: id, version the caller read, name
  # key, external id, namespace, the other side's set prefix (`members` when
  # deleting a user, `usergroups` when deleting a group).
  @delete """
  local id, version = ARGV[1], tonumber(ARGV[2])
  local stored = redis.call('GET', KEYS[1])
  if not stored then return {'not_found'} end
  if tonumber(cjson.decode(stored).meta.version) ~= version then return {'stale'} end
  for _, other in ipairs(redis.call('SMEMBERS', KEYS[5])) do
    redis.call('SREM', ARGV[5] .. ':' .. ARGV[6] .. ':' .. other, id)
  end
  redis.call('DEL', KEYS[5])
  redis.call('DEL', KEYS[1])
  redis.call('ZREM', KEYS[4], id)
  if redis.call('HGET', KEYS[2], ARGV[3]) == id then redis.call('HDEL', KEYS[2], ARGV[3]) end
  if ARGV[4] ~= '' and redis.call('HGET', KEYS[3], ARGV[4]) == id then
    redis.call('HDEL', KEYS[3], ARGV[4])
  end
  return {'ok'}
  """

  # KEYS: group resource, members set. ARGV: group id, namespace, user ids.
  @add_members """
  if redis.call('EXISTS', KEYS[1]) == 0 then return {'not_found'} end
  local missing = {}
  for i = 3, #ARGV do
    if redis.call('EXISTS', ARGV[2] .. ':user:' .. ARGV[i]) == 0 then table.insert(missing, ARGV[i]) end
  end
  if #missing > 0 then return {'unknown', unpack(missing)} end
  for i = 3, #ARGV do
    redis.call('SADD', KEYS[2], ARGV[i])
    redis.call('SADD', ARGV[2] .. ':usergroups:' .. ARGV[i], ARGV[1])
  end
  return {'ok'}
  """

  # KEYS: group resource, members set. ARGV: group id, namespace, user ids.
  @remove_members """
  if redis.call('EXISTS', KEYS[1]) == 0 then return {'not_found'} end
  for i = 3, #ARGV do
    redis.call('SREM', KEYS[2], ARGV[i])
    redis.call('SREM', ARGV[2] .. ':usergroups:' .. ARGV[i], ARGV[1])
  end
  return {'ok'}
  """

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    Supervisor.start_link(__MODULE__, instance, name: Ankusa.via(instance, :directory_sup))
  end

  @impl Supervisor
  def init(instance) do
    url = Keyword.fetch!(options(instance), :url)

    # The password is an MFA, read back on every connect: this supervisor's
    # reports and Redix's own state never hold it.
    redis = Ankusa.Redis.Options.start_opts(url, {__MODULE__, :password, [instance]})

    # `sync_connect: false`: a Redis that is down at boot is `:unavailable`
    # until it is back, not a node that cannot start.
    Supervisor.init(
      [{Redix, redis ++ [name: Ankusa.via(instance, :directory_redis), sync_connect: false]}],
      strategy: :one_for_one
    )
  end

  @doc false
  # Redix calls this on every connect.
  @spec password(atom()) :: String.t() | nil
  def password(instance) do
    instance |> options() |> Keyword.fetch!(:url) |> Ankusa.Redis.Options.password()
  end

  # ── reads ──────────────────────────────────────────────────────────────────

  @impl Ankusa.Directory.Store
  def get_user(instance, id) when is_binary(id), do: get(instance, :user, id)

  @impl Ankusa.Directory.Store
  def get_group(instance, id) when is_binary(id), do: get(instance, :group, id)

  @impl Ankusa.Directory.Store
  def find_user(instance, :user_name, name) when is_binary(name),
    do: find(instance, :user, "username", String.downcase(name))

  def find_user(instance, :external_id, external_id) when is_binary(external_id),
    do: find(instance, :user, "userext", external_id)

  @impl Ankusa.Directory.Store
  def find_group(instance, :display_name, name) when is_binary(name),
    do: find(instance, :group, "groupname", String.downcase(name))

  def find_group(instance, :external_id, external_id) when is_binary(external_id),
    do: find(instance, :group, "groupext", external_id)

  @impl Ankusa.Directory.Store
  def list_users(instance), do: list(instance, :user)

  @impl Ankusa.Directory.Store
  def list_groups(instance), do: list(instance, :group)

  @impl Ankusa.Directory.Store
  def members(instance, group_id) when is_binary(group_id) do
    ns = namespace(instance)

    case transaction(instance, [
           ["EXISTS", key(ns, :group, group_id)],
           ["SMEMBERS", ns <> ":members:" <> group_id]
         ]) do
      {:ok, [0, _members]} -> {:error, :not_found}
      {:ok, [1, members]} -> {:ok, Enum.sort(members)}
      {:error, _reason} = error -> error
    end
  end

  @impl Ankusa.Directory.Store
  def groups_for_user(instance, user_id) when is_binary(user_id) do
    ns = namespace(instance)

    with {:ok, group_ids} <- command(instance, ["SMEMBERS", ns <> ":usergroups:" <> user_id]),
         {:ok, docs} <- mget(instance, Enum.map(Enum.sort(group_ids), &key(ns, :group, &1))) do
      {:ok, docs}
    end
  end

  defp get(instance, kind, id) do
    case command(instance, ["GET", key(namespace(instance), kind, id)]) do
      {:ok, nil} -> :not_found
      {:ok, json} -> decode_one(json)
      {:error, _reason} = error -> error
    end
  end

  defp find(instance, kind, index, field) do
    ns = namespace(instance)

    with {:ok, id} when is_binary(id) <- command(instance, ["HGET", ns <> ":" <> index, field]) do
      get(instance, kind, id)
    else
      {:ok, nil} -> :not_found
      {:error, _reason} = error -> error
    end
  end

  # Ids in order (the index is a sorted set of equal scores), then the
  # resources in chunks. One deleted between the two reads is skipped.
  defp list(instance, kind) do
    ns = namespace(instance)

    with {:ok, ids} <- command(instance, ["ZRANGE", ns <> ":" <> index_name(kind), 0, -1]),
         {:ok, docs} <- mget(instance, Enum.map(ids, &key(ns, kind, &1))) do
      {:ok, docs}
    end
  end

  defp mget(_instance, []), do: {:ok, []}

  defp mget(instance, keys) do
    keys
    |> Enum.chunk_every(@chunk)
    |> Enum.reduce_while({:ok, []}, fn chunk, {:ok, acc} ->
      case command(instance, ["MGET" | chunk]) do
        {:ok, values} -> {:cont, {:ok, [decode_all(values) | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, chunks} -> {:ok, chunks |> Enum.reverse() |> Enum.concat()}
      error -> error
    end
  end

  defp decode_all(values) do
    for json <- values, is_binary(json), {:ok, doc} <- [decode(json)], do: doc
  end

  defp decode_one(json) do
    case decode(json) do
      {:ok, doc} -> {:ok, doc}
      :error -> {:error, :unavailable}
    end
  end

  defp decode(json) do
    case JSON.decode(json) do
      {:ok, %{} = doc} -> {:ok, doc}
      _ -> :error
    end
  end

  # ── writes ─────────────────────────────────────────────────────────────────

  @impl Ankusa.Directory.Store
  def create_user(instance, %{"id" => id, "userName" => name} = resource)
      when is_binary(id) and is_binary(name) do
    ns = namespace(instance)
    doc = new_doc(:user, resource)

    script(instance, @create, keys(ns, :user, id), [
      id,
      JSON.encode!(doc),
      fold_case(name),
      external_id(doc)
    ])
    |> created(doc)
  end

  @impl Ankusa.Directory.Store
  def create_group(instance, %{"id" => id, "displayName" => name} = resource, member_ids)
      when is_binary(id) and is_binary(name) and is_list(member_ids) do
    ns = namespace(instance)
    doc = new_doc(:group, resource)

    script(
      instance,
      @create_group,
      keys(ns, :group, id) ++ [ns <> ":members:" <> id],
      [id, JSON.encode!(doc), fold_case(name), external_id(doc), ns | Enum.uniq(member_ids)]
    )
    |> created(doc)
  end

  defp created({:ok, ["ok"]}, doc), do: {:ok, doc}
  defp created({:ok, ["conflict"]}, _doc), do: {:error, :conflict}
  defp created({:ok, ["unknown" | ids]}, _doc), do: {:error, {:unknown_members, ids}}
  defp created({:error, _reason} = error, _doc), do: error

  @impl Ankusa.Directory.Store
  def replace_user(instance, id, %{"userName" => name} = resource, expected_version)
      when is_binary(id) and is_binary(name),
      do: replace(instance, :user, id, resource, :none, expected_version, @max_attempts)

  @impl Ankusa.Directory.Store
  def replace_group(instance, id, %{"displayName" => name} = resource, members, expected_version)
      when is_binary(id) and is_binary(name) and (is_list(members) or members == :keep) do
    members = if members == :keep, do: :keep, else: Enum.uniq(members)
    replace(instance, :group, id, resource, members, expected_version, @max_attempts)
  end

  defp replace(instance, kind, id, resource, members, expected, attempts) do
    ns = namespace(instance)
    spec = Map.fetch!(@kinds, kind)

    with {:ok, old} <- found(get(instance, kind, id)),
         :ok <- check_version(old, expected) do
      version = old["meta"]["version"]
      meta = %{old["meta"] | "lastModified" => timestamp(), "version" => version + 1}
      doc = resource |> Map.drop(spec.drop) |> Map.merge(%{"id" => id, "meta" => meta})

      {mode, wanted} =
        case members do
          :none -> {"none", []}
          :keep -> {"keep", []}
          list -> {"set", list}
        end

      args =
        [
          id,
          JSON.encode!(doc),
          version,
          fold_case(old[spec.name]),
          fold_case(resource[spec.name]),
          external_id(old),
          external_id(doc),
          ns,
          mode
        ] ++ wanted

      # KEYS: resource, name hash, ext hash, then the group's member set
      # (the user script has no use for a fourth key and is given the index)
      [resource_key, name_hash, ext_hash, index] = keys(ns, kind, id)
      fourth = if kind == :group, do: ns <> ":members:" <> id, else: index
      keys = [resource_key, name_hash, ext_hash, fourth]

      case script(instance, @replace, keys, args) do
        {:ok, ["ok"]} -> {:ok, doc}
        {:ok, ["conflict"]} -> {:error, :conflict}
        {:ok, ["not_found"]} -> {:error, :not_found}
        {:ok, ["unknown" | ids]} -> {:error, {:unknown_members, ids}}
        {:ok, ["stale"]} -> stale(instance, kind, id, resource, members, expected, attempts)
        {:error, _reason} = error -> error
      end
    end
  end

  # The stored version moved between the read and the script. A caller that
  # named a version has lost; one that did not simply goes again.
  defp stale(_instance, _kind, _id, _resource, _members, expected, _attempts)
       when expected != nil,
       do: {:error, :version_conflict}

  defp stale(_instance, _kind, _id, _resource, _members, nil, attempts) when attempts <= 1,
    do: {:error, :version_conflict}

  defp stale(instance, kind, id, resource, members, nil, attempts),
    do: replace(instance, kind, id, resource, members, nil, attempts - 1)

  @impl Ankusa.Directory.Store
  def delete_user(instance, id) when is_binary(id),
    do: delete(instance, :user, id, "usergroups", "members", @max_attempts)

  @impl Ankusa.Directory.Store
  def delete_group(instance, id) when is_binary(id),
    do: delete(instance, :group, id, "members", "usergroups", @max_attempts)

  defp delete(instance, kind, id, relation, other_side, attempts) do
    ns = namespace(instance)
    spec = Map.fetch!(@kinds, kind)

    with {:ok, old} <- found(get(instance, kind, id)) do
      args = [
        id,
        old["meta"]["version"],
        fold_case(old[spec.name]),
        external_id(old),
        ns,
        other_side
      ]

      keys = keys(ns, kind, id) ++ [ns <> ":" <> relation <> ":" <> id]

      case script(instance, @delete, keys, args) do
        {:ok, ["ok"]} ->
          :ok

        {:ok, ["not_found"]} ->
          {:error, :not_found}

        {:ok, ["stale"]} when attempts > 1 ->
          delete(instance, kind, id, relation, other_side, attempts - 1)

        {:ok, ["stale"]} ->
          {:error, :unavailable}

        {:error, _reason} = error ->
          error
      end
    end
  end

  @impl Ankusa.Directory.Store
  def add_members(instance, group_id, user_ids) when is_binary(group_id) and is_list(user_ids) do
    ns = namespace(instance)

    case script(
           instance,
           @add_members,
           [key(ns, :group, group_id), ns <> ":members:" <> group_id],
           [group_id, ns | Enum.uniq(user_ids)]
         ) do
      {:ok, ["ok"]} -> :ok
      {:ok, ["not_found"]} -> {:error, :not_found}
      {:ok, ["unknown" | ids]} -> {:error, {:unknown_members, ids}}
      {:error, _reason} = error -> error
    end
  end

  @impl Ankusa.Directory.Store
  def remove_members(instance, group_id, user_ids)
      when is_binary(group_id) and is_list(user_ids) do
    ns = namespace(instance)

    case script(
           instance,
           @remove_members,
           [key(ns, :group, group_id), ns <> ":members:" <> group_id],
           [group_id, ns | Enum.uniq(user_ids)]
         ) do
      {:ok, ["ok"]} -> :ok
      {:ok, ["not_found"]} -> {:error, :not_found}
      {:error, _reason} = error -> error
    end
  end

  # ── documents ──────────────────────────────────────────────────────────────

  defp new_doc(kind, resource) do
    spec = Map.fetch!(@kinds, kind)
    now = timestamp()
    meta = %{"resourceType" => spec.type, "created" => now, "lastModified" => now, "version" => 1}
    resource |> Map.drop(spec.drop) |> Map.merge(%{"id" => resource["id"], "meta" => meta})
  end

  defp check_version(_old, nil), do: :ok
  defp check_version(%{"meta" => %{"version" => version}}, version), do: :ok
  defp check_version(_old, _expected), do: {:error, :version_conflict}

  defp found({:ok, _resource} = ok), do: ok
  defp found(:not_found), do: {:error, :not_found}
  defp found({:error, _reason} = error), do: error

  defp external_id(%{"externalId" => external_id}) when is_binary(external_id), do: external_id
  defp external_id(_resource), do: ""

  defp fold_case(name), do: String.downcase(name)

  defp timestamp,
    do: DateTime.utc_now() |> DateTime.truncate(:millisecond) |> DateTime.to_iso8601()

  # ── keys ───────────────────────────────────────────────────────────────────

  defp key(ns, :user, id), do: ns <> ":user:" <> id
  defp key(ns, :group, id), do: ns <> ":group:" <> id

  defp index_name(:user), do: "users"
  defp index_name(:group), do: "groups"

  # resource, name hash, ext hash, index
  defp keys(ns, :user, id),
    do: [key(ns, :user, id), ns <> ":username", ns <> ":userext", ns <> ":users"]

  defp keys(ns, :group, id),
    do: [key(ns, :group, id), ns <> ":groupname", ns <> ":groupext", ns <> ":groups"]

  defp options(instance) do
    {_mod, opts} = Ankusa.config(instance).scim.directory
    opts
  end

  defp namespace(instance) do
    Keyword.get(options(instance), :namespace) || "ankusa:directory:#{instance}"
  end

  # ── Redis ──────────────────────────────────────────────────────────────────

  defp script(instance, lua, keys, args),
    do: command(instance, ["EVAL", lua, length(keys)] ++ keys ++ Enum.map(args, &to_arg/1))

  defp to_arg(arg) when is_binary(arg), do: arg
  defp to_arg(arg) when is_integer(arg), do: Integer.to_string(arg)

  defp command(instance, command) do
    instance
    |> conn()
    |> Redix.command(command, timeout: @timeout)
    |> unavailable(command)
  end

  defp transaction(instance, commands) do
    instance
    |> conn()
    |> Redix.transaction_pipeline(commands, timeout: @timeout)
    |> unavailable(commands)
  end

  defp unavailable({:ok, _reply} = ok, _command), do: ok

  defp unavailable({:error, reason}, command) do
    Logger.warning("[ankusa] directory Redis #{command_name(command)} failed: #{inspect(reason)}")

    {:error, :unavailable}
  end

  defp command_name([name | _]) when is_binary(name), do: name
  defp command_name(_commands), do: "transaction"

  defp conn(instance), do: Ankusa.via(instance, :directory_redis)
end
