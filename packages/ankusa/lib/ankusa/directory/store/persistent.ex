defmodule Ankusa.Directory.Store.Persistent do
  @moduledoc """
  The directory in this node's store (`Ankusa.Store`, RocksDB): the default
  `scim.directory`.

  Reads go straight to the store. Writes go through one process
  (`Ankusa.via(instance, :directory)`), which does each check-then-write (a
  name that must be unique, a version that must match, members that must exist)
  with no other writer in between, and commits it as one synced batch. So a
  user, its name index, and its membership rows appear or vanish together, and
  a crash cannot leave a half-written resource.

  The directory is node-local, like the rest of the store: with several admin
  nodes, SCIM must reach the node that serves the admin API users authenticate
  to, or use `Ankusa.Directory.Store.Redis` (the `ankusa_redis` package), which
  every node shares.

  Layout, in the store's default column family: the
  resources (`du:<id>` users, `dg:<id>` groups), their case-folded name indexes
  (`dU:`, `dG:`), exact `externalId` indexes (`dX:`, `dY:`), and membership in
  both directions (`dm:<group>\\0<user>`, `dn:<user>\\0<group>`) so "who is in
  this group" and "which groups is this user in" are each one prefix scan.
  """

  @behaviour Ankusa.Directory.Store

  use GenServer

  alias Ankusa.Store
  alias Ankusa.Store.Keys

  @call_timeout 10_000

  @kinds %{
    user: %{type: "User", name: "userName", drop: ~w(groups password members id meta)},
    group: %{type: "Group", name: "displayName", drop: ~w(members groups password id meta)}
  }

  @doc false
  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, instance, name: Ankusa.via(instance, :directory))
  end

  @impl GenServer
  def init(instance), do: {:ok, instance}

  # ── reads ──────────────────────────────────────────────────────────────────

  @impl Ankusa.Directory.Store
  def get_user(instance, id) when is_binary(id), do: get(instance, Keys.directory_user(id))

  @impl Ankusa.Directory.Store
  def find_user(instance, :user_name, name) when is_binary(name),
    do: via_index(instance, Keys.directory_user_name(String.downcase(name)), &get_user/2)

  def find_user(instance, :external_id, external_id) when is_binary(external_id),
    do: via_index(instance, Keys.directory_user_external(external_id), &get_user/2)

  @impl Ankusa.Directory.Store
  def list_users(instance), do: list(instance, :directory_users)

  @impl Ankusa.Directory.Store
  def get_group(instance, id) when is_binary(id), do: get(instance, Keys.directory_group(id))

  @impl Ankusa.Directory.Store
  def find_group(instance, :display_name, name) when is_binary(name),
    do: via_index(instance, Keys.directory_group_name(String.downcase(name)), &get_group/2)

  def find_group(instance, :external_id, external_id) when is_binary(external_id),
    do: via_index(instance, Keys.directory_group_external(external_id), &get_group/2)

  @impl Ankusa.Directory.Store
  def list_groups(instance), do: list(instance, :directory_groups)

  @impl Ankusa.Directory.Store
  def members(instance, group_id) when is_binary(group_id) do
    with {:ok, _group} <- found(get_group(instance, group_id)),
         do: scan_members(instance, group_id)
  end

  @impl Ankusa.Directory.Store
  def groups_for_user(instance, user_id) when is_binary(user_id) do
    with {:ok, group_ids} <- scan_groups(instance, user_id),
         {:ok, results} <- multi_get(instance, Enum.map(group_ids, &Keys.directory_group/1)) do
      {:ok, for({:ok, json} <- results, {:ok, group} <- [decode(json)], do: group)}
    end
  end

  defp get(instance, key) do
    case Store.get(instance, :default, key) do
      {:ok, json} -> decode_one(json)
      :not_found -> :not_found
      {:error, _reason} -> {:error, :unavailable}
    end
  end

  defp decode_one(json) do
    case decode(json) do
      {:ok, resource} -> {:ok, resource}
      :error -> {:error, :unavailable}
    end
  end

  defp decode(json) do
    case JSON.decode(json) do
      {:ok, %{} = resource} -> {:ok, resource}
      _ -> :error
    end
  end

  # An index row names an id; the resource it names may be gone (the index is
  # read and the resource deleted in between): that is `:not_found`.
  defp via_index(instance, index_key, getter) do
    case Store.get(instance, :default, index_key) do
      {:ok, id} -> getter.(instance, id)
      :not_found -> :not_found
      {:error, _reason} -> {:error, :unavailable}
    end
  end

  defp list(instance, family) do
    %{lo: lo, hi: hi} = Keys.family(family)

    case Store.fold(instance, family, {lo, hi}, [], fn _key, json, acc ->
           case decode(json) do
             {:ok, resource} -> {:cont, [resource | acc]}
             :error -> {:cont, acc}
           end
         end) do
      {:ok, resources} -> {:ok, Enum.reverse(resources)}
      {:error, _reason} -> {:error, :unavailable}
    end
  end

  defp multi_get(_instance, []), do: {:ok, []}

  defp multi_get(instance, keys) do
    case Store.multi_get(instance, :default, keys) do
      {:ok, results} -> {:ok, results}
      {:error, _reason} -> {:error, :unavailable}
    end
  end

  defp scan_members(instance, group_id),
    do: scan_suffixes(instance, :group_members, <<"dm:", group_id::binary, 0>>)

  defp scan_groups(instance, user_id),
    do: scan_suffixes(instance, :user_groups, <<"dn:", user_id::binary, 0>>)

  # The ids after `prefix` in every key that starts with it, in key order.
  defp scan_suffixes(instance, family, prefix) do
    size = byte_size(prefix)

    case Store.fold(instance, family, Keys.range(prefix), [], fn key, _value, acc ->
           {:cont, [binary_part(key, size, byte_size(key) - size) | acc]}
         end) do
      {:ok, ids} -> {:ok, Enum.reverse(ids)}
      {:error, _reason} -> {:error, :unavailable}
    end
  end

  defp found({:ok, _resource} = ok), do: ok
  defp found(:not_found), do: {:error, :not_found}
  defp found({:error, _reason} = error), do: error

  # ── writes: validated here, performed by the process ───────────────────────

  @impl Ankusa.Directory.Store
  def create_user(instance, %{"id" => id, "userName" => name} = resource)
      when is_binary(id) and is_binary(name),
      do: call(instance, {:create_user, resource})

  @impl Ankusa.Directory.Store
  def replace_user(instance, id, %{"userName" => name} = resource, expected_version)
      when is_binary(id) and is_binary(name),
      do: call(instance, {:replace_user, id, resource, expected_version})

  @impl Ankusa.Directory.Store
  def delete_user(instance, id) when is_binary(id), do: call(instance, {:delete_user, id})

  @impl Ankusa.Directory.Store
  def create_group(instance, %{"id" => id, "displayName" => name} = resource, member_ids)
      when is_binary(id) and is_binary(name) and is_list(member_ids),
      do: call(instance, {:create_group, resource, Enum.uniq(member_ids)})

  @impl Ankusa.Directory.Store
  def replace_group(
        instance,
        id,
        %{"displayName" => name} = resource,
        member_ids,
        expected_version
      )
      when is_binary(id) and is_binary(name) and (is_list(member_ids) or member_ids == :keep) do
    members = if member_ids == :keep, do: :keep, else: Enum.uniq(member_ids)
    call(instance, {:replace_group, id, resource, members, expected_version})
  end

  @impl Ankusa.Directory.Store
  def delete_group(instance, id) when is_binary(id), do: call(instance, {:delete_group, id})

  @impl Ankusa.Directory.Store
  def add_members(instance, group_id, user_ids) when is_binary(group_id) and is_list(user_ids),
    do: call(instance, {:add_members, group_id, Enum.uniq(user_ids)})

  @impl Ankusa.Directory.Store
  def remove_members(instance, group_id, user_ids) when is_binary(group_id) and is_list(user_ids),
    do: call(instance, {:remove_members, group_id, Enum.uniq(user_ids)})

  defp call(instance, request) do
    GenServer.call(Ankusa.via(instance, :directory), request, @call_timeout)
  catch
    :exit, _reason -> {:error, :unavailable}
  end

  @impl GenServer
  def handle_call(request, _from, instance), do: {:reply, perform(instance, request), instance}

  # ── writes: the process ────────────────────────────────────────────────────

  defp perform(instance, {:create_user, resource}) do
    with {:ok, doc, ops} <- create_ops(instance, :user, resource) do
      commit(instance, ops, doc)
    end
  end

  defp perform(instance, {:create_group, resource, member_ids}) do
    with {:ok, missing} <- missing_users(instance, member_ids),
         :ok <- no_unknown(missing),
         {:ok, doc, ops} <- create_ops(instance, :group, resource) do
      member_ops = Enum.flat_map(member_ids, &member_put(doc["id"], &1))
      commit(instance, ops ++ member_ops, doc)
    end
  end

  defp perform(instance, {:replace_user, id, resource, expected}) do
    with {:ok, old} <- found(get_user(instance, id)),
         {:ok, doc, ops} <- replace_ops(instance, :user, old, resource, expected) do
      commit(instance, ops, doc)
    end
  end

  defp perform(instance, {:replace_group, id, resource, members, expected}) do
    with {:ok, old} <- found(get_group(instance, id)),
         {:ok, member_ops} <- membership_ops(instance, id, members),
         {:ok, doc, ops} <- replace_ops(instance, :group, old, resource, expected) do
      commit(instance, ops ++ member_ops, doc)
    end
  end

  defp perform(instance, {:delete_user, id}) do
    with {:ok, user} <- found(get_user(instance, id)),
         {:ok, group_ids} <- scan_groups(instance, id),
         {:ok, external} <- external_delete(instance, :user, id, user) do
      ops =
        [
          delete(Keys.directory_user(id)),
          delete(Keys.directory_user_name(fold_case(user["userName"])))
        ] ++
          external ++ Enum.flat_map(group_ids, &member_delete(&1, id))

      commit(instance, ops, :ok)
    end
  end

  defp perform(instance, {:delete_group, id}) do
    with {:ok, group} <- found(get_group(instance, id)),
         {:ok, user_ids} <- scan_members(instance, id),
         {:ok, external} <- external_delete(instance, :group, id, group) do
      ops =
        [
          delete(Keys.directory_group(id)),
          delete(Keys.directory_group_name(fold_case(group["displayName"])))
        ] ++ external ++ Enum.flat_map(user_ids, &member_delete(id, &1))

      commit(instance, ops, :ok)
    end
  end

  defp perform(instance, {:add_members, group_id, user_ids}) do
    with {:ok, _group} <- found(get_group(instance, group_id)),
         {:ok, missing} <- missing_users(instance, user_ids),
         :ok <- no_unknown(missing) do
      commit(instance, Enum.flat_map(user_ids, &member_put(group_id, &1)), :ok)
    end
  end

  defp perform(instance, {:remove_members, group_id, user_ids}) do
    with {:ok, _group} <- found(get_group(instance, group_id)) do
      commit(instance, Enum.flat_map(user_ids, &member_delete(group_id, &1)), :ok)
    end
  end

  # ── write plans ────────────────────────────────────────────────────────────

  # `{:ok, document, ops}` for a new resource, or `{:error, :conflict}` when its
  # name is taken.
  defp create_ops(instance, kind, resource) do
    spec = Map.fetch!(@kinds, kind)
    id = resource["id"]
    name_key = name_key(kind, resource[spec.name])

    case Store.get(instance, :default, name_key) do
      {:ok, _holder} ->
        {:error, :conflict}

      {:error, _reason} ->
        {:error, :unavailable}

      :not_found ->
        now = timestamp()

        meta = %{
          "resourceType" => spec.type,
          "created" => now,
          "lastModified" => now,
          "version" => 1
        }

        doc = resource |> Map.drop(spec.drop) |> Map.merge(%{"id" => id, "meta" => meta})

        ops =
          [put(resource_key(kind, id), JSON.encode!(doc)), put(name_key, id)] ++
            external_put(kind, id, doc["externalId"])

        {:ok, doc, ops}
    end
  end

  # The new document and the ops that turn `old` into it: the resource row, the
  # name index when the name changed, the external-id index when that changed.
  defp replace_ops(instance, kind, old, resource, expected) do
    spec = Map.fetch!(@kinds, kind)
    id = old["id"]
    old_version = old["meta"]["version"]

    with :ok <- check_version(old_version, expected),
         {:ok, name_ops} <- rename_ops(instance, kind, id, old[spec.name], resource[spec.name]),
         {:ok, external_ops} <-
           external_swap(instance, kind, id, old["externalId"], resource["externalId"]) do
      meta = %{old["meta"] | "lastModified" => timestamp(), "version" => old_version + 1}
      doc = resource |> Map.drop(spec.drop) |> Map.merge(%{"id" => id, "meta" => meta})
      {:ok, doc, [put(resource_key(kind, id), JSON.encode!(doc))] ++ name_ops ++ external_ops}
    end
  end

  defp check_version(_current, nil), do: :ok
  defp check_version(current, current), do: :ok
  defp check_version(_current, _expected), do: {:error, :version_conflict}

  defp rename_ops(instance, kind, id, old_name, new_name) do
    old_key = name_key(kind, old_name)
    new_key = name_key(kind, new_name)

    if old_key == new_key do
      {:ok, []}
    else
      case Store.get(instance, :default, new_key) do
        {:ok, holder} when holder != id -> {:error, :conflict}
        {:ok, _self} -> {:ok, []}
        :not_found -> {:ok, [delete(old_key), put(new_key, id)]}
        {:error, _reason} -> {:error, :unavailable}
      end
    end
  end

  defp external_put(_kind, _id, external_id) when not is_binary(external_id), do: []
  defp external_put(kind, id, external_id), do: [put(external_key(kind, external_id), id)]

  defp external_delete(instance, kind, id, resource),
    do: external_swap(instance, kind, id, resource["externalId"], nil)

  # Drop the old external-id row if it is still this resource's, then claim the
  # new one. `externalId` is not unique: the newest holder owns the row.
  defp external_swap(_instance, _kind, _id, same, same) when is_binary(same), do: {:ok, []}

  defp external_swap(instance, kind, id, old, new) do
    with {:ok, release} <- external_release(instance, kind, id, old) do
      {:ok, release ++ external_put(kind, id, new)}
    end
  end

  defp external_release(_instance, _kind, _id, old) when not is_binary(old), do: {:ok, []}

  defp external_release(instance, kind, id, old) do
    key = external_key(kind, old)

    case Store.get(instance, :default, key) do
      {:ok, ^id} -> {:ok, [delete(key)]}
      {:ok, _other_holder} -> {:ok, []}
      :not_found -> {:ok, []}
      {:error, _reason} -> {:error, :unavailable}
    end
  end

  # The ops that make the group's members exactly `members` (`:keep`: none).
  defp membership_ops(_instance, _group_id, :keep), do: {:ok, []}

  defp membership_ops(instance, group_id, members) do
    with {:ok, current} <- scan_members(instance, group_id),
         added = members -- current,
         {:ok, missing} <- missing_users(instance, added),
         :ok <- no_unknown(missing) do
      removed = current -- members

      {:ok,
       Enum.flat_map(removed, &member_delete(group_id, &1)) ++
         Enum.flat_map(added, &member_put(group_id, &1))}
    end
  end

  defp missing_users(_instance, []), do: {:ok, []}

  defp missing_users(instance, user_ids) do
    with {:ok, results} <- multi_get(instance, Enum.map(user_ids, &Keys.directory_user/1)) do
      {:ok, for({id, :not_found} <- Enum.zip(user_ids, results), do: id)}
    end
  end

  defp no_unknown([]), do: :ok
  defp no_unknown(missing), do: {:error, {:unknown_members, missing}}

  defp commit(instance, ops, result) do
    case Store.write(instance, ops, sync: true) do
      :ok -> if result == :ok, do: :ok, else: {:ok, result}
      {:error, _reason} -> {:error, :unavailable}
    end
  end

  # ── keys and ops ───────────────────────────────────────────────────────────

  defp resource_key(:user, id), do: Keys.directory_user(id)
  defp resource_key(:group, id), do: Keys.directory_group(id)

  defp name_key(:user, name), do: Keys.directory_user_name(fold_case(name))
  defp name_key(:group, name), do: Keys.directory_group_name(fold_case(name))

  defp external_key(:user, external_id), do: Keys.directory_user_external(external_id)
  defp external_key(:group, external_id), do: Keys.directory_group_external(external_id)

  defp fold_case(name), do: String.downcase(name)

  defp member_put(group_id, user_id) do
    [put(Keys.group_member(group_id, user_id), ""), put(Keys.user_group(user_id, group_id), "")]
  end

  defp member_delete(group_id, user_id) do
    [delete(Keys.group_member(group_id, user_id)), delete(Keys.user_group(user_id, group_id))]
  end

  defp put(key, value), do: {:put, :default, key, value}
  defp delete(key), do: {:delete, :default, key}

  defp timestamp,
    do: DateTime.utc_now() |> DateTime.truncate(:millisecond) |> DateTime.to_iso8601()
end
