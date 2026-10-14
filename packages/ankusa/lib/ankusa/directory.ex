defmodule Ankusa.Directory do
  @moduledoc """
  The users and groups an identity provider provisions over SCIM
  (`Ankusa.Scim.Router`), and what `Ankusa.Auth` reads to turn a token into
  grants.

  This module is the facade; storage is the `Ankusa.Directory.Store` named in
  `config.scim.directory`: `Ankusa.Directory.Store.Persistent` (this node's
  store, the default) or `Ankusa.Directory.Store.Redis` (the `ankusa_redis`
  package, shared by every edge node). The facade owns what a store should not
  have to: it assigns ids (UUIDv7), folds `userName` and `displayName` to lower
  case before looking one up, and retries an update that lost a race
  (`update_user/3`, `update_group/3`).

  Resources are SCIM JSON maps with string keys (`"id"`, `"userName"`,
  `"active"`, `"meta"`...). The functions mirror the store callbacks, instance
  first, with the return shapes `Ankusa.Directory.Store` documents.
  """

  alias Ankusa.Directory.Store

  @max_update_attempts 5

  defp store(instance), do: instance |> Ankusa.config() |> Map.fetch!(:scim) |> store_module()
  defp store_module(%{directory: {mod, _opts}}), do: mod

  # ── users ──────────────────────────────────────────────────────────────────

  @spec get_user(atom(), Store.id()) :: {:ok, map()} | :not_found | {:error, :unavailable}
  def get_user(instance, id), do: store(instance).get_user(instance, id)

  @doc """
  The user whose `userName` (case-insensitively) or `externalId` (exactly) is
  `value`.
  """
  @spec find_user(atom(), :user_name | :external_id, String.t()) ::
          {:ok, map()} | :not_found | {:error, :unavailable}
  def find_user(instance, :user_name, value) when is_binary(value),
    do: store(instance).find_user(instance, :user_name, String.downcase(value))

  def find_user(instance, :external_id, value) when is_binary(value),
    do: store(instance).find_user(instance, :external_id, value)

  @spec list_users(atom()) :: {:ok, [map()]} | {:error, :unavailable}
  def list_users(instance), do: store(instance).list_users(instance)

  @doc "Create a user: the id is assigned here, whatever `resource` says."
  @spec create_user(atom(), map()) :: {:ok, map()} | {:error, :conflict | :unavailable}
  def create_user(instance, resource) when is_map(resource),
    do: store(instance).create_user(instance, Map.put(resource, "id", Ankusa.UUIDv7.generate()))

  @spec replace_user(atom(), Store.id(), map(), Store.version()) ::
          {:ok, map()} | {:error, :not_found | :conflict | :version_conflict | :unavailable}
  def replace_user(instance, id, resource, expected_version \\ nil),
    do: store(instance).replace_user(instance, id, resource, expected_version)

  @spec delete_user(atom(), Store.id()) :: :ok | {:error, :not_found | :unavailable}
  def delete_user(instance, id), do: store(instance).delete_user(instance, id)

  @doc """
  Read-modify-write a user. `fun` gets the stored user and returns
  `{:ok, new_user}` or `{:error, reason}`; the write carries the version that
  was read, so a concurrent change makes it lose, and the whole thing is
  retried (re-reading) up to #{@max_update_attempts} times. `{:error,
  :version_conflict}` means every attempt lost.
  """
  @spec update_user(atom(), Store.id(), (map() -> {:ok, map()} | {:error, term()})) ::
          {:ok, map()} | {:error, term()}
  def update_user(instance, id, fun) when is_function(fun, 1) do
    retrying(fn ->
      with {:ok, user} <- fetch(get_user(instance, id)),
           {:ok, updated} <- fun.(user) do
        replace_user(instance, id, updated, version(user))
      end
    end)
  end

  # ── groups ─────────────────────────────────────────────────────────────────

  @spec get_group(atom(), Store.id()) :: {:ok, map()} | :not_found | {:error, :unavailable}
  def get_group(instance, id), do: store(instance).get_group(instance, id)

  @doc """
  The group whose `displayName` (case-insensitively) or `externalId` (exactly)
  is `value`.
  """
  @spec find_group(atom(), :display_name | :external_id, String.t()) ::
          {:ok, map()} | :not_found | {:error, :unavailable}
  def find_group(instance, :display_name, value) when is_binary(value),
    do: store(instance).find_group(instance, :display_name, String.downcase(value))

  def find_group(instance, :external_id, value) when is_binary(value),
    do: store(instance).find_group(instance, :external_id, value)

  @spec list_groups(atom()) :: {:ok, [map()]} | {:error, :unavailable}
  def list_groups(instance), do: store(instance).list_groups(instance)

  @doc "Create a group with these members: the id is assigned here."
  @spec create_group(atom(), map(), [Store.id()]) ::
          {:ok, map()} | {:error, :conflict | Store.unknown_members() | :unavailable}
  def create_group(instance, resource, member_ids \\ []) when is_map(resource) do
    store(instance).create_group(
      instance,
      Map.put(resource, "id", Ankusa.UUIDv7.generate()),
      member_ids
    )
  end

  @spec replace_group(atom(), Store.id(), map(), [Store.id()] | :keep, Store.version()) ::
          {:ok, map()}
          | {:error,
             :not_found | :conflict | :version_conflict | Store.unknown_members() | :unavailable}
  def replace_group(instance, id, resource, member_ids \\ :keep, expected_version \\ nil),
    do: store(instance).replace_group(instance, id, resource, member_ids, expected_version)

  @spec delete_group(atom(), Store.id()) :: :ok | {:error, :not_found | :unavailable}
  def delete_group(instance, id), do: store(instance).delete_group(instance, id)

  @doc """
  Read-modify-write a group, as `update_user/3`. `fun` returns
  `{:ok, new_group, member_ids | :keep}`: `:keep` leaves the membership alone.
  """
  @spec update_group(
          atom(),
          Store.id(),
          (map() -> {:ok, map(), [Store.id()] | :keep} | {:error, term()})
        ) :: {:ok, map()} | {:error, term()}
  def update_group(instance, id, fun) when is_function(fun, 1) do
    retrying(fn ->
      with {:ok, group} <- fetch(get_group(instance, id)),
           {:ok, updated, members} <- fun.(group) do
        replace_group(instance, id, updated, members, version(group))
      end
    end)
  end

  # ── membership ─────────────────────────────────────────────────────────────

  @doc "Add users to a group. Atomic and idempotent; no read-modify-write of the group."
  @spec add_members(atom(), Store.id(), [Store.id()]) ::
          :ok | {:error, :not_found | Store.unknown_members() | :unavailable}
  def add_members(instance, group_id, user_ids),
    do: store(instance).add_members(instance, group_id, user_ids)

  @doc "Remove users from a group. Atomic and idempotent; users that are not members are ignored."
  @spec remove_members(atom(), Store.id(), [Store.id()]) ::
          :ok | {:error, :not_found | :unavailable}
  def remove_members(instance, group_id, user_ids),
    do: store(instance).remove_members(instance, group_id, user_ids)

  @spec members(atom(), Store.id()) :: {:ok, [Store.id()]} | {:error, :not_found | :unavailable}
  def members(instance, group_id), do: store(instance).members(instance, group_id)

  @doc "The groups a user belongs to."
  @spec groups_for_user(atom(), Store.id()) :: {:ok, [map()]} | {:error, :unavailable}
  def groups_for_user(instance, user_id), do: store(instance).groups_for_user(instance, user_id)

  # ── update plumbing ────────────────────────────────────────────────────────

  defp fetch({:ok, resource}), do: {:ok, resource}
  defp fetch(:not_found), do: {:error, :not_found}
  defp fetch({:error, _reason} = error), do: error

  defp version(%{"meta" => %{"version" => version}}), do: version

  defp retrying(fun), do: retrying(fun, @max_update_attempts)

  defp retrying(fun, attempts_left) do
    case fun.() do
      {:error, :version_conflict} when attempts_left > 1 -> retrying(fun, attempts_left - 1)
      result -> result
    end
  end
end
