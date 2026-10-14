defmodule Ankusa.Directory.Store do
  @moduledoc """
  Where `Ankusa.Directory` keeps the users and groups SCIM provisions.

  A store is configured as `config.scim.directory`, a `{module, opts}` pair. It
  holds SCIM resources as JSON-shaped maps with string keys. The facade
  (`Ankusa.Directory`) assigns ids and sanitizes bodies; a store owns
  uniqueness, versions, membership and indexes.

  ## Invariants every implementation keeps

    * A stored user never holds `groups` or `password`; a stored group never
      holds `members`. Membership is kept apart so adding or removing one
      member is one atomic operation, with no read-modify-write of the group.
    * `userName` and `displayName` are unique case-insensitively: the index key
      is `String.downcase/1` of the value, the resource keeps its original
      casing. A clash is `{:error, :conflict}`.
    * `externalId` is indexed exactly, and is not required to be unique: the
      index points at the most recent holder.
    * `create_*` takes a resource that already carries its `"id"`, and sets
      `"meta"` to `%{"resourceType" => "User" | "Group", "created" => iso8601,
      "lastModified" => iso8601, "version" => 1}`.
    * `replace_*` keeps the stored `"id"` and `meta.created` whatever the new
      body says, sets `meta.lastModified`, and sets `meta.version` to the old
      one plus one. A non-nil `expected_version` that differs from the stored
      one is `{:error, :version_conflict}`. Renaming onto another resource's
      `userName`/`displayName` is `{:error, :conflict}`; index rows follow the
      rename.
    * `delete_user/2` also drops the user's memberships; `delete_group/2` drops
      the group's membership rows.
    * `{:error, :unavailable}` means the backing store failed or timed out; it
      is never a crash.

  `groups_for_user/2` returns the group resources a user belongs to, `members/2`
  the member user ids of a group. Lists are in id order: stable between calls,
  and creation order to the millisecond (ids are UUIDv7).

  A store that exports `start_link/1` is supervised by the instance, started as
  `{module, instance: instance}` (the way `Ankusa.SourceStore` adapters are).
  The `opts` of the config pair are read through `Ankusa.config/1`.
  """

  @type instance :: atom()
  @type id :: String.t()
  @type resource :: map()
  @type version :: pos_integer() | nil
  @type unknown_members :: {:unknown_members, [id()]}

  @callback get_user(instance(), id()) :: {:ok, resource()} | :not_found | {:error, :unavailable}

  @callback find_user(instance(), :user_name | :external_id, String.t()) ::
              {:ok, resource()} | :not_found | {:error, :unavailable}

  @callback list_users(instance()) :: {:ok, [resource()]} | {:error, :unavailable}

  @callback create_user(instance(), resource()) ::
              {:ok, resource()} | {:error, :conflict | :unavailable}

  @callback replace_user(instance(), id(), resource(), version()) ::
              {:ok, resource()}
              | {:error, :not_found | :conflict | :version_conflict | :unavailable}

  @callback delete_user(instance(), id()) :: :ok | {:error, :not_found | :unavailable}

  @callback get_group(instance(), id()) ::
              {:ok, resource()} | :not_found | {:error, :unavailable}

  @callback find_group(instance(), :display_name | :external_id, String.t()) ::
              {:ok, resource()} | :not_found | {:error, :unavailable}

  @callback list_groups(instance()) :: {:ok, [resource()]} | {:error, :unavailable}

  @callback create_group(instance(), resource(), member_ids :: [id()]) ::
              {:ok, resource()} | {:error, :conflict | unknown_members() | :unavailable}

  @callback replace_group(instance(), id(), resource(), member_ids :: [id()] | :keep, version()) ::
              {:ok, resource()}
              | {:error,
                 :not_found | :conflict | :version_conflict | unknown_members() | :unavailable}

  @callback delete_group(instance(), id()) :: :ok | {:error, :not_found | :unavailable}

  @callback add_members(instance(), group_id :: id(), [id()]) ::
              :ok | {:error, :not_found | unknown_members() | :unavailable}

  @callback remove_members(instance(), group_id :: id(), [id()]) ::
              :ok | {:error, :not_found | :unavailable}

  @callback members(instance(), group_id :: id()) ::
              {:ok, [id()]} | {:error, :not_found | :unavailable}

  @callback groups_for_user(instance(), user_id :: id()) ::
              {:ok, [resource()]} | {:error, :unavailable}
end
