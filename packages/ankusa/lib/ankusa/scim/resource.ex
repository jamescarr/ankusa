defmodule Ankusa.Scim.Resource do
  @moduledoc """
  Turns the body of a SCIM request into a resource the directory may store.

  An incoming body is *sanitized*, not trusted: only the attributes Ankusa keeps
  survive, and everything else (`id`, `meta`, `groups`, `password`, vendor
  extensions, whatever a client invents) is dropped. The directory is for
  deciding who may use the admin API, not a general user store.

  Kept on a user: `schemas externalId userName name displayName nickName title
  userType preferredLanguage locale timezone active emails phoneNumbers` and the
  enterprise extension. Kept on a group: `schemas externalId displayName`;
  `members` are taken apart and handed back as ids.

  A violation is `{:error, "invalidValue", detail}`, ready for
  `Ankusa.Scim.Errors.send/4`.
  """

  @enterprise "urn:ietf:params:scim:schemas:extension:enterprise:2.0:User"
  @user_keys ~w(schemas externalId userName name displayName nickName title userType
                preferredLanguage locale timezone active emails phoneNumbers) ++ [@enterprise]
  @group_keys ~w(schemas externalId displayName)
  @max_name_bytes 256

  @type error :: {:error, String.t(), String.t()}

  @doc "The enterprise user extension's schema URN."
  @spec enterprise_schema() :: String.t()
  def enterprise_schema, do: @enterprise

  @doc "The attributes kept on a user."
  @spec user_keys() :: [String.t()]
  def user_keys, do: @user_keys

  @doc "The attributes kept on a group (members aside)."
  @spec group_keys() :: [String.t()]
  def group_keys, do: @group_keys

  @doc "Sanitize a user body."
  @spec user(term()) :: {:ok, map()} | error()
  def user(body) when is_map(body) do
    body
    |> Map.take(@user_keys)
    |> check(:user)
  end

  def user(_body), do: invalid("the body must be a JSON object")

  @doc """
  Sanitize a group body. Also returns its member ids: `nil` when the body has no
  `members` (so a PUT without them can leave membership alone), a list when it
  has, even an empty one.
  """
  @spec group(term()) :: {:ok, map(), [String.t()] | nil} | error()
  def group(body) when is_map(body) do
    with {:ok, group} <- body |> Map.take(@group_keys) |> check(:group),
         {:ok, members} <- members(Map.get(body, "members")) do
      {:ok, group, members}
    end
  end

  def group(_body), do: invalid("the body must be a JSON object")

  @doc """
  Check a resource after a PATCH changed it: the name is still a non-empty
  string, `active` is a boolean (a client may PATCH it as `"False"`), the
  external id a string.
  """
  @spec check(map(), :user | :group) :: {:ok, map()} | error()
  def check(resource, type) do
    name_key = if type == :user, do: "userName", else: "displayName"

    with :ok <- name(resource, name_key),
         :ok <- external_id(resource),
         {:ok, resource} <- active(resource, type) do
      {:ok, resource}
    end
  end

  defp name(resource, key) do
    case Map.get(resource, key) do
      value when is_binary(value) and value != "" and byte_size(value) <= @max_name_bytes ->
        if String.trim(value) == "",
          do: invalid("#{key} must not be blank"),
          else: :ok

      _ ->
        invalid("#{key} is required: a non-empty string of at most #{@max_name_bytes} bytes")
    end
  end

  defp external_id(resource) do
    case Map.get(resource, "externalId") do
      nil -> :ok
      value when is_binary(value) and value != "" and byte_size(value) <= @max_name_bytes -> :ok
      _ -> invalid("externalId must be a non-empty string of at most #{@max_name_bytes} bytes")
    end
  end

  defp active(resource, :group), do: {:ok, resource}

  defp active(resource, :user) do
    case coerce_active(Map.get(resource, "active", true)) do
      {:ok, value} -> {:ok, Map.put(resource, "active", value)}
      :error -> invalid("active must be true or false")
    end
  end

  @doc "Coerce `active`: booleans and their string forms, as IdPs send them."
  @spec coerce_active(term()) :: {:ok, boolean()} | :error
  def coerce_active(value) when is_boolean(value), do: {:ok, value}
  def coerce_active(value) when value in ["True", "true"], do: {:ok, true}
  def coerce_active(value) when value in ["False", "false"], do: {:ok, false}
  def coerce_active(_value), do: :error

  @doc """
  The member ids in a `members` attribute value (a list of `%{"value" => id}`),
  or `nil` for none given.
  """
  @spec members(term()) :: {:ok, [String.t()] | nil} | error()
  def members(nil), do: {:ok, nil}

  def members(list) when is_list(list) do
    if Enum.all?(list, &match?(%{"value" => v} when is_binary(v) and v != "", &1)) do
      {:ok, list |> Enum.map(& &1["value"]) |> Enum.uniq()}
    else
      invalid("members must be a list of objects with a non-empty string \"value\"")
    end
  end

  def members(_other), do: invalid("members must be a list")

  defp invalid(detail), do: {:error, "invalidValue", detail}
end
