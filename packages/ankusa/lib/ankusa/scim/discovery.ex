defmodule Ankusa.Scim.Discovery do
  @moduledoc """
  The SCIM discovery documents (RFC 7643 section 5-7): `ServiceProviderConfig`,
  `ResourceTypes` and `Schemas`, which an identity provider reads to learn what
  this endpoint supports.

  They are static: PATCH and filtering (`eq` only) are supported; bulk, sort,
  ETags and password changes are not; the one authentication scheme is an OAuth
  bearer token (`scim.token`). `base` is the URL prefix for `meta.location`.
  """

  alias Ankusa.Scim.Resource

  @user_schema "urn:ietf:params:scim:schemas:core:2.0:User"
  @group_schema "urn:ietf:params:scim:schemas:core:2.0:Group"
  @enterprise Resource.enterprise_schema()
  @spc_schema "urn:ietf:params:scim:schemas:core:2.0:ServiceProviderConfig"
  @rt_schema "urn:ietf:params:scim:schemas:core:2.0:ResourceType"
  @schema_schema "urn:ietf:params:scim:schemas:core:2.0:Schema"
  @list_schema "urn:ietf:params:scim:api:messages:2.0:ListResponse"

  @doc "The core User schema URN."
  @spec user_schema() :: String.t()
  def user_schema, do: @user_schema

  @doc "The core Group schema URN."
  @spec group_schema() :: String.t()
  def group_schema, do: @group_schema

  @doc "`GET /ServiceProviderConfig`."
  @spec service_provider_config(String.t()) :: map()
  def service_provider_config(base) do
    %{
      "schemas" => [@spc_schema],
      "documentationUri" =>
        "https://github.com/jamescarr/ankusa/blob/main/docs/authentication.md#scim",
      "patch" => %{"supported" => true},
      "bulk" => %{"supported" => false, "maxOperations" => 0, "maxPayloadSize" => 0},
      "filter" => %{"supported" => true, "maxResults" => 1000},
      "changePassword" => %{"supported" => false},
      "sort" => %{"supported" => false},
      "etag" => %{"supported" => false},
      "authenticationSchemes" => [
        %{
          "type" => "oauthbearertoken",
          "name" => "OAuth Bearer Token",
          "description" => "Authentication with the SCIM bearer token (scim.token)",
          "primary" => true
        }
      ],
      "meta" => %{
        "resourceType" => "ServiceProviderConfig",
        "location" => base <> "/scim/v2/ServiceProviderConfig"
      }
    }
  end

  @doc "`GET /ResourceTypes`: a ListResponse."
  @spec resource_types(String.t()) :: map()
  def resource_types(base), do: list(Enum.map(["User", "Group"], &resource_type!(base, &1)))

  @doc "`GET /ResourceTypes/:id`."
  @spec resource_type(String.t(), String.t()) :: {:ok, map()} | :not_found
  def resource_type(base, id) when id in ["User", "Group"], do: {:ok, resource_type!(base, id)}
  def resource_type(_base, _id), do: :not_found

  defp resource_type!(base, "User") do
    %{
      "schemas" => [@rt_schema],
      "id" => "User",
      "name" => "User",
      "endpoint" => "/Users",
      "description" => "User account",
      "schema" => @user_schema,
      "schemaExtensions" => [%{"schema" => @enterprise, "required" => false}],
      "meta" => meta("ResourceType", base <> "/scim/v2/ResourceTypes/User")
    }
  end

  defp resource_type!(base, "Group") do
    %{
      "schemas" => [@rt_schema],
      "id" => "Group",
      "name" => "Group",
      "endpoint" => "/Groups",
      "description" => "Group",
      "schema" => @group_schema,
      "meta" => meta("ResourceType", base <> "/scim/v2/ResourceTypes/Group")
    }
  end

  @doc "`GET /Schemas`: a ListResponse."
  @spec schemas(String.t()) :: map()
  def schemas(base),
    do: list(Enum.map([@user_schema, @group_schema, @enterprise], &schema!(base, &1)))

  @doc "`GET /Schemas/:id`."
  @spec schema(String.t(), String.t()) :: {:ok, map()} | :not_found
  def schema(base, id) when id in [@user_schema, @group_schema, @enterprise],
    do: {:ok, schema!(base, id)}

  def schema(_base, _id), do: :not_found

  defp schema!(base, @user_schema) do
    schema_doc(base, @user_schema, "User", "User Account", [
      attr("userName", "string", required: true, uniqueness: "server"),
      attr("name", "complex",
        sub: [
          attr("formatted", "string"),
          attr("familyName", "string"),
          attr("givenName", "string"),
          attr("middleName", "string"),
          attr("honorificPrefix", "string"),
          attr("honorificSuffix", "string")
        ]
      ),
      attr("displayName", "string"),
      attr("nickName", "string"),
      attr("title", "string"),
      attr("userType", "string"),
      attr("preferredLanguage", "string"),
      attr("locale", "string"),
      attr("timezone", "string"),
      attr("active", "boolean"),
      attr("emails", "complex", multi: true, sub: typed_value()),
      attr("phoneNumbers", "complex", multi: true, sub: typed_value()),
      attr("groups", "complex",
        multi: true,
        mutability: "readOnly",
        sub: [
          attr("value", "string", mutability: "readOnly"),
          attr("display", "string", mutability: "readOnly")
        ]
      ),
      attr("externalId", "string")
    ])
  end

  defp schema!(base, @group_schema) do
    schema_doc(base, @group_schema, "Group", "Group", [
      attr("displayName", "string", required: true, uniqueness: "server"),
      attr("members", "complex",
        multi: true,
        sub: [
          attr("value", "string", mutability: "immutable"),
          attr("display", "string", mutability: "readOnly")
        ]
      ),
      attr("externalId", "string")
    ])
  end

  defp schema!(base, @enterprise) do
    schema_doc(base, @enterprise, "EnterpriseUser", "Enterprise User", [
      attr("employeeNumber", "string"),
      attr("costCenter", "string"),
      attr("organization", "string"),
      attr("division", "string"),
      attr("department", "string"),
      attr("manager", "complex",
        sub: [
          attr("value", "string"),
          attr("displayName", "string", mutability: "readOnly")
        ]
      )
    ])
  end

  defp schema_doc(base, id, name, description, attributes) do
    %{
      "schemas" => [@schema_schema],
      "id" => id,
      "name" => name,
      "description" => description,
      "attributes" => attributes,
      "meta" => meta("Schema", base <> "/scim/v2/Schemas/" <> id)
    }
  end

  defp typed_value do
    [
      attr("value", "string"),
      attr("display", "string"),
      attr("type", "string"),
      attr("primary", "boolean")
    ]
  end

  defp attr(name, type, opts \\ []) do
    base = %{
      "name" => name,
      "type" => type,
      "multiValued" => Keyword.get(opts, :multi, false),
      "required" => Keyword.get(opts, :required, false),
      "caseExact" => false,
      "mutability" => Keyword.get(opts, :mutability, "readWrite"),
      "returned" => "default",
      "uniqueness" => Keyword.get(opts, :uniqueness, "none")
    }

    case Keyword.get(opts, :sub) do
      nil -> base
      sub -> Map.put(base, "subAttributes", sub)
    end
  end

  defp meta(type, location), do: %{"resourceType" => type, "location" => location}

  defp list(resources) do
    %{
      "schemas" => [@list_schema],
      "totalResults" => length(resources),
      "startIndex" => 1,
      "itemsPerPage" => length(resources),
      "Resources" => resources
    }
  end
end
