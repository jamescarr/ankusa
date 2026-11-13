defmodule Ankusa.Gateway.Authz.Request do
  @moduledoc """
  What an authorizer is asked about: who (`principal`, authenticated by
  `via`, fingerprinted as `subject_key`), may do what (`action`) to which
  `resource`.

  A resource is `%{kind, space, key, tenant}`: a claim is
  `%{kind: :claim, space: "claims", key: "<tenant_id>/<claim_id>", tenant: tenant_id}`
  (the key carries the tenant, so no two tenants' claims share a resource).

  `conn` is for `:inline` authorizers only: a `:remote` one runs in another
  process and gets the request without it. What a remote one may need from
  the connection travels as two strings: `authorization` (the caller's own
  `authorization` header, for `forward_authorization`) and `request_id`.
  """

  @enforce_keys [:principal, :via, :subject_key, :action, :resource]
  defstruct [
    :principal,
    :via,
    :subject_key,
    :action,
    :resource,
    :conn,
    :authorization,
    :request_id
  ]

  @type action :: :read | :write | :delete | :list | :share
  @type resource :: %{
          kind: :claim | :object | :prefix,
          space: String.t(),
          key: String.t(),
          tenant: String.t() | nil
        }
  @type t :: %__MODULE__{
          principal: Ankusa.Principal.t(),
          via: atom(),
          subject_key: binary(),
          action: action(),
          resource: resource(),
          conn: Plug.Conn.t() | nil,
          authorization: String.t() | nil,
          request_id: String.t() | nil
        }

  @doc """
  The request for `action` on `resource` by the caller `Ankusa.Gateway.Authn`
  authenticated: `principal`, `principal_via` and `subject_key` from its
  assigns.
  """
  @spec from_conn(Plug.Conn.t(), action(), resource()) :: t()
  def from_conn(%Plug.Conn{assigns: assigns} = conn, action, resource) do
    %__MODULE__{
      principal: Map.fetch!(assigns, :principal),
      via: Map.fetch!(assigns, :principal_via),
      subject_key: Map.fetch!(assigns, :subject_key),
      action: action,
      resource: resource,
      conn: conn,
      authorization: conn |> Plug.Conn.get_req_header("authorization") |> List.first(),
      request_id: Ankusa.Audit.Context.from_conn(conn).request_id
    }
  end
end
