defmodule Ankusa.Auth.Principal do
  @moduledoc """
  Who is making an admin request, and what they may do.

    * `:anonymous`: auth is off; the principal holds every grant, which
      preserves the open listeners of a deployment that never enabled auth.
    * `:user`: a token whose subject resolved to a (provisioned, active) user.
    * `:service`: a token whose subject matched a subject binding but no
      directory user: a machine identity.
    * `:scim`: the SCIM client, authenticated by the static SCIM token.

  `grants` is `[%{role: :viewer | :operator | :admin, tenants: :all | [String.t()]}]`.
  """

  @type grant :: %{role: :viewer | :operator | :admin, tenants: :all | [String.t()]}

  @type t :: %__MODULE__{
          type: :anonymous | :user | :service | :scim,
          subject: String.t() | nil,
          username: String.t() | nil,
          user_id: String.t() | nil,
          groups: [String.t()],
          grants: [grant()],
          ip: String.t() | nil
        }

  defstruct type: :anonymous,
            subject: nil,
            username: nil,
            user_id: nil,
            groups: [],
            grants: [],
            ip: nil

  @doc "The actor recorded in an audit entry: string keys, no grants."
  @spec to_audit(t()) :: map()
  def to_audit(%__MODULE__{} = principal) do
    %{
      "type" => Atom.to_string(principal.type),
      "subject" => principal.subject,
      "username" => principal.username,
      "user_id" => principal.user_id,
      "ip" => principal.ip
    }
  end
end
