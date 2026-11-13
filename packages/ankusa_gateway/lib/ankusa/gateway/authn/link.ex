defmodule Ankusa.Gateway.Authn.Link do
  @moduledoc """
  The `link` authenticator (`principal_via: :link`), the last of the chain
  while `gateway.sharing.enabled`: a share link (`Ankusa.Gateway.Sharing.Link`)
  authenticates as the principal `link:<grant id>`, with no scope, tenant or
  group.

  It reads a credential only from a `GET` or `HEAD` of
  `/v1/objects/<space>/<key>` whose query carries `link`, `exp` and `sig`: a
  link therefore never authenticates a listing, a write, a delete or a sharing
  route. The credential is `space`, `key`, `link`, `exp` and `sig` alone, so
  another query parameter neither changes it nor mints a second authentication
  cache entry.

  `authenticate/2`: the three parameters in grammar, the grant id an object
  grant whose hash is the key's sha256, and a valid signature, else
  `{:error, :unauthenticated}`; a valid link past its `exp` is
  `{:error, :link_expired}` (`410`). A revoked link still authenticates: the
  `grants` authorizer refuses it.
  """

  @behaviour Ankusa.Gateway.Authenticator

  alias Ankusa.Gateway.{ACL, Spaces}
  alias Ankusa.Gateway.Sharing.Link
  alias Ankusa.Principal

  @impl true
  def init!(sharing, _key), do: %{signing_keys: Map.fetch!(sharing, :signing_keys)}

  @impl true
  def credential(conn, _ctx) do
    case parse(conn) do
      {:ok, link} ->
        {:ok, Enum.join(["link", link.space, link.key, link.id, link.exp, link.sig], "\n")}

      :none ->
        :none
    end
  end

  @impl true
  def authenticate(conn, %{config: %{signing_keys: keys}}) do
    with {:ok, link} <- parse(conn),
         true <- Link.grammar?(link.exp, link.sig),
         {:ok, :object, hash} <- ACL.parse_grant_id(link.id),
         true <- hash == ACL.hash(link.key),
         true <- Link.valid?(keys, link.space, link.id, link.exp, link.sig) do
      exp = String.to_integer(link.exp)

      if exp > System.os_time(:second),
        do: {:ok, Principal.new("link:" <> link.id, []), exp},
        else: {:error, :link_expired}
    else
      _ -> {:error, :unauthenticated}
    end
  end

  # `GET`/`HEAD /v1/objects/<space>/<key...>?link=&exp=&sig=`, from the raw
  # path (as `Ankusa.Gateway.Objects` reads it), or `:none`.
  defp parse(%Plug.Conn{method: method} = conn) when method in ["GET", "HEAD"] do
    with ["", "v1", "objects", space | [_ | _] = segments] <- String.split(conn.request_path, "/"),
         {:ok, key} <- Spaces.key_from_segments(segments),
         %{"link" => id, "exp" => exp, "sig" => sig}
         when is_binary(id) and is_binary(exp) and is_binary(sig) <-
           URI.decode_query(conn.query_string) do
      {:ok, %{space: space, key: key, id: id, exp: exp, sig: sig}}
    else
      _ -> :none
    end
  rescue
    ArgumentError -> :none
  end

  defp parse(_conn), do: :none
end
