defmodule Ankusa.Scim.Errors do
  @moduledoc """
  SCIM error responses (RFC 7644 section 3.12): an
  `urn:ietf:params:scim:api:messages:2.0:Error` body, `status` as a string, the
  `scimType` where the RFC defines one for the failure, a human-readable
  `detail`.
  """

  @schema "urn:ietf:params:scim:api:messages:2.0:Error"

  @doc "The media type of every SCIM response."
  @spec content_type() :: String.t()
  def content_type, do: "application/scim+json"

  @doc """
  Send a SCIM error and return the conn. `scim_type` is one of the RFC's
  (`invalidFilter`, `uniqueness`, `invalidValue`, `invalidPath`, `noTarget`,
  `mutability`, `invalidSyntax`...) or `nil` when there is none (a `401`, a
  `404`, a lost update).
  """
  @spec send(Plug.Conn.t(), pos_integer(), String.t() | nil, String.t()) :: Plug.Conn.t()
  def send(conn, status, scim_type, detail) do
    body =
      %{"schemas" => [@schema], "status" => Integer.to_string(status), "detail" => detail}
      |> then(&if(scim_type, do: Map.put(&1, "scimType", scim_type), else: &1))

    conn
    |> Plug.Conn.put_resp_content_type(content_type())
    |> Plug.Conn.send_resp(status, JSON.encode!(body))
  end

  @doc """
  Send the SCIM answer for a directory failure: `:conflict` is `409
  uniqueness`, `:not_found` a `404`, unknown members a `400 invalidValue`, a
  lost update (every retry raced) a plain `409`, an unavailable store a `503`
  to retry.
  """
  @spec send_store_error(Plug.Conn.t(), term()) :: Plug.Conn.t()
  def send_store_error(conn, :conflict),
    do: send(conn, 409, "uniqueness", "userName or displayName is already in use")

  def send_store_error(conn, :not_found), do: send(conn, 404, nil, "Resource not found")

  def send_store_error(conn, {:unknown_members, ids}),
    do: send(conn, 400, "invalidValue", "Unknown member(s): #{Enum.join(ids, ", ")}")

  def send_store_error(conn, :version_conflict),
    do: send(conn, 409, nil, "The resource was changed concurrently; retry")

  def send_store_error(conn, :unavailable) do
    conn
    |> Plug.Conn.put_resp_header("retry-after", "1")
    |> send(503, nil, "The directory is unavailable")
  end
end
