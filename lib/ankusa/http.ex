defmodule Ankusa.Http do
  @moduledoc """
  Small `Plug.Conn` helpers shared by every Bandit-fronted router in the
  framework (`Ankusa.Edge.Router`, `Ankusa.ClaimCheck.Router`). Kept here
  instead of duplicated so both surfaces enforce request bodies identically.
  """

  @doc """
  Read the body, refusing anything over `max` bytes without buffering it all.

  `{:too_large, conn}` is a body that exceeded the limit; `{:error, reason, conn}`
  is one that could not be read at all (client disconnect, read timeout). Those
  are different failures and are reported as such — folding a dropped connection
  into "payload too large" tells the caller to shrink a body that was never the
  problem.
  """
  @spec read_body_limited(Plug.Conn.t(), pos_integer()) ::
          {:ok, binary(), Plug.Conn.t()}
          | {:too_large, Plug.Conn.t()}
          | {:error, term(), Plug.Conn.t()}
  def read_body_limited(conn, max) do
    case Plug.Conn.read_body(conn, length: max, read_length: 1_000_000) do
      {:ok, body, conn} -> {:ok, body, conn}
      {:more, _partial, conn} -> {:too_large, conn}
      {:error, reason} -> {:error, reason, conn}
    end
  end

  @doc "Send a JSON-encoded response body with the given status."
  @spec send_json(Plug.Conn.t(), pos_integer(), map()) :: Plug.Conn.t()
  def send_json(conn, status, payload) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, JSON.encode!(payload))
  end
end
