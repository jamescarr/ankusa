defmodule Ankusa.Gateway.Http do
  @moduledoc """
  The answers the claims and objects APIs share for a store that failed:

    * `:not_found` — `404 {"error":"not_found"}`;
    * `{:status, 403, _}` or `:forbidden` — `503 {"error":"store_forbidden"}`
      with `retry-after: 60` and a warning: this node's credential is refused,
      a configuration fault, kept retryable so no worker dead-letters an
      object that is there;
    * `:integrity` or `:integrity_mismatch` — `502 {"error":"integrity_mismatch"}`
      and a warning: the stored bytes are not what was recorded;
    * anything else — `503 {"error":"store_unavailable"}` with `retry-after`
      1-3; logged at debug when it is load (shedding, a deadline, throttling),
      else a warning.
  """

  require Logger

  alias Ankusa.ClaimCheck.Stores
  alias Ankusa.Http

  @doc "Answer `conn` for the store error `reason`."
  @spec store_error(Plug.Conn.t(), term()) :: Plug.Conn.t()
  def store_error(conn, :not_found), do: Http.send_json(conn, 404, %{error: "not_found"})

  def store_error(conn, reason) when reason in [:integrity, :integrity_mismatch] do
    Logger.warning(
      "[ankusa] gateway: #{conn.method} #{conn.request_path}: the stored bytes do not match " <>
        "what was recorded for them"
    )

    Http.send_json(conn, 502, %{error: "integrity_mismatch"})
  end

  def store_error(conn, :forbidden), do: forbidden(conn)
  def store_error(conn, {:status, 403, _body}), do: forbidden(conn)

  def store_error(conn, reason), do: unavailable(conn, reason)

  defp forbidden(conn) do
    Logger.warning(
      "[ankusa] gateway: the object store refused this node's credential " <>
        "(#{conn.method} #{conn.request_path})"
    )

    conn
    |> Plug.Conn.put_resp_header("retry-after", "60")
    |> Http.send_json(503, %{error: "store_forbidden"})
  end

  # Shedding, the deadline and a throttling store are expected under load:
  # logged at debug. A store that could not answer at all is a warning.
  defp unavailable(conn, reason) do
    message = fn ->
      "[ankusa] gateway: store unavailable (#{conn.method} #{conn.request_path}): " <>
        inspect(reason, limit: 20, printable_limit: 512)
    end

    if load?(reason), do: Logger.debug(message), else: Logger.warning(message)

    conn
    |> Plug.Conn.put_resp_header("retry-after", Integer.to_string(Enum.random(1..3)))
    |> Http.send_json(503, %{error: "store_unavailable"})
  end

  defp load?({:unavailable, reason}) when reason in [:overloaded, :deadline], do: true
  defp load?({:unavailable, _} = reason), do: Stores.throttled?(reason)
  defp load?(reason) when reason in [:deadline, :breaker_open], do: true
  defp load?({:throttled, _seconds}), do: true
  defp load?(_reason), do: false
end
