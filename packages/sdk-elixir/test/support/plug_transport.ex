defmodule Ankusa.SDK.PlugTransport do
  @moduledoc false

  # The Req `:plug` transport the unit suites drive their clients with: it
  # records every request in the same shape the conformance gateway does and
  # answers through `responder.(conn)`.

  alias Ankusa.SDK.Recorder

  import Plug.Conn, only: [put_resp_content_type: 2, send_resp: 3]

  @spec transport((Plug.Conn.t() -> Plug.Conn.t())) :: {function(), pid()}
  def transport(responder) do
    recorder = Recorder.new()

    plug = fn conn ->
      Recorder.record(recorder, %{
        "method" => conn.method,
        "path" => conn.request_path <> query_suffix(conn.query_string),
        "headers" => Map.new(conn.req_headers),
        "body" => Recorder.decode_body(Req.Test.raw_body(conn))
      })

      responder.(conn)
    end

    {plug, recorder}
  end

  @doc "Answer with a JSON body."
  @spec json(Plug.Conn.t(), non_neg_integer(), term()) :: Plug.Conn.t()
  def json(conn, status, payload) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, JSON.encode!(payload))
  end

  @doc "Answer with a raw body (`\"\"` for none)."
  @spec text(Plug.Conn.t(), non_neg_integer(), binary()) :: Plug.Conn.t()
  def text(conn, status, body), do: send_resp(conn, status, body)

  @doc "Fail the request as a transport error, as an unreachable server would."
  @spec transport_error(Plug.Conn.t(), atom()) :: no_return()
  def transport_error(conn, reason), do: Req.Test.transport_error(conn, reason)

  @doc "The requests recorded so far, oldest first."
  @spec requests(pid()) :: [map()]
  def requests(recorder), do: Recorder.requests(recorder)

  defp query_suffix(""), do: ""
  defp query_suffix(query_string), do: "?" <> query_string
end
