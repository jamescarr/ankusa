if Code.ensure_loaded?(Plug) do
  defmodule AsyncApiSpex.Plug.RenderSpec do
    @moduledoc """
    A Plug that serves an AsyncAPI document with the `application/asyncapi+json`
    content type.

    Configure it with a module implementing `AsyncApiSpex.Spec`:

        forward "/asyncapi.json", AsyncApiSpex.Plug.RenderSpec, spec: MyApp.AsyncApi

    Or send a document directly from your own route with `send_spec/2`.
    """

    @behaviour Plug

    @doc """
    Returns the configured `AsyncApiSpex.Spec` module.

    Raises `ArgumentError` when the `:spec` option is missing.
    """
    @impl Plug
    @spec init(keyword()) :: module()
    def init(opts) do
      case Keyword.fetch(opts, :spec) do
        {:ok, spec} -> spec
        :error -> raise ArgumentError, "AsyncApiSpex.Plug.RenderSpec requires a :spec option"
      end
    end

    @doc """
    Calls `spec/0` on the configured module and sends the encoded document.
    """
    @impl Plug
    @spec call(Plug.Conn.t(), module()) :: Plug.Conn.t()
    def call(conn, spec), do: send_spec(conn, spec.spec())

    @doc """
    Sends `document` as a `200` response with the AsyncAPI content type and
    halts the connection.
    """
    @spec send_spec(Plug.Conn.t(), AsyncApiSpex.Document.t()) :: Plug.Conn.t()
    def send_spec(conn, %AsyncApiSpex.Document{} = document) do
      conn
      |> Plug.Conn.put_resp_content_type("application/asyncapi+json")
      |> Plug.Conn.send_resp(200, AsyncApiSpex.encode!(document))
      |> Plug.Conn.halt()
    end
  end
end
