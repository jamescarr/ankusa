defmodule Ankusa.Metrics.Router do
  @moduledoc """
  The metrics listener (`metrics.enabled`, default port 4004): `GET /metrics`,
  `GET /health` and `GET /ready`, and a `404` for everything else. A scraper or
  a kubelet probe gets a route to those three and none to the operator API
  (`Ankusa.Admin.Router`), which mutates sources, replays and the pen and so
  belongs behind a different network rule than a Prometheus job.

  The admin listener serves the same three routes through the same functions
  (`metrics/1`, `health/1`, `ready/1`), so a scrape answers identically on
  either port.

  Like every listener it carries no authentication of its own: bind it to an
  interface only the scraper reaches, or give it a `metrics.tls` block with
  `verify: :verify_peer` so only a client holding a certificate from your CA
  can connect (see `Ankusa.Listener`).
  """

  use Plug.Router, copy_opts_to_assign: :ankusa_opts

  plug(:match)
  plug(:dispatch)

  get "/health" do
    health(conn)
  end

  get "/ready" do
    ready(conn)
  end

  get "/metrics" do
    metrics(conn)
  end

  match _ do
    Ankusa.Http.send_json(conn, 404, %{error: "not_found"})
  end

  @doc "Answer `GET /health`: the instance, its version and its roles."
  @spec health(Plug.Conn.t()) :: Plug.Conn.t()
  def health(conn) do
    config = Ankusa.config(instance(conn))

    Ankusa.Http.send_json(conn, 200, %{
      status: "ok",
      instance: to_string(config.instance),
      version: to_string(Application.spec(:ankusa, :vsn)),
      roles: config.roles |> Enum.map(&to_string/1) |> Enum.sort()
    })
  end

  @doc """
  Answer `GET /ready`: readiness, not liveness. `503` while this node's store
  cannot take a synced write. See `Ankusa.Health`.
  """
  @spec ready(Plug.Conn.t()) :: Plug.Conn.t()
  def ready(conn) do
    case Ankusa.Health.ready(instance(conn)) do
      {:ok, body} ->
        Ankusa.Http.send_json(conn, 200, body)

      {:error, body} ->
        conn
        |> Plug.Conn.put_resp_header("retry-after", "1")
        |> Ankusa.Http.send_json(503, body)
    end
  end

  @doc "Answer `GET /metrics` with the Prometheus text exposition."
  @spec metrics(Plug.Conn.t()) :: Plug.Conn.t()
  def metrics(conn) do
    conn
    # Prometheus's text exposition format version is a parameter, not a
    # charset: the header is written verbatim.
    |> Plug.Conn.put_resp_header("content-type", "text/plain; version=0.0.4")
    |> Plug.Conn.send_resp(200, Ankusa.Metrics.scrape(instance(conn)))
  end

  defp instance(%Plug.Conn{} = conn) do
    Keyword.get(conn.assigns[:ankusa_opts] || [], :instance, :default)
  end
end
