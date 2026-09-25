defmodule AnkusaChaosSink do
  @moduledoc """
  The chaos harness's consumer: an HTTP endpoint that records every delivery it
  receives, so the invariants can be checked against what a real consumer saw.

  It writes the same `processed_webhooks` shape `mix loadgen.verify` polls
  (`ankusa_id`, `body_sha256`, `deliveries`), plus the seq the WAL assigned —
  which is what lets `Ankusa.WAL.Checker` reason about I3 (a cursor-following
  reader misses nothing) and I10 (extras are attributable).

  A repeated delivery increments `deliveries` rather than inserting a second
  row: at-least-once means the same id may arrive twice, and the count is the
  evidence of how many times.
  """

  use Application

  @impl true
  def start(_type, _args) do
    port = String.to_integer(System.get_env("PORT", "4200"))

    children = [
      {Postgrex, url_opts() |> Keyword.put(:name, __MODULE__.DB)},
      {Bandit, plug: {__MODULE__.Router, []}, scheme: :http, port: port}
    ]

    with {:ok, pid} <- Supervisor.start_link(children, strategy: :one_for_one) do
      create_schema()
      {:ok, pid}
    end
  end

  def url_opts do
    url = System.get_env("DATABASE_URL", "postgres://ankusa:ankusa@postgres/chaos")
    uri = URI.parse(url)
    {user, password} = credentials(uri.userinfo)

    [
      hostname: uri.host,
      port: uri.port || 5432,
      username: user,
      password: password,
      database: String.trim_leading(uri.path || "", "/"),
      pool_size: 32
    ]
  end

  defp credentials(nil), do: {"ankusa", "ankusa"}

  defp credentials(userinfo) do
    case String.split(userinfo, ":", parts: 2) do
      [user, password] -> {user, password}
      [user] -> {user, nil}
    end
  end

  defp create_schema do
    Postgrex.query!(
      __MODULE__.DB,
      """
      CREATE TABLE IF NOT EXISTS processed_webhooks (
        ankusa_id TEXT PRIMARY KEY,
        body_sha256 TEXT NOT NULL,
        ankusa_seq BIGINT,
        deliveries INTEGER NOT NULL DEFAULT 1,
        first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )
      """,
      []
    )
  end

  defmodule Router do
    @moduledoc "One route: record the delivery, answer `202`; `500` if the record failed."

    import Plug.Conn

    def init(opts), do: opts

    def call(conn, _opts) do
      try do
        {:ok, body, conn} = read_body(conn)
        id = header(conn, "x-ankusa-id") || "unknown-#{System.unique_integer([:positive])}"
        seq = header(conn, "x-ankusa-seq")

        record(id, body, seq)

        conn
        |> put_resp_content_type("application/json")
        |> send_resp(202, ~s({"ok":true}))
      rescue
        _ ->
          conn
          |> put_resp_content_type("application/json")
          |> send_resp(500, ~s({"ok":false}))
      end
    end

    defp header(conn, name) do
      case get_req_header(conn, name) do
        [value | _] -> value
        [] -> nil
      end
    end

    defp record(id, body, seq) do
      sha = Base.encode16(:crypto.hash(:sha256, body), case: :lower)
      seq = if seq, do: String.to_integer(seq), else: nil

      Postgrex.query!(
        AnkusaChaosSink.DB,
        """
        INSERT INTO processed_webhooks (ankusa_id, body_sha256, ankusa_seq)
        VALUES ($1, $2, $3)
        ON CONFLICT (ankusa_id) DO UPDATE
          SET deliveries = processed_webhooks.deliveries + 1,
              last_seen_at = now()
        """,
        [id, sha, seq]
      )
    end
  end
end
