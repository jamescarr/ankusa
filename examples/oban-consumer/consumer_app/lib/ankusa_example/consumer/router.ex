defmodule AnkusaExample.Consumer.Router do
  @moduledoc """
  The HTTP handoff surface `Ankusa.Sink.Http` calls (see `CONSUMER_URL` on
  the ingest side). Every accepted delivery is recorded in
  `processed_webhooks` under its idempotency key — the `x-ankusa-idempotency-key`
  header the sink ships (the ankusa id when a legacy sender omits it) — and becomes
  one Oban job. The worker fleet (`AnkusaExample.Consumer.WebhookWorker`)
  runs the actual business effect and stamps `processed_at`, so a provider
  retry or a DLQ replay collapses into a `deliveries + 1` bump instead of a
  second effect.
  """

  use Plug.Router

  alias AnkusaExample.Consumer.{Repo, WebhookWorker}

  # Running cap on the body Ankusa may hand off in one request. Chosen to be
  # generous for a webhook payload while still bounding memory per request;
  # `Plug.Conn.read_body/2`'s own `:length` option caps a single read call,
  # not the full body, so we loop and check the accumulated size ourselves.
  @max_body_bytes 8_000_000

  plug :match
  plug :dispatch

  post "/deliveries" do
    case read_full_body(conn) do
      {:ok, body, conn} -> handle_delivery(conn, body)
      {:too_large, conn} -> send_resp(conn, 413, "")
    end
  end

  get "/health" do
    send_resp(conn, 200, JSON.encode!(%{status: "ok"}))
  end

  match _ do
    send_resp(conn, 404, "")
  end

  defp read_full_body(conn), do: read_full_body(conn, "")

  defp read_full_body(conn, acc) do
    case Plug.Conn.read_body(conn) do
      {:ok, chunk, conn} ->
        acc = acc <> chunk

        if byte_size(acc) > @max_body_bytes do
          {:too_large, conn}
        else
          {:ok, acc, conn}
        end

      {:more, chunk, conn} ->
        acc = acc <> chunk

        if byte_size(acc) > @max_body_bytes do
          {:too_large, conn}
        else
          read_full_body(conn, acc)
        end

      {:error, _reason} ->
        {:too_large, conn}
    end
  end

  defp handle_delivery(conn, body) do
    case header(conn, "x-ankusa-id") do
      nil ->
        send_resp(conn, 400, JSON.encode!(%{error: "missing x-ankusa-id"}))

      ankusa_id ->
        insert_job(conn, ankusa_id, body)
    end
  end

  defp insert_job(conn, ankusa_id, body) do
    source_id = header(conn, "x-ankusa-source") || ""

    # The idempotency key is the one `Ankusa.Sink.Http` ships:
    # `tenant:source:dedupe_key` when the hook carries a provider event key,
    # else the ankusa id. Read it, never rebuild it, so two tenants that share
    # a provider event id stay two hooks here too. A sender that predates the
    # header falls back to the ankusa id. A deliberately re-sent hook with
    # `x-ankusa-replay-id` is the consumer's to dedupe or reprocess, so this
    # example treats it as a duplicate.
    key = header(conn, "x-ankusa-idempotency-key") || ankusa_id

    attrs = %{
      idempotency_key: key,
      ankusa_id: ankusa_id,
      source_id: source_id,
      tenant_id: header(conn, "x-ankusa-tenant"),
      body: body,
      body_sha256: :crypto.hash(:sha256, body) |> Base.encode16(case: :lower)
    }

    # One transaction: the row is the dedupe, and the job joins it. `xmax = 0`
    # means this insert created the row; the update path means the event
    # already arrived, and the reply says so without queuing a second job.
    # The job insert runs inside the same transaction, so a row and its job
    # commit together or not at all — a lost job insert rolls the row back,
    # and Ankusa's retry can insert it afresh.
    result =
      Repo.transaction(fn ->
        %Postgrex.Result{rows: [[inserted]]} =
          Repo.query!(
            """
            INSERT INTO processed_webhooks
              (idempotency_key, ankusa_id, source_id, tenant_id, body, body_sha256, deliveries)
            VALUES ($1, $2, $3, $4, $5, $6, 1)
            ON CONFLICT (idempotency_key) DO UPDATE
              SET deliveries = processed_webhooks.deliveries + 1
            RETURNING (xmax = 0) AS inserted
            """,
            [
              key,
              ankusa_id,
              source_id,
              attrs.tenant_id,
              attrs.body,
              attrs.body_sha256
            ]
          )

        case inserted do
          true ->
            case %{"idempotency_key" => key} |> WebhookWorker.new() |> Oban.insert() do
              {:ok, %Oban.Job{id: id}} -> id
              {:error, reason} -> Repo.rollback({:job_insert, reason})
            end

          false ->
            :duplicate
        end
      end)

    case result do
      {:ok, job_id} when is_integer(job_id) ->
        send_resp(conn, 202, JSON.encode!(%{job_id: job_id, duplicate: false}))

      {:ok, :duplicate} ->
        send_resp(conn, 202, JSON.encode!(%{job_id: nil, duplicate: true}))

      {:error, _reason} ->
        send_resp(conn, 503, "")
    end
  end

  defp header(conn, name) do
    case Plug.Conn.get_req_header(conn, name) do
      [value | _] when value != "" -> value
      _ -> nil
    end
  end
end
