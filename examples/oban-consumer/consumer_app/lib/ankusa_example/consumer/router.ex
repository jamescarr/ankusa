defmodule AnkusaExample.Consumer.Router do
  @moduledoc """
  The HTTP handoff surface `Ankusa.Sink.Http` calls (see `CONSUMER_URL` on
  the ingest side). Every accepted delivery becomes one uniquely-keyed Oban
  job; the worker fleet (`AnkusaExample.Consumer.WebhookWorker`) does the
  actual, idempotent bookkeeping in `processed_webhooks`.
  """

  use Plug.Router

  alias AnkusaExample.Consumer.WebhookWorker

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
    args = %{
      "ankusa_id" => ankusa_id,
      "source_id" => header(conn, "x-ankusa-source"),
      "tenant_id" => header(conn, "x-ankusa-tenant"),
      "content_type" => header(conn, "content-type"),
      "body_base64" => Base.encode64(body)
    }

    args
    |> WebhookWorker.new(unique: [period: :infinity, keys: [:ankusa_id]])
    |> Oban.insert()
    |> case do
      # Oban 2.24's `%Oban.Job{}` carries a `conflict?` boolean: when the
      # `unique:` key matches an already-inserted job, `Oban.insert/1` still
      # returns `{:ok, job}`, but `job` is the *existing* row (not a fresh
      # insert) and `conflict?` is `true`. That's the documented way to
      # distinguish "this ankusa_id was already queued" from "brand new job"
      # without a second query — see https://hexdocs.pm/oban/unique_jobs.html.
      {:ok, %Oban.Job{id: id, conflict?: conflict?}} ->
        send_resp(conn, 202, JSON.encode!(%{job_id: id, duplicate: conflict?}))

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
