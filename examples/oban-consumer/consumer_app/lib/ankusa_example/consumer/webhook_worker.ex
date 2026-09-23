defmodule AnkusaExample.Consumer.WebhookWorker do
  @moduledoc """
  The Oban side of the HTTP handoff: `AnkusaExample.Consumer.Router` inserts
  one of these per accepted `POST /deliveries` request, keyed uniquely on
  `ankusa_id` (see the router's `Oban.insert/1` call). Recording into
  `processed_webhooks` is idempotent by design — Ankusa's HTTP sink retries
  on any non-2xx, so the same `ankusa_id` can legitimately land here more
  than once; the `ON CONFLICT` bump of `deliveries` is what proves "zero
  loss, at-least-once" without double-counting business effects.
  """

  use Oban.Worker, queue: :webhooks, max_attempts: 10

  require Logger

  alias AnkusaExample.Consumer.Repo

  @impl Oban.Worker
  def perform(%Oban.Job{args: args}) do
    body = Base.decode64!(args["body_base64"])
    sha_hex = :crypto.hash(:sha256, body) |> Base.encode16(case: :lower)

    Repo.query!(
      """
      INSERT INTO processed_webhooks
        (ankusa_id, source_id, tenant_id, body_sha256, deliveries, processed_at)
      VALUES ($1, $2, $3, $4, 1, now())
      ON CONFLICT (ankusa_id) DO UPDATE
        SET deliveries = processed_webhooks.deliveries + 1
      """,
      [args["ankusa_id"], args["source_id"], args["tenant_id"], sha_hex]
    )

    Logger.info("processed ankusa_id=#{args["ankusa_id"]}")

    :ok
  end
end
