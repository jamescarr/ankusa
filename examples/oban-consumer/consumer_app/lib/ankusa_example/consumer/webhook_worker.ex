defmodule AnkusaExample.Consumer.WebhookWorker do
  @moduledoc """
  The Oban side of the HTTP handoff: `AnkusaExample.Consumer.Router` inserts
  one of these per newly-seen idempotency key, in the same transaction that
  records the row. The job runs the business effect once and stamps
  `processed_at`; the row (not Oban's job uniqueness) is the dedupe, so
  pruning completed jobs can never let a provider retry or a replay re-run
  the effect. The body stays in the table — job args carry only the key.
  """

  use Oban.Worker, queue: :webhooks, max_attempts: 10

  require Logger

  alias AnkusaExample.Consumer.Repo

  @impl Oban.Worker
  def perform(%Oban.Job{args: args}) do
    key = args["idempotency_key"]

    result =
      Repo.transaction(fn ->
        %Postgrex.Result{rows: rows} =
          Repo.query!(
            """
            SELECT ankusa_id, processed_at
            FROM processed_webhooks
            WHERE idempotency_key = $1
            FOR UPDATE
            """,
            [key]
          )

        case rows do
          # The row was pruned or never existed: nothing to do.
          [] ->
            :ok

          [[ankusa_id, processed_at]] ->
            if is_nil(processed_at) do
              # The effect runs here, inside the transaction that marks it
              # done, so a crash mid-run redelivers the job and runs it again
              # — at-least-once, which the callers' own idempotency absorbs.
              Repo.query!(
                "UPDATE processed_webhooks SET processed_at = now() WHERE idempotency_key = $1",
                [key]
              )

              Logger.info("processed ankusa_id=#{ankusa_id}")
            end

            :ok
        end
      end)

    case result do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
