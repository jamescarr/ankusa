defmodule AnkusaExample.Consumer.Repo.Migrations.CreateProcessedWebhooks do
  use Ecto.Migration

  def change do
    create table(:processed_webhooks, primary_key: false) do
      # The idempotency key the router read off the sink's
      # `x-ankusa-idempotency-key` header (the ankusa id when a legacy sender
      # omits it). The primary key, so a provider retry and a replay of the
      # same event land on one row no matter how often they arrive.
      add :idempotency_key, :text, primary_key: true
      add :ankusa_id, :text, null: false
      add :source_id, :text, null: false
      add :tenant_id, :text
      add :body, :binary, null: false
      add :body_sha256, :text, null: false
      add :deliveries, :integer, null: false, default: 1
      # Set by the worker once the business effect has run; null means the job
      # is still queued. Survives Oban pruning, which the dedupe no longer
      # depends on.
      add :processed_at, :utc_datetime_usec
    end

    create index(:processed_webhooks, [:ankusa_id])
  end
end
