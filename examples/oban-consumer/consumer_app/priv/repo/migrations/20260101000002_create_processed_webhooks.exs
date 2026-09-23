defmodule AnkusaExample.Consumer.Repo.Migrations.CreateProcessedWebhooks do
  use Ecto.Migration

  def change do
    create table(:processed_webhooks, primary_key: false) do
      add :ankusa_id, :text, primary_key: true
      add :source_id, :text, null: false
      add :tenant_id, :text
      add :body_sha256, :text, null: false
      add :deliveries, :integer, null: false, default: 1
      add :processed_at, :utc_datetime_usec, null: false
    end
  end
end
