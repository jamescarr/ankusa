import Config

config :ankusa_example_consumer, AnkusaExample.Consumer.Repo,
  url: System.get_env("DATABASE_URL", "ecto://ankusa:ankusa@localhost:5432/consumer"),
  pool_size: String.to_integer(System.get_env("POOL_SIZE", "10"))

config :ankusa_example_consumer, Oban,
  repo: AnkusaExample.Consumer.Repo,
  queues: [webhooks: 20],
  plugins: [
    {Oban.Plugins.Lifeline, rescue_after: :timer.seconds(30)},
    # Pruning is safe here because the dedupe lives in `processed_webhooks`,
    # not in Oban job uniqueness: a re-sent event collides on the
    # idempotency_key primary key no matter how old its jobs are.
    {Oban.Plugins.Pruner, max_age: 86_400}
  ]

config :ankusa_example_consumer, port: String.to_integer(System.get_env("PORT", "4200"))
