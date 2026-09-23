import Config

config :ankusa_example_consumer, AnkusaExample.Consumer.Repo,
  url: System.get_env("DATABASE_URL", "ecto://ankusa:ankusa@localhost:5432/consumer"),
  pool_size: String.to_integer(System.get_env("POOL_SIZE", "10"))

config :ankusa_example_consumer, Oban,
  repo: AnkusaExample.Consumer.Repo,
  queues: [webhooks: 20],
  plugins: [{Oban.Plugins.Lifeline, rescue_after: :timer.seconds(30)}]

config :ankusa_example_consumer, port: String.to_integer(System.get_env("PORT", "4200"))
