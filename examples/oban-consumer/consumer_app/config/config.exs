import Config

config :ankusa_example_consumer, ecto_repos: [AnkusaExample.Consumer.Repo]

config :logger, :console, format: "$time [$level] $message\n"
