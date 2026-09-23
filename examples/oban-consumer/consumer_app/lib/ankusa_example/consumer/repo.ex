defmodule AnkusaExample.Consumer.Repo do
  use Ecto.Repo,
    otp_app: :ankusa_example_consumer,
    adapter: Ecto.Adapters.Postgres
end
