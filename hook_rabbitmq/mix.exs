defmodule HookRabbitmq.MixProject do
  use Mix.Project

  # Same forced-split pattern as hook_postgres: `hook` core stays free of the
  # `:amqp` dependency (and everything it pulls in — amqp_client, rabbit_common
  # NIFs) — this package exists only for deployments that opt into a RabbitMQ
  # sink.
  def project do
    [
      app: :hook_rabbitmq,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Hook.Sink.RabbitMQ.Application, []}
    ]
  end

  defp deps do
    [
      {:hook, path: ".."},
      {:amqp, "~> 4.0"}
    ]
  end
end
