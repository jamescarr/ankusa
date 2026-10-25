defmodule Ankusa.Sink.RabbitMQ.Application do
  @moduledoc """
  Starts the connection tree (`Ankusa.Sink.Connections`) for the
  `Ankusa.Sink.RabbitMQ.Connection`s `Ankusa.Sink.RabbitMQ` opens on demand
  from `publish/4`, one per instance and `{url, exchange}`.

  Self-contained: `ankusa` core has no idea this package exists, no changes to
  `Ankusa.Instance` were needed. Connections register through the same
  `Ankusa.Registry` every other instance-scoped process in the framework uses
  (`Ankusa.via/2`), so they're crash-isolated from the dispatch pipeline that
  calls them — a lost RabbitMQ connection never takes down ingest or dispatch,
  it just makes `deliver/3` return `{:error, :not_connected}`, which the
  source's `Ankusa.RetryPolicy` already handles.
  """

  use Application

  @impl true
  def start(_type, _args) do
    # amqp_client encrypts every connection's password with credentials_obfuscation,
    # but only once a secret is set; until then it keeps {:plaintext, password} in its
    # processes' state. Set a random one unless the host application already set its own.
    if :credentials_obfuscation.secret() == :"$pending-secret" do
      :ok = :credentials_obfuscation.set_secret(:crypto.strong_rand_bytes(32))
    end

    Ankusa.Sink.Connections.start_link(Ankusa.Sink.RabbitMQ)
  end
end
