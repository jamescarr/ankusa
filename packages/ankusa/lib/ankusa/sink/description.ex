defmodule Ankusa.Sink.Description do
  @moduledoc """
  Where a messaging sink publishes, as `c:Ankusa.Sink.describe/2` reports it:
  the broker, the channel address, and the protocol bindings a consumer needs.

  `Ankusa.AsyncApi` folds these into the AsyncAPI document the admin API serves.

    * `:protocol` / `:host` — the broker (`"kafka"`, `"amqp"`, `"nats"`, `"redis"`
      and `"host:port"`, comma-joined when the sink has several). Never carries
      userinfo: a description is served to anyone who can reach the admin port.
    * `:pathname` — the broker's path (a RabbitMQ vhost as `"/prod"`, a Redis
      database as `"/2"`), or `nil`.
    * `:address` — the topic, routing key, subject, or channel; `nil` when a
      configured function computes it per hook.
    * `:ankusa_headers` — true when each message also carries the five
      `ankusa_*` / `content_type` application headers.
    * `:channel_bindings` / `:message_bindings` — AsyncAPI binding objects keyed
      by protocol (`%{"kafka" => %{...}}`), `%{}` when the protocol defines none.
  """

  @enforce_keys [:protocol, :host]
  defstruct [
    :protocol,
    :host,
    :address,
    pathname: nil,
    ankusa_headers: false,
    channel_bindings: %{},
    message_bindings: %{}
  ]

  @type t :: %__MODULE__{
          protocol: String.t(),
          host: String.t(),
          address: String.t() | nil,
          pathname: String.t() | nil,
          ankusa_headers: boolean(),
          channel_bindings: map(),
          message_bindings: map()
        }
end
