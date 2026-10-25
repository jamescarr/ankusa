defmodule Ankusa.Sink.Description do
  @moduledoc """
  Where a sink delivers, as `c:Ankusa.Sink.describe/2` reports it: the broker,
  the channel address, and the protocol bindings a consumer needs.

  `Ankusa.AsyncApi` folds the ones that publish the envelope (`:payload` is
  `:message`) into the AsyncAPI document the admin API serves.

    * `:protocol` / `:host` — the broker (`"kafka"`, `"amqp"`, `"nats"`, `"redis"`, `"sqs"`
      and `"host:port"`, comma-joined when the sink has several). Never carries
      userinfo: a description is served to anyone who can reach the admin port.
      `:host` is `nil` when a function computes the destination per hook.
    * `:pathname` — the broker's path (a RabbitMQ vhost as `"/prod"`, a Redis
      database as `"/2"`), or `nil`.
    * `:address` — the topic, routing key, subject, or channel; `nil` when a
      configured function computes it per hook.
    * `:ankusa_headers` — true when each message also carries the `ankusa_*` /
      `content_type` application headers (Kafka and NATS headers, SQS and Pub/Sub
      message attributes); the AsyncAPI document then references
      `SinkMessageHeadersV1` for the message.
    * `:channel_bindings` / `:message_bindings` — AsyncAPI binding objects keyed
      by protocol (`%{"kafka" => %{...}}`), `%{}` when the protocol defines none.
    * `:payload` — what the sink sends: `:message` (the `Ankusa.Sink.Message`
      envelope, which the AsyncAPI document describes) or `:body` (the provider's
      raw bytes, as `Ankusa.Sink.Http` does, which it does not).
    * `:computed` — the options a function computes per hook (`:url`, `:headers`
      and `:secret` of `Ankusa.Sink.Http`), so this description holds for the
      sink's configuration but not for any one hook; `[]` when every option is
      static.
  """

  @enforce_keys [:protocol, :host]
  defstruct [
    :protocol,
    :host,
    :address,
    pathname: nil,
    ankusa_headers: false,
    channel_bindings: %{},
    message_bindings: %{},
    payload: :message,
    computed: []
  ]

  @type t :: %__MODULE__{
          protocol: String.t(),
          host: String.t() | nil,
          address: String.t() | nil,
          pathname: String.t() | nil,
          ankusa_headers: boolean(),
          channel_bindings: map(),
          message_bindings: map(),
          payload: :message | :body,
          computed: [atom()]
        }
end
