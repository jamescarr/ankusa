defmodule AsyncApiSpex.Server do
  @moduledoc """
  A message broker or other server the application connects to.

  `host` is the host (and port) a client connects to; `protocol` names the
  protocol (`"kafka"`, `"amqp"`, `"nats"`, `"redis"`, …). `pathname` carries a
  path component such as a vhost, written with a leading slash (`"/production"`).
  """

  defstruct host: nil,
            protocol: nil,
            protocol_version: nil,
            pathname: nil,
            title: nil,
            summary: nil,
            description: nil,
            tags: nil,
            bindings: nil,
            extensions: %{}

  @type t :: %__MODULE__{
          host: String.t() | nil,
          protocol: String.t() | nil,
          protocol_version: String.t() | nil,
          pathname: String.t() | nil,
          title: String.t() | nil,
          summary: String.t() | nil,
          description: String.t() | nil,
          tags: [AsyncApiSpex.Tag.t()] | nil,
          bindings: map() | nil,
          extensions: map()
        }
end
