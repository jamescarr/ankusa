defmodule Hook.Sink do
  @moduledoc """
  What happens to a delivered hook. The interesting extension point: it lets the
  framework be a library inside a Phoenix app, a standalone forwarding gateway,
  or the front door to someone else's pipeline — with the same ingest guarantees.

  Delivery is at-least-once. Return `:ok` on success; `{:error, reason}` triggers
  the source's `Hook.RetryPolicy`.
  """

  alias Hook.Envelope

  @type ctx :: %{
          required(:instance) => atom(),
          required(:source_id) => String.t(),
          required(:tenant_id) => String.t() | nil,
          required(:attempt) => pos_integer(),
          optional(atom()) => term()
        }

  @callback deliver(Envelope.t(), ctx(), opts :: keyword()) :: :ok | {:error, term()}
end
