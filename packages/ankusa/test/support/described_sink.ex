defmodule Ankusa.Test.DescribedSink do
  @moduledoc """
  A sink that advertises a channel, for the AsyncAPI document's tests: core has
  no messaging sink of its own (those live in the adapter packages).

  Options: `:address` (default `nil`, a computed address), `:host` (default
  `"broker:9092"`), `:protocol` (default `"kafka"`), `:pathname`, `:headers`
  (the `ankusa_headers` flag), `:channel_bindings`, `:message_bindings`. Any
  other option, a credential say, is never described.
  """

  @behaviour Ankusa.Sink

  alias Ankusa.Sink.Description

  @impl true
  def deliver(_env, _ctx, _opts), do: :ok

  @impl true
  def describe(_subject, opts) do
    %Description{
      protocol: Keyword.get(opts, :protocol, "kafka"),
      host: Keyword.get(opts, :host, "broker:9092"),
      pathname: Keyword.get(opts, :pathname),
      address: Keyword.get(opts, :address),
      ankusa_headers: Keyword.get(opts, :headers, false),
      channel_bindings: Keyword.get(opts, :channel_bindings, %{}),
      message_bindings: Keyword.get(opts, :message_bindings, %{})
    }
  end
end
