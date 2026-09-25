defmodule Ankusa.TestSink do
  @moduledoc """
  A sink that reports each delivery to a test process, so a test can assert what
  dispatch actually delivered — and, more often, what it did *not* deliver.

  `opts` takes `:pid` (the process to send to, defaulting to the registered
  `:test_sink`) and `:tag` (default `:delivered`). Each delivery sends
  `{tag, envelope.id}`, so the same sink can be used for two sources with
  different tags.
  """

  @behaviour Ankusa.Sink

  @impl true
  def deliver(env, _ctx, opts) do
    send(Keyword.get(opts, :pid, :test_sink), {Keyword.get(opts, :tag, :delivered), env.id})
    :ok
  end

  @impl true
  def ordering_key(_env, _opts), do: nil

  @impl true
  def inline_max_bytes(_opts), do: nil
end
