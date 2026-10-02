defmodule Ankusa.Sink.RabbitMQ do
  @moduledoc """
  Publishes delivered hooks to a RabbitMQ **exchange**. Never touches a queue
  — binding a queue to the exchange, and everything downstream of that, is
  the consumer's job, not this sink's. That mirrors real AMQP topology
  ownership: producers own exchanges, consumers own their own queues.

  Messages are small on purpose: the body rides inline up to
  `:inline_max_bytes` (default 64 KiB) and is checked in through
  `Ankusa.ClaimCheck` above that, so RabbitMQ throughput and memory stay flat
  regardless of payload size, and any consumer — BEAM or not — redeems the
  ticket without blob-store credentials of its own. The message is
  `Ankusa.Sink.Message`, byte-identical to what `Ankusa.Sink.Kafka` produces.

  ## opts

    * `:exchange`          — required
    * `:exchange_type`     — default `:topic`
    * `:url`                — AMQP URL, default `"amqp://guest:guest@localhost:5672"`
    * `:routing_key`       — a static string, or a 1-arity fun `(Envelope.t() -> String.t())`;
                              default `"ankusa.\#{source_id}"`
    * `:inline_max_bytes`  — default 64 KiB (65,536), configurable
    * `:retry_ms`          — reconnect backoff, default `5_000`
    * `:confirm_timeout_ms` — publisher-confirm wait, default `5_000`
  """

  @behaviour Ankusa.Sink

  alias Ankusa.Envelope
  alias Ankusa.Sink.Description
  alias Ankusa.Sink.Message
  alias Ankusa.Sink.RabbitMQ.Connection

  @impl true
  def deliver(%Envelope{} = env, ctx, opts) do
    exchange = Keyword.fetch!(opts, :exchange)
    inline_max_bytes = Message.inline_max_bytes(opts)

    with {:ok, name} <- ensure_started(ctx.instance, exchange, opts),
         {:ok, payload} <- Message.encode(env, ctx, inline_max_bytes) do
      Connection.publish(name, routing_key(env, opts), payload)
    end
  end

  @impl true
  def inline_max_bytes(opts), do: Message.inline_max_bytes(opts)

  @impl true
  def describe(subject, opts) do
    uri = URI.parse(Keyword.get(opts, :url, Connection.default_url()))
    vhost = vhost(uri)
    exchange = Keyword.fetch!(opts, :exchange)

    %Description{
      protocol: uri.scheme,
      host: "#{uri.host}:#{uri.port || 5672}",
      pathname: if(vhost == "/", do: nil, else: "/" <> vhost),
      address: address(subject, opts),
      channel_bindings: %{
        "amqp" => %{
          "is" => "routingKey",
          "exchange" => %{
            "name" => exchange,
            "type" => to_string(Keyword.get(opts, :exchange_type, :topic)),
            "durable" => true,
            "vhost" => vhost
          },
          "bindingVersion" => "0.3.0"
        }
      },
      ankusa_headers: false
    }
  end

  # The vhost is the URL path; the default vhost shows as the absent `"/"`.
  defp vhost(%URI{path: path}) when path in [nil, "", "/"], do: "/"
  defp vhost(%URI{path: path}), do: URI.decode(String.trim_leading(path, "/"))

  # Only a configured static routing key is a fixed address; a function
  # computes it per hook, so the document cannot name it.
  defp address(%{source_id: source_id}, opts) do
    case Keyword.get(opts, :routing_key) do
      key when is_binary(key) -> key
      fun when is_function(fun, 1) -> nil
      nil -> "ankusa.#{source_id}"
    end
  end

  # ── connection lifecycle ────────────────────────────────────────────────

  defp ensure_started(instance, exchange, opts) do
    name = Ankusa.via(instance, {:rabbitmq_conn, exchange})

    child = %{
      id: {Connection, instance, exchange},
      start: {Connection, :start_link, [Keyword.merge(opts, instance: instance, name: name)]}
    }

    case DynamicSupervisor.start_child(Ankusa.Sink.RabbitMQ.Supervisor, child) do
      {:ok, _pid} -> {:ok, name}
      {:error, {:already_started, _pid}} -> {:ok, name}
      {:error, reason} -> {:error, reason}
    end
  end

  defp routing_key(env, opts) do
    case Keyword.get(opts, :routing_key, &default_routing_key/1) do
      key when is_binary(key) -> key
      fun when is_function(fun, 1) -> fun.(env)
    end
  end

  defp default_routing_key(env), do: "ankusa.#{env.source_id}"
end
