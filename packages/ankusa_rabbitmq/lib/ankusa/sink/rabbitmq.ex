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

  `deliver/3` answers `:ok` only when the broker confirmed the message and at
  least one queue bound to the exchange accepted it: every publish is
  `mandatory`, so a message the exchange routes to no queue is
  `{:error, {:unroutable, routing_key}}`, retried by the source's
  `Ankusa.RetryPolicy` and then dead-lettered like any other sink failure.
  Every publish carries the hook's `id` as AMQP `message_id`, plus the
  `ankusa_idempotency_key` header (the key consumers dedupe on, the message
  JSON's `idempotency_key`) and the `ankusa_dedupe_key` and `ankusa_replay_id`
  headers when the hook carries them, so consumers dedupe on the same identity
  the message JSON exposes.
  Surviving a broker restart is the queue's property: messages are always
  published `persistent`, and durable classic and quorum queues persist them
  before confirming. The other errors are `{:error, :nacked}` (the broker
  refused it, e.g. a queue's `reject-publish` overflow),
  `{:error, :confirm_timeout}`, `{:error, {:channel_closed, reason}}` (the
  channel is reopened at once, re-declaring the exchange),
  `{:error, {:publish_failed, reason}}` and `{:error, :not_connected}`.

  ## opts

    * `:exchange`          — required
    * `:exchange_type`     — default `:topic`
    * `:url`                — AMQP URL, default `"amqp://guest:guest@localhost:5672"`
    * `:routing_key`       — a static string, or a 1-arity fun `(Envelope.t() -> String.t())`;
                              default `"ankusa.\#{source_id}"`
    * `:inline_max_bytes`  — default 64 KiB (65,536), configurable
    * `:retry_ms`          — reconnect backoff in milliseconds, default `5_000`
    * `:confirm_timeout_ms` — publisher-confirm wait in milliseconds, default `5_000`
    * `:idle_timeout_ms`   — close the connection after this long without a
                              delivery, default `600_000` (10 min); `0` never.
                              Never under the publish call's 15 s plus 1 s.
                              See `Ankusa.Sink.Reaper`
  """

  use Ankusa.Sink.Messaging

  alias Ankusa.Envelope
  alias Ankusa.Sink.{Connections, Description, Messaging}
  alias Ankusa.Sink.RabbitMQ.Connection
  alias Ankusa.Sink.Reaper

  @impl true
  def publish(payload, %Envelope{} = env, ctx, opts) do
    with {:ok, pid} <- connection(ctx.instance, Keyword.fetch!(opts, :exchange), opts) do
      routing_key =
        Messaging.resolve(Keyword.get(opts, :routing_key, &default_routing_key/1), env)

      Connection.publish(pid, routing_key, payload,
        message_id: env.id,
        headers: amqp_headers(env, ctx)
      )
    end
  end

  defp idle_ms(opts) do
    confirm = Keyword.get(opts, :confirm_timeout_ms, 0)
    Reaper.idle_ms(opts, max(confirm, Connection.call_timeout_ms()) + 1_000)
  end

  # `env.id` rides as `message_id`; the idempotency key always travels as an
  # AMQP header, the dedupe key and the replay marker only when present, so
  # consumers see the same identity as the message JSON.
  defp amqp_headers(env, ctx) do
    [{"ankusa_idempotency_key", :longstr, Envelope.idempotency_key(env)}]
    |> maybe_put_amqp("ankusa_dedupe_key", env.dedupe_key)
    |> maybe_put_amqp("ankusa_replay_id", ctx[:replay_id])
    |> maybe_put_amqp("ankusa_ordering_key", ctx[:ordering_key])
  end

  defp maybe_put_amqp(headers, _name, nil), do: headers

  defp maybe_put_amqp(headers, name, value) when is_binary(value),
    do: [{name, :longstr, value} | headers]

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
  defp address(%{source_id: source_id}, opts),
    do: Messaging.static(Keyword.get(opts, :routing_key, "ankusa.#{source_id}"))

  # ── connection lifecycle ────────────────────────────────────────────────

  # One connection per `{url, exchange}`: two sinks for one exchange on
  # different brokers never share one. The URL is in the key as a digest,
  # because a registered name is printed by crash reports and
  # `:sys.get_status/1` and the URL may carry a password. The start returns
  # before the connection dials, so a first publish may be `:not_connected`.
  defp connection(instance, exchange, opts) do
    url = Keyword.get(opts, :url, Connection.default_url())
    key = connection_key(url, exchange)

    Connections.ensure(
      __MODULE__,
      {instance, key},
      fn -> Ankusa.whereis(instance, key) end,
      fn -> connection_spec(instance, key, exchange, url, opts) end,
      idle_ms(opts)
    )
  end

  # The URL rides only inside a closure (`url: fn -> url end`), which `inspect`
  # prints without its captured values: the child spec is in the supervisor's
  # status and the URL may carry a password.
  defp connection_spec(instance, key, exchange, url, opts) do
    conn_opts =
      [
        instance: instance,
        name: Ankusa.via(instance, key),
        exchange: exchange,
        url: fn -> url end
      ] ++
        Keyword.take(opts, [:exchange_type, :retry_ms, :confirm_timeout_ms, :max_inflight])

    %{id: {Connection, instance, key}, start: {Connection, :start_link, [conn_opts]}}
  end

  @doc false
  # The registry key of the connection for `url` and `exchange`.
  @spec connection_key(String.t(), String.t()) :: {:rabbitmq_conn, {binary(), String.t()}}
  def connection_key(url, exchange),
    do: {:rabbitmq_conn, {binary_part(:crypto.hash(:sha256, url), 0, 16), exchange}}

  defp default_routing_key(env), do: "ankusa.#{env.source_id}"
end
