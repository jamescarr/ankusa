defmodule Ankusa.Sink.Kafka do
  @moduledoc """
  Produces delivered hooks to a Kafka **topic** (any Kafka-API broker,
  e.g. Redpanda). The value is `Ankusa.Sink.Message`, byte-identical to what
  `Ankusa.Sink.RabbitMQ` publishes: inline up to `:inline_max_bytes`, a claim
  ticket above it.

  Each record:

    * **key** — `"tenant_id/source_id"` by default. It picks the partition, so
      it is the ordering scope: records with the same key are consumed in the
      order they were produced.
    * **headers** — `ankusa_id`, `ankusa_source_id`, `ankusa_tenant_id`,
      `ankusa_message_version`, `content_type` (`application/json`).
      Underscores, not hyphens, so they are usable unquoted as Redpanda
      Connect metadata and SQS attribute names.
    * **timestamp** — `env.received_at` (event time, not dispatch time).

  `deliver/3` returns `:ok` only after every in-sync replica has the record
  (`required_acks: -1`, not configurable). Anything else — unreachable broker,
  unknown topic, oversized message, timeout, failed claim check — returns
  `{:error, reason}` into the source's `Ankusa.RetryPolicy`, then the DLQ.

  brod's producer is not idempotent, so a produce retried after a lost ack
  can write the record twice. Delivery is at-least-once anyway; consumers
  dedupe on `id`.

  The sink **never creates topics**. Partition count decides which keys share
  a partition and can only grow, remapping keys when it does — that belongs
  to whoever operates the topic, not to the first hook that happens to be
  delivered. An unknown topic is an error, never an auto-created
  single-partition topic.

  Keys are hashed with brod's `:hash` partitioner (`erlang:phash2/1`), which
  is not the Java client's murmur2: the same key can land on a different
  partition than a Java producer would pick. That only matters if another
  producer writes the same topic or a consumer relies on co-partitioning.

  ## Process model

  The first `deliver/3` for an `{instance, :client}` pair starts a brod client
  under this package's `DynamicSupervisor`; brod handles reconnects, leader
  changes, and per-topic producers from there. brod requires the client id to
  be a registered atom, so it is `:"ankusa_kafka.<instance>.<client>"`: both
  parts are config atoms, so the atom count is bounded and two instances never
  share a client. The client's brokers and ssl/sasl settings are fixed by the
  first delivery that starts it; use a different `:client` for a different
  cluster.

  ## opts

    * `:brokers`            — required; `"host:port"` strings or `{host, port}` tuples
    * `:topic`              — required
    * `:key`                — a string, or `(Envelope.t() -> String.t())`;
                              default `"\#{tenant_id}/\#{source_id}"`
    * `:inline_max_bytes`   — default 64 KiB (65,536), configurable
    * `:produce_timeout_ms` — broker ack timeout and the longest `deliver/3`
                              waits for it; default `5_000`
    * `:client`             — atom naming the brod client; default `:default`
    * `:ssl`, `:sasl`       — passed to brod's client config when given
  """

  @behaviour Ankusa.Sink

  alias Ankusa.Envelope
  alias Ankusa.Sink.Message

  @impl true
  def deliver(%Envelope{} = env, ctx, opts) do
    topic = Keyword.fetch!(opts, :topic)
    timeout = Keyword.get(opts, :produce_timeout_ms, 5_000)
    client = client_id(ctx.instance, Keyword.get(opts, :client, :default))

    with :ok <- ensure_client(client, opts, timeout),
         {:ok, payload} <- Message.encode(env, ctx, Message.inline_max_bytes(opts)),
         key = key(env, opts),
         {:ok, partition} <- partition(client, topic, key),
         {:ok, call_ref} <- :brod.produce(client, topic, partition, key, record(env, payload)) do
      :brod.sync_produce_request(call_ref, timeout)
    end
  end

  # The record key *is* the ordering scope: Kafka delivers records with equal
  # keys in produced order. Saying so lets dispatch run different keys
  # concurrently instead of serializing the whole topic.
  @impl true
  def ordering_key(env, opts), do: key(env, opts)

  @impl true
  def inline_max_bytes(opts), do: Message.inline_max_bytes(opts)

  # Not `:brod.produce/5` with the `:hash` partitioner: that path looks the
  # partition count up with auto-creation allowed, regardless of the client's
  # `allow_topic_auto_creation: false`. `get_partitions_count_safe/2` never
  # creates the topic; the hash is the same one brod's `:hash` uses.
  defp partition(client, topic, key) do
    with {:ok, count} <- :brod_client.get_partitions_count_safe(client, topic) do
      {:ok, rem(:erlang.phash2(key), count)}
    end
  end

  defp record(env, payload) do
    %{
      ts: env.received_at,
      value: payload,
      headers: [
        {"ankusa_id", env.id},
        {"ankusa_source_id", env.source_id},
        {"ankusa_tenant_id", env.tenant_id || ""},
        {"ankusa_message_version", "1"},
        {"content_type", "application/json"}
      ]
    }
  end

  defp key(env, opts) do
    case Keyword.get(opts, :key, &default_key/1) do
      key when is_binary(key) -> key
      fun when is_function(fun, 1) -> fun.(env)
    end
  end

  defp default_key(env), do: "#{env.tenant_id}/#{env.source_id}"

  # ── client lifecycle ────────────────────────────────────────────────────

  defp client_id(instance, client), do: :"ankusa_kafka.#{instance}.#{client}"

  # `whereis` first: the common case must not serialize every delivery
  # through the DynamicSupervisor.
  defp ensure_client(client, opts, timeout) do
    if Process.whereis(client), do: :ok, else: start_client(client, opts, timeout)
  end

  defp start_client(client, opts, timeout) do
    endpoints = opts |> Keyword.fetch!(:brokers) |> Enum.map(&endpoint/1)

    config =
      [
        auto_start_producers: true,
        allow_topic_auto_creation: false,
        default_producer_config: [required_acks: -1, ack_timeout: timeout]
      ] ++ Keyword.take(opts, [:ssl, :sasl])

    # :temporary: brod's client exits during init when no broker answers.
    # Restarting it here would crash-loop past the supervisor's restart
    # intensity and take the application down; the next `deliver/3` starts
    # it again instead, inside the source's retry policy.
    child = %{
      id: client,
      start: {:brod_client, :start_link, [endpoints, client, config]},
      restart: :temporary
    }

    case DynamicSupervisor.start_child(Ankusa.Sink.Kafka.Supervisor, child) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp endpoint({host, port}), do: {to_charlist(host), port}

  defp endpoint(host_port) when is_binary(host_port) do
    [host, port] = String.split(host_port, ":", parts: 2)
    {to_charlist(host), String.to_integer(port)}
  end
end
