defmodule Ankusa.Sink.Kafka do
  @moduledoc """
  Publishes delivered hooks to a Kafka **topic**. Never creates the topic —
  partition count is a capacity contract that affects ordering, so topic
  creation belongs to infrastructure/bootstrap, not this sink.

  Messages are small on purpose. A body under `:inline_max_bytes` (default
  8 KiB) rides along base64-encoded; anything larger is checked in through
  `Ankusa.ClaimCheck` and the message carries a claim ticket instead. This is
  the same "small hot path, big payloads elsewhere" principle the WAL/segment
  design applies, now extended to Kafka.

  ## Message shape (JSON v1)

      {
        "v": 1,
        "id": "01a0...", "source_id": "stripe", "tenant_id": "acme",
        "received_at": 1737500000000, "content_type": "application/json", "size": 245,
        "body_base64": "eyJpZCI6...."           // inline, when size <= threshold
      }

      {
        "v": 1,
        "id": "01a0...", "source_id": "stripe", "tenant_id": "acme",
        "received_at": 1737500000000, "content_type": "application/octet-stream", "size": 524288,
        "claim": {"v": 1, "tenant_id": "acme", "id": "01a0...", "size": 524288,
                  "sha256": "d4e5f6...", "content_type": "application/octet-stream"}
      }

  This is the canonical message format shared across every queue-style sink
  (see `Ankusa.Sink.Message`). The `"v": 1` field is additive: consumers that
  ignore unknown keys keep working when the format gains new fields.

  ## Kafka record structure

  - **Key**: `"\#{tenant_id}/\#{source_id}"` by default (configurable via `:key`).
    This key picks the partition and defines the ordering scope.
  - **Value**: `Ankusa.Sink.Message.encode/3`, byte-identical to RabbitMQ.
  - **Headers**: `ankusa_id`, `ankusa_source_id`, `ankusa_tenant_id`,
    `ankusa_message_version`, `content_type`. Names use underscores (not hyphens)
    so they work unquoted in Bloblang, SQS attributes, and JS property access.
  - **Timestamp**: `env.received_at` (CreateTime = event time, not dispatch time).

  ## Delivery semantics

  - **acks**: `required_acks: -1` (all in-sync replicas). Fixed, not configurable.
    `deliver/3` returns `:ok` only after `:brod.produce_sync` confirms the broker
    has the record. This is Kafka's equivalent of RabbitMQ publisher confirms.
  - **Duplicates**: brod's regular producer doesn't use Kafka idempotence (no
    producer-id or sequence numbers). A retried produce after a lost ack can write
    the same message twice. That's acceptable: delivery is already at-least-once
    end to end, consumers dedupe on `id`, and most duplicates are absorbed by FIFO
    dedup at the channel.
  - **Errors**: every failure returns `{:error, reason}` into the existing
    `Ankusa.RetryPolicy`, and on give-up the DLQ takes over. This includes
    unreachable broker, timeout, unknown topic, message too large, and claim-check
    failure.
  - **Topic creation**: the sink **never creates topics**. An unknown topic fails
    loudly into retry then the DLQ. It is never silently auto-created with one
    partition. Topics are owned by infrastructure, with explicit partition count
    chosen for throughput and ordering.

  ## Ordering

  Order is preserved **per key, per dispatch node**. The dispatch pipeline is a
  single sequential poller, and inline retries block the batch, so a retried
  record never overtakes a later one from the same key.

  Order is **not** preserved:
  - after a DLQ replay;
  - across a fleet of dispatch nodes sharing a `WAL.Postgres`;
  - after the topic's partition count changes (remaps keys to different partitions).

  ## Process model and global atoms

  This deviates from "no global names." brod requires a locally registered atom
  as the client id. The sink confines it to `:"ankusa_kafka.\#{instance}.\#{client}"`.
  Both parts come from config (the instance atom plus the `:client` opt atom), so
  the number of atoms is bounded and two instances never collide.

  `Ankusa.Sink.Kafka.Application` starts a `DynamicSupervisor`. The first
  `deliver/3` call starts a brod client with `{:brod_client, :start_link, ...}`
  under that supervisor, treating `{:error, {:already_started, _}}` as success,
  then calls `:brod.start_producer/3` (also idempotent). brod's client handles
  reconnects, leader changes, and metadata refresh internally.

  ## opts

    * `:brokers`           — required; list of `"host:port"` strings or `{host, port}` tuples
    * `:topic`             — required; static topic name
    * `:key`               — a static binary, or a 1-arity fun `(Envelope.t() -> binary())`;
                              default `fn env -> "\#{env.tenant_id}/\#{env.source_id}" end`
    * `:inline_max_bytes`  — default `8_192`
    * `:client`            — atom naming the brod client; default `:default`;
                              one TCP connection per broker per client
    * `:ssl`               — passed through to brod: `false | true | [ssl_opts]`
    * `:sasl`              — passed through: `nil | {:plain | :scram_sha_256 | :scram_sha_512, user, pass}`
    * `:produce_timeout_ms` — default `5_000`
  """

  @behaviour Ankusa.Sink

  require Logger
  alias Ankusa.Envelope

  @impl true
  def deliver(%Envelope{} = env, ctx, opts) do
    brokers = Keyword.fetch!(opts, :brokers)
    topic = Keyword.fetch!(opts, :topic)
    client_name = Keyword.get(opts, :client, :default)

    with :ok <- ensure_started(ctx.instance, client_name, brokers, topic, opts),
         {:ok, payload} <-
           Ankusa.Sink.Message.encode(env, ctx, Keyword.get(opts, :inline_max_bytes, 8_192)) do
      produce(ctx.instance, client_name, topic, env, payload, opts)
    end
  end

  # ── connection lifecycle ────────────────────────────────────────────────

  defp ensure_started(instance, client_name, brokers, topic, opts) do
    client_id = :"ankusa_kafka.#{instance}.#{client_name}"
    endpoints = Enum.map(brokers, &parse_endpoint/1)

    client_config = [
      auto_start_producers: true,
      allow_topic_auto_creation: false,
      ssl: Keyword.get(opts, :ssl, false),
      sasl: Keyword.get(opts, :sasl, nil)
    ]

    child_spec = %{
      id: {__MODULE__, instance, client_name},
      start: {:brod_client, :start_link, [endpoints, client_id, client_config]},
      restart: :permanent
    }

    case DynamicSupervisor.start_child(Ankusa.Sink.Kafka.Supervisor, child_spec) do
      {:ok, _pid} ->
        start_producer(client_id, topic)

      {:error, {:already_started, _pid}} ->
        start_producer(client_id, topic)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_endpoint(endpoint) when is_binary(endpoint) do
    case String.split(endpoint, ":") do
      [host, port] -> {to_charlist(host), String.to_integer(port)}
      [host] -> {to_charlist(host), 9092}
    end
  end

  defp parse_endpoint({host, port}) when is_binary(host), do: {to_charlist(host), port}
  defp parse_endpoint({host, port}) when is_list(host), do: {host, port}

  defp start_producer(client_id, topic) do
    case :brod.start_producer(client_id, to_charlist(topic), []) do
      :ok -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  # ── message production ──────────────────────────────────────────────────

  defp produce(instance, client_name, topic, env, payload, opts) do
    client_id = :"ankusa_kafka.#{instance}.#{client_name}"
    partition = :hash
    key = record_key(env, opts)

    headers = [
      {"ankusa_id", env.id},
      {"ankusa_source_id", env.source_id},
      {"ankusa_tenant_id", env.tenant_id || ""},
      {"ankusa_message_version", "1"},
      {"content_type", env.content_type || "application/octet-stream"}
    ]

    produce_opts = [
      required_acks: -1,
      ack_timeout: Keyword.get(opts, :produce_timeout_ms, 5_000)
    ]

    case :brod.produce_sync(
           client_id,
           to_charlist(topic),
           partition,
           key,
           payload,
           headers,
           env.received_at,
           produce_opts
         ) do
      {:ok, _offset} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp record_key(env, opts) do
    case Keyword.get(opts, :key, &default_key/1) do
      key when is_binary(key) -> key
      fun when is_function(fun, 1) -> fun.(env)
    end
  end

  defp default_key(env), do: "#{env.tenant_id}/#{env.source_id}"
end
