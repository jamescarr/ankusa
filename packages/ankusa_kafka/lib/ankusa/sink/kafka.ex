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
      `ankusa_message_version`, `content_type` (`application/json`) and
      `ankusa_idempotency_key` (the key consumers dedupe on, the message's
      `idempotency_key`), plus `ankusa_dedupe_key` and `ankusa_replay_id` when
      the hook carries them.
      Underscores, not hyphens, so they are usable unquoted as Redpanda
      Connect metadata and SQS attribute names.
    * **timestamp** — `env.received_at` (event time, not dispatch time).

  `deliver/3` returns `:ok` only after every in-sync replica has the record
  (`required_acks: -1`, not configurable). A record the broker will never take
  is `{:error, {:permanent, reason}}`, dead-lettered at once (see "Error
  classes" in `Ankusa.Sink`): a value plus key over `:max_record_bytes` is
  refused before it is produced (a body that cannot fit at all; a message whose
  inline base64 would not fit is checked in instead, see
  `Ankusa.Sink.Message`), and a produce the broker answers with
  `message_too_large`, `invalid_message` or `invalid_record` is permanent too.
  Anything else — unreachable broker, unknown topic, timeout, failed claim
  check — returns `{:error, reason}` into the source's `Ankusa.RetryPolicy`,
  then the DLQ.

  A produce that times out may still land: brod cannot cancel one in flight,
  and its producer is not idempotent, so a produce retried after a lost ack
  can write the record twice. Delivery is at-least-once anyway; consumers
  dedupe on the `idempotency_key`.

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
    * `:max_record_bytes`   — the largest value + key produced; default
                              `1_000_000` (the broker's `message.max.bytes`
                              default, 1 MiB, less batch overhead). Raise it
                              with the topic's `max.message.bytes`
    * `:produce_timeout_ms` — broker ack timeout and the longest `deliver/3`
                              waits for it; default `5_000`
    * `:client`             — atom naming the brod client; default `:default`
    * `:idle_timeout_ms`    — stop the brod client after this long without a
                              delivery through it, default `600_000` (10 min);
                              `0` never. Never under `:produce_timeout_ms` plus
                              1 s. See `Ankusa.Sink.Reaper`
    * `:ssl`, `:sasl`       — passed to brod's client config when given
  """

  use Ankusa.Sink.Messaging

  alias Ankusa.Envelope
  alias Ankusa.Sink.{Connections, Description, Message, Messaging}
  alias Ankusa.Sink.Reaper

  @default_max_record_bytes 1_000_000

  # Produce error codes no retry can fix.
  @permanent_codes [:message_too_large, :invalid_message, :invalid_record]

  # The message ceiling is the record's: `Message.encode/4` checks the value
  # against it before `publish/4`, which then adds the key.
  @impl Ankusa.Sink.Messaging
  def max_message_bytes(opts), do: Keyword.get(opts, :max_record_bytes, @default_max_record_bytes)

  @impl true
  def publish(payload, %Envelope{} = env, ctx, opts) do
    topic = Keyword.fetch!(opts, :topic)
    timeout = Keyword.get(opts, :produce_timeout_ms, 5_000)
    client = client_id(ctx.instance, Keyword.get(opts, :client, :default))
    key = Messaging.resolve(Keyword.get(opts, :key, &default_key/1), env)

    with :ok <- Message.fits(byte_size(payload) + byte_size(key || ""), max_message_bytes(opts)),
         {:ok, _pid} <- client(client, opts, timeout),
         {:ok, partition} <- partition(client, topic, key),
         {:ok, call_ref} <-
           :brod.produce(client, topic, partition, key, record(env, ctx, payload)) do
      case :brod.sync_produce_request(call_ref, timeout) do
        :ok -> :ok
        {:error, reason} -> {:error, classify(reason)}
      end
    end
  end

  # A produce waits up to `timeout` for its ack; the client is never stopped
  # under one.
  defp idle_ms(opts, timeout), do: Reaper.idle_ms(opts, timeout + 1_000)

  # brod reports a broker's error code as an atom somewhere inside the reason
  # (`{:producer_down, {:not_retriable, {_, :message_too_large}}}` and the like).
  defp classify(reason) do
    if permanent?(reason), do: {:permanent, reason}, else: reason
  end

  defp permanent?(code) when code in @permanent_codes, do: true
  defp permanent?(tuple) when is_tuple(tuple), do: tuple |> Tuple.to_list() |> permanent?()
  defp permanent?(list) when is_list(list), do: Enum.any?(list, &permanent?/1)
  defp permanent?(_other), do: false

  @impl true
  def describe(subject, opts) do
    topic = Keyword.fetch!(opts, :topic)

    %Description{
      protocol: "kafka",
      host:
        opts
        |> Keyword.fetch!(:brokers)
        |> Messaging.endpoints()
        |> Enum.map_join(",", fn {host, port} -> "#{host}:#{port}" end),
      address: topic,
      channel_bindings: %{"kafka" => %{"topic" => topic, "bindingVersion" => "0.5.0"}},
      message_bindings: %{
        "kafka" => %{"key" => key_schema(subject, opts), "bindingVersion" => "0.5.0"}
      },
      ankusa_headers: true
    }
  end

  # `subject.tenant_id` is nil when the tenant varies per hook, so the default
  # key is only a single constant when the tenant is known up front.
  defp key_schema(subject, opts) do
    case Keyword.get(opts, :key) do
      key when is_binary(key) ->
        %{"type" => "string", "const" => key}

      fun when is_function(fun, 1) ->
        %{"type" => "string", "description" => "Computed per hook by a configured function."}

      nil ->
        default_key_schema(subject)
    end
  end

  defp default_key_schema(%{tenant_id: tenant, source_id: source_id}) when is_binary(tenant) do
    %{"type" => "string", "const" => "#{tenant}/#{source_id}"}
  end

  defp default_key_schema(%{source_id: source_id}) do
    %{
      "type" => "string",
      "pattern" => "^[A-Za-z0-9_-]{1,64}/" <> Regex.escape(source_id) <> "$"
    }
  end

  # Not `:brod.produce/5` with the `:hash` partitioner: that path looks the
  # partition count up with auto-creation allowed, regardless of the client's
  # `allow_topic_auto_creation: false`. `get_partitions_count_safe/2` never
  # creates the topic; the hash is the same one brod's `:hash` uses.
  defp partition(client, topic, key) do
    with {:ok, count} <- :brod_client.get_partitions_count_safe(client, topic) do
      {:ok, rem(:erlang.phash2(key), count)}
    end
  end

  defp record(env, ctx, payload),
    do: %{ts: env.received_at, value: payload, headers: Message.headers(env, ctx)}

  defp default_key(env), do: "#{env.tenant_id}/#{env.source_id}"

  # ── client lifecycle ────────────────────────────────────────────────────

  defp client_id(instance, client), do: :"ankusa_kafka.#{instance}.#{client}"

  # Registered globally, as brod requires; the client id is also the reaper's
  # key, and it names no broker or credential.
  defp client(client, opts, timeout) do
    Connections.ensure(
      __MODULE__,
      client,
      fn -> Process.whereis(client) end,
      fn -> client_spec(client, opts, timeout) end,
      idle_ms(opts, timeout)
    )
  end

  defp client_spec(client, opts, timeout) do
    endpoints =
      opts
      |> Keyword.fetch!(:brokers)
      |> Messaging.endpoints()
      |> Enum.map(fn {host, port} -> {to_charlist(host), port} end)

    config =
      [
        auto_start_producers: true,
        allow_topic_auto_creation: false,
        default_producer_config: [required_acks: -1, ack_timeout: timeout]
      ] ++ Keyword.take(opts, [:ssl, :sasl])

    # brod_sup hides a SASL password in a closure (brod_utils:init_sasl_opt/1) before
    # starting a client; this starts brod_client directly, so it does the same, or the
    # password sits in the client's state and every crash report of it.
    config = :brod_utils.init_sasl_opt(config)

    # :temporary: brod's client exits during init when no broker answers.
    # Restarting it here would crash-loop past the supervisor's restart
    # intensity and take the application down; the next `deliver/3` starts
    # it again instead, inside the source's retry policy.
    %{
      id: client,
      start: {:brod_client, :start_link, [endpoints, client, config]},
      restart: :temporary
    }
  end
end
