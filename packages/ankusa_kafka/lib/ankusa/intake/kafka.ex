defmodule Ankusa.Intake.Kafka do
  @moduledoc """
  The Kafka adapter of `wal: :dual` (`Ankusa.Intake`, ADR 0002): every acked hook
  is also a record in a replicated Kafka topic, so a lost host loses nothing.

  "Confirmed" means a produce sent with `required_acks: -1` and
  `ack_timeout: intake.publish_timeout_ms` that brod reports as acked: every
  in-sync replica has the batch, and the in-sync set was at least the topic's
  `min.insync.replicas` (the broker, not this client, refuses with
  `not_enough_replicas` when it is smaller).

  The adapter has a brod client of its own, `:"ankusa_kafka.<instance>.intake"`,
  never the sink's. Its producer is configured for one attempt per call:

    * `partition_onwire_limit: 1` — one produce on the wire per partition, so
      seq order is log order
    * `max_retries: 0` — the reconciler owns every retry, which keeps the
      attempt deadline real
    * `allow_topic_auto_creation: false` — the topic is the operator's

  Every record goes to one **explicit partition**, the node's stamped
  `m:intake_partition`: `phash2(node_id) rem partition_count`, chosen at the
  node's first dual boot and never recomputed, so growing the topic never remaps
  a node. The record key is the node id.

  brod has no idempotent producer: a produce whose ack is lost and which the
  reconciler then retries can land twice, and a record may land behind newer
  ones. Every reader applies a record through the seq-and-id rule of
  `Ankusa.Intake.Recovery`, so a duplicate or reordered record changes nothing.

  ## Record

  The value is the `hooks` column family value (`Ankusa.Envelope.to_binary/2`).
  Headers: `ankusa_intake_v` (`"1"`), `ankusa_intake_kind` (`"hook"`),
  `ankusa_intake_node`, `ankusa_intake_seq`, `ankusa_intake_id`,
  `ankusa_intake_incarnation`, `ankusa_intake_tenant`, `ankusa_intake_source`.

  ## Errors

  A broker error code, wherever brod nests it, is one of the four classes of
  `Ankusa.Intake`: `unknown_topic_or_partition` is `:not_found`;
  `message_too_large`, `invalid_message`, `invalid_record` and
  `topic_authorization_failed` are `:permanent`; `throttling_quota_exceeded` is
  `:throttled`; everything else (`not_enough_replicas`,
  `not_enough_replicas_after_append` — the record *was* appended, so the
  republish is what resolves it —, `leader_not_available`, `request_timed_out`,
  a missed deadline, a client that is down) is `:unavailable`. brod does not
  surface a produce response's `throttle_time_ms`, so a quota that only delays
  shows as latency.

  ## `verify/1`

  Boot refuses a topic the promise does not hold on (DescribeConfigs and
  metadata, through `kafka_protocol`, with a connection of its own so it works
  before the client is up): replication factor at least 3,
  `min.insync.replicas` at least 2, `unclean.leader.election.enable` false,
  `retention.ms` at least twice `backup.max_age_ms` (or infinite) and
  `max.message.bytes` at least `intake.max_record_bytes`.

  ## Options (`intake.kafka`)

    * `:brokers` — required, `"host:port"` strings
    * `:ssl`, `:sasl` — as `Ankusa.Sink.Kafka`
    * `:health_interval_ms` — how often `ready?/1` is refreshed, default `1_000`
  """

  @behaviour Ankusa.Intake

  use GenServer

  require Logger
  require Record

  alias Ankusa.Intake

  Record.defrecordp(
    :kafka_message,
    Record.extract(:kafka_message, from_lib: "kafka_protocol/include/kpro_public.hrl")
  )

  Record.defrecordp(
    :kpro_rsp,
    Record.extract(:kpro_rsp, from_lib: "kafka_protocol/include/kpro.hrl")
  )

  @topic_resource 2
  @reconnect_ms 1_000
  @health_timeout_ms 3_000
  @fetch_bytes 8 * 1_048_576
  @empty_fetches 3

  @permanent [:message_too_large, :invalid_message, :invalid_record, :topic_authorization_failed]
  @not_found [:unknown_topic_or_partition]
  @throttled [:throttling_quota_exceeded]

  # ── adapter ───────────────────────────────────────────────────────────────

  @impl Ankusa.Intake
  def child_spec(opts) do
    instance = Keyword.fetch!(opts, :instance)
    %{id: {__MODULE__, instance}, start: {__MODULE__, :start_link, [opts]}}
  end

  def start_link(opts) do
    instance = Keyword.fetch!(opts, :instance)
    GenServer.start_link(__MODULE__, opts, name: Ankusa.via(instance, :intake_kafka))
  end

  @impl Ankusa.Intake
  def publish_batch(instance, records, opts) do
    with {:ok, s} <- settings(instance) do
      case records do
        [] -> head(s, opts)
        _ -> produce_batch(s, records, opts)
      end
    end
  end

  @impl Ankusa.Intake
  def replay(instance, from, fun, opts) do
    node = Keyword.fetch!(opts, :node)

    with {:ok, s} <- settings(instance),
         {:ok, partitions} <- partitions(s, Keyword.get(opts, :partition)) do
      Enum.reduce_while(partitions, {:ok, nil}, fn partition, {:ok, last} ->
        case replay_partition(s, instance, partition, from, node, fun) do
          {:ok, position} -> {:cont, {:ok, max_position(last, position)}}
          {:error, class} -> {:halt, {:error, class}}
        end
      end)
    end
  end

  @impl Ankusa.Intake
  def ready?(instance) do
    case :ets.lookup(table(instance), :ready) do
      [{:ready, ready?}] -> ready?
      [] -> false
    end
  rescue
    ArgumentError -> false
  end

  @impl Ankusa.Intake
  def verify(instance) do
    with {:ok, s} <- settings(instance),
         {:ok, topic} <- topic_metadata(s),
         {:ok, configs} <- describe_configs(s) do
      check_topic(instance, s, topic, configs)
    else
      {:error, class} when is_atom(class) -> {:error, {:intake_topic, class}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl Ankusa.Intake
  def describe(instance) do
    case :ets.lookup(table(instance), :describe) do
      [{:describe, text}] -> text
      [] -> "acks=all, topic settings not read yet"
    end
  rescue
    ArgumentError -> "acks=all, topic settings not read yet"
  end

  @impl Ankusa.Intake
  def earliest(instance) do
    with {:ok, s} <- settings(instance),
         {:ok, partitions} <- partitions(s, Intake.Pending.partition(instance)) do
      Enum.reduce_while(partitions, {:ok, nil}, fn partition, {:ok, best} ->
        case earliest_of(s, partition) do
          {:ok, nil} -> {:cont, {:ok, best}}
          {:ok, position} -> {:cont, {:ok, min_position(best, position)}}
          {:error, class} -> {:halt, {:error, class}}
        end
      end)
    end
  end

  @impl Ankusa.Intake
  def assign_partition(instance, node_id) do
    with {:ok, s} <- settings(instance),
         {:ok, topic} <- topic_metadata(s),
         count when count > 0 <- length(topic.partitions) do
      {:ok, partition_for(node_id, count)}
    else
      _ -> :none
    end
  end

  @doc """
  The class a broker or client failure belongs to (`t:Ankusa.Intake.class/0`).
  brod nests an error code anywhere in its reason; the first class that matches,
  most specific first, wins.
  """
  @spec classify(term()) :: Intake.class()
  def classify(reason) do
    atoms = atoms(reason, [])

    cond do
      Enum.any?(atoms, &(&1 in @permanent)) -> :permanent
      Enum.any?(atoms, &(&1 in @not_found)) -> :not_found
      Enum.any?(atoms, &(&1 in @throttled)) -> :throttled
      true -> :unavailable
    end
  end

  @doc "The partition a node id maps to in a topic of `count` partitions (the same hash as `Ankusa.Sink.Kafka`)."
  @spec partition_for(String.t(), pos_integer()) :: non_neg_integer()
  def partition_for(node_id, count), do: rem(:erlang.phash2(node_id), count)

  @doc "The brod client of an instance's intake."
  @spec client(atom()) :: atom()
  def client(instance), do: :"ankusa_kafka.#{instance}.intake"

  # ── produce ───────────────────────────────────────────────────────────────

  defp produce_batch(s, records, opts) do
    timeout = Keyword.get(opts, :timeout, s.publish_timeout_ms)
    node = Keyword.get(opts, :node, s.node)

    with {:ok, partition} <- produce_partition(s, Keyword.get(opts, :partition), node),
         {:ok, producer} <- guard(fn -> :brod.get_producer(s.client, s.topic, partition) end),
         {:ok, ref} <-
           guard(fn -> :brod.produce(producer, node, Enum.map(records, &message(&1, node))) end),
         {:ok, offset} <- guard(fn -> :brod.sync_produce_request_offset(ref, timeout) end) do
      {:ok, %{position: offset + length(records) - 1}}
    else
      {:error, reason} -> {:error, classify(reason)}
    end
  end

  # An empty batch has no offset of its own: the head of the partition stands in.
  defp head(s, opts) do
    with {:ok, partition} <- produce_partition(s, Keyword.get(opts, :partition), s.node),
         {:ok, latest} <-
           guard(fn -> :brod.resolve_offset(s.endpoints, s.topic, partition, :latest, s.conn) end) do
      {:ok, %{position: latest - 1}}
    else
      {:error, reason} -> {:error, classify(reason)}
    end
  end

  defp produce_partition(_s, partition, _node) when is_integer(partition), do: {:ok, partition}

  defp produce_partition(s, nil, node) do
    with {:ok, count} <- :brod_client.get_partitions_count_safe(s.client, s.topic) do
      {:ok, partition_for(node, count)}
    end
  end

  defp message({seq, bin, meta}, node) do
    %{
      key: node,
      value: bin,
      headers: [
        {"ankusa_intake_v", "1"},
        {"ankusa_intake_kind", "hook"},
        {"ankusa_intake_node", node},
        {"ankusa_intake_seq", Integer.to_string(seq)},
        {"ankusa_intake_id", meta.id},
        {"ankusa_intake_incarnation", meta.incarnation},
        {"ankusa_intake_tenant", meta.tenant},
        {"ankusa_intake_source", meta.source}
      ]
    }
  end

  # ── replay ────────────────────────────────────────────────────────────────

  defp partitions(_s, partition) when is_integer(partition), do: {:ok, [partition]}

  defp partitions(s, nil) do
    with {:ok, topic} <- topic_metadata(s) do
      {:ok, Enum.to_list(0..(length(topic.partitions) - 1)//1)}
    end
  end

  defp replay_partition(s, instance, partition, from, node, fun) do
    with {:ok, start} <- start_offset(s, partition, from),
         {:ok, latest} <- resolve(s, partition, :latest) do
      replay_from(s, instance, partition, start, latest, node, fun, nil, 0)
    end
  end

  defp start_offset(s, partition, :earliest), do: resolve(s, partition, :earliest)
  defp start_offset(_s, _partition, position) when is_integer(position), do: {:ok, position}

  defp replay_from(_s, _instance, _partition, cursor, latest, _node, _fun, last, _empty)
       when cursor >= latest,
       do: {:ok, last}

  defp replay_from(s, instance, partition, cursor, latest, node, fun, last, empty) do
    fetch_opts = %{max_wait_time: 1_000, min_bytes: 1, max_bytes: @fetch_bytes}

    case guard(fn ->
           :brod.fetch({s.endpoints, s.conn}, s.topic, partition, cursor, fetch_opts)
         end) do
      {:ok, {_high_watermark, []}} when empty + 1 >= @empty_fetches ->
        {:error, :unavailable}

      {:ok, {_high_watermark, []}} ->
        replay_from(s, instance, partition, cursor, latest, node, fun, last, empty + 1)

      {:ok, {_high_watermark, messages}} ->
        messages = Enum.filter(messages, &(kafka_message(&1, :offset) >= cursor))

        case apply_messages(messages, instance, node, fun, last) do
          {:ok, last} ->
            next = messages |> List.last() |> kafka_message(:offset) |> Kernel.+(1)
            replay_from(s, instance, partition, max(next, cursor + 1), latest, node, fun, last, 0)

          {:error, class} ->
            {:error, class}
        end

      {:error, reason} ->
        {:error, classify(reason)}
    end
  end

  defp apply_messages(messages, instance, node, fun, last) do
    Enum.reduce_while(messages, {:ok, last}, fn message, {:ok, last} ->
      headers = Map.new(kafka_message(message, :headers))

      cond do
        headers["ankusa_intake_node"] != node ->
          {:cont, {:ok, last}}

        not known?(headers) ->
          Intake.skipped(instance)
          {:cont, {:ok, last}}

        true ->
          position = kafka_message(message, :offset)

          replayed = %{
            position: position,
            node: node,
            seq: String.to_integer(headers["ankusa_intake_seq"]),
            id: headers["ankusa_intake_id"],
            kind: headers["ankusa_intake_kind"],
            version: headers["ankusa_intake_v"],
            bin: kafka_message(message, :value)
          }

          case fun.(replayed) do
            :ok -> {:cont, {:ok, position}}
            {:error, _reason} -> {:halt, {:error, :unavailable}}
          end
      end
    end)
  end

  # A record this release cannot read: another kind or version, or one whose seq
  # or hook id is missing or not a number.
  defp known?(%{
         "ankusa_intake_kind" => kind,
         "ankusa_intake_v" => version,
         "ankusa_intake_id" => id,
         "ankusa_intake_seq" => seq
       })
       when is_binary(id) and is_binary(seq) do
    Intake.known?(kind, version) and match?({_, ""}, Integer.parse(seq))
  end

  defp known?(_headers), do: false

  defp earliest_of(s, partition) do
    with {:ok, first} <- resolve(s, partition, :earliest),
         {:ok, latest} <- resolve(s, partition, :latest) do
      {:ok, if(first >= latest, do: nil, else: first)}
    end
  end

  # brod throws `{:failed_to_connect, _}` when no broker answers and exits when a
  # producer or client dies mid-call: the adapter returns values (rule 2).
  defp guard(fun) do
    fun.()
  catch
    :throw, reason -> {:error, reason}
    :exit, reason -> {:error, reason}
  end

  defp resolve(s, partition, time) do
    case guard(fn -> :brod.resolve_offset(s.endpoints, s.topic, partition, time, s.conn) end) do
      {:ok, offset} -> {:ok, offset}
      {:error, reason} -> {:error, classify(reason)}
    end
  end

  defp max_position(nil, position), do: position
  defp max_position(last, nil), do: last
  defp max_position(last, position), do: max(last, position)

  defp min_position(nil, position), do: position
  defp min_position(best, position), do: min(best, position)

  # ── verify ────────────────────────────────────────────────────────────────

  defp topic_metadata(s) do
    case guard(fn -> :brod.get_metadata(s.endpoints, [s.topic], s.conn) end) do
      {:ok, metadata} -> find_topic(metadata, s.topic)
      {:error, reason} -> {:error, classify(reason)}
    end
  end

  defp find_topic(metadata, name) do
    topic = Enum.find(:kpro.find(:topics, metadata), &(:kpro.find(:name, &1) == name))

    case topic && :kpro.find(:error_code, topic) do
      nil -> {:error, :not_found}
      :no_error -> {:ok, topic_with_partitions(topic)}
      code -> {:error, classify(code)}
    end
  end

  # The partition list's field names differ between metadata versions.
  defp topic_with_partitions(topic) do
    partitions =
      Enum.map(:kpro.find(:partitions, topic), fn partition ->
        %{
          index: kpro_find(partition, [:partition_index, :partition]),
          leader: kpro_find(partition, [:leader_id, :leader]),
          replicas: kpro_find(partition, [:replica_nodes, :replicas]) || []
        }
      end)

    %{partitions: partitions}
  end

  defp kpro_find(struct, [name | rest]) do
    case :kpro.find(name, struct, :__missing__) do
      :__missing__ -> kpro_find(struct, rest)
      value -> value
    end
  end

  defp kpro_find(_struct, []), do: nil

  defp describe_configs(s) do
    names = [
      "min.insync.replicas",
      "unclean.leader.election.enable",
      "retention.ms",
      "max.message.bytes"
    ]

    resource = %{resource_type: @topic_resource, resource_name: s.topic, config_names: names}
    request = :kpro_req_lib.describe_configs(1, [resource], %{})

    case guard(fn -> :kpro.connect_any(s.endpoints, s.conn) end) do
      {:ok, connection} ->
        try do
          with {:ok, kpro_rsp(msg: msg)} <- :kpro.request_sync(connection, request, 5_000) do
            read_configs(msg)
          else
            {:error, reason} -> {:error, classify(reason)}
          end
        after
          :kpro.close_connection(connection)
        end

      {:error, reason} ->
        {:error, classify(reason)}
    end
  end

  defp read_configs(msg) do
    case :kpro.find(:resources, msg) do
      [resource | _] ->
        case :kpro.find(:error_code, resource) do
          :no_error ->
            {:ok,
             Map.new(:kpro.find(:config_entries, resource), fn entry ->
               {:kpro.find(:config_name, entry), :kpro.find(:config_value, entry)}
             end)}

          code ->
            # a topic the broker answering has not heard of (yet) is "not there", like
            # the metadata answer; any other code is the broker's refusal to describe
            if classify(code) == :not_found,
              do: {:error, :not_found},
              else: {:error, {:describe_configs, code}}
        end

      [] ->
        {:error, {:describe_configs, :no_resource}}
    end
  end

  defp check_topic(instance, s, topic, configs) do
    max_age = s.max_age_ms
    replicas = topic.partitions |> Enum.map(&length(&1.replicas)) |> Enum.min(fn -> 0 end)
    min_isr = int(configs["min.insync.replicas"])
    retention = int(configs["retention.ms"])
    max_message = int(configs["max.message.bytes"])

    problems =
      Enum.reject(
        [
          replicas < 3 && "replication.factor is #{replicas}, needs at least 3",
          min_isr < 2 && "min.insync.replicas is #{min_isr}, needs at least 2",
          configs["unclean.leader.election.enable"] != "false" &&
            "unclean.leader.election.enable is #{configs["unclean.leader.election.enable"]}, needs false",
          (max_age != nil and retention != -1 and retention < 2 * max_age) &&
            "retention.ms is #{retention}, needs at least 2 x backup.max_age_ms (#{2 * max_age})",
          max_message < s.max_record_bytes &&
            "max.message.bytes is #{max_message}, needs at least intake.max_record_bytes (#{s.max_record_bytes})"
        ],
        &(&1 == false)
      )

    if problems == [] do
      :ets.insert(
        table(instance),
        {:describe, "acks=all, min.insync.replicas=#{min_isr}, replicas=#{replicas}"}
      )

      :ok
    else
      {:error, {:intake_topic_settings, problems}}
    end
  end

  defp int(nil), do: 0

  defp int(value) when is_binary(value) do
    case Integer.parse(value) do
      {n, _} -> n
      :error -> 0
    end
  end

  # ── settings ──────────────────────────────────────────────────────────────

  defp settings(instance) do
    case :ets.lookup(table(instance), :settings) do
      [{:settings, settings}] -> {:ok, settings}
      [] -> {:error, :unavailable}
    end
  rescue
    ArgumentError -> {:error, :unavailable}
  end

  defp table(instance), do: :"ankusa_intake_kafka.#{instance}"

  defp build_settings(instance) do
    config = Ankusa.config(instance)
    opts = config.intake.kafka

    brokers =
      case Keyword.get(opts, :brokers) do
        [_ | _] = brokers -> brokers
        _ -> raise ArgumentError, "intake.kafka.brokers is required for intake.type: :kafka"
      end

    %{
      client: client(instance),
      topic: Intake.topic(config),
      node: config.intake.node_id,
      endpoints: Enum.map(brokers, &endpoint/1),
      client_opts: :brod_utils.init_sasl_opt(Keyword.take(opts, [:ssl, :sasl])),
      conn:
        opts
        |> Keyword.take([:ssl, :sasl])
        |> :brod_utils.init_sasl_opt()
        |> Keyword.merge(connect_timeout: 3_000, request_timeout: 5_000),
      publish_timeout_ms: config.intake.publish_timeout_ms,
      max_record_bytes: config.intake.max_record_bytes,
      max_age_ms: config.backup.max_age_ms,
      health_interval_ms: Keyword.get(opts, :health_interval_ms, 1_000)
    }
  end

  defp endpoint({host, port}), do: {to_charlist(host), port}

  defp endpoint(host_port) when is_binary(host_port) do
    [host, port] = String.split(host_port, ":", parts: 2)
    {to_charlist(host), String.to_integer(port)}
  end

  defp atoms(atom, acc) when is_atom(atom), do: [atom | acc]
  defp atoms(tuple, acc) when is_tuple(tuple), do: atoms(Tuple.to_list(tuple), acc)
  defp atoms(list, acc) when is_list(list), do: Enum.reduce(list, acc, &atoms/2)
  defp atoms(_other, acc), do: acc

  # ── client process ────────────────────────────────────────────────────────

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)
    instance = Keyword.fetch!(opts, :instance)
    settings = build_settings(instance)
    table = table(instance)

    :ets.new(table, [:named_table, :public, read_concurrency: true])
    :ets.insert(table, [{:ready, false}, {:settings, settings}])

    send(self(), :connect)
    {:ok, %{instance: instance, settings: settings, pid: nil, check: nil}}
  end

  @impl GenServer
  def handle_info(:connect, %{pid: nil} = state) do
    s = state.settings

    config =
      [
        auto_start_producers: true,
        allow_topic_auto_creation: false,
        connect_timeout: 3_000,
        request_timeout: s.publish_timeout_ms + 3_000,
        default_producer_config: [
          required_acks: -1,
          ack_timeout: s.publish_timeout_ms,
          partition_onwire_limit: 1,
          max_retries: 0,
          max_linger_ms: 0,
          max_batch_size: max(1_048_576, s.max_record_bytes),
          partition_restart_delay_seconds: 1
        ]
      ] ++ s.client_opts

    case :brod_client.start_link(s.endpoints, s.client, config) do
      {:ok, pid} ->
        send(self(), :health)
        {:noreply, %{state | pid: pid}}

      {:error, reason} ->
        Logger.warning("[ankusa] intake kafka client did not start: #{inspect(reason)}")
        Process.send_after(self(), :connect, @reconnect_ms)
        {:noreply, state}
    end
  end

  def handle_info(:connect, state), do: {:noreply, state}

  # The client exits during its own init when no broker answers.
  def handle_info({:EXIT, pid, reason}, %{pid: pid} = state) do
    Logger.warning("[ankusa] intake kafka client stopped: #{inspect(reason)}; reconnecting")
    ready(state, false)
    Process.send_after(self(), :connect, @reconnect_ms)
    {:noreply, %{state | pid: nil}}
  end

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  def handle_info(:health, %{pid: nil} = state), do: {:noreply, state}

  def handle_info(:health, %{check: nil} = state) do
    parent = self()
    s = state.settings
    ref = make_ref()

    pid =
      spawn(fn ->
        result =
          try do
            :brod_client.get_metadata_safe(s.client, s.topic)
          catch
            kind, reason -> {:error, {kind, reason}}
          end

        send(parent, {:health, ref, result})
      end)

    timer = Process.send_after(self(), {:health_timeout, ref}, @health_timeout_ms)
    {:noreply, %{state | check: {ref, pid, timer}}}
  end

  def handle_info(:health, state), do: {:noreply, state}

  def handle_info({:health, ref, result}, %{check: {ref, _pid, timer}} = state) do
    Process.cancel_timer(timer)
    ready(state, healthy?(state, result))
    Process.send_after(self(), :health, state.settings.health_interval_ms)
    {:noreply, %{state | check: nil}}
  end

  def handle_info({:health_timeout, ref}, %{check: {ref, pid, _timer}} = state) do
    Process.exit(pid, :kill)
    ready(state, false)
    Process.send_after(self(), :health, state.settings.health_interval_ms)
    {:noreply, %{state | check: nil}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  # The log is usable when the topic answers and the node's partition has a
  # leader: the partition the node is stamped with, else the one its id maps to.
  defp healthy?(state, {:ok, metadata}) do
    with {:ok, topic} <- find_topic(metadata, state.settings.topic) do
      count = length(topic.partitions)

      wanted =
        Intake.Pending.partition(state.instance) || partition_for(state.settings.node, count)

      Enum.any?(
        topic.partitions,
        &(&1.index == wanted and is_integer(&1.leader) and &1.leader >= 0)
      )
    else
      _ -> false
    end
  end

  defp healthy?(_state, _result), do: false

  defp ready(state, ready?), do: :ets.insert(table(state.instance), {:ready, ready?})
end
