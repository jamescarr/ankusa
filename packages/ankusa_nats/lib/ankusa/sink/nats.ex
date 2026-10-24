defmodule Ankusa.Sink.NATS do
  @moduledoc """
  Publishes delivered hooks to a **NATS JetStream** subject. The value is
  `Ankusa.Sink.Message`, byte-identical to what `Ankusa.Sink.RabbitMQ` and
  `Ankusa.Sink.Kafka` publish: inline up to `:inline_max_bytes`, a claim
  ticket above it.

  The subject the hook lands on — not the stream — is the sink's business, and
  it belongs to whoever operates the stream: the stream's configured subjects
  decide whether the publish is *stored* at all. So this sink **never creates
  or updates a stream**. A stream's storage, retention, replicas, and subject
  set are an operator's capacity and ordering contract, not something the
  first hook to arrive should decide. A subject no stream covers is a
  `{:error, :no_stream}` that flows into the source's
  `Ankusa.RetryPolicy`, then the DLQ — never a silently auto-created stream.
  (Compare `Ankusa.Sink.Kafka`, which likewise never creates its topic.)

  ## Publishing is confirmed

  `deliver/3` returns `:ok` only after JetStream's own **publish
  acknowledgement** — the `{"stream": ..., "seq": ...}` the server sends back
  once the message is committed to the stream. It is not "the bytes reached a
  socket": the publish is a NATS request with a reply inbox, and the reply is
  awaited (bounded by `:publish_timeout_ms`, default 5s), the equivalent of
  RabbitMQ's publisher confirms and Kafka's `acks=all`. Everything else is an
  `{:error, reason}` that dispatch retries, then dead-letters: `:publish_timeout`
  when the ack never arrives, `:no_stream` when no stream covers the subject,
  and `{:jetstream, %{"code" => 400, "description" => "message size exceeds
  maximum allowed"}}` for a publish the stream itself refuses (size, TTL,
  permissions). The one exception is `{:permanent, :max_payload_exceeded}`: a
  publish larger than the server's `max_payload` is refused by gnat before it is
  sent, and is dead-lettered at once.

  A publish whose ack is lost can still have been stored, so delivery is
  at-least-once. Every publish sets `Nats-Msg-Id` to the hook's `id` — or
  `id:replay:<replay_id>` for a replay — so JetStream's duplicate window
  collapses a lost-ack retry of one delivery, while a deliberate replay of the
  same hook carries a distinct id and is always stored. Consumers dedupe on
  the message's `idempotency_key`.

  ## What each message carries

    * **subject** — `:subject` (a static string, or a 1-arity fun over the
      `Ankusa.Envelope`).
    * **headers** — `Nats-Msg-Id` (`id`, or `id:replay:<replay_id>` on a
      replay), then `Ankusa.Sink.Message.headers/2`: `ankusa_id`,
      `ankusa_source_id`, `ankusa_tenant_id`, `ankusa_message_version`,
      `ankusa_idempotency_key`, `content_type` — the same set
      `Ankusa.Sink.Kafka` sets, plus `ankusa_dedupe_key` and
      `ankusa_replay_id` when the hook carries them — so a consumer parses one
      set regardless of transport.
    * **body** — the `Ankusa.Sink.Message` JSON.

  ## Process model

  The first `deliver/3` for an `{instance, connection}` pair starts a
  `Gnat.ConnectionSupervisor` under this package's `DynamicSupervisor`. The
  start returns at once: the NATS handshake runs in that supervisor, never in
  the caller or the `DynamicSupervisor`, so a slow or dead server cannot stall
  other deliveries behind it. It connects to one of `:servers`, picked at
  random per attempt (gnat's behaviour), and reconnects with a 2 s backoff
  whenever the socket closes.

  Both processes are found through the instance's registry
  (`Ankusa.via(instance, {:nats_conn, connection})`): nothing is registered
  globally, and two instances never share a connection. While the connection
  is not up — the first delivery, a reconnect — `deliver/3` returns
  `{:error, :not_connected}`, retried by the source's `Ankusa.RetryPolicy`
  like any other sink failure. Servers and credentials are fixed by the first
  delivery that starts it; use a different `:connection` for a different
  cluster.

  ## opts

    * `:servers`             — required; `"host:port"` strings or `{host, port}`
                               tuples. More than one is failover, not fan-out:
                               one connection to one server (picked at random
                               per attempt) at a time.
    * `:subject`             — required; a string, or `(Envelope.t() -> String.t())`
    * `:inline_max_bytes`    — default 64 KiB (65,536), configurable
    * `:publish_timeout_ms`  — how long `deliver/3` waits for the JetStream
                               publish ack; default `5_000`
    * `:connection`          — atom naming the gnat connection; default `:default`
    * `:idle_timeout_ms`     — close the connection after this long without a
                               delivery through it, default `600_000` (10 min);
                               `0` never. Never under `:publish_timeout_ms` plus
                               1 s. See `Ankusa.Sink.Reaper`
    * `:client_name`         — the name this client reports to NATS monitoring
    * `:tls`, `:ssl_opts`, `:tcp_opts`, `:connection_timeout`, `:ping_interval`,
      `:inbox_prefix`        — passed to gnat's connection settings
    * credentials            — `:username` (needs `:password`), `:password`,
                               `:token`, or `:nkey_seed` (with `:jwt` for
                               operator-mode accounts); each secret is handed
                               to gnat as a zero-arity function, so no gnat
                               process holds it in the clear. gnat sends exactly
                               one scheme, choosing in that order, so set one.

  `:no_responders` is always on. It costs nothing when the server answers, and
  it turns "no stream covers this subject and nothing else is listening" into
  an immediate `{:error, :no_stream}` rather than a `:publish_timeout_ms`
  hang.
  """

  use Ankusa.Sink.Messaging

  alias Ankusa.Envelope
  alias Ankusa.Sink.{Connections, Description, Message, Messaging}
  alias Ankusa.Sink.Reaper

  @default_publish_timeout_ms 5_000

  # gnat's connection_settings, minus the `:host`/`:port` this sink derives from
  # `:servers` and minus `:name`, which `:client_name` maps onto.
  @connection_settings ~w(tls ssl_opts tcp_opts connection_timeout ping_interval inbox_prefix username password token nkey_seed jwt)a

  # The settings gnat (>= 1.18) resolves through a zero-arity function; see
  # `hide_secret/1`. `:jwt` and `:username` are not secrets gnat resolves.
  @secret_settings [:password, :token, :nkey_seed]

  @impl true
  def publish(payload, %Envelope{} = env, ctx, opts) do
    timeout = Keyword.get(opts, :publish_timeout_ms, @default_publish_timeout_ms)
    connection = Keyword.get(opts, :connection, :default)

    with {:ok, conn} <- connection(ctx.instance, connection, opts, timeout) do
      subject = Messaging.resolve(Keyword.fetch!(opts, :subject), env)
      request_ack(conn, subject, payload, headers(env, ctx), timeout)
    end
  end

  @impl true
  def describe(subject, opts) do
    %Description{
      protocol: "nats",
      host:
        opts
        |> servers()
        |> Enum.map_join(",", fn %{host: host, port: port} -> "#{host}:#{port}" end),
      address: address(subject, opts),
      ankusa_headers: true
    }
  end

  # Only a configured static subject is a fixed address; a function computes it
  # per hook, so the document cannot name it.
  defp address(_subject, opts), do: Messaging.static(Keyword.fetch!(opts, :subject))

  # The publish is a request, not a `Gnat.pub/3`: `pub/3` returns once the bytes
  # are handed to the socket, while the whole point here is to wait for
  # JetStream's ack. gnat sends the publish with a reply inbox either way; only
  # `request/4` listens for the answer.
  defp request_ack(conn, subject, payload, headers, timeout) do
    case request(conn, subject, payload, headers, timeout) do
      {:ok, %{body: body}} when is_binary(body) and body != "" ->
        ack(body)

      {:ok, %{status: status} = reply} when is_binary(status) ->
        {:error, {:jetstream, status, Map.get(reply, :description)}}

      {:ok, reply} ->
        {:error, {:unexpected_reply, reply}}

      {:error, :timeout} ->
        {:error, :publish_timeout}

      {:error, :no_responders} ->
        {:error, :no_stream}

      # gnat refuses a body over the server's `max_payload` before sending it
      # (it used to be the server closing the connection, cutting off every
      # publish in flight). Retrying cannot shrink the hook.
      {:error, :max_payload_exceeded} ->
        {:error, {:permanent, :max_payload_exceeded}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # gnat stops the connection process when the socket closes (its supervisor
  # starts a new one), so a pid looked up a moment ago may be gone and the call
  # exits. That is this sink's "not connected", and it is the caller's to
  # absorb — the dispatch pipeline retries it like any other sink error.
  defp request(conn, subject, payload, headers, timeout) do
    Gnat.request(conn, subject, payload, headers: headers, receive_timeout: timeout)
  catch
    :exit, _reason -> {:error, :not_connected}
  end

  # `{"stream": "...", "seq": 42}` is the ack; a stored-but-identical message
  # adds `"duplicate": true` and is still `:ok`.
  #
  # `error` is checked first, and `seq` must be positive, because a rejected
  # publish answers with both: a message over the stream's `max_msg_size` is
  # `{"error":{"code":400,...},"stream":"X","seq":0}`. Reading that as success
  # would tell dispatch a hook was stored that the stream refused.
  defp ack(body) do
    case JSON.decode(body) do
      {:ok, %{"error" => error}} when is_map(error) ->
        {:error, {:jetstream, error}}

      {:ok, %{"stream" => stream, "seq" => seq}}
      when is_binary(stream) and is_integer(seq) and seq > 0 ->
        :ok

      {:ok, other} ->
        {:error, {:unexpected_reply, other}}

      {:error, _reason} ->
        {:error, {:unexpected_reply, body}}
    end
  end

  defp headers(env, ctx),
    do: [{"Nats-Msg-Id", Message.delivery_id(env, ctx)} | Message.headers(env, ctx)]

  # ── connection lifecycle ────────────────────────────────────────────────

  # The supervisor answers its start at once and connects (and reconnects) on
  # its own; the connection is there or it is not. The supervisor is the reaped
  # process: the idle sweep stops it, which takes its connection with it.
  defp connection(instance, connection, opts, timeout) do
    key = {:nats_sup, connection}

    with {:ok, _sup} <-
           Connections.ensure(
             __MODULE__,
             {instance, key},
             fn -> Ankusa.whereis(instance, key) end,
             fn -> supervisor_spec(instance, connection, opts) end,
             Reaper.idle_ms(opts, timeout + 1_000)
           ) do
      case Ankusa.whereis(instance, {:nats_conn, connection}) do
        nil -> {:error, :not_connected}
        pid -> {:ok, pid}
      end
    end
  end

  # The settings carry credentials (password, token, nkey seed): the child spec
  # holds them only inside a closure, which `inspect` prints without its
  # captured values, so the DynamicSupervisor's status and reports never show
  # them. Each secret is also a closure inside the settings (`hide_secret/1`),
  # so gnat's processes hold no plaintext either.
  defp supervisor_spec(instance, connection, opts) do
    settings = %{
      connection_settings: servers(opts),
      name: Ankusa.via(instance, {:nats_conn, connection}),
      backoff_period: 2_000
    }

    %{
      id: {Gnat.ConnectionSupervisor, instance, connection},
      start:
        {__MODULE__, :start_connection_supervisor,
         [fn -> settings end, [name: Ankusa.via(instance, {:nats_sup, connection})]]},
      restart: :permanent
    }
  end

  @doc false
  @spec start_connection_supervisor((-> map()), keyword()) :: GenServer.on_start()
  def start_connection_supervisor(settings_fun, opts) when is_function(settings_fun, 0),
    do: Gnat.ConnectionSupervisor.start_link(settings_fun.(), opts)

  defp servers(opts) do
    overrides =
      opts
      |> Keyword.take(@connection_settings)
      |> Map.new(&hide_secret/1)
      |> put_opt(:name, Keyword.get(opts, :client_name))
      |> Map.put(:no_responders, true)

    opts
    |> Keyword.fetch!(:servers)
    |> Messaging.endpoints()
    |> Enum.map(fn {host, port} ->
      Map.merge(overrides, %{host: to_charlist(host), port: port})
    end)
  end

  # gnat (>= 1.18) calls a zero-arity function for these at each handshake, so
  # neither its connection nor its supervisor ever holds the plaintext.
  defp hide_secret({key, value}) when key in @secret_settings and is_binary(value),
    do: {key, fn -> value end}

  defp hide_secret(pair), do: pair

  defp put_opt(map, _key, nil), do: map
  defp put_opt(map, key, value), do: Map.put(map, key, value)
end
