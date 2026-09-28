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
  permissions).

  A publish whose ack is lost can still have been stored, so delivery is
  at-least-once; consumers dedupe on `id` (or on JetStream's own
  `Nats-Msg-Id` duplicate window, if they set one themselves — this sink does
  not, because a hook replayed from the DLQ is a *new* deliberate publish).

  ## What each message carries

    * **subject** — `:subject` (a static string, or a 1-arity fun over the
      `Ankusa.Envelope`).
    * **headers** — `ankusa_id`, `ankusa_source_id`, `ankusa_tenant_id`,
      `ankusa_message_version`, `content_type`. The same five
      `Ankusa.Sink.Kafka` sets, so a consumer parses one set regardless of
      transport.
    * **body** — the `Ankusa.Sink.Message` JSON.

  ## Process model

  The first `deliver/3` for an `{instance, connection}` pair starts a gnat
  connection under this package's `DynamicSupervisor`, **synchronously** —
  gnat completes the NATS handshake before its start call returns — so by the
  time `deliver/3` has a connection to publish through, the socket is up or
  the start already returned the reason it isn't. Servers are tried in the
  order given; a failed connect is `{:error, reason}` (`:econnrefused`,
  `:timeout`, a rejected credential), retried by the source's
  `Ankusa.RetryPolicy`, then dead-lettered, exactly like any other sink
  failure.

  gnat stops its process when the socket closes, and the child is
  `:temporary`: nothing crash-loops trying to reach a server that is gone, and
  the next `deliver/3` reconnects inside the retry policy. There is no
  separate reconnect policy to get wrong. In the narrow window where the
  connection dies between that check and the publish, `deliver/3` returns
  `{:error, :not_connected}` — also just a retry.

  The connection has to register a name, and a name has to be an atom, so it
  is `:"ankusa_nats.<instance>.<connection>"`: both parts come from config, so
  the atom count is bounded and two instances never share a connection.
  Servers and credentials are fixed by the first delivery that starts it; use
  a different `:connection` for a different cluster.

  ## opts

    * `:servers`             — required; `"host:port"` strings or `{host, port}`
                               tuples, tried in order. More than one is
                               failover, not fan-out: one connection to one
                               server at a time.
    * `:subject`             — required; a string, or `(Envelope.t() -> String.t())`
    * `:inline_max_bytes`    — default 64 KiB (65,536), configurable
    * `:publish_timeout_ms`  — how long `deliver/3` waits for the JetStream
                               publish ack; default `5_000`
    * `:connection`          — atom naming the gnat connection; default `:default`
    * `:client_name`         — the name this client reports to NATS monitoring
    * `:tls`, `:ssl_opts`, `:tcp_opts`, `:connection_timeout`, `:ping_interval`,
      `:inbox_prefix`        — passed to gnat's connection settings
    * credentials            — `:username` (needs `:password`), `:password`,
                               `:token`, or `:nkey_seed` (with `:jwt` for
                               operator-mode accounts). gnat sends exactly one
                               scheme, choosing in that order, so set one.

  `:no_responders` is always on. It costs nothing when the server answers, and
  it turns "no stream covers this subject and nothing else is listening" into
  an immediate `{:error, :no_stream}` rather than a `:publish_timeout_ms`
  hang.
  """

  @behaviour Ankusa.Sink

  alias Ankusa.Envelope
  alias Ankusa.Sink.Message

  @default_publish_timeout_ms 5_000

  # gnat's connection_settings, minus the `:host`/`:port` this sink derives from
  # `:servers` and minus `:name`, which `:client_name` maps onto.
  @connection_settings ~w(tls ssl_opts tcp_opts connection_timeout ping_interval inbox_prefix username password token nkey_seed jwt)a

  @impl true
  def deliver(%Envelope{} = env, ctx, opts) do
    subject = subject(env, opts)
    timeout = Keyword.get(opts, :publish_timeout_ms, @default_publish_timeout_ms)
    conn = connection(ctx.instance, Keyword.get(opts, :connection, :default))

    with :ok <- ensure_connection(conn, opts),
         {:ok, payload} <- Message.encode(env, ctx, Message.inline_max_bytes(opts)) do
      publish(conn, subject, payload, headers(env), timeout)
    end
  end

  # Order within a subject is the order the stream received it, so the subject
  # is this sink's ordering scope — the same shape as `Sink.Kafka`'s record key,
  # minus the partition indirection.
  @impl true
  def ordering_key(env, opts), do: subject(env, opts)

  @impl true
  def inline_max_bytes(opts), do: Message.inline_max_bytes(opts)

  # The publish is a request, not a `Gnat.pub/3`: `pub/3` returns once the bytes
  # are handed to the socket, while the whole point here is to wait for
  # JetStream's ack. gnat sends the publish with a reply inbox either way; only
  # `request/4` listens for the answer.
  defp publish(conn, subject, payload, headers, timeout) do
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

      {:error, reason} ->
        {:error, reason}
    end
  end

  # The connection is addressed by its registered name, not a pid: gnat stops
  # that process when the socket closes, so the name is unregistered and the
  # call exits. That is this sink's "not connected", and it is the caller's to
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

  defp headers(env) do
    [
      {"ankusa_id", env.id},
      {"ankusa_source_id", env.source_id},
      {"ankusa_tenant_id", env.tenant_id || ""},
      {"ankusa_message_version", "1"},
      {"content_type", "application/json"}
    ]
  end

  defp subject(env, opts) do
    case Keyword.fetch!(opts, :subject) do
      subject when is_binary(subject) -> subject
      fun when is_function(fun, 1) -> fun.(env)
    end
  end

  # ── connection lifecycle ────────────────────────────────────────────────

  defp connection(instance, connection), do: :"ankusa_nats.#{instance}.#{connection}"

  # `whereis` first: the common case must not serialize every delivery through
  # the DynamicSupervisor.
  defp ensure_connection(conn, opts) do
    if Process.whereis(conn), do: :ok, else: start_connection(conn, opts)
  end

  # Servers in order, first one that connects wins. gnat handshakes inside
  # `init/1`, so `start_link` answers once the socket is either up or
  # definitively not.
  defp start_connection(conn, opts) do
    Enum.reduce_while(servers(opts), {:error, :no_servers}, fn settings, _last_error ->
      child = %{
        id: conn,
        start: {Gnat, :start_link, [settings, [name: conn]]},
        # :temporary: a connection that drops, or a server that is down, must
        # not be restarted here — restarting a synchronous connect that keeps
        # failing would crash-loop past the supervisor's intensity and take the
        # application down. The next `deliver/3` starts it again, inside the
        # source's retry policy.
        restart: :temporary
      }

      case DynamicSupervisor.start_child(Ankusa.Sink.NATS.Supervisor, child) do
        {:ok, _pid} -> {:halt, :ok}
        {:error, {:already_started, _pid}} -> {:halt, :ok}
        {:error, reason} -> {:cont, {:error, reason}}
      end
    end)
  end

  defp servers(opts) do
    overrides =
      opts
      |> Keyword.take(@connection_settings)
      |> Map.new()
      |> put_opt(:name, Keyword.get(opts, :client_name))
      |> Map.put(:no_responders, true)

    opts
    |> Keyword.fetch!(:servers)
    |> Enum.map(fn server -> Map.merge(overrides, server_settings(server)) end)
  end

  defp server_settings({host, port}), do: %{host: to_charlist(host), port: port}

  defp server_settings(host_port) when is_binary(host_port) do
    [host, port] = String.split(host_port, ":", parts: 2)
    %{host: to_charlist(host), port: String.to_integer(port)}
  end

  defp put_opt(map, _key, nil), do: map
  defp put_opt(map, key, value), do: Map.put(map, key, value)
end
