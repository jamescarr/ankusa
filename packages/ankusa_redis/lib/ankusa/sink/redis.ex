defmodule Ankusa.Sink.Redis do
  @moduledoc """
  Publishes delivered hooks to a **Redis pub/sub channel** (`PUBLISH`). The
  value is `Ankusa.Sink.Message`, byte-identical to what `Ankusa.Sink.RabbitMQ`,
  `Ankusa.Sink.Kafka`, and `Ankusa.Sink.NATS` publish: inline up to
  `:inline_max_bytes`, a claim ticket above it.

  Pub/sub has no headers, so the envelope's `id`, `source_id`, and `tenant_id`
  travel only inside that JSON — a consumer decodes the `Ankusa.Sink.Message`
  body and reads them there.

  ## Pub/sub keeps no copy

  `PUBLISH` is fan-out to *live* subscribers and nothing else: Redis stores
  nothing on a channel, so a subscriber that is disconnected — or connects a
  moment later — never sees the message. `deliver/3` therefore returns `:ok`
  only when the server's reply says at least one subscriber received the
  publish, and `{:error, :no_subscribers}` when it says zero: a hook nobody
  heard is retried by the source's `Ankusa.RetryPolicy`, then dead-lettered
  and replayable, instead of being recorded as delivered.

  For the same reason `durable?/1` is `false` — a subscriber that disconnects
  after the publish loses the message. A source whose only sinks are Redis
  cannot run `wal.type: none` (`Ankusa.WAL` rejects it at boot), and a Redis
  sink listed next to a durable one still gates every ack in that mode: boot
  only needs one durable sink, but every sink has to confirm, so
  `{:error, :no_subscribers}` is a `503` for the whole request. With the
  default WAL the hook stays in the log and the DLQ, which is what makes
  replay possible at all. A Redis that *keeps* messages is a Redis Stream
  (`XADD`), a different sink than this one.

  Delivery is at-least-once: a retry or a DLQ replay republishes, so
  consumers dedupe on `id`.

  ## Process model

  The first `deliver/3` for an `{instance, url}` pair starts a `Redix`
  connection under this package's `DynamicSupervisor`, **synchronously**
  (`sync_connect: true`): by the time `deliver/3` has a connection to publish
  through, the socket is up or the start already returned the reason it
  isn't. A failed first connect is `{:error, {:connection, reason}}`
  (`:econnrefused`, `:timeout`, a rejected credential), retried by the
  source's `Ankusa.RetryPolicy`, then dead-lettered, exactly like any other
  sink failure; errors answered by a live server (NOAUTH, WRONGPASS) are
  `{:error, {:redis, message}}`.

  The child is `:temporary` — a server that is down must not crash-loop
  against the supervisor — so nothing is restarted here, and the next
  `deliver/3` tries again inside the retry policy. Once connected, Redix
  reconnects on its own with backoff, and a publish sent meanwhile is
  `{:error, {:connection, :closed}}`. In the narrow window where the
  connection dies between the registry lookup and the call, `deliver/3`
  returns `{:error, :not_connected}` — also just a retry.

  Connections are keyed by URL and registered as `{:redis_sink, url}` under
  the instance's registry, so two sinks pointed at the same server share one
  connection, and two pointed at different servers never do. Servers,
  credentials, and database are fixed by the first delivery that starts the
  connection; use a different `:url` for a different server.

  ## opts

    * `:url`                — required; `redis://[:user:password@]host:port[/db]`
                              (`rediss://` for TLS), passed to `Redix.start_link/2`
    * `:channel`            — required; a string, or `(Envelope.t() -> String.t())`
    * `:inline_max_bytes`   — default 64 KiB (65,536), configurable
    * `:publish_timeout_ms` — how long `deliver/3` waits for the `PUBLISH`
                              reply; default `5_000`
  """

  @behaviour Ankusa.Sink

  alias Ankusa.Envelope
  alias Ankusa.Sink.Description
  alias Ankusa.Sink.Message

  @default_publish_timeout_ms 5_000

  @impl true
  def deliver(%Envelope{} = env, ctx, opts) do
    url = Keyword.fetch!(opts, :url)
    timeout = Keyword.get(opts, :publish_timeout_ms, @default_publish_timeout_ms)

    with {:ok, conn} <- ensure_connection(ctx.instance, url),
         {:ok, payload} <- Message.encode(env, ctx, Message.inline_max_bytes(opts)) do
      publish(conn, channel(env, opts), payload, timeout)
    end
  end

  # Redis delivers the publishes of one connection to a channel in order, so
  # the channel is the ordering scope — the same shape as `Sink.NATS`'s
  # subject, and a sink-level `Sink.Kafka`'s record key.
  @impl true
  def ordering_key(env, opts), do: channel(env, opts)

  @impl true
  def inline_max_bytes(opts), do: Message.inline_max_bytes(opts)

  @impl true
  def describe(subject, opts) do
    uri = URI.parse(Keyword.fetch!(opts, :url))

    %Description{
      protocol: uri.scheme,
      host: "#{uri.host}:#{uri.port || 6379}",
      pathname: pathname(uri),
      address: address(subject, opts),
      ankusa_headers: false
    }
  end

  # The Redis database is the URL path; userinfo is dropped.
  defp pathname(%URI{path: path}) when path in [nil, "", "/"], do: nil
  defp pathname(%URI{path: path}), do: path

  # Only a configured static channel is a fixed address; a function computes it
  # per hook, so the document cannot name it.
  defp address(_subject, opts) do
    case Keyword.fetch!(opts, :channel) do
      channel when is_binary(channel) -> channel
      fun when is_function(fun, 1) -> nil
    end
  end

  # Pub/sub keeps no copy, so a hook this sink accepts can be lost to a
  # subscriber that disconnects afterwards (see the moduledoc). Dispatch must
  # keep it in the WAL.
  @impl true
  def durable?(_opts), do: false

  # `PUBLISH`'s reply is the number of subscribers the message was handed to.
  # Zero of them is the failure mode this sink exists to make visible.
  defp publish(conn, channel, payload, timeout) do
    case command(conn, ["PUBLISH", channel, payload], timeout) do
      {:ok, 0} ->
        {:error, :no_subscribers}

      {:ok, count} when is_integer(count) and count > 0 ->
        :ok

      {:error, :not_connected} ->
        {:error, :not_connected}

      {:error, %Redix.ConnectionError{reason: :timeout}} ->
        {:error, :publish_timeout}

      {:error, %Redix.ConnectionError{reason: reason}} ->
        {:error, {:connection, reason}}

      {:error, %Redix.Error{message: message}} ->
        {:error, {:redis, message}}

      other ->
        {:error, {:unexpected_reply, other}}
    end
  end

  # Redix answers `{:error, %Redix.ConnectionError{reason: :closed}}` while its
  # socket is down, and exits (`{:redix_exited_during_call, reason}`) when the
  # connection process dies between our lookup and the cast. Both are "retry
  # me", not a crash of the dispatch worker.
  defp command(conn, command, timeout) do
    Redix.command(conn, command, timeout: timeout)
  catch
    :exit, _reason -> {:error, :not_connected}
  end

  defp channel(env, opts) do
    case Keyword.fetch!(opts, :channel) do
      channel when is_binary(channel) -> channel
      fun when is_function(fun, 1) -> fun.(env)
    end
  end

  # ── connection lifecycle ────────────────────────────────────────────────

  # The registry lookup first: the common case must not serialize every
  # delivery through the DynamicSupervisor.
  defp ensure_connection(instance, url) do
    case Ankusa.whereis(instance, key(url)) do
      pid when is_pid(pid) -> {:ok, pid}
      nil -> start_connection(instance, url)
    end
  end

  # `sync_connect: true`: the socket is up, or this returns the reason it
  # isn't — no "connected" connection that is still dialing.
  defp start_connection(instance, url) do
    child = %{
      id: key(url),
      start:
        {Redix, :start_link, [url, [name: Ankusa.via(instance, key(url)), sync_connect: true]]},
      # :temporary: restarting a synchronous connect that keeps failing would
      # crash-loop past the supervisor's intensity and take the application
      # down. The next `deliver/3` starts it again, inside the source's retry
      # policy.
      restart: :temporary
    }

    case DynamicSupervisor.start_child(Ankusa.Sink.Redis.Supervisor, child) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, %Redix.ConnectionError{reason: reason}} -> {:error, {:connection, reason}}
      {:error, reason} -> {:error, {:connection, reason}}
    end
  end

  defp key(url), do: {:redis_sink, url}
end
