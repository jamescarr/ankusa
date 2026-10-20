defmodule Ankusa.Sink.SQS do
  @moduledoc """
  Sends delivered hooks to an **Amazon SQS** queue, standard or FIFO (or to
  anything that speaks SQS's JSON protocol, such as the `floci` emulator in
  `docker-compose.integration.yml`). The message body is
  `Ankusa.Sink.Message`, byte-identical to what `Ankusa.Sink.Kafka` and
  `Ankusa.Sink.NATS` publish: inline up to `:inline_max_bytes`, a claim ticket
  above it.

  Dispatch's deliveries are **batched** (see "Batching"): up to 10 of them to one
  queue go out as one SigV4-signed `SendMessageBatch` `POST`. Anything else
  (direct publishing, `batch: false`) is one `SendMessage` per delivery. Both go
  through `Ankusa.HttpClient` (Req's connection pool, no hidden retries or
  redirects). The sink needs nothing beyond core's `req` and `aws_signature`;
  its batchers are started by `Ankusa.Instance`. Credentials come from
  `Ankusa.AWS.Credentials`: static opts, then the `AWS_*` environment, web
  identity (IRSA), then the EC2 instance role (IMDSv2).

  This sink **never creates a queue**. A queue's type, retention, visibility
  timeout, redrive policy and encryption are an operator's contract, not
  something the first hook to arrive should decide. A queue that does not
  exist is `{:error, {:sqs, 400, "QueueDoesNotExist", message}}`, which flows
  into the source's `Ankusa.RetryPolicy`, then the DLQ. (Compare
  `Ankusa.Sink.Kafka` and `Ankusa.Sink.NATS`, which likewise never create
  their topic or stream.)

  ## Publishing is confirmed

  `deliver/3` returns `:ok` only on a `200` from `SendMessage` (or, batched, an
  entry of a `SendMessageBatch` reply under `Successful`) whose
  `MD5OfMessageBody` matches the MD5 of the body sent. SQS answers `200` once
  it has stored the message redundantly, so the sink keeps the default
  `c:Ankusa.Sink.durable?/1` of `true`. Everything else is an error (see
  "Error classes" in `Ankusa.Sink`):

    * `{:permanent, {:message_too_large, size, max}}` — body plus message
      attributes over `:max_message_bytes`, refused before any request.
    * `{:permanent, {:sqs, status, code, message}}` — SQS refused the message
      itself: `InvalidMessageContents` (characters outside SQS's allowed set),
      `InvalidParameterValue`, `MissingParameter`, `InvalidAttributeName` or
      `InvalidAttributeValue`. Dead-lettered at once.
    * `{:retry_after, ms, {:sqs, status, code, message}}` — SQS asked for less
      traffic (`ThrottlingException`, `RequestThrottled`, `ServiceUnavailable`,
      `KmsThrottled`), or a proxy answered `429`/`503` (`{:status, status,
      body}`): the next attempt waits `ms`, full jitter, or the policy's backoff.
    * `{:sqs, status, code, message}` — any other SQS error
      (`QueueDoesNotExist`, `AccessDenied`, a 5xx): an operator
      can fix these, so they get the retry policy, then the DLQ.
    * `{:status, status, body}` — a non-2xx response that is not an SQS error
      document (a proxy, a load balancer).
    * `{:md5_mismatch, expected, got}`, `{:unexpected_reply, body}`,
      `:no_credentials` and transport errors — retried.

  ## FIFO queues

  A `:queue_url` ending in `.fifo` is a FIFO queue (AWS requires the suffix on
  FIFO queue names). Each message then carries:

    * `MessageGroupId` — `:message_group_id`, default
      `"tenant_id/source_id"` (`"/source_id"` without a tenant), the scope
      `Ankusa.Sink.Kafka` uses for its default key: one group's messages are
      received in the order they were sent.
    * `MessageDeduplicationId` — the hook's `id`, or `id:replay:<replay_id>`
      for a replay (the rule `Ankusa.Sink.NATS` uses for `Nats-Msg-Id`), so
      SQS collapses a retry after a lost response, while a deliberate replay
      of the same hook is always stored.

  SQS's deduplication window is **5 minutes**. A retry later than that (a
  long outage, a slow backoff) can store a second copy, so delivery is
  at-least-once; consumers dedupe on the message's `idempotency_key`.

  On a standard queue `MessageGroupId` is sent only when `:message_group_id`
  is set (fair queues), and `MessageDeduplicationId` never is.

  ## Batching

  A delivery from dispatch waits in its queue's `Ankusa.Sink.Batcher` (one per
  queue and set of credentials) and is sent in a `SendMessageBatch` with up to 9
  others, 20 ms after the first arrived or as soon as the batch is full (10
  entries, 256 KiB). `deliver/3` returns when the batch's response is known: the
  hook's row stays claimed until then, so a batcher that dies loses nothing and
  the attempt is retried. The response is read entry by entry:

    * `Successful` with the MD5 of the body sent: `:ok`. A different MD5 is
      `{:md5_mismatch, expected, got}`.
    * `Failed`: classified on its own, as above, so a batch with one refused
      message dead-letters exactly that message. A throttled entry
      (`ThrottlingException`, `RequestThrottled`, `ServiceUnavailable`,
      `KmsThrottled`) is `{:retry_after, ms, reason}`, `ms` full jitter.
    * a `Failed` or a missing entry never fails its neighbours; a request that
      fails as a whole (a `5xx`, a timeout, a `429`) fails every entry the way a
      single `SendMessage` would, and each retries on its own schedule.

  Entry `Id`s are positions in the request (`"0"` to `"9"`), not hook ids; the
  `MessageDeduplicationId` is unchanged. A batch may hold hooks of several
  tenants and sources; the batcher takes entries round-robin across tenants, and
  refuses, with `{:retry_after, ms, {:batch_queue_full, reason}}`, a delivery that
  would pass its queue's ceilings (see `Ankusa.Sink.Batcher`).

  On a FIFO queue a `MessageGroupId` has one request in flight at a time, and its
  entries leave in the order they were admitted; SQS enqueues a batch's messages
  in the order they are sent. With the default group (`tenant/source`) that
  means a source's messages to one FIFO queue go out as fast as SQS answers,
  up to 10 to a request. A message that fails and is retried is a later send,
  as it always was.

  ## What each message carries

    * **body** — the `Ankusa.Sink.Message` JSON.
    * **message attributes** (all `String`) — `ankusa_id`, `ankusa_source_id`,
      `ankusa_tenant_id`, `ankusa_message_version`, `ankusa_idempotency_key`,
      `content_type` (`application/json`) — the set `Ankusa.Sink.Kafka` sets
      as headers — plus `ankusa_dedupe_key` and `ankusa_replay_id` when the
      hook carries them. `ankusa_tenant_id` is left out when the hook has no
      tenant, because SQS refuses an empty attribute value. At most 8 of SQS's
      10.

  ## opts

    * `:queue_url`         — required, e.g.
                             `"https://sqs.us-east-1.amazonaws.com/123456789012/hooks.fifo"`
    * `:region`            — required, e.g. `"us-east-1"`
    * `:endpoint`          — default: the queue URL's origin
                             (`scheme://host[:port]`). Set it for floci or a
                             VPC endpoint.
    * `:message_group_id`  — a string, or `(Envelope.t() -> String.t())`;
                             see "FIFO queues"
    * `:inline_max_bytes`  — default 64 KiB (65,536), configurable
    * `:max_message_bytes` — the largest body plus attributes sent; default
                             `1_048_576`, SQS's limit (1 MiB). Lower it to a
                             queue's own `MaximumMessageSize`.
    * `:timeout_ms`        — default `5_000`, for both connect and response
    * `:access_key_id`, `:secret_access_key`, `:session_token` — static
                             credentials. Without them the chain in
                             `Ankusa.AWS.Credentials` runs.
    * `:req_options`       — transport options for the HTTP client (a Finch
                             pool, a proxy via `:connect_options`, `plug:` for
                             `Req.Test`); see `Ankusa.HttpClient`.
    * `:batch`             — default `true`; `false` sends every delivery in its
                             own `SendMessage` and counts it against
                             `dispatch.sink_concurrency` as before
    * `:batch_linger_ms`   — default 20, how long a batch waits to fill
    * `:batch_max_entries` — default and maximum 10
    * `:batch_max_bytes`   — default 262,144, at most 1,048,576 (SQS's limit
                             for a batch's messages together)
    * `:batch_queue_max`, `:batch_queue_max_bytes`, `:batch_queue_tenant_max`
                           — the queue's ceilings, default 4,096 entries,
                             128 MiB and 1,024 entries a tenant
    * `:batch_max_inflight` — requests in flight per queue; default the
                             instance's `dispatch.sink_concurrency` (8)
  """

  @behaviour Ankusa.Sink

  alias Ankusa.Envelope
  alias Ankusa.Sink.{Batcher, Description, Message}

  @codec Ankusa.Sink.SQS.Batcher
  @default_max_message_bytes 1_048_576

  # SQS refusing the message itself: no retry changes the answer. Both spellings
  # of the parameter error, because the JSON protocol's `__type` for it is not
  # pinned down.
  @permanent_codes ~w(InvalidMessageContents InvalidParameterValue InvalidParameterValueException
                      MissingParameter InvalidAttributeName InvalidAttributeValue)

  # SQS asking for less traffic: the next attempt waits (with full jitter) as
  # long as the policy's own backoff or longer.
  @throttle_codes ~w(Throttling ThrottlingException RequestThrottled ServiceUnavailable
                     KmsThrottled)

  @impl true
  def deliver(%Envelope{} = env, ctx, opts) do
    max = Keyword.get(opts, :max_message_bytes, @default_max_message_bytes)

    with {:ok, payload} <- Message.encode(env, ctx, Message.inline_max_bytes(opts)),
         attributes = attributes(env, ctx),
         {:ok, size} <- fits(payload, attributes, max) do
      entry = message_entry(env, ctx, opts, payload, attributes)

      if batch?(ctx, opts) do
        # One of up to 10 entries of a `SendMessageBatch`; the batch's reply is
        # mapped back to this one.
        Batcher.deliver(@codec, ctx.instance, opts, %{
          tenant: env.tenant_id || "",
          bytes: size,
          group: ordered_group(entry, opts),
          attempt: ctx[:attempt] || 1,
          item: %{entry: entry, md5: md5(payload), attempt: ctx[:attempt] || 1}
        })
      else
        with {:ok, creds} <- Ankusa.AWS.Credentials.get(opts) do
          body = entry |> Map.put("QueueUrl", Keyword.fetch!(opts, :queue_url)) |> JSON.encode!()

          opts
          |> send_message(creds, body)
          |> reply(payload, ctx[:attempt] || 1)
        end
      end
    end
  end

  @impl true
  def inline_max_bytes(opts), do: Message.inline_max_bytes(opts)

  @impl true
  def batched?(opts), do: Keyword.get(opts, :batch, true) == true

  @impl true
  def describe(_subject, opts) do
    queue_url = Keyword.fetch!(opts, :queue_url)
    name = queue_name(queue_url)

    %Description{
      protocol: "sqs",
      host: @codec.authority(@codec.endpoint(opts) <> "/"),
      address: name,
      channel_bindings: %{
        "sqs" => %{
          "queue" => %{"name" => name, "fifoQueue" => fifo?(queue_url)},
          "bindingVersion" => "0.3.0"
        }
      },
      message_bindings: %{},
      # The `ankusa_*` values travel as message attributes, and
      # `ankusa_tenant_id` is absent without a tenant, so the document's
      # header schema (which requires it) would not describe them.
      ankusa_headers: false
    }
  end

  @doc """
  The `{:error, reason}` for an SQS error `code`, whether it answered a whole
  request (`status` is the HTTP status) or one entry of a batch (`400` for a
  sender's fault, `500` otherwise).

    * a refused message (`InvalidMessageContents`, `InvalidParameterValue`, …):
      `{:permanent, {:sqs, status, code, message}}`
    * throttling (`ThrottlingException`, `RequestThrottled`,
      `ServiceUnavailable`, `KmsThrottled`): `{:retry_after, ms, reason}`, `ms`
      full jitter for `attempt`
    * anything else: `{:sqs, status, code, message}`, transient
  """
  @spec classify_error(integer(), String.t(), String.t() | nil, pos_integer()) ::
          {:error, term()}
  def classify_error(status, code, message, attempt) do
    reason = {:sqs, status, code, message}

    cond do
      code in @permanent_codes -> {:error, {:permanent, reason}}
      code in @throttle_codes -> {:error, {:retry_after, Batcher.jitter_ms(attempt), reason}}
      true -> {:error, reason}
    end
  end

  @doc false
  # A non-`200` answer to a whole request. `{"__type": "com.amazonaws.sqs#QueueDoesNotExist",
  # "message": "..."}` is an SQS error; any other body came from something in
  # front of it (a `429` or `503` from it is throttling all the same).
  @spec error_reply(integer(), term(), pos_integer()) :: {:error, term()}
  def error_reply(status, body, attempt) do
    case decode(body) do
      {:ok, %{"__type" => type} = error} when is_binary(type) ->
        code = type |> String.split("#") |> List.last()
        classify_error(status, code, error["message"] || error["Message"], attempt)

      _ when status in [429, 503] ->
        {:error, {:retry_after, Batcher.jitter_ms(attempt), {:status, status, body}}}

      _ ->
        {:error, {:status, status, body}}
    end
  end

  # A delivery waits in its destination's batcher only when dispatch says so
  # (`ctx.batch`), the sink has not opted out (`batch: false`), and the instance
  # runs batchers; direct publishing and the lifecycle publisher send at once.
  defp batch?(ctx, opts),
    do: ctx[:batch] == true and batched?(opts) and Batcher.available?(ctx[:instance])

  # Only an ordered group serializes a destination's requests: FIFO's
  # `MessageGroupId`. A standard queue's group id (fair queues) orders nothing.
  defp ordered_group(entry, opts) do
    if fifo?(Keyword.fetch!(opts, :queue_url)), do: Map.get(entry, "MessageGroupId")
  end

  defp md5(payload), do: Base.encode16(:crypto.hash(:md5, payload), case: :lower)

  # SQS counts every attribute's name, data type and value toward the message
  # size limit, alongside the body.
  defp fits(payload, attributes, max) do
    size =
      Enum.reduce(attributes, byte_size(payload), fn {name, attribute}, acc ->
        acc + byte_size(name) + byte_size(attribute["DataType"]) +
          byte_size(attribute["StringValue"])
      end)

    if size > max,
      do: {:error, {:permanent, {:message_too_large, size, max}}},
      else: {:ok, size}
  end

  defp attributes(env, ctx) do
    env
    |> Message.attributes(ctx)
    |> Map.new(fn {name, value} -> {name, %{"DataType" => "String", "StringValue" => value}} end)
  end

  # What one message is, in a `SendMessage` request and as an entry of a
  # `SendMessageBatch`.
  defp message_entry(env, ctx, opts, payload, attributes) do
    %{"MessageBody" => payload, "MessageAttributes" => attributes}
    |> put_ordering(env, ctx, opts, fifo?(Keyword.fetch!(opts, :queue_url)))
  end

  defp put_ordering(request, env, ctx, opts, true = _fifo?) do
    Map.merge(request, %{
      "MessageGroupId" => group(env, opts),
      "MessageDeduplicationId" => deduplication_id(env, ctx)
    })
  end

  # SQS rejects `MessageDeduplicationId` on a standard queue; a group id is only
  # sent when configured (fair queues).
  defp put_ordering(request, env, _ctx, opts, false = _fifo?) do
    if Keyword.get(opts, :message_group_id) != nil,
      do: Map.put(request, "MessageGroupId", group(env, opts)),
      else: request
  end

  defp group(env, opts) do
    case Keyword.get(opts, :message_group_id) do
      nil -> "#{env.tenant_id}/#{env.source_id}"
      group when is_binary(group) -> group
      fun when is_function(fun, 1) -> fun.(env)
    end
  end

  defp deduplication_id(env, ctx) do
    case ctx[:replay_id] do
      replay_id when is_binary(replay_id) -> env.id <> ":replay:" <> replay_id
      _ -> env.id
    end
  end

  defp send_message(opts, creds, body),
    do: @codec.post(opts, creds, "AmazonSQS.SendMessage", body)

  defp reply({:ok, 200, body}, payload, _attempt) do
    expected = md5(payload)

    case decode(body) do
      {:ok, %{"MessageId" => id, "MD5OfMessageBody" => ^expected}} when is_binary(id) ->
        :ok

      {:ok, %{"MessageId" => id, "MD5OfMessageBody" => md5}}
      when is_binary(id) and is_binary(md5) ->
        {:error, {:md5_mismatch, expected, md5}}

      _ ->
        {:error, {:unexpected_reply, body}}
    end
  end

  defp reply({:ok, status, body}, _payload, attempt), do: error_reply(status, body, attempt)

  defp reply({:error, reason}, _payload, _attempt), do: {:error, reason}

  defp decode(body) when is_binary(body), do: JSON.decode(body)
  defp decode(_body), do: :error

  defp fifo?(queue_url),
    do: queue_url |> String.trim_trailing("/") |> String.ends_with?(".fifo")

  defp queue_name(queue_url) do
    (URI.parse(queue_url).path || "")
    |> String.split("/", trim: true)
    |> List.last()
  end
end
