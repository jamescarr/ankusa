defmodule Ankusa.Sink.SQS do
  @moduledoc """
  Sends delivered hooks to an **Amazon SQS** queue, standard or FIFO (or to
  anything that speaks SQS's JSON protocol, such as the `floci` emulator in
  `docker-compose.integration.yml`). The message body is
  `Ankusa.Sink.Message`, byte-identical to what `Ankusa.Sink.Kafka` and
  `Ankusa.Sink.NATS` publish: inline up to `:inline_max_bytes`, a claim ticket
  above it.

  Each delivery is one SigV4-signed `SendMessage` `POST` through
  `Ankusa.HttpClient` (Req's connection pool, no hidden retries or redirects).
  The sink starts no processes and needs nothing beyond core's `req` and
  `aws_signature`. Credentials come from `Ankusa.AWS.Credentials`: static
  opts, then the `AWS_*` environment, web identity (IRSA), then the EC2
  instance role (IMDSv2).

  This sink **never creates a queue**. A queue's type, retention, visibility
  timeout, redrive policy and encryption are an operator's contract, not
  something the first hook to arrive should decide. A queue that does not
  exist is `{:error, {:sqs, 400, "QueueDoesNotExist", message}}`, which flows
  into the source's `Ankusa.RetryPolicy`, then the DLQ. (Compare
  `Ankusa.Sink.Kafka` and `Ankusa.Sink.NATS`, which likewise never create
  their topic or stream.)

  ## Publishing is confirmed

  `deliver/3` returns `:ok` only on a `200` from `SendMessage` whose
  `MD5OfMessageBody` matches the MD5 of the body sent. SQS answers `200` once
  it has stored the message redundantly, so the sink keeps the default
  `c:Ankusa.Sink.durable?/1` of `true`. Everything else is an error (see
  "Error classes" in `Ankusa.Sink`):

    * `{:permanent, {:message_too_large, size, max}}` — body plus message
      attributes over `:max_message_bytes`, refused before any request.
    * `{:permanent, {:sqs, status, code, message}}` — SQS refused the message
      itself: `InvalidMessageContents` (characters outside SQS's allowed set)
      or `InvalidParameterValue`. Dead-lettered at once.
    * `{:sqs, status, code, message}` — any other SQS error
      (`QueueDoesNotExist`, `AccessDenied`, throttling, a 5xx): an operator
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
  """

  @behaviour Ankusa.Sink

  alias Ankusa.{Envelope, HttpClient}
  alias Ankusa.Sink.{Description, Message}

  @default_max_message_bytes 1_048_576
  @default_timeout_ms 5_000

  # SQS refusing the message itself: no retry changes the answer. Both spellings
  # of the parameter error, because the JSON protocol's `__type` for it is not
  # pinned down.
  @permanent_codes ~w(InvalidMessageContents InvalidParameterValue InvalidParameterValueException)

  @impl true
  def deliver(%Envelope{} = env, ctx, opts) do
    max = Keyword.get(opts, :max_message_bytes, @default_max_message_bytes)

    with {:ok, payload} <- Message.encode(env, ctx, Message.inline_max_bytes(opts)),
         attributes = attributes(env, ctx),
         :ok <- fits(payload, attributes, max),
         {:ok, creds} <- Ankusa.AWS.Credentials.get(opts) do
      body = request_body(env, ctx, opts, payload, attributes)

      opts
      |> send_message(creds, body)
      |> reply(payload)
    end
  end

  @impl true
  def inline_max_bytes(opts), do: Message.inline_max_bytes(opts)

  @impl true
  def describe(_subject, opts) do
    queue_url = Keyword.fetch!(opts, :queue_url)
    name = queue_name(queue_url)

    %Description{
      protocol: "sqs",
      host: authority(endpoint(opts) <> "/"),
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
      else: :ok
  end

  defp attributes(env, ctx) do
    [
      {"ankusa_id", env.id},
      {"ankusa_source_id", env.source_id},
      {"ankusa_tenant_id", env.tenant_id},
      {"ankusa_message_version", "1"},
      {"ankusa_idempotency_key", Envelope.idempotency_key(env)},
      {"content_type", "application/json"},
      {"ankusa_dedupe_key", env.dedupe_key},
      {"ankusa_replay_id", ctx[:replay_id]}
    ]
    |> Enum.filter(fn {_name, value} -> is_binary(value) and value != "" end)
    |> Map.new(fn {name, value} -> {name, %{"DataType" => "String", "StringValue" => value}} end)
  end

  defp request_body(env, ctx, opts, payload, attributes) do
    queue_url = Keyword.fetch!(opts, :queue_url)

    %{"QueueUrl" => queue_url, "MessageBody" => payload, "MessageAttributes" => attributes}
    |> put_ordering(env, ctx, opts, fifo?(queue_url))
    |> JSON.encode!()
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

  defp send_message(opts, creds, body) do
    url = endpoint(opts) <> "/"

    # The signature covers the host header, so it is derived from the same URL
    # handed to the signer. Temporary credentials add their session token as a
    # signed header.
    headers =
      [
        {"host", authority(url)},
        {"content-type", "application/x-amz-json-1.0"},
        {"x-amz-target", "AmazonSQS.SendMessage"}
      ] ++
        if(creds.session_token, do: [{"x-amz-security-token", creds.session_token}], else: [])

    signed =
      :aws_signature.sign_v4(
        creds.access_key_id,
        creds.secret_access_key,
        Keyword.fetch!(opts, :region),
        "sqs",
        :calendar.universal_time(),
        "POST",
        url,
        headers,
        body,
        []
      )

    HttpClient.request(
      :post,
      url,
      signed,
      body,
      Keyword.get(opts, :timeout_ms, @default_timeout_ms),
      Keyword.get(opts, :req_options, [])
    )
  end

  defp reply({:ok, 200, body}, payload) do
    expected = Base.encode16(:crypto.hash(:md5, payload), case: :lower)

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

  # `{"__type": "com.amazonaws.sqs#QueueDoesNotExist", "message": "..."}` is an
  # SQS error; any other body came from something in front of it.
  defp reply({:ok, status, body}, _payload) do
    case decode(body) do
      {:ok, %{"__type" => type} = error} when is_binary(type) ->
        code = type |> String.split("#") |> List.last()
        reason = {:sqs, status, code, error["message"] || error["Message"]}

        if code in @permanent_codes,
          do: {:error, {:permanent, reason}},
          else: {:error, reason}

      _ ->
        {:error, {:status, status, body}}
    end
  end

  defp reply({:error, reason}, _payload), do: {:error, reason}

  defp decode(body) when is_binary(body), do: JSON.decode(body)
  defp decode(_body), do: :error

  defp fifo?(queue_url),
    do: queue_url |> String.trim_trailing("/") |> String.ends_with?(".fifo")

  defp queue_name(queue_url) do
    (URI.parse(queue_url).path || "")
    |> String.split("/", trim: true)
    |> List.last()
  end

  defp endpoint(opts) do
    case Keyword.get(opts, :endpoint) do
      nil ->
        url = Keyword.fetch!(opts, :queue_url)
        URI.parse(url).scheme <> "://" <> authority(url)

      endpoint ->
        String.trim_trailing(endpoint, "/")
    end
  end

  defp authority(url) do
    %URI{host: host, port: port, scheme: scheme} = URI.parse(url)
    default = if scheme == "https", do: 443, else: 80
    if port == default, do: host, else: "#{host}:#{port}"
  end
end
