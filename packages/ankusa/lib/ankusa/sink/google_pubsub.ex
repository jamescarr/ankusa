defmodule Ankusa.Sink.GooglePubSub do
  @moduledoc """
  Publishes delivered hooks to a **Google Cloud Pub/Sub** topic (or to anything
  that speaks Pub/Sub's REST API, such as the `floci-gcp` emulator in
  `docker-compose.integration.yml`). The message data is `Ankusa.Sink.Message`,
  byte-identical to what `Ankusa.Sink.Kafka`, `Ankusa.Sink.NATS` and
  `Ankusa.Sink.SQS` publish: inline up to `:inline_max_bytes`, a claim ticket
  above it.

  Dispatch's deliveries are **batched** (see "Batching"): up to 1 000 of them to
  one topic are published in one `topics.publish` `POST`. Anything else (direct
  publishing, `batch: false`) is one request per delivery. Both go through
  `Ankusa.HttpClient` (Req's connection pool, no hidden retries or redirects).
  The sink needs nothing beyond core's `req`; its batchers are started by
  `Ankusa.Instance`. Credentials are a bearer token from a caller-supplied
  `:token_provider`, the way `Ankusa.BlobStore.GCS` gets one: Ankusa bundles no
  OAuth client.

  This sink **never creates a topic or a subscription**. Retention, schema,
  encryption and who may subscribe are an operator's contract, not something
  the first hook to arrive should decide. A topic that does not exist is
  `{:pubsub, 404, "NOT_FOUND", message}`, which is transient: it flows into the
  source's `Ankusa.RetryPolicy`, then the DLQ. (Compare `Ankusa.Sink.SQS`,
  `Ankusa.Sink.Kafka` and `Ankusa.Sink.NATS`, which likewise never create their
  queue, topic or stream.)

  ## Publishing is confirmed

  `deliver/3` returns `:ok` only on a `200` whose reply carries a `messageIds`
  entry for each message sent (one, or a batch's). Pub/Sub answers `200` once it
  has persisted the messages, so the sink keeps the default
  `c:Ankusa.Sink.durable?/1` of `true`. Everything else is an error (see "Error
  classes" in `Ankusa.Sink`):

    * `{:permanent, {:message_too_large, size, max}}` — data plus attributes
      plus ordering key over `:max_message_bytes`, refused before any request.
    * `{:permanent, {:pubsub, status, "INVALID_ARGUMENT", message}}` — Pub/Sub
      refused the message itself. Dead-lettered at once.
    * `{:retry_after, ms, {:pubsub, status, "RESOURCE_EXHAUSTED", message}}` —
      Pub/Sub asked for less traffic (a proxy's `429`/`503` is the same): the
      next attempt waits `ms`, full jitter, or the policy's backoff.
    * `{:pubsub, status, google_status, message}` — any other Pub/Sub error
      (`NOT_FOUND` topic, `PERMISSION_DENIED`, `UNAUTHENTICATED`,
      `UNAVAILABLE`, ...): an operator can fix these, so they get the retry
      policy, then the DLQ.
    * `{:status, status, body}` — a non-200 response that is not a Google error
      document (a proxy, a load balancer).
    * `{:unexpected_reply, body}`, `:no_credentials` (the `:token_provider`
      answered `:error`; no request is sent) and transport errors — retried.

  ## Caveats

    * A topic with **no subscription** (and no topic message retention) still
      answers `200` and discards the message. The sink cannot detect this.
    * There is no publish-side deduplication. Delivery is at-least-once, so
      consumers dedupe on the message's `idempotency_key` (the
      `ankusa_idempotency_key` attribute).
    * An ordering key only orders messages on a subscription with message
      ordering enabled, published through one regional endpoint. Ankusa's own
      deliveries are unordered anyway (see `Ankusa.Sink`), so no key is sent
      unless `:ordering_key` is set; a key also caps that key's throughput at
      about 1 MB/s.

  ## Batching

  A delivery from dispatch waits in its topic's `Ankusa.Sink.Batcher` (one per
  topic and set of credentials) and is published in one `topics.publish` with up
  to 999 others, 20 ms after the first arrived or as soon as the batch is full
  (1,000 messages, 7.5 MB of data). `deliver/3` returns when the request's
  response is known: the hook's row stays claimed until then, so a batcher that
  dies loses nothing and the attempt is retried.

  `publish` has no per-message results: the topic accepts the request or refuses
  it as a whole. A transient error or throttle (`RESOURCE_EXHAUSTED`) fails every
  message of it, each retrying on its own schedule. `INVALID_ARGUMENT` means one
  message was refused, and dead-lettering them all would lose the good ones, so
  the request is **bisected** (see `Ankusa.Sink.GooglePubSub.Batcher`): the one
  bad message is `{:permanent, ...}` and the others are delivered, for
  `2 * log2(n)` extra requests. The size and attribute checks that make a message
  invalid run before it joins a batch, so this is the exception.

  A batch may hold hooks of several tenants; entries are taken round-robin
  across tenants, and a delivery that would pass the queue's ceilings is refused
  with `{:retry_after, ms, {:batch_queue_full, reason}}` (see
  `Ankusa.Sink.Batcher`). With `:ordering_key` set, a key has one request in
  flight at a time and its messages leave in the order they were admitted.

  ## What each message carries

    * **data** — the `Ankusa.Sink.Message` JSON, base64 on the wire as the REST
      API requires.
    * **attributes** — the list `Ankusa.Sink.Message.attributes/2` builds:
      `ankusa_id`, `ankusa_source_id`, `ankusa_tenant_id`,
      `ankusa_message_version`, `ankusa_idempotency_key`, `content_type`
      (`application/json`), plus `ankusa_dedupe_key` and `ankusa_replay_id` when
      the hook carries them. `ankusa_tenant_id` is left out when the hook has no
      tenant.
    * **orderingKey** — only when `:ordering_key` yields a non-empty string.

  ## opts

    * `:project`           — required, the GCP project id
    * `:topic`             — required, the topic id (the short name, not
                             `projects/.../topics/...`)
    * `:endpoint`          — default `"https://pubsub.googleapis.com"`. Set it
                             for a regional endpoint
                             (`"https://us-east1-pubsub.googleapis.com"`) or
                             `floci-gcp` (`"http://localhost:4588"`).
    * `:token_provider`    — `{mod, fun, args}`, applied once per delivery, and
                             answering `{:ok, token}` or `:error`. Omit it to
                             send no `authorization` header (an emulator only).
    * `:ordering_key`      — `nil` (default: no key), a string, or
                             `(Envelope.t() -> String.t())`
    * `:inline_max_bytes`  — default 64 KiB (65,536), configurable
    * `:max_message_bytes` — the largest data plus attributes plus ordering key
                             sent; default `10_000_000`, Pub/Sub's 10 MB message
                             limit
    * `:timeout_ms`        — default `5_000`, for both connect and response
    * `:req_options`       — transport options for the HTTP client (a Finch
                             pool, a proxy via `:connect_options`, `plug:` for
                             `Req.Test`); see `Ankusa.HttpClient`.
    * `:batch`             — default `true`; `false` publishes every delivery in
                             its own request and counts it against
                             `dispatch.sink_concurrency` as before
    * `:batch_linger_ms`   — default 20, how long a batch waits to fill
    * `:batch_max_entries` — default and maximum 1,000
    * `:batch_max_bytes`   — default and maximum 7,500,000 of data before base64
                             (the REST body is 4/3 of it; Pub/Sub's request limit
                             is 10 MB)
    * `:batch_queue_max`, `:batch_queue_max_bytes`, `:batch_queue_tenant_max`
                           — the queue's ceilings, default 4,096 entries,
                             128 MiB and 1,024 entries a tenant
    * `:batch_max_inflight` — requests in flight per topic; default the
                             instance's `dispatch.sink_concurrency` (8)
  """

  @behaviour Ankusa.Sink

  alias Ankusa.Envelope
  alias Ankusa.Sink.{Batcher, Description, Message}

  @codec Ankusa.Sink.GooglePubSub.Batcher
  @default_max_message_bytes 10_000_000
  @binding_version "0.2.0"

  # Pub/Sub refusing the message itself: no retry changes the answer.
  @permanent_statuses ~w(INVALID_ARGUMENT)

  # Pub/Sub asking for less traffic: the next attempt waits (with full jitter)
  # as long as the policy's own backoff or longer.
  @throttle_statuses ~w(RESOURCE_EXHAUSTED)

  @impl true
  def deliver(%Envelope{} = env, ctx, opts) do
    max = Keyword.get(opts, :max_message_bytes, @default_max_message_bytes)

    with {:ok, payload} <- Message.encode(env, ctx, Message.inline_max_bytes(opts)),
         attributes = Map.new(Message.attributes(env, ctx)),
         key = ordering_key(env, opts),
         {:ok, size} <- fits(payload, attributes, key, max) do
      message = message(payload, attributes, key)

      if batch?(ctx, opts) do
        # One message of a `topics.publish` of up to 1 000; the request's reply
        # is mapped back to this one.
        Batcher.deliver(@codec, ctx.instance, opts, %{
          tenant: env.tenant_id || "",
          bytes: size,
          group: if(is_binary(key) and key != "", do: key),
          attempt: ctx[:attempt] || 1,
          item: %{message: message, attempt: ctx[:attempt] || 1}
        })
      else
        with {:ok, auth_headers} <- @codec.auth(opts) do
          opts
          |> @codec.post(auth_headers, [message])
          |> reply(ctx[:attempt] || 1)
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
    project = Keyword.fetch!(opts, :project)
    topic = Keyword.fetch!(opts, :topic)

    message_bindings =
      case Keyword.get(opts, :ordering_key) do
        key when is_binary(key) ->
          %{"googlepubsub" => %{"orderingKey" => key, "bindingVersion" => @binding_version}}

        _ ->
          %{}
      end

    %Description{
      protocol: "googlepubsub",
      host: authority(@codec.endpoint(opts)),
      address: "projects/#{project}/topics/#{topic}",
      channel_bindings: %{"googlepubsub" => %{"bindingVersion" => @binding_version}},
      message_bindings: message_bindings,
      # The `ankusa_*` values travel as message attributes, and
      # `ankusa_tenant_id` is absent without a tenant, so the document's
      # header schema (which requires it) would not describe them.
      ankusa_headers: false
    }
  end

  @doc """
  The `{:error, reason}` for a Pub/Sub error `google_status` that answered a whole
  request with HTTP `http_status`.

    * `INVALID_ARGUMENT`: `{:permanent, {:pubsub, http_status, google_status, message}}`
    * `RESOURCE_EXHAUSTED`: `{:retry_after, ms, reason}`, `ms` full jitter for `attempt`
    * anything else: `{:pubsub, http_status, google_status, message}`, transient
  """
  @spec classify_error(integer(), String.t(), String.t() | nil, pos_integer()) ::
          {:error, term()}
  def classify_error(http_status, google_status, message, attempt) do
    reason = {:pubsub, http_status, google_status, message}

    cond do
      google_status in @permanent_statuses ->
        {:error, {:permanent, reason}}

      google_status in @throttle_statuses ->
        {:error, {:retry_after, Batcher.jitter_ms(attempt), reason}}

      true ->
        {:error, reason}
    end
  end

  @doc false
  # A non-`200` answer to a whole request. `{"error": {"code": 404, "message":
  # "...", "status": "NOT_FOUND"}}` is a Pub/Sub error; any other body came from
  # something in front of it (a `429` or `503` from it is throttling all the same).
  @spec error_reply(integer(), term(), pos_integer()) :: {:error, term()}
  def error_reply(http_status, body, attempt) do
    case decode(body) do
      {:ok, %{"error" => %{"status" => status} = error}} when is_binary(status) ->
        classify_error(http_status, status, error["message"], attempt)

      _ when http_status in [429, 503] ->
        {:error, {:retry_after, Batcher.jitter_ms(attempt), {:status, http_status, body}}}

      _ ->
        {:error, {:status, http_status, body}}
    end
  end

  # A delivery waits in its topic's batcher only when dispatch says so
  # (`ctx.batch`), the sink has not opted out (`batch: false`), and the instance
  # runs batchers; direct publishing and the lifecycle publisher send at once.
  defp batch?(ctx, opts),
    do: ctx[:batch] == true and batched?(opts) and Batcher.available?(ctx[:instance])

  defp ordering_key(env, opts) do
    case Keyword.get(opts, :ordering_key) do
      nil -> nil
      key when is_binary(key) -> key
      fun when is_function(fun, 1) -> fun.(env)
    end
  end

  # Pub/Sub counts the data, every attribute's name and value, and the ordering
  # key toward the message size limit. Checked before a message joins a batch:
  # a request is refused as a whole, so a message over a limit must never be in
  # one.
  defp fits(payload, attributes, key, max) do
    size =
      Enum.reduce(attributes, byte_size(payload) + byte_size(key || ""), fn {name, value}, acc ->
        acc + byte_size(name) + byte_size(value)
      end)

    if size > max,
      do: {:error, {:permanent, {:message_too_large, size, max}}},
      else: {:ok, size}
  end

  defp message(payload, attributes, key) do
    message = %{"data" => Base.encode64(payload), "attributes" => attributes}

    if is_binary(key) and key != "",
      do: Map.put(message, "orderingKey", key),
      else: message
  end

  defp reply({:ok, 200, body}, _attempt) do
    case decode(body) do
      {:ok, %{"messageIds" => [id]}} when is_binary(id) -> :ok
      _ -> {:error, {:unexpected_reply, body}}
    end
  end

  defp reply({:ok, http_status, body}, attempt), do: error_reply(http_status, body, attempt)

  defp reply({:error, reason}, _attempt), do: {:error, reason}

  defp decode(body) when is_binary(body), do: JSON.decode(body)
  defp decode(_body), do: :error

  defp authority(url) do
    %URI{host: host, port: port, scheme: scheme} = URI.parse(url)
    default = if scheme == "https", do: 443, else: 80
    if port == default, do: host, else: "#{host}:#{port}"
  end
end
