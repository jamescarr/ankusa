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
  `{:error, {:permanent, {:config, {:pubsub, 404, "NOT_FOUND", message}}}}`:
  dead-lettered at once, since only an operator creating the topic fixes it, and
  a DLQ replay then re-publishes the hooks. (Compare `Ankusa.Sink.SQS`,
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
    * `{:permanent, {:config, {:pubsub, status, google_status, message}}}` —
      Pub/Sub refused this sink's configuration or credentials: `NOT_FOUND`
      (topic), `PERMISSION_DENIED`, `UNAUTHENTICATED`. Dead-lettered at once and
      counted in `ankusa_dispatch_config_errors_total{sink}`; fix the source,
      then replay the dead letters.
    * `{:retry_after, ms, {:pubsub, status, google_status, message}}` — Pub/Sub
      throttled the publish (`RESOURCE_EXHAUSTED`, or any HTTP `429` or `503`).
      `ms` is a full-jitter draw (`Ankusa.Sink.throttle_after_ms/1`).
    * `{:pubsub, status, google_status, message}` — any other Pub/Sub error
      (`UNAVAILABLE`, `INTERNAL`, ...): transient, the retry policy.
    * `{:status, status, body}` — a non-200 response that is not a Google error
      document (a proxy, a load balancer); a `429` or `503` is a
      `{:retry_after, ms, {:status, status, body}}`.
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
  message of it, each retrying on its own schedule; so does a config error
  (`NOT_FOUND`, `PERMISSION_DENIED`, `UNAUTHENTICATED`), which no message caused.
  `INVALID_ARGUMENT` means one message was refused, and dead-lettering them all
  would lose the good ones, so the request is **bisected** (see
  `Ankusa.Sink.GooglePubSub.Batcher`): the one bad message is `{:permanent, ...}`
  and the others are delivered, for `2 * log2(n)` extra requests. The size and
  attribute checks that make a message invalid run before it joins a batch, so
  this is the exception.

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
    * `:token_provider`    — `{mod, fun, args}`, applied once per request, and
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
    * `:batch_linger_ms`   — default 20 (1 to 60,000), how long a batch waits to
                             fill
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

  use Ankusa.Sink.Messaging

  alias Ankusa.{Envelope, HttpClient}
  alias Ankusa.Sink.{Batcher, Description, Message, Messaging}
  alias Ankusa.Sink.GooglePubSub.Batcher, as: Codec

  @default_max_message_bytes 10_000_000
  @binding_version "0.2.0"

  # Pub/Sub refusing the message itself: no retry changes the answer.
  @permanent_statuses ~w(INVALID_ARGUMENT)

  @impl Ankusa.Sink.Messaging
  def max_message_bytes(opts),
    do: Keyword.get(opts, :max_message_bytes, @default_max_message_bytes)

  # What only the operator can fix: a credential or a role Pub/Sub refuses, a
  # topic that is not there. Dead-lettered at once, and counted as a config error.
  @config_statuses ~w(PERMISSION_DENIED NOT_FOUND UNAUTHENTICATED)

  # Pub/Sub asking the sender to slow down.
  @throttle_statuses ~w(RESOURCE_EXHAUSTED)

  @impl true
  def publish(payload, %Envelope{} = env, ctx, opts) do
    max = max_message_bytes(opts)
    attributes = Map.new(Message.attributes(env, ctx))
    # An ordered sink's key is the lane's (`ctx.ordering_key`, ADR 0004); else the
    # sink's own `:ordering_key`, which keeps its meaning.
    key =
      case ctx[:ordering_key] do
        lane_key when is_binary(lane_key) -> lane_key
        _ -> Messaging.resolve(Keyword.get(opts, :ordering_key), env)
      end

    size = size(payload, attributes, key)

    # Checked before a message joins a batch: a request is refused as a whole, so
    # a message over a limit must never be in one.
    with :ok <- Message.fits(size, max) do
      message = message(payload, attributes, key)

      if batch?(ctx, opts) do
        # One message of a `topics.publish` of up to 1 000; the request's reply
        # is mapped back to this one.
        Batcher.deliver(Codec, ctx.instance, opts, %{
          tenant: env.tenant_id || "",
          bytes: size,
          group: if(is_binary(key) and key != "", do: key),
          attempt: ctx.attempt,
          item: %{message: message, attempt: ctx.attempt}
        })
      else
        with {:ok, auth_headers} <- Codec.auth(opts) do
          opts
          |> Codec.post(auth_headers, [message])
          |> reply(ctx.attempt)
        end
      end
    end
  end

  @impl Ankusa.Sink
  def batched?(opts), do: Keyword.get(opts, :batch, true) == true

  @impl true
  def describe(_subject, opts) do
    project = Keyword.fetch!(opts, :project)
    topic = Keyword.fetch!(opts, :topic)

    message_bindings =
      case Messaging.static(Keyword.get(opts, :ordering_key)) do
        key when is_binary(key) ->
          %{"googlepubsub" => %{"orderingKey" => key, "bindingVersion" => @binding_version}}

        nil ->
          %{}
      end

    %Description{
      protocol: "googlepubsub",
      host: HttpClient.authority(Codec.endpoint(opts)),
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
  Raise `ArgumentError` naming the key unless `opts`' `:batch` is a boolean and
  its `batch_*` options are in range (see "opts"). The image's config loader
  calls it, so a bad value fails at load instead of at the first delivery.
  """
  @spec validate_opts!(keyword()) :: :ok
  def validate_opts!(opts), do: Batcher.validate_opts!(Codec, opts)

  @doc """
  The `{:error, reason}` for a Pub/Sub error `google_status` that answered a whole
  request with HTTP `http_status`.

    * `INVALID_ARGUMENT`: `{:permanent, {:pubsub, http_status, google_status, message}}`
    * `PERMISSION_DENIED`, `NOT_FOUND`, `UNAUTHENTICATED`:
      `{:permanent, {:config, {:pubsub, http_status, google_status, message}}}`
    * `RESOURCE_EXHAUSTED`, or any `429`/`503`: `{:retry_after, ms, reason}`, `ms`
      full jitter for `attempt` (`Ankusa.Sink.throttle_after_ms/1`)
    * anything else: `{:pubsub, http_status, google_status, message}`, transient
  """
  @spec classify_error(integer(), String.t(), String.t() | nil, pos_integer()) ::
          {:error, term()}
  def classify_error(http_status, google_status, message, attempt) do
    reason = {:pubsub, http_status, google_status, message}

    cond do
      google_status in @permanent_statuses ->
        {:error, {:permanent, reason}}

      google_status in @config_statuses ->
        {:error, {:permanent, {:config, reason}}}

      google_status in @throttle_statuses or http_status in [429, 503] ->
        throttled(reason, attempt)

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
        throttled({:status, http_status, body}, attempt)

      _ ->
        {:error, {:status, http_status, body}}
    end
  end

  defp throttled(reason, attempt),
    do: {:error, {:retry_after, Ankusa.Sink.throttle_after_ms(attempt), reason}}

  # A delivery waits in its topic's batcher only when dispatch says so
  # (`ctx.batch`), the sink has not opted out (`batch: false`), and the instance
  # runs batchers; direct publishing and the lifecycle publisher send at once.
  defp batch?(ctx, opts),
    do: ctx[:batch] == true and batched?(opts) and Batcher.available?(ctx[:instance])

  # Pub/Sub counts the data, every attribute's name and value, and the ordering
  # key toward the message size limit.
  defp size(payload, attributes, key) do
    Enum.reduce(attributes, byte_size(payload) + byte_size(key || ""), fn {name, value}, acc ->
      acc + byte_size(name) + byte_size(value)
    end)
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
end
