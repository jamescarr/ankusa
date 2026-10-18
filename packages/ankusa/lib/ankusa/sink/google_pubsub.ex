defmodule Ankusa.Sink.GooglePubSub do
  @moduledoc """
  Publishes delivered hooks to a **Google Cloud Pub/Sub** topic (or to anything
  that speaks Pub/Sub's REST API, such as the `floci-gcp` emulator in
  `docker-compose.integration.yml`). The message data is `Ankusa.Sink.Message`,
  byte-identical to what `Ankusa.Sink.Kafka`, `Ankusa.Sink.NATS` and
  `Ankusa.Sink.SQS` publish: inline up to `:inline_max_bytes`, a claim ticket
  above it.

  Each delivery is one `topics.publish` `POST` through `Ankusa.HttpClient`
  (Req's connection pool, no hidden retries or redirects). The sink starts no
  processes and needs nothing beyond core's `req`. Credentials are a bearer
  token from a caller-supplied `:token_provider`, the way
  `Ankusa.BlobStore.GCS` gets one: Ankusa bundles no OAuth client.

  This sink **never creates a topic or a subscription**. Retention, schema,
  encryption and who may subscribe are an operator's contract, not something
  the first hook to arrive should decide. A topic that does not exist is
  `{:pubsub, 404, "NOT_FOUND", message}`, which is transient: it flows into the
  source's `Ankusa.RetryPolicy`, then the DLQ. (Compare `Ankusa.Sink.SQS`,
  `Ankusa.Sink.Kafka` and `Ankusa.Sink.NATS`, which likewise never create their
  queue, topic or stream.)

  ## Publishing is confirmed

  `deliver/3` returns `:ok` only on a `200` whose reply carries the one
  `messageIds` entry for the one message sent. Pub/Sub answers `200` once it has
  persisted the message, so the sink keeps the default `c:Ankusa.Sink.durable?/1`
  of `true`. Everything else is an error (see "Error classes" in `Ankusa.Sink`):

    * `{:permanent, {:message_too_large, size, max}}` — data plus attributes
      plus ordering key over `:max_message_bytes`, refused before any request.
    * `{:permanent, {:pubsub, status, "INVALID_ARGUMENT", message}}` — Pub/Sub
      refused the message itself. Dead-lettered at once.
    * `{:pubsub, status, google_status, message}` — any other Pub/Sub error
      (`NOT_FOUND` topic, `PERMISSION_DENIED`, `UNAUTHENTICATED`,
      `RESOURCE_EXHAUSTED`, `UNAVAILABLE`, ...): an operator can fix these, so
      they get the retry policy, then the DLQ.
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
  """

  @behaviour Ankusa.Sink

  alias Ankusa.{Envelope, HttpClient}
  alias Ankusa.Sink.{Description, Message}

  @default_endpoint "https://pubsub.googleapis.com"
  @default_max_message_bytes 10_000_000
  @default_timeout_ms 5_000
  @binding_version "0.2.0"

  # Pub/Sub refusing the message itself: no retry changes the answer.
  @permanent_statuses ~w(INVALID_ARGUMENT)

  @impl true
  def deliver(%Envelope{} = env, ctx, opts) do
    max = Keyword.get(opts, :max_message_bytes, @default_max_message_bytes)

    with {:ok, payload} <- Message.encode(env, ctx, Message.inline_max_bytes(opts)),
         attributes = Map.new(Message.attributes(env, ctx)),
         key = ordering_key(env, opts),
         :ok <- fits(payload, attributes, key, max),
         {:ok, auth_headers} <- auth(opts) do
      body = request_body(payload, attributes, key)

      HttpClient.request(
        :post,
        publish_url(opts),
        auth_headers ++ [{"content-type", "application/json"}],
        body,
        Keyword.get(opts, :timeout_ms, @default_timeout_ms),
        Keyword.get(opts, :req_options, [])
      )
      |> reply()
    end
  end

  @impl true
  def inline_max_bytes(opts), do: Message.inline_max_bytes(opts)

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
      host: authority(endpoint(opts)),
      address: "projects/#{project}/topics/#{topic}",
      channel_bindings: %{"googlepubsub" => %{"bindingVersion" => @binding_version}},
      message_bindings: message_bindings,
      # The `ankusa_*` values travel as message attributes, and
      # `ankusa_tenant_id` is absent without a tenant, so the document's
      # header schema (which requires it) would not describe them.
      ankusa_headers: false
    }
  end

  defp ordering_key(env, opts) do
    case Keyword.get(opts, :ordering_key) do
      nil -> nil
      key when is_binary(key) -> key
      fun when is_function(fun, 1) -> fun.(env)
    end
  end

  # Pub/Sub counts the data, every attribute's name and value, and the ordering
  # key toward the message size limit.
  defp fits(payload, attributes, key, max) do
    size =
      Enum.reduce(attributes, byte_size(payload) + byte_size(key || ""), fn {name, value}, acc ->
        acc + byte_size(name) + byte_size(value)
      end)

    if size > max,
      do: {:error, {:permanent, {:message_too_large, size, max}}},
      else: :ok
  end

  # No `:token_provider` sends no credential at all (an emulator). A provider
  # that cannot produce a token is `:no_credentials`, not an anonymous request
  # that Pub/Sub would answer `UNAUTHENTICATED`.
  defp auth(opts) do
    case Keyword.get(opts, :token_provider) do
      nil ->
        {:ok, []}

      {mod, fun, args} ->
        case apply(mod, fun, args) do
          {:ok, token} when is_binary(token) -> {:ok, [{"authorization", "Bearer " <> token}]}
          :error -> {:error, :no_credentials}
        end
    end
  end

  defp request_body(payload, attributes, key) do
    message = %{"data" => Base.encode64(payload), "attributes" => attributes}

    message =
      if is_binary(key) and key != "",
        do: Map.put(message, "orderingKey", key),
        else: message

    JSON.encode!(%{"messages" => [message]})
  end

  defp publish_url(opts) do
    project = Keyword.fetch!(opts, :project)
    topic = Keyword.fetch!(opts, :topic)

    endpoint(opts) <>
      "/v1/projects/" <> encode(project) <> "/topics/" <> encode(topic) <> ":publish"
  end

  defp encode(segment), do: URI.encode(segment, &URI.char_unreserved?/1)

  defp reply({:ok, 200, body}) do
    case decode(body) do
      {:ok, %{"messageIds" => [id]}} when is_binary(id) -> :ok
      _ -> {:error, {:unexpected_reply, body}}
    end
  end

  # `{"error": {"code": 404, "message": "...", "status": "NOT_FOUND"}}` is a
  # Pub/Sub error; any other body came from something in front of it.
  defp reply({:ok, http_status, body}) do
    case decode(body) do
      {:ok, %{"error" => %{"status" => status} = error}} when is_binary(status) ->
        reason = {:pubsub, http_status, status, error["message"]}

        if status in @permanent_statuses,
          do: {:error, {:permanent, reason}},
          else: {:error, reason}

      _ ->
        {:error, {:status, http_status, body}}
    end
  end

  defp reply({:error, reason}), do: {:error, reason}

  defp decode(body) when is_binary(body), do: JSON.decode(body)
  defp decode(_body), do: :error

  defp endpoint(opts) do
    opts |> Keyword.get(:endpoint, @default_endpoint) |> String.trim_trailing("/")
  end

  defp authority(url) do
    %URI{host: host, port: port, scheme: scheme} = URI.parse(url)
    default = if scheme == "https", do: 443, else: 80
    if port == default, do: host, else: "#{host}:#{port}"
  end
end
