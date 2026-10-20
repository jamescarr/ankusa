defmodule Ankusa.Sink.GooglePubSub.Batcher do
  @moduledoc """
  `Ankusa.Sink.GooglePubSub`'s codec for `Ankusa.Sink.Batcher`: up to 1 000
  deliveries to one topic become one `topics.publish` request.

  `publish` is all or nothing: a `200` names one message id per message, and a
  refusal answers the request as a whole with no per-message result. So the
  reply maps back like this:

    * a `200` with one id for each message: every entry is `:ok`; any other
      `200` fails every entry with `{:unexpected_reply, body}`.
    * a transient error (`UNAVAILABLE`, `NOT_FOUND`, a `5xx`, a timeout, no
      credentials) or a throttle (`RESOURCE_EXHAUSTED`, `429`, `503`): every
      entry gets it, with its own retry class and jitter, and retries on its own.
    * `INVALID_ARGUMENT` for one message: the request is **bisected**. Its two
      halves are published one after the other (so the entries' order is kept),
      and a half that is refused again is split again, until the refused message
      stands alone and is `{:permanent, {:pubsub, status, "INVALID_ARGUMENT",
      message}}` while every other message of the batch is delivered. One bad
      message among 999 good ones takes `2 * log2(1000)` extra requests, and the
      work is bounded: a batch of `n` spends at most `2 * (ceil(log2 n) + 1)`
      splits, after which the entries still unresolved are `{:batch_rejected,
      reason}` (transient: they retry, in other batches). The extra requests are
      counted in `ankusa_sink_batch_bisections_total`.

  `Ankusa.Sink.GooglePubSub` checks a message's size and attributes before it
  enters a batch, so the usual causes of a refusal never reach one.
  """

  @behaviour Ankusa.Sink.Batcher

  alias Ankusa.HttpClient
  alias Ankusa.Sink.GooglePubSub

  @max_entries 1_000
  # Pub/Sub limits the request to 10 MB, and the REST body carries the data in
  # base64 (4/3): this much raw data fits.
  @max_bytes 7_500_000
  @default_timeout_ms 5_000
  # Splits allowed per batch beyond the one request: `2 * (ceil(log2 n) + 1)`.
  @split_factor 2

  @impl true
  def sink, do: GooglePubSub

  @impl true
  def batch_identity(opts) do
    {opts
     |> Keyword.take([
       :project,
       :topic,
       :endpoint,
       :token_provider,
       :timeout_ms,
       :req_options
     ])
     |> Map.new()}
  end

  @impl true
  def batch_config(opts, instance) do
    timeout = Keyword.get(opts, :timeout_ms, @default_timeout_ms)

    Ankusa.Sink.Batcher.config(opts, instance,
      entries: {@max_entries, @max_entries},
      bytes: {@max_bytes, @max_bytes},
      timeout_ms: timeout,
      # the longest a refused 1 000-message batch can take to isolate
      extra_requests: 2 * @split_factor * (ceil(:math.log2(@max_entries)) + 1)
    )
  end

  @impl true
  def send_batch(items, opts) do
    case auth(opts) do
      {:ok, headers} ->
        budget = @split_factor * (ceil(:math.log2(max(length(items), 2))) + 1)
        {results, requests, _budget} = publish(items, opts, headers, budget)
        {results, %{requests: requests, bisections: max(requests - 1, 0)}}

      {:error, reason} ->
        {fail_all(items, reason), %{requests: 0, bisections: 0}}
    end
  end

  # `{results, requests, splits_left}`
  defp publish(items, opts, headers, budget) do
    case outcome(post(opts, headers, Enum.map(items, & &1.message)), items) do
      {:ok, results} ->
        {results, 1, budget}

      {:fail, results} ->
        {results, 1, budget}

      {:permanent, reason} ->
        isolate(items, reason, opts, headers, budget)
    end
  end

  # A request the topic refused as invalid: one message alone is the culprit;
  # several are split, in order.
  defp isolate([_one], reason, _opts, _headers, budget),
    do: {[{:error, {:permanent, reason}}], 1, budget}

  defp isolate(items, reason, _opts, _headers, 0),
    do: {fail_all(items, {:batch_rejected, reason}), 1, 0}

  defp isolate(items, _reason, opts, headers, budget) do
    {left, right} = Enum.split(items, div(length(items), 2))
    {left_results, left_requests, budget} = publish(left, opts, headers, budget - 1)
    {right_results, right_requests, budget} = publish(right, opts, headers, budget)

    {left_results ++ right_results, 1 + left_requests + right_requests, budget}
  end

  defp outcome({:ok, 200, body}, items) do
    count = length(items)

    case decode(body) do
      {:ok, %{"messageIds" => ids}} when is_list(ids) and length(ids) == count ->
        {:ok, List.duplicate(:ok, count)}

      _ ->
        {:fail, fail_all(items, {:unexpected_reply, body})}
    end
  end

  defp outcome({:ok, status, body}, items) do
    results = Enum.map(items, &GooglePubSub.error_reply(status, body, &1.attempt))

    case results do
      [{:error, {:permanent, reason}} | _] -> {:permanent, reason}
      _ -> {:fail, results}
    end
  end

  defp outcome({:error, reason}, items), do: {:fail, fail_all(items, reason)}

  defp fail_all(items, reason), do: Enum.map(items, fn _ -> {:error, reason} end)

  defp decode(body) when is_binary(body), do: JSON.decode(body)
  defp decode(_body), do: :error

  @doc false
  # No `:token_provider` sends no credential at all (an emulator). A provider
  # that cannot produce a token is `:no_credentials`, not an anonymous request
  # that Pub/Sub would answer `UNAUTHENTICATED`.
  def auth(opts) do
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

  @doc false
  def post(opts, auth_headers, messages) do
    HttpClient.request(
      :post,
      publish_url(opts),
      auth_headers ++ [{"content-type", "application/json"}],
      JSON.encode!(%{"messages" => messages}),
      Keyword.get(opts, :timeout_ms, @default_timeout_ms),
      Keyword.get(opts, :req_options, [])
    )
  end

  defp publish_url(opts) do
    project = Keyword.fetch!(opts, :project)
    topic = Keyword.fetch!(opts, :topic)

    endpoint(opts) <>
      "/v1/projects/" <> encode(project) <> "/topics/" <> encode(topic) <> ":publish"
  end

  defp encode(segment), do: URI.encode(segment, &URI.char_unreserved?/1)

  @doc false
  def endpoint(opts) do
    opts
    |> Keyword.get(:endpoint, "https://pubsub.googleapis.com")
    |> String.trim_trailing("/")
  end
end
