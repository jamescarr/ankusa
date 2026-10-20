defmodule Ankusa.Sink.SQS.Batcher do
  @moduledoc """
  `Ankusa.Sink.SQS`'s codec for `Ankusa.Sink.Batcher`: up to 10 deliveries to one
  queue become one SigV4-signed `SendMessageBatch`.

  Each entry's `Id` is its position in the request (`"0"` … `"9"`), never the
  hook's id: an `Id` must be unique within the request, at most 80 characters of
  `[A-Za-z0-9_-]`, and a hook id has `:` in it and can appear twice in one batch
  (an original and its replay). The `MessageGroupId` and `MessageDeduplicationId`
  of an entry are exactly what a single `SendMessage` carries.

  SQS answers `200` for a batch that partly failed. The reply is mapped back
  per entry:

    * `Successful` with the `MD5OfMessageBody` of the body sent: `:ok`; with
      another MD5: `{:md5_mismatch, expected, got}`.
    * `Failed`: classified by its `Code` through
      `Ankusa.Sink.SQS.classify_error/4`, so a throttled entry is retried with
      jitter and a refused one (`InvalidMessageContents`, …) goes to the DLQ,
      each on its own.
    * an entry in neither list, or a `200` that is not a batch reply:
      `{:unexpected_reply, body}`.

  A reply that is not a `200` (an error document, a proxy, a transport error)
  fails every entry with the same class a single send gets.
  """

  @behaviour Ankusa.Sink.Batcher

  alias Ankusa.AWS.Credentials
  alias Ankusa.HttpClient
  alias Ankusa.Sink.SQS

  @max_entries 10
  # SQS's own limit is 1 MiB for the sum of a batch's messages; a batch is cut
  # well below it so one large message does not make every other one wait on it.
  @default_max_bytes 262_144
  @max_bytes 1_048_576
  @default_timeout_ms 5_000

  @impl true
  def sink, do: SQS

  @impl true
  def batch_identity(opts) do
    {opts
     |> Keyword.take([
       :queue_url,
       :region,
       :endpoint,
       :access_key_id,
       :secret_access_key,
       :session_token,
       :timeout_ms,
       :req_options,
       :env
     ])
     |> Map.new()}
  end

  @impl true
  def batch_config(opts, instance) do
    Ankusa.Sink.Batcher.config(opts, instance,
      entries: {@max_entries, @max_entries},
      bytes: {@default_max_bytes, @max_bytes},
      timeout_ms: Keyword.get(opts, :timeout_ms, @default_timeout_ms)
    )
  end

  @impl true
  def send_batch(items, opts) do
    case Credentials.get(opts) do
      {:ok, creds} ->
        {request(items, opts, creds), %{requests: 1, bisections: 0}}

      {:error, reason} ->
        {fail_all(items, reason), %{requests: 0, bisections: 0}}
    end
  end

  defp request(items, opts, creds) do
    entries =
      items
      |> Enum.with_index()
      |> Enum.map(fn {item, index} -> Map.put(item.entry, "Id", Integer.to_string(index)) end)

    body =
      JSON.encode!(%{"QueueUrl" => Keyword.fetch!(opts, :queue_url), "Entries" => entries})

    case post(opts, creds, "AmazonSQS.SendMessageBatch", body) do
      {:ok, 200, reply} -> entry_results(items, reply)
      {:ok, status, reply} -> Enum.map(items, &SQS.error_reply(status, reply, &1.attempt))
      {:error, reason} -> fail_all(items, reason)
    end
  end

  defp entry_results(items, body) do
    with {:ok, %{} = reply} <- decode(body),
         successful when is_list(successful) <- Map.get(reply, "Successful", []),
         failed when is_list(failed) <- Map.get(reply, "Failed", []) do
      md5s = Map.new(successful, &{&1["Id"], &1["MD5OfMessageBody"]})
      failures = Map.new(failed, &{&1["Id"], &1})

      items
      |> Enum.with_index()
      |> Enum.map(fn {item, index} ->
        id = Integer.to_string(index)

        case {md5s, failures} do
          {%{^id => md5}, _} when md5 == item.md5 -> :ok
          {%{^id => md5}, _} when is_binary(md5) -> {:error, {:md5_mismatch, item.md5, md5}}
          {_, %{^id => failure}} -> entry_failure(failure, item)
          _ -> {:error, {:unexpected_reply, body}}
        end
      end)
    else
      _ -> fail_all(items, {:unexpected_reply, body})
    end
  end

  # `{"Id": "3", "Code": "InvalidParameterValue", "Message": "...", "SenderFault": true}`
  defp entry_failure(%{"Code" => code} = failure, item) when is_binary(code) do
    status = if failure["SenderFault"] == true, do: 400, else: 500
    SQS.classify_error(status, code, failure["Message"], item.attempt)
  end

  defp entry_failure(failure, _item), do: {:error, {:unexpected_reply, failure}}

  defp fail_all(items, reason), do: Enum.map(items, fn _ -> {:error, reason} end)

  defp decode(body) when is_binary(body), do: JSON.decode(body)
  defp decode(_body), do: :error

  @doc false
  # The signature covers the host header, so it is derived from the same URL
  # handed to the signer. Temporary credentials add their session token as a
  # signed header.
  def post(opts, creds, target, body) do
    url = endpoint(opts) <> "/"

    headers =
      [
        {"host", authority(url)},
        {"content-type", "application/x-amz-json-1.0"},
        {"x-amz-target", target}
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

  @doc false
  def endpoint(opts) do
    case Keyword.get(opts, :endpoint) do
      nil ->
        url = Keyword.fetch!(opts, :queue_url)
        URI.parse(url).scheme <> "://" <> authority(url)

      endpoint ->
        String.trim_trailing(endpoint, "/")
    end
  end

  @doc false
  def authority(url) do
    %URI{host: host, port: port, scheme: scheme} = URI.parse(url)
    default = if scheme == "https", do: 443, else: 80
    if port == default, do: host, else: "#{host}:#{port}"
  end
end
