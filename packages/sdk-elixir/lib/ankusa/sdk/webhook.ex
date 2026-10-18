defmodule Ankusa.SDK.Webhook do
  @moduledoc """
  The headers every receiver of Ankusa's HTTP sink needs, parsed off a request.

  `Ankusa.Sink.Http` sends these headers; the hook id is the only one that is
  always there:

  | Header | Field | Default |
  | --- | --- | --- |
  | `x-ankusa-id` | `id` | required — its absence means the request isn't a delivery |
  | `x-ankusa-source` | `source` | `""` |
  | `x-ankusa-tenant` | `tenant` | `nil` |
  | `content-type` | `content_type` | `nil` |
  | `x-ankusa-dedupe-key` | `dedupe_key` | `nil` (also when empty) |
  | `x-ankusa-replay-id` | `replay_id` | `nil` (also when empty) |
  | `x-ankusa-idempotency-key` | `idempotency_key` | `nil` (also when empty) |

  The parsed struct also carries every request header in `headers`
  (lowercased), so a receiver can read the provider's own headers the sink
  forwarded.

  Names are matched case-insensitively, so anything from a Plug `req_headers`
  list to a hand-built map works. In a list the *first* occurrence of a name
  wins, matching HTTP's rule for repeated headers.

  `receive_delivery/3` is the receiver-side decision on top of them: it checks
  the delivery's signature (HTTP sinks are signed by default) and then parses
  the headers. `Ankusa.SDK.Receiver` is a Plug around it.
  """

  alias Ankusa.SDK.{InvalidSignatureError, MissingHookIdError, Signature}

  defmodule Headers do
    @moduledoc """
    The parsed `x-ankusa-*` headers of one HTTP-sink delivery, as returned by
    `Ankusa.SDK.Webhook.parse_headers/1`.
    """

    @type t :: %__MODULE__{
            id: String.t(),
            source: String.t(),
            tenant: String.t() | nil,
            content_type: String.t() | nil,
            dedupe_key: String.t() | nil,
            replay_id: String.t() | nil,
            idempotency_key: String.t() | nil,
            headers: %{String.t() => String.t()}
          }

    defstruct [
      :id,
      :source,
      :tenant,
      :content_type,
      :dedupe_key,
      :replay_id,
      :idempotency_key,
      headers: %{}
    ]
  end

  @doc """
  Parse the Ankusa headers out of `headers`, a map or a list of
  `{name, value}` pairs (`conn.req_headers` is already the latter).

  Returns `{:error, %Ankusa.SDK.MissingHookIdError{}}` when `x-ankusa-id` is
  absent or empty.
  """
  @spec parse_headers(term()) ::
          {:ok, Headers.t()} | {:error, MissingHookIdError.t()}
  def parse_headers(headers) when is_list(headers) do
    # `Map.put_new` (not `put`): the first occurrence of a name wins.
    headers
    |> Enum.reduce(%{}, fn
      {name, value}, acc when is_binary(name) -> Map.put_new(acc, String.downcase(name), value)
      _other, acc -> acc
    end)
    |> build()
  end

  def parse_headers(headers) when is_map(headers) do
    headers
    |> Enum.map(fn {name, value} -> {to_string(name) |> String.downcase(), value} end)
    |> Map.new()
    |> build()
  end

  def parse_headers(_headers), do: build(%{})

  @doc """
  Decide whether to accept one HTTP-sink delivery, and parse its headers.

  `Ankusa.Sink.Http` signs every delivery unless it is configured
  `allow_unsigned: true`, so a receiver is closed unless it is told
  otherwise. Options:

    * `:secrets` — the sink's secret (`whsec_` + base64, or any other string
      used as its own bytes), or a list of them during a rotation. With any,
      the delivery MUST verify (`Ankusa.SDK.Signature.verify/4`, same errors,
      `:tolerance_seconds` and `:now`); `:allow_unsigned` does not weaken it.
    * `:allow_unsigned` — `true` accepts a delivery with no secret configured,
      unverified. Default `false`: with no `:secrets` every delivery is
      refused with an `invalid_secret` `Ankusa.SDK.InvalidSignatureError`.
    * `:tolerance_seconds`, `:now` — see `Ankusa.SDK.Signature.verify/4`.

  Returns `{:ok, %{headers: headers, verified: verified?}}` (`verified` is
  `false` only for an allowed unsigned delivery),
  `{:error, %Ankusa.SDK.InvalidSignatureError{}}`
  (answer `401`), or `{:error, %Ankusa.SDK.MissingHookIdError{}}` (`400`). The
  signature is judged first, so a delivery that fails it is a `401` even when
  it also lacks `x-ankusa-id`. `body` must be the raw bytes received.
  """
  @spec receive_delivery(term(), binary(), keyword()) ::
          {:ok, %{headers: Headers.t(), verified: boolean()}}
          | {:error, InvalidSignatureError.t() | MissingHookIdError.t()}
  def receive_delivery(headers, body, opts \\ []) when is_binary(body) do
    secrets = opts |> Keyword.get(:secrets, []) |> List.wrap()
    allow_unsigned? = Keyword.get(opts, :allow_unsigned, false)

    with {:ok, verified?} <- check_signature(headers, body, secrets, allow_unsigned?, opts),
         {:ok, parsed} <- parse_headers(headers) do
      {:ok, %{headers: parsed, verified: verified?}}
    end
  end

  defp check_signature(_headers, _body, [], false, _opts) do
    {:error,
     %InvalidSignatureError{
       message:
         "no secret configured: deliveries are signed, so the receiver needs the sink's " <>
           "secret (or allow_unsigned: true to accept unsigned ones)",
       code: "invalid_secret",
       field: nil
     }}
  end

  defp check_signature(_headers, _body, [], true, _opts), do: {:ok, false}

  defp check_signature(headers, body, secrets, _allow_unsigned?, opts) do
    case Signature.verify(headers, body, secrets, Keyword.take(opts, [:tolerance_seconds, :now])) do
      {:ok, _verified} -> {:ok, true}
      {:error, _error} = error -> error
    end
  end

  defp build(headers) do
    case fetch(headers, "x-ankusa-id") do
      id when is_binary(id) and id != "" ->
        {:ok,
         %Headers{
           id: id,
           source: fetch(headers, "x-ankusa-source") || "",
           tenant: fetch(headers, "x-ankusa-tenant"),
           content_type: fetch(headers, "content-type"),
           dedupe_key: non_empty(headers, "x-ankusa-dedupe-key"),
           replay_id: non_empty(headers, "x-ankusa-replay-id"),
           idempotency_key: non_empty(headers, "x-ankusa-idempotency-key"),
           headers: headers
         }}

      _ ->
        {:error, %MissingHookIdError{message: "missing x-ankusa-id header"}}
    end
  end

  # `dedupe_key`, `replay_id` and `idempotency_key` are `nil` when the header is
  # absent or empty, so a consumer never keys on `""`.
  defp non_empty(headers, name) do
    case fetch(headers, name) do
      "" -> nil
      value -> value
    end
  end

  defp fetch(headers, name) do
    case Map.get(headers, name) do
      value when is_binary(value) -> value
      value when is_number(value) -> to_string(value)
      value when is_atom(value) and not is_nil(value) -> to_string(value)
      _other -> nil
    end
  end
end
