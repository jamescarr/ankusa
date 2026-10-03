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

  The parsed struct also carries every request header in `headers`
  (lowercased), so a receiver can read the provider's own headers the sink
  forwarded.

  Names are matched case-insensitively, so anything from a Plug `req_headers`
  list to a hand-built map works. In a list the *first* occurrence of a name
  wins, matching HTTP's rule for repeated headers.
  """

  alias Ankusa.SDK.MissingHookIdError

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
            headers: %{String.t() => String.t()}
          }

    defstruct [:id, :source, :tenant, :content_type, :dedupe_key, :replay_id, headers: %{}]
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
           headers: headers
         }}

      _ ->
        {:error, %MissingHookIdError{message: "missing x-ankusa-id header"}}
    end
  end

  # `dedupe_key` and `replay_id` are `nil` when the header is absent or empty,
  # so a consumer never keys on `""`.
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
