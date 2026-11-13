defmodule Ankusa.Gateway.Authz.Table.Source.Http do
  @moduledoc """
  A `table` source behind an HTTP endpoint: pages of permission rows
  (`type: http`).

      source:
        type: http
        url: https://permissions.internal.example/rows
        headers: {authorization: "Bearer ..."}

  `GET url` answers a JSON object whose array at `rows_path` (`"rows"`) holds
  the rows, and whose value at `next_path` (`"next"`) is a cursor to the next
  page (absent or `null` ends the listing). Pages are fetched as
  `url?<cursor_param>=<cursor>`, `cursor_param` defaulting to `"cursor"`.

  | Key | Default | |
  | --- | --- | --- |
  | `url` | required | `https://…`, or `http://` to a loopback host; never a template |
  | `headers` | `%{}` | at most 16 request headers, values redacted |
  | `rows_path` | `"rows"` | dotted path to the row array (an integer segment indexes an array) |
  | `next_path` | `"next"` | dotted path to the cursor |
  | `cursor_param` | `"cursor"` | the query parameter the cursor goes in |
  | `timeout_ms` | `15000` | per request |
  | `max_pages` | `10000` | a listing longer than this is an error |
  | `req_options` | `[]` | Elixir config only: transport options passed to `Ankusa.HttpClient` (a test's `plug`), never a change to the request |

  Every page is read with a 16 MiB response cap. There is no `version/2`:
  the listing is the version, so every `refresh_ms` fetches it.
  """

  @behaviour Ankusa.Gateway.Authz.Table.Source

  alias Ankusa.Gateway.Options

  @keys [
    :url,
    :headers,
    :rows_path,
    :next_path,
    :cursor_param,
    :timeout_ms,
    :max_pages,
    :req_options
  ]
  @path_segment ~r/\A[A-Za-z0-9_-]+\z/
  @param ~r/\A[A-Za-z0-9_-]+\z/
  @max_headers 16
  @max_response_bytes 16_777_216

  @impl true
  @spec init!(map(), String.t()) :: map()
  def init!(opts, key) do
    given = Options.map!(opts, key)
    Options.unknown_keys!(given, @keys, key)

    %{
      url: url!(Map.get(given, :url), "#{key}.url"),
      headers: headers!(Map.get(given, :headers, %{}), "#{key}.headers"),
      rows_path: path!(Map.get(given, :rows_path, "rows"), "#{key}.rows_path"),
      next_path: path!(Map.get(given, :next_path, "next"), "#{key}.next_path"),
      cursor_param: param!(Map.get(given, :cursor_param, "cursor"), "#{key}.cursor_param"),
      timeout_ms:
        Options.int_in!(Map.get(given, :timeout_ms, 15_000), 100..600_000, "#{key}.timeout_ms"),
      max_pages:
        Options.int_in!(Map.get(given, :max_pages, 10_000), 1..1_000_000, "#{key}.max_pages"),
      req_options: req_options!(Map.get(given, :req_options, []), "#{key}.req_options")
    }
  end

  defp url!(url, key) when is_binary(url) do
    uri = URI.parse(url)

    cond do
      String.contains?(url, "{{") ->
        raise ArgumentError, "#{key} must be a literal URL (its origin cannot be templated)"

      uri.scheme not in ["https", "http"] or uri.host in [nil, ""] ->
        raise ArgumentError,
              "#{key} must be an https:// (or loopback http://) URL, got #{inspect(url)}"

      uri.scheme == "http" and not loopback?(uri.host) ->
        raise ArgumentError,
              "#{key} is http:// to #{uri.host}, which is not loopback: use https, or run the " <>
                "permission endpoint on this host"

      true ->
        url
    end
  end

  defp url!(other, key),
    do: raise(ArgumentError, "#{key} must be a URL string, got #{inspect(other)}")

  defp loopback?(host) when host in ["localhost", "::1", "[::1]"], do: true

  defp loopback?(host) do
    case :inet.parse_address(String.to_charlist(host)) do
      {:ok, {127, _, _, _}} -> true
      _other -> false
    end
  end

  defp headers!(headers, key) when is_map(headers) do
    if map_size(headers) > @max_headers do
      raise ArgumentError, "#{key} has #{map_size(headers)} entries; at most #{@max_headers}"
    end

    Map.new(headers, fn {name, value} ->
      {to_string(name), Options.string!(value, "#{key}.#{name}")}
    end)
  end

  defp headers!(other, key),
    do: raise(ArgumentError, "#{key} must be a map of headers, got #{inspect(other)}")

  defp path!(path, key) when is_binary(path) do
    segments = String.split(path, ".")

    if Enum.all?(segments, &Regex.match?(@path_segment, &1)),
      do: segments,
      else: raise(ArgumentError, "#{key} must be a dotted path, got #{inspect(path)}")
  end

  defp path!(other, key),
    do: raise(ArgumentError, "#{key} must be a dotted path string, got #{inspect(other)}")

  defp param!(value, key) when is_binary(value) do
    if Regex.match?(@param, value),
      do: value,
      else: raise(ArgumentError, "#{key} must be one query parameter name, got #{inspect(value)}")
  end

  defp param!(other, key),
    do: raise(ArgumentError, "#{key} must be a string, got #{inspect(other)}")

  defp req_options!(options, key) do
    if Keyword.keyword?(options),
      do: options,
      else: raise(ArgumentError, "#{key} must be a keyword list, got #{inspect(options)}")
  end

  @impl true
  @spec load(map(), map(), acc, (map(), acc -> {:cont, acc} | {:halt, acc})) ::
          {:ok, acc} | {:error, term()}
        when acc: term()
  def load(state, ctx, acc, fun), do: fetch(state, ctx, state.url, 1, acc, fun)

  defp fetch(state, ctx, url, page, acc, fun) do
    if page > state.max_pages do
      {:error, :too_many_pages}
    else
      with {:ok, json} <- get(state, ctx, url),
           {:ok, rows} <- rows(json, state.rows_path) do
        case reduce_rows(rows, acc, fun) do
          {:halt, acc} ->
            {:ok, acc}

          {:cont, acc} ->
            case next(json, state.next_path) do
              nil -> {:ok, acc}
              cursor -> fetch(state, ctx, cursor_url(state, cursor), page + 1, acc, fun)
            end
        end
      end
    end
  end

  defp get(state, ctx, url) do
    headers = state.headers |> Map.to_list() |> Kernel.++([{"accept", "application/json"}])

    case Ankusa.HttpClient.request(
           :get,
           url,
           headers,
           nil,
           state.timeout_ms,
           state.req_options ++
             [max_response_bytes: @max_response_bytes, instance: ctx.instance]
         ) do
      {:ok, 200, body} when is_binary(body) ->
        case JSON.decode(body) do
          {:ok, json} when is_map(json) -> {:ok, json}
          _other -> {:error, :bad_response}
        end

      {:ok, 200, _truncated} ->
        {:error, :bad_response}

      {:ok, status, _body} ->
        {:error, {:status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp rows(json, path) do
    case dig(json, path) do
      rows when is_list(rows) -> {:ok, rows}
      _other -> {:error, :bad_response}
    end
  end

  defp next(json, path) do
    case dig(json, path) do
      cursor when is_binary(cursor) and cursor != "" -> cursor
      _other -> nil
    end
  end

  defp reduce_rows(rows, acc, fun) do
    Enum.reduce_while(rows, {:cont, acc}, fn row, {:cont, acc} ->
      row = if is_map(row), do: row, else: %{}

      case fun.(row, acc) do
        {:cont, acc} -> {:cont, {:cont, acc}}
        {:halt, acc} -> {:halt, {:halt, acc}}
      end
    end)
  end

  defp dig(value, []), do: value

  defp dig(value, [segment | rest]) when is_map(value) do
    case Map.fetch(value, segment) do
      {:ok, next} -> dig(next, rest)
      :error -> nil
    end
  end

  defp dig(value, [segment | rest]) when is_list(value) do
    case Integer.parse(segment) do
      {index, ""} when index >= 0 -> dig(Enum.at(value, index), rest)
      _other -> nil
    end
  end

  defp dig(_value, _path), do: nil

  defp cursor_url(state, cursor) do
    separator = if String.contains?(state.url, "?"), do: "&", else: "?"

    state.url <>
      separator <> state.cursor_param <> "=" <> URI.encode_www_form(cursor)
  end
end
