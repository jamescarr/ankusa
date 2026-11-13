defmodule Ankusa.Gateway.Enricher.Http do
  @moduledoc """
  Groups and claims from a JSON API of your own (`type: http`):

      %{type: :http, url: "https://directory.internal/users/{{ claims.uid }}",
        headers: %{"authorization" => "Bearer ..."},
        groups_path: "data.groups", claims_path: "data.profile"}

  | Key | Default | |
  | --- | --- | --- |
  | `url` | required | `https` (or `http` to a loopback host). The origin is literal: only the path and query may be templated (`principal.*`, `claims.*`), and every substituted value is percent-encoded |
  | `headers` | `%{}` | at most 16 request headers; values are redacted in `print-config` |
  | `groups_path` | none | a dotted path into the response: one string, or a list of strings, added to the principal's groups (at most 1000 in total). An integer segment indexes an array |
  | `claims_path` | none | a dotted path to a JSON object merged into `principal.claims`; a key the principal already has is not overridden |

  One has to be set: an enricher that reads neither changes nothing. `GET`
  only; `200` with a JSON object. A non-`200` status and a transport failure
  are `{:error, {:unavailable, reason}}`, which the entry's `on_unavailable`
  decides; a URL that does not render (a missing claim) leaves the principal
  unchanged.
  """

  @behaviour Ankusa.Gateway.Enricher

  alias Ankusa.Gateway.{Enricher, Options, Template, Url}
  alias Ankusa.HttpClient
  alias Ankusa.Principal

  @own_keys [:url, :headers, :groups_path, :claims_path, :req_options]
  @max_headers 16
  @max_bytes 1_048_576
  @max_groups 1_000
  @segment ~r/\A[A-Za-z0-9_-]+\z/
  @index ~r/\A[0-9]+\z/

  @impl true
  def init!(opts, key) do
    Options.unknown_keys!(opts, @own_keys ++ Enricher.keys(), key)

    url = Map.get(opts, :url)

    unless is_binary(url) and url != "" do
      raise ArgumentError, "#{key}.url must be set"
    end

    compiled = Template.compile_url!(url, "#{key}.url", [:principal, :claims])
    Url.https!(url, "#{key}.url")

    groups_path = path_opt(Map.get(opts, :groups_path), "#{key}.groups_path")
    claims_path = path_opt(Map.get(opts, :claims_path), "#{key}.claims_path")

    if groups_path == nil and claims_path == nil do
      raise ArgumentError, "#{key} needs groups_path or claims_path (or both)"
    end

    Map.merge(
      %{
        url: compiled,
        headers: headers!(Map.get(opts, :headers, %{}), "#{key}.headers"),
        groups_path: groups_path,
        claims_path: claims_path,
        req_options: Options.keyword_list!(Map.get(opts, :req_options, []), "#{key}.req_options")
      },
      Enricher.common!(opts, key)
    )
  end

  @impl true
  def enrich(%Principal{} = principal, state, ctx) do
    case Template.render(state.url, %{principal: principal, claims: principal.claims}) do
      {:ok, url} -> get(url, state, ctx, principal)
      {:error, _missing} -> {:ok, principal}
    end
  end

  @impl true
  def child_specs(instance, index, state),
    do: [Ankusa.Gateway.Enricher.caller_spec(instance, index, __MODULE__, state)]

  defp get(url, state, ctx, principal) do
    headers = [{"accept", "application/json"} | Map.to_list(state.headers)]

    opts = [max_response_bytes: @max_bytes, instance: ctx.instance] ++ state.req_options

    case HttpClient.request_with_headers(:get, url, headers, nil, ctx.timeout_ms, opts) do
      {:ok, 200, _headers, body} -> merged(body, state, principal)
      {:ok, status, _headers, _body} -> {:error, {:unavailable, {:status, status}}}
      {:error, reason} -> {:error, {:unavailable, reason}}
    end
  end

  defp merged(body, state, principal) when is_binary(body) do
    case JSON.decode(body) do
      {:ok, document} when is_map(document) -> {:ok, merge(principal, document, state)}
      _other -> {:error, {:unavailable, :bad_response}}
    end
  end

  defp merged({:truncated, _bytes}, _state, _principal), do: {:error, {:unavailable, :too_large}}
  defp merged(_other, _state, _principal), do: {:error, {:unavailable, :bad_response}}

  defp merge(principal, document, state) do
    groups = groups_at(document, state.groups_path)
    claims = claims_at(document, state.claims_path)

    %{
      principal
      | groups: (principal.groups ++ groups) |> Enum.uniq() |> Enum.take(@max_groups),
        # What the token says wins: an enricher adds, it does not rewrite.
        claims: Map.merge(claims, principal.claims)
    }
  end

  defp groups_at(_document, nil), do: []

  defp groups_at(document, path) do
    case dig(document, path) do
      {:ok, group} when is_binary(group) -> [group]
      {:ok, groups} when is_list(groups) -> Enum.filter(groups, &is_binary/1)
      _other -> []
    end
  end

  defp claims_at(_document, nil), do: %{}

  defp claims_at(document, path) do
    case dig(document, path) do
      {:ok, %{} = claims} -> claims
      _other -> %{}
    end
  end

  defp dig(value, []), do: {:ok, value}

  defp dig(%{} = map, [segment | rest]) when is_binary(segment) do
    case Map.fetch(map, segment) do
      {:ok, value} -> dig(value, rest)
      :error -> :error
    end
  end

  defp dig(list, [index | rest]) when is_list(list) and is_integer(index) do
    case Enum.fetch(list, index) do
      {:ok, value} -> dig(value, rest)
      :error -> :error
    end
  end

  defp dig(_other, _path), do: :error

  defp path_opt(nil, _key), do: nil

  defp path_opt(value, key) when is_binary(value) do
    segments = String.split(value, ".")

    unless value != "" and Enum.all?(segments, &Regex.match?(@segment, &1)) do
      raise ArgumentError,
            "#{key} must be dotted segments of A-Z a-z 0-9 _ - (an integer segment indexes an " <>
              "array), got #{inspect(value)}"
    end

    Enum.map(segments, fn segment ->
      if Regex.match?(@index, segment), do: String.to_integer(segment), else: segment
    end)
  end

  defp path_opt(other, key),
    do: raise(ArgumentError, "#{key} must be a dotted path string, got #{inspect(other)}")

  defp headers!(headers, key) when is_map(headers) and map_size(headers) <= @max_headers do
    if Enum.all?(headers, fn {name, value} -> is_binary(name) and is_binary(value) end),
      do: Map.new(headers, fn {name, value} -> {String.downcase(name), value} end),
      else: raise(ArgumentError, "#{key} must map header names to string values")
  end

  defp headers!(other, key) do
    raise ArgumentError,
          "#{key} must be a map of at most #{@max_headers} header names to values, " <>
            "got #{inspect(other)}"
  end
end
