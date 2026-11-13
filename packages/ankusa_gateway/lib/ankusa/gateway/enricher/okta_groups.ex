defmodule Ankusa.Gateway.Enricher.OktaGroups do
  @moduledoc """
  A principal's Okta groups, from Okta's Users API (`type: okta_groups`), for
  tokens whose `groups` claim is absent or too coarse:

      %{type: :okta_groups, domain: "acme.okta.com", api_token: "00abc...",
        user_id: "{{ claims.uid }}", max_groups: 500}

  | Key | Default | |
  | --- | --- | --- |
  | `domain` | required | the Okta org's host: `[a-z0-9.-]+` |
  | `api_token` | required | an Okta API token, sent as `Authorization: SSWS`, redacted |
  | `user_id` | `"{{ claims.uid }}"` | a template (`principal.*`, `claims.*`) naming the user; a value that does not render leaves the principal unchanged |
  | `max_groups` | `500` | how many groups one call fetches (1..5000) |

  `GET https://<domain>/api/v1/users/<id>/groups?limit=200`, following the
  `link` header's `rel="next"` page while it stays on that host. A group's
  name is `profile.name`; the principal's groups become the union, truncated
  to 1000. A `404` (an unknown user) leaves the principal unchanged; `429`,
  `5xx` and transport failures are `{:error, {:unavailable, reason}}`, which
  the entry's `on_unavailable` decides.
  """

  @behaviour Ankusa.Gateway.Enricher

  alias Ankusa.Gateway.{Enricher, Options, Template}
  alias Ankusa.HttpClient
  alias Ankusa.Principal

  @own_keys [:domain, :api_token, :user_id, :max_groups, :req_options]
  @domain ~r/\A[a-z0-9.-]+\z/
  @page_limit 200
  @max_bytes 1_048_576
  @max_principal_groups 1_000
  @next ~r/<([^>]*)>\s*;\s*rel="next"/

  @impl true
  def init!(opts, key) do
    Options.unknown_keys!(opts, @own_keys ++ Enricher.keys(), key)

    domain = Map.get(opts, :domain)

    unless is_binary(domain) and Regex.match?(@domain, domain) do
      raise ArgumentError,
            "#{key}.domain must be a lowercase host name (A-Z 0-9 . -), got #{inspect(domain)}"
    end

    api_token = Map.get(opts, :api_token)

    unless is_binary(api_token) and api_token != "" do
      raise ArgumentError, "#{key}.api_token must be a non-empty Okta API token"
    end

    Map.merge(
      %{
        domain: domain,
        api_token: api_token,
        user_id:
          Template.compile!(
            Map.get(opts, :user_id, "{{ claims.uid }}"),
            "#{key}.user_id",
            [:principal, :claims]
          ),
        max_groups:
          Options.int_in!(Map.get(opts, :max_groups, 500), 1..5_000, "#{key}.max_groups"),
        req_options: Options.keyword_list!(Map.get(opts, :req_options, []), "#{key}.req_options")
      },
      Enricher.common!(opts, key)
    )
  end

  @impl true
  def enrich(%Principal{} = principal, state, ctx) do
    bindings = %{principal: principal, claims: principal.claims}

    case Template.render(state.user_id, bindings) do
      {:ok, id} -> fetch(id, state, ctx, principal)
      {:error, _missing} -> {:ok, principal}
    end
  end

  @impl true
  def child_specs(instance, index, state),
    do: [Ankusa.Gateway.Enricher.caller_spec(instance, index, __MODULE__, state)]

  defp fetch(id, state, ctx, principal) do
    url =
      "https://#{state.domain}/api/v1/users/#{escape(id)}/groups?limit=#{@page_limit}"

    case page(url, state, ctx, []) do
      {:ok, groups} ->
        merged = (principal.groups ++ groups) |> Enum.uniq() |> Enum.take(@max_principal_groups)
        {:ok, %{principal | groups: merged}}

      :unchanged ->
        {:ok, principal}

      {:error, reason} ->
        {:error, {:unavailable, reason}}
    end
  end

  # One page at a time, until the source has no next page or `max_groups` is
  # reached; `link`'s next URL is followed only while it stays on the org.
  defp page(url, state, ctx, acc) do
    case get(url, state, ctx) do
      {:ok, 200, headers, body} ->
        case JSON.decode(body) do
          {:ok, list} when is_list(list) ->
            names = for %{"profile" => %{"name" => name}} <- list, is_binary(name), do: name
            acc = acc ++ names

            if length(acc) >= state.max_groups,
              do: {:ok, Enum.take(acc, state.max_groups)},
              else: next_page(headers, state, ctx, acc)

          _other ->
            {:error, :bad_response}
        end

      {:ok, 404, _headers, _body} ->
        if acc == [], do: :unchanged, else: {:ok, Enum.take(acc, state.max_groups)}

      {:ok, status, _headers, _body} ->
        {:error, {:status, status}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp next_page(headers, state, ctx, acc) do
    case next(headers, state.domain) do
      nil -> {:ok, Enum.take(acc, state.max_groups)}
      url -> page(url, state, ctx, acc)
    end
  end

  defp get(url, state, ctx) do
    HttpClient.request_with_headers(
      :get,
      url,
      [{"authorization", "SSWS " <> state.api_token}, {"accept", "application/json"}],
      nil,
      ctx.timeout_ms,
      [max_response_bytes: @max_bytes, instance: ctx.instance] ++ state.req_options
    )
  end

  # `link` may arrive as several headers, each holding several comma-separated
  # `rel="next"` entries; only one on the org's own host is followed.
  defp next(headers, domain) do
    headers
    |> Map.get("link", [])
    |> Enum.flat_map(&String.split(&1, ","))
    |> Enum.find_value(fn entry ->
      case Regex.run(@next, entry) do
        [_, url] ->
          case URI.parse(url) do
            %URI{scheme: "https", host: ^domain} -> url
            _other -> nil
          end

        nil ->
          nil
      end
    end)
  end

  defp escape(id), do: URI.encode(id, &URI.char_unreserved?/1)
end
