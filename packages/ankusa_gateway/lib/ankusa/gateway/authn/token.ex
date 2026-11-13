defmodule Ankusa.Gateway.Authn.Token do
  @moduledoc """
  Static bearer tokens (`type: :token`), for workers, CI and scripts:

      %{type: :token, tokens: [
        %{id: "worker", sha256: "<hex of sha256(token)>", scopes: ["claims:read", "tenant:*"],
          groups: ["workers"], claims: %{"team" => "billing"}}
      ]}

  The configuration holds each token's SHA-256, never the token (see
  `Ankusa.Admin.Auth.Token`, which does the constant-time match). `scopes`
  are the operation and `tenant:` scopes of `Ankusa.Principal` (unknown ones
  are refused at boot; empty is allowed), `groups` and `claims` are carried
  onto the principal for authorizers that read them.

  A bearer token no entry matches is `:skip`, not a refusal: another
  authenticator of the chain (a `jwt` one) may own it. `ids` are unique within
  the authenticator.
  """

  @behaviour Ankusa.Gateway.Authenticator

  alias Ankusa.Admin.Auth
  alias Ankusa.Gateway.Options

  @keys [:tokens]
  @token_keys [:id, :sha256, :scopes, :groups, :claims]

  @impl true
  def init!(config, key) do
    Options.unknown_keys!(config, @keys, key)

    tokens =
      case Map.get(config, :tokens) do
        [_ | _] = tokens ->
          tokens
          |> Enum.with_index()
          |> Enum.map(fn {t, i} -> token!(t, "#{key}.tokens[#{i}]") end)

        other ->
          raise ArgumentError, "#{key}.tokens must be a non-empty list, got #{inspect(other)}"
      end

    ids = Enum.map(tokens, & &1.id)

    case ids -- Enum.uniq(ids) do
      [] -> :ok
      [dup | _] -> raise ArgumentError, "#{key}.tokens: duplicate id #{dup}"
    end

    %{tokens: tokens}
  end

  defp token!(entry, key) do
    given = Options.map!(entry, key)
    Options.unknown_keys!(given, @token_keys, key)
    id = Map.get(given, :id)

    unless is_binary(id) and byte_size(id) in 1..128 and String.printable?(id) do
      raise ArgumentError, "#{key}.id must be a non-empty printable string, got #{inspect(id)}"
    end

    sha256 = Map.get(given, :sha256)

    unless is_binary(sha256) and Regex.match?(~r/\A[0-9a-fA-F]{64}\z/, sha256) do
      raise ArgumentError,
            "#{key}.sha256 must be the 64-character hex SHA-256 of the token " <>
              "(`printf %s \"$TOKEN\" | shasum -a 256`)"
    end

    claims = Map.get(given, :claims, %{})

    unless is_map(claims) and Enum.all?(Map.keys(claims), &is_binary/1) do
      raise ArgumentError, "#{key}.claims must be a map with string keys, got #{inspect(claims)}"
    end

    %{
      id: id,
      sha256: String.downcase(sha256),
      scopes: Options.scopes!(Map.get(given, :scopes, []), "#{key}.scopes"),
      groups: Options.string_list!(Map.get(given, :groups, []), "#{key}.groups"),
      claims: claims
    }
  end

  @impl true
  def credential(conn, _ctx) do
    case Auth.bearer(conn) do
      {:ok, token} -> {:ok, token}
      :error -> :none
    end
  end

  @impl true
  def authenticate(conn, %{config: %{tokens: tokens}}) do
    case Ankusa.Admin.Auth.Token.authenticate(conn, %{config: %{tokens: tokens}}) do
      {:ok, principal} ->
        entry = Enum.find(tokens, &(&1.id == principal.id))
        {:ok, %{principal | groups: entry.groups, claims: entry.claims}, nil}

      {:error, :unauthenticated} ->
        :skip
    end
  end
end
