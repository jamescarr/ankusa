defmodule Ankusa.Gateway.Authn.MTLS do
  @moduledoc """
  Client certificates (`type: :mtls`): the identity is the certificate the
  listener's `gateway.tls: %{verify: :verify_peer}` handshake accepted. What it
  may do is configured per identity:

      %{type: :mtls, subject_map: %{
        "uri:spiffe://prod/ns/billing/sa/worker" => ["claims:read", "tenant:billing"],
        "cn:reports-exporter" => %{scopes: ["claims:read", "tenant:*"], groups: ["reports"]}
      }}

  A certificate offers identities in the order of
  `Ankusa.Admin.Auth.MTLS.identities/1` (subject alternative names, then the
  common name); the first one in the map is the principal. A request with no
  peer certificate, or one none of whose identities is mapped, is `:skip`, so
  a bearer authenticator later in the chain can still identify it.

  Needs `gateway.tls` with `verify: :verify_peer` and a `cacertfile`; at most
  one per chain.
  """

  @behaviour Ankusa.Gateway.Authenticator

  alias Ankusa.Admin.Auth.MTLS
  alias Ankusa.Gateway.Options
  alias Ankusa.Principal

  @impl true
  def init!(config, key) do
    Options.unknown_keys!(config, [:subject_map], key)

    subject_map =
      case Map.get(config, :subject_map) do
        map when is_map(map) and map_size(map) > 0 ->
          Map.new(map, fn {identity, entry} -> subject!(identity, entry, key) end)

        other ->
          raise ArgumentError,
                "#{key}.subject_map must be a non-empty map of identity => scopes, got #{inspect(other)}"
      end

    %{subject_map: subject_map}
  end

  defp subject!(identity, entry, key) do
    identity = to_string(identity)
    entry_key = "#{key}.subject_map[#{identity}]"

    if identity == "" do
      raise ArgumentError, "#{key}.subject_map has an empty identity"
    end

    message =
      "#{entry_key} must be a list of scopes or a map with scopes and groups"

    case entry do
      scopes when is_list(scopes) ->
        {identity, %{scopes: scopes!(scopes, entry_key, message), groups: []}}

      %{} = map ->
        unless Enum.all?(Map.keys(map), &(&1 in [:scopes, :groups])) do
          raise ArgumentError, message
        end

        {identity,
         %{
           scopes: scopes!(Map.get(map, :scopes, []), entry_key, message),
           groups: groups!(Map.get(map, :groups, []), message)
         }}

      _other ->
        raise ArgumentError, message
    end
  end

  defp scopes!(scopes, key, message) do
    if is_list(scopes) and Enum.all?(scopes, &is_binary/1),
      do: Options.scopes!(scopes, "#{key}.scopes"),
      else: raise(ArgumentError, message)
  end

  defp groups!(groups, message) do
    if is_list(groups) and Enum.all?(groups, &is_binary/1),
      do: groups,
      else: raise(ArgumentError, message)
  end

  @impl true
  def credential(conn, _ctx) do
    case Plug.Conn.get_peer_data(conn) do
      %{ssl_cert: der} when is_binary(der) -> {:ok, der}
      _ -> :none
    end
  end

  @impl true
  def authenticate(conn, %{config: %{subject_map: subject_map}} = ctx) do
    with {:ok, der} <- credential(conn, ctx),
         {identity, %{scopes: scopes, groups: groups}} <- first_mapped(der, subject_map) do
      {:ok, %{Principal.new(identity, scopes) | groups: groups}, nil}
    else
      _ -> :skip
    end
  end

  defp first_mapped(der, subject_map) do
    Enum.find_value(MTLS.identities(der), fn identity ->
      case subject_map do
        %{^identity => entry} -> {identity, entry}
        _ -> nil
      end
    end)
  end
end
