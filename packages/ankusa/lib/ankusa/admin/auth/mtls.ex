defmodule Ankusa.Admin.Auth.MTLS do
  @moduledoc """
  Mutual TLS (`mode: :mtls`): the identity is the client certificate the
  listener's `tls: %{verify: :verify_peer}` handshake accepted
  (`Ankusa.Listener`), so a request with no certificate or one from another CA
  never reaches this module. What the certificate may do is configured:

      mtls: %{
        subject_map: %{
          "cn:ops-laptop" => ["dlq:read", "replays:write", "tenant:*"],
          "uri:spiffe://prod/ns/billing/sa/exporter" => ["metrics:read"]
        }
      }

  A certificate offers identities of five forms, looked up in this order: its
  subject alternative names (`dns:<name>`, `email:<address>`, `uri:<uri>`,
  `ip:<address>`), then its subject common name (`cn:<name>`). The first one
  in the map is the principal, named by that identity. A valid certificate none
  of whose identities is mapped is `401`: the CA vouches for who it is, not for
  what it may do.
  """

  @behaviour Ankusa.Admin.Auth

  alias Ankusa.Principal

  @oid_cn {2, 5, 4, 3}
  @oid_san {2, 5, 29, 17}

  @impl true
  def authenticate(conn, %{config: %{mtls: %{subject_map: subject_map}}}) do
    with %{ssl_cert: der} when is_binary(der) <- Plug.Conn.get_peer_data(conn),
         {:ok, identity, scopes} <- first_mapped(identities(der), subject_map) do
      {:ok, Principal.new(identity, scopes)}
    else
      _ -> {:error, :unauthenticated}
    end
  end

  defp first_mapped(identities, subject_map) do
    Enum.find_value(identities, :error, fn identity ->
      case subject_map do
        %{^identity => scopes} -> {:ok, identity, scopes}
        _ -> nil
      end
    end)
  end

  @doc """
  The identities a DER certificate offers, in lookup order: subject alternative
  names, then the common name. `[]` for a certificate that cannot be parsed.
  """
  @spec identities(binary()) :: [String.t()]
  def identities(der) do
    {:OTPCertificate,
     {:OTPTBSCertificate, _v, _serial, _tbs_sig, _issuer, _validity, subject, _spki, _iuid, _suid,
      extensions}, _alg, _cert_sig} =
      :public_key.pkix_decode_cert(der, :otp)

    sans(extensions) ++ common_names(subject)
  rescue
    _ -> []
  catch
    _, _ -> []
  end

  defp sans(:asn1_NOVALUE), do: []

  defp sans(extensions) do
    for {:Extension, @oid_san, _critical, names} <- extensions,
        name <- names,
        identity = san(name),
        do: identity
  end

  defp san({:dNSName, name}), do: "dns:" <> to_string(name)
  defp san({:rfc822Name, name}), do: "email:" <> to_string(name)
  defp san({:uniformResourceIdentifier, uri}), do: "uri:" <> to_string(uri)

  defp san({:iPAddress, bytes}) when byte_size(bytes) in [4, 16] do
    "ip:" <> (bytes |> ip_tuple() |> :inet.ntoa() |> to_string())
  end

  defp san(_other), do: nil

  defp ip_tuple(<<a, b, c, d>>), do: {a, b, c, d}

  defp ip_tuple(<<a::16, b::16, c::16, d::16, e::16, f::16, g::16, h::16>>),
    do: {a, b, c, d, e, f, g, h}

  defp common_names({:rdnSequence, rdns}) do
    for rdn <- rdns,
        {:AttributeTypeAndValue, @oid_cn, value} <- rdn,
        name = directory_string(value),
        do: "cn:" <> name
  end

  defp directory_string({tag, value}) when tag in [:utf8String, :printableString, :teletexString],
    do: to_string(value)

  defp directory_string(value) when is_binary(value), do: value
  defp directory_string(value) when is_list(value), do: to_string(value)
  defp directory_string(_other), do: nil
end
