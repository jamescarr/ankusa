defmodule Ankusa.BlobStore.OCI.InstancePrincipal do
  @moduledoc """
  Session credentials for `Ankusa.BlobStore.OCI` from the instance's own
  identity, for a node on OCI compute or OKE: no API key to store or rotate.

      {Ankusa.BlobStore.OCI,
       region: "us-ashburn-1",
       namespace: "…",
       bucket: "ankusa-segments",
       key_provider:
         {Ankusa.BlobStore.OCI.InstancePrincipal, :credentials, [[region: "us-ashburn-1"]]}}

  The federation the OCI SDKs perform, in four steps:

    1. the instance metadata service (`http://169.254.169.254/opc/v2`, header
       `Authorization: Bearer Oracle`) gives the instance's leaf certificate, its
       private key and the intermediate certificates;
    2. a fresh RSA session key pair is generated;
    3. `POST https://auth.<region>.oraclecloud.com/v1/x509` sends the leaf
       certificate, the intermediates and the session public key, signed with the
       leaf key (`keyId` `<tenancy>/fed-x509-sha256/<sha256 fingerprint>`, the
       tenancy read from the certificate's `opc-tenant:` organizational unit);
    4. the answer is a short-lived `ST$<token>` session token; objects are then
       signed with the **session** private key under `key_id: "ST$<token>"`.

  The credentials are cached node-wide until five minutes before the token's own
  `exp`, so the adapter's per-request call is an ETS read; a `401` makes the
  adapter call `invalidate/1`, and its retry federates again.

  Arguments (a keyword list, the one optional argument of `credentials/1` and
  `invalidate/1`):

    * `:region` — required, e.g. `"us-ashburn-1"`; picks the auth service
    * `:tenancy_ocid` — override the tenancy read from the certificate
    * `:imds_endpoint` — default `"http://169.254.169.254/opc/v2"`
    * `:auth_endpoint` — default `"https://auth.<region>.oraclecloud.com"`
    * `:timeout_ms` — default `10_000`
    * `:req_options` — transport options, e.g. `plug:` for `Req.Test`
  """

  alias Ankusa.BlobStore.OCI.Signer
  alias Ankusa.BlobStore.TokenCache
  alias Ankusa.HttpClient

  @window_s 300
  @fallback_ttl_s 600

  @doc "`{:ok, %{key_id: \"ST$…\", private_key: pem}}`, or `:error` when federation failed."
  @spec credentials(keyword()) :: {:ok, %{key_id: String.t(), private_key: String.t()}} | :error
  def credentials(opts \\ []) do
    case TokenCache.fetch(key(opts), @window_s, fn -> federate(opts) end) do
      {:ok, credentials} -> {:ok, credentials}
      _error -> :error
    end
  end

  @doc "Drop the cached session: the next `credentials/1` federates again."
  @spec invalidate(keyword()) :: :ok
  def invalidate(opts \\ []), do: TokenCache.invalidate(key(opts))

  defp key(opts), do: {:oci_instance_principal, auth_endpoint(opts)}

  defp auth_endpoint(opts) do
    Keyword.get_lazy(opts, :auth_endpoint, fn ->
      "https://auth.#{Keyword.fetch!(opts, :region)}.oraclecloud.com"
    end)
  end

  defp federate(opts) do
    with {:ok, leaf_pem} <- imds(opts, "/identity/cert.pem"),
         {:ok, leaf_key} <- imds(opts, "/identity/key.pem"),
         {:ok, intermediate_pem} <- imds(opts, "/identity/intermediate.pem"),
         [{:Certificate, leaf_der, _} | _] <- :public_key.pem_decode(leaf_pem),
         {:ok, tenancy} <- tenancy(opts, leaf_der) do
      {session_pem, session_public} = session_key()

      body =
        JSON.encode!(%{
          "certificate" => Base.encode64(leaf_der),
          "publicKey" => session_public,
          "intermediateCertificates" =>
            for(
              {:Certificate, der, _} <- :public_key.pem_decode(intermediate_pem),
              do: Base.encode64(der)
            )
        })

      key_id = "#{tenancy}/fed-x509-sha256/#{fingerprint(leaf_der)}"

      with {:ok, token} <- exchange(opts, key_id, leaf_key, body) do
        {:ok, %{key_id: "ST$" <> token, private_key: session_pem}, expires_at(token)}
      end
    else
      _failed -> :error
    end
  end

  defp imds(opts, path) do
    base = Keyword.get(opts, :imds_endpoint, "http://169.254.169.254/opc/v2")

    case HttpClient.request(
           :get,
           base <> path,
           [{"authorization", "Bearer Oracle"}],
           nil,
           timeout(opts),
           req_options(opts)
         ) do
      {:ok, 200, pem} when is_binary(pem) -> {:ok, pem}
      _failed -> :error
    end
  end

  # The signed `POST /v1/x509`: `date`, the request target and the body's three
  # headers (no `host`), the way the OCI SDKs sign it.
  defp exchange(opts, key_id, leaf_key, body) do
    url = auth_endpoint(opts) <> "/v1/x509"

    headers = [
      {"date", Signer.rfc1123_date(:calendar.universal_time())},
      {"content-type", "application/json"},
      {"x-content-sha256", Signer.sha256_base64(body)}
    ]

    authorization =
      Signer.authorization(key_id, leaf_key, :post, url, headers, body, [
        "date",
        "(request-target)",
        "content-length",
        "content-type",
        "x-content-sha256"
      ])

    case HttpClient.request(
           :post,
           url,
           [{"authorization", authorization} | headers],
           body,
           timeout(opts),
           req_options(opts)
         ) do
      {:ok, 200, response} ->
        case JSON.decode(response) do
          {:ok, %{"token" => token}} when is_binary(token) -> {:ok, token}
          _unreadable -> :error
        end

      _failed ->
        :error
    end
  end

  defp session_key do
    private = :public_key.generate_key({:rsa, 2048, 65_537})
    {:RSAPrivateKey, _v, modulus, exponent, _d, _p, _q, _e1, _e2, _c, _other} = private

    {:SubjectPublicKeyInfo, spki, _} =
      :public_key.pem_entry_encode(:SubjectPublicKeyInfo, {:RSAPublicKey, modulus, exponent})

    pem = :public_key.pem_encode([:public_key.pem_entry_encode(:RSAPrivateKey, private)])
    {pem, Base.encode64(spki)}
  end

  defp tenancy(opts, der) do
    case Keyword.fetch(opts, :tenancy_ocid) do
      {:ok, ocid} -> {:ok, ocid}
      :error -> tenancy_from_certificate(der)
    end
  end

  # `OU=opc-tenant:<ocid>` in the certificate's subject.
  defp tenancy_from_certificate(der) do
    {:OTPCertificate,
     {:OTPTBSCertificate, _v, _s, _sig, _issuer, _validity, {:rdnSequence, subject}, _, _, _, _},
     _, _} =
      :public_key.pkix_decode_cert(der, :otp)

    subject
    |> List.flatten()
    |> Enum.find_value(:error, fn
      {:AttributeTypeAndValue, {2, 5, 4, 11}, value} ->
        case to_string_value(value) do
          "opc-tenant:" <> ocid -> {:ok, ocid}
          _other -> nil
        end

      _other ->
        nil
    end)
  end

  defp to_string_value({_tag, value}) when is_binary(value), do: value
  defp to_string_value({_tag, value}) when is_list(value), do: List.to_string(value)
  defp to_string_value(_other), do: ""

  defp fingerprint(der) do
    :sha256
    |> :crypto.hash(der)
    |> Base.encode16()
    |> String.graphemes()
    |> Enum.chunk_every(2)
    |> Enum.map_join(":", &Enum.join/1)
  end

  # The token is a JWT: its `exp` claim is when the session ends.
  defp expires_at(token) do
    with [_header, payload, _signature] <- String.split(token, "."),
         {:ok, json} <- Base.url_decode64(payload, padding: false),
         {:ok, %{"exp" => exp}} when is_integer(exp) <- JSON.decode(json) do
      exp
    else
      _unreadable -> System.system_time(:second) + @fallback_ttl_s
    end
  end

  defp timeout(opts), do: Keyword.get(opts, :timeout_ms, 10_000)
  defp req_options(opts), do: Keyword.get(opts, :req_options, [])
end
