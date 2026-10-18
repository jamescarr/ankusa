defmodule AnkusaServer.SecretProvider.AwsSecretsManager do
  @moduledoc """
  `${sm:<name>:<secret>}` against AWS Secrets Manager (`type: aws`).

  `<secret>` is a secret's name or ARN. A secret that holds a JSON object is read
  field by field with `#`: `${sm:aws:prod/stripe#whsec}`. Without `#` the whole
  `SecretString` is the value (a `SecretBinary` is decoded from the base64 the
  API returns).

  The request is `GetSecretValue`, signed with SigV4. Credentials are the
  provider's `access_key_id` / `secret_access_key` (/ `session_token`) when set,
  else the chain `Ankusa.AWS.Credentials` walks (environment, web identity as
  EKS sets it, the EC2 instance profile). `endpoint` replaces
  `https://secretsmanager.<region>.amazonaws.com` for a VPC endpoint or a
  local emulator.

  `ResourceNotFoundException` and `AccessDeniedException` are `:not_found`;
  everything else that is not a `200` (throttling, a 5xx, a refused
  connection, a malformed reply) is `:unavailable`.
  """

  @behaviour Ankusa.SecretProvider

  alias Ankusa.AWS.Credentials
  alias Ankusa.HttpClient

  @max_response_bytes 1_048_576
  @not_found ["ResourceNotFoundException", "AccessDeniedException"]

  @impl true
  def fetch(ref, opts) do
    {id, field} = split(ref)
    region = Keyword.fetch!(opts, :region)

    url =
      Keyword.get(opts, :endpoint, "https://secretsmanager.#{region}.amazonaws.com") <> "/"

    with {:ok, creds} <- credentials(opts),
         {:ok, secret} <- request(url, region, creds, id, Keyword.get(opts, :timeout_ms, 5_000)) do
      pick(secret, field)
    end
  end

  defp credentials(opts) do
    case Credentials.get(opts) do
      {:ok, creds} -> {:ok, creds}
      {:error, _reason} -> {:error, :unavailable}
    end
  end

  defp request(url, region, creds, id, timeout_ms) do
    body = JSON.encode!(%{"SecretId" => id})

    headers =
      [
        {"host", authority(url)},
        {"content-type", "application/x-amz-json-1.1"},
        {"x-amz-target", "secretsmanager.GetSecretValue"}
      ] ++
        if(creds.session_token, do: [{"x-amz-security-token", creds.session_token}], else: [])

    signed =
      :aws_signature.sign_v4(
        creds.access_key_id,
        creds.secret_access_key,
        region,
        "secretsmanager",
        :calendar.universal_time(),
        "POST",
        url,
        headers,
        body,
        []
      )

    case HttpClient.request(:post, url, signed, body, timeout_ms,
           max_response_bytes: @max_response_bytes
         ) do
      {:ok, 200, reply} when is_binary(reply) -> secret_value(reply)
      {:ok, 400, reply} when is_binary(reply) -> classify(reply)
      _other -> {:error, :unavailable}
    end
  end

  defp secret_value(reply) do
    case JSON.decode(reply) do
      {:ok, %{"SecretString" => value}} when is_binary(value) -> {:ok, value}
      {:ok, %{"SecretBinary" => value}} when is_binary(value) -> decode_binary(value)
      _other -> {:error, :unavailable}
    end
  end

  defp decode_binary(value) do
    case Base.decode64(value) do
      {:ok, decoded} -> {:ok, decoded}
      :error -> {:error, :unavailable}
    end
  end

  defp classify(reply) do
    case JSON.decode(reply) do
      {:ok, %{"__type" => type}} when is_binary(type) ->
        if Enum.any?(@not_found, &String.ends_with?(type, &1)),
          do: {:error, :not_found},
          else: {:error, :unavailable}

      _other ->
        {:error, :unavailable}
    end
  end

  defp split(ref) do
    case String.split(ref, "#", parts: 2) do
      [id, field] when field != "" -> {id, field}
      [id | _] -> {id, nil}
    end
  end

  defp pick(value, nil), do: {:ok, value}

  defp pick(value, field) do
    case JSON.decode(value) do
      {:ok, %{^field => found}} when is_binary(found) -> {:ok, found}
      _other -> {:error, :not_found}
    end
  end

  # The host header is signed, so it is derived from the same URL handed to the
  # signer: the port only when it is not the scheme's own.
  defp authority(url) do
    %URI{host: host, port: port, scheme: scheme} = URI.parse(url)
    if port in [nil, URI.default_port(scheme)], do: host, else: "#{host}:#{port}"
  end
end
