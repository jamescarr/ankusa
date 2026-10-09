defmodule Ankusa.BlobStore.S3.Credentials do
  @moduledoc """
  Where `Ankusa.BlobStore.S3` gets the credentials it signs with, in the order
  the AWS SDKs look:

    1. `opts`: `:access_key_id` and `:secret_access_key` (plus an optional
       `:session_token`).
    2. The environment: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and
       `AWS_SESSION_TOKEN` when set.
    3. Web identity (EKS IRSA and similar): `AWS_WEB_IDENTITY_TOKEN_FILE` and
       `AWS_ROLE_ARN` (`AWS_ROLE_SESSION_NAME` optional) exchanged at STS
       (`AssumeRoleWithWebIdentity`, an unsigned call).
    4. The EC2 instance metadata service, IMDSv2 (a session token, then the
       instance role's credentials), with a 1 s connect timeout so a node off
       EC2 falls through quickly.

  Temporary credentials (3 and 4) are cached per source in a public ETS table
  until 5 minutes before they expire. Nothing here raises: no source at all is
  `{:error, :no_credentials}`, which the blob store returns like any failed
  request.

  The STS and IMDS endpoints can be overridden with `:sts_endpoint` and
  `:imds_endpoint`, and the environment with `:env` (a map), for tests.
  """

  @compile {:no_warn_undefined, [:xmerl_scan, :xmerl_xpath]}

  alias Ankusa.HttpClient

  @table :ankusa_s3_credentials
  @refresh_window_s 300
  @imds_endpoint "http://169.254.169.254"
  @imds_ttl_s "21600"

  @type t :: %{
          access_key_id: String.t(),
          secret_access_key: String.t(),
          session_token: String.t() | nil
        }

  @spec get(keyword()) :: {:ok, t()} | {:error, :no_credentials | term()}
  def get(opts) do
    env = Keyword.get_lazy(opts, :env, &System.get_env/0)

    with :miss <- from_opts(opts),
         :miss <- from_env(env),
         :miss <- web_identity(opts, env) do
      imds(opts)
    end
  end

  defp from_opts(opts) do
    case {Keyword.get(opts, :access_key_id), Keyword.get(opts, :secret_access_key)} do
      {id, secret} when is_binary(id) and is_binary(secret) ->
        {:ok, creds(id, secret, Keyword.get(opts, :session_token))}

      _ ->
        :miss
    end
  end

  defp from_env(env) do
    case {non_empty(env["AWS_ACCESS_KEY_ID"]), non_empty(env["AWS_SECRET_ACCESS_KEY"])} do
      {id, secret} when is_binary(id) and is_binary(secret) ->
        {:ok, creds(id, secret, non_empty(env["AWS_SESSION_TOKEN"]))}

      _ ->
        :miss
    end
  end

  # ── web identity ──────────────────────────────────────────────────────────

  defp web_identity(opts, env) do
    with file when is_binary(file) <- non_empty(env["AWS_WEB_IDENTITY_TOKEN_FILE"]),
         role when is_binary(role) <- non_empty(env["AWS_ROLE_ARN"]) do
      cached({:web_identity, role, file}, fn -> assume_role(opts, env, file, role) end)
    else
      _ -> :miss
    end
  end

  defp assume_role(opts, env, file, role) do
    with {:ok, token} <- File.read(file) do
      session = non_empty(env["AWS_ROLE_SESSION_NAME"]) || "ankusa"

      query =
        URI.encode_query(
          [
            {"Action", "AssumeRoleWithWebIdentity"},
            {"Version", "2011-06-15"},
            {"RoleArn", role},
            {"RoleSessionName", session},
            {"WebIdentityToken", String.trim(token)}
          ],
          :rfc3986
        )

      url = sts_endpoint(opts) <> "/?" <> query

      case HttpClient.request(:get, url, [], nil, timeout(opts), req_options(opts)) do
        {:ok, 200, body} -> parse_sts(body)
        {:ok, status, body} -> {:error, {:sts, status, body}}
        {:error, reason} -> {:error, {:sts, reason}}
      end
    end
  end

  defp sts_endpoint(opts) do
    case Keyword.get(opts, :sts_endpoint) do
      nil -> "https://sts.#{Keyword.fetch!(opts, :region)}.amazonaws.com"
      endpoint -> endpoint
    end
  end

  defp parse_sts(xml) do
    {doc, _rest} = :xmerl_scan.string(:binary.bin_to_list(xml), quiet: true)

    text = fn name ->
      case :xmerl_xpath.string(~c"//Credentials/#{name}/text()", doc) do
        [{:xmlText, _parents, _pos, _lang, value, _type} | _] -> List.to_string(value)
        _ -> nil
      end
    end

    with id when is_binary(id) <- text.("AccessKeyId"),
         secret when is_binary(secret) <- text.("SecretAccessKey"),
         token when is_binary(token) <- text.("SessionToken"),
         {:ok, expires_at} <- expiry(text.("Expiration")) do
      {:ok, creds(id, secret, token), expires_at}
    else
      _ -> {:error, :sts_unreadable}
    end
  catch
    :exit, _not_xml -> {:error, :sts_unreadable}
  end

  # ── IMDSv2 ────────────────────────────────────────────────────────────────

  defp imds(opts) do
    case cached(:imds, fn -> imds_fetch(opts) end) do
      :miss -> {:error, :no_credentials}
      other -> other
    end
  end

  defp imds_fetch(opts) do
    base = Keyword.get(opts, :imds_endpoint, @imds_endpoint) <> "/latest"
    # IMDS is link-local: off EC2 nothing answers, and a slow fall-through
    # would stall every request this store makes.
    req = Keyword.merge(req_options(opts), connect_options: [timeout: 1_000])

    with {:ok, 200, token} <-
           HttpClient.request(
             :put,
             base <> "/api/token",
             [{"x-aws-ec2-metadata-token-ttl-seconds", @imds_ttl_s}],
             "",
             2_000,
             req
           ),
         headers = [{"x-aws-ec2-metadata-token", token}],
         {:ok, 200, roles} <-
           HttpClient.request(
             :get,
             base <> "/meta-data/iam/security-credentials/",
             headers,
             nil,
             2_000,
             req
           ),
         [role | _] <- String.split(roles, "\n", trim: true),
         {:ok, 200, body} <-
           HttpClient.request(
             :get,
             base <> "/meta-data/iam/security-credentials/" <> role,
             headers,
             nil,
             2_000,
             req
           ),
         {:ok,
          %{
            "AccessKeyId" => id,
            "SecretAccessKey" => secret,
            "Token" => token,
            "Expiration" => expiration
          }} <- JSON.decode(body),
         {:ok, expires_at} <- expiry(expiration) do
      {:ok, creds(id, secret, token), expires_at}
    else
      _ -> :miss
    end
  end

  # ── cache ─────────────────────────────────────────────────────────────────

  defp cached(key, fetch) do
    now = System.system_time(:second)

    case :ets.lookup(table(), key) do
      [{^key, creds, expires_at}] when expires_at - now > @refresh_window_s ->
        {:ok, creds}

      _ ->
        case fetch.() do
          {:ok, creds, expires_at} ->
            :ets.insert(table(), {key, creds, expires_at})
            {:ok, creds}

          other ->
            other
        end
    end
  end

  defp table do
    case :ets.whereis(@table) do
      :undefined ->
        # Two processes can race to create it; the loser adopts the winner's.
        try do
          :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
        rescue
          ArgumentError -> :ets.whereis(@table)
        end

      tid ->
        tid
    end
  end

  @doc false
  # Drop every cached credential (tests).
  def clear do
    case :ets.whereis(@table) do
      :undefined -> :ok
      tid -> :ets.delete_all_objects(tid) && :ok
    end
  end

  # ── helpers ───────────────────────────────────────────────────────────────

  defp expiry(iso8601) when is_binary(iso8601) do
    case DateTime.from_iso8601(iso8601) do
      {:ok, at, _offset} -> {:ok, DateTime.to_unix(at)}
      _ -> :error
    end
  end

  defp expiry(_other), do: :error

  defp creds(id, secret, token),
    do: %{access_key_id: id, secret_access_key: secret, session_token: token}

  defp non_empty(value) when is_binary(value) and value != "", do: value
  defp non_empty(_value), do: nil

  defp timeout(opts), do: Keyword.get(opts, :timeout_ms, 10_000)
  defp req_options(opts), do: Keyword.get(opts, :req_options, [])
end
