defmodule AnkusaServer.SecretProvider.Vault do
  @moduledoc """
  `${sm:<name>:<path>}` against HashiCorp Vault's KV version 2 engine
  (`type: vault`).

  `<path>` is the secret's path under the engine's mount (`mount`, default
  `secret`), as `vault kv get` takes it: `${sm:vault:ankusa/stripe#whsec}` reads
  field `whsec` of `secret/ankusa/stripe`. Without `#` the secret must hold
  exactly one field, which is the value; more than one and the reference has to
  name one.

  The request is `GET <address>/v1/<mount>/data/<path>` with the provider's
  `token` in `X-Vault-Token` (and `namespace` in `X-Vault-Namespace` for
  Vault Enterprise). `404` and `403` are `:not_found`; every other status that
  is not `200` (a sealed or standby Vault, a 5xx, a refused connection, a
  malformed reply) is `:unavailable`.
  """

  @behaviour Ankusa.SecretProvider

  alias Ankusa.HttpClient

  @max_response_bytes 1_048_576
  @encode &URI.char_unreserved?/1

  @impl true
  def fetch(ref, opts) do
    {path, field} = split(ref)

    with {:ok, segments} <- segments(path) do
      mount = opts |> Keyword.get(:mount, "secret") |> String.split("/", trim: true)

      url =
        Enum.join(
          [Keyword.fetch!(opts, :address), "v1"] ++ encode(mount) ++ ["data" | encode(segments)],
          "/"
        )

      headers =
        [{"x-vault-token", Keyword.fetch!(opts, :token)}, {"accept", "application/json"}] ++
          case Keyword.get(opts, :namespace) do
            nil -> []
            namespace -> [{"x-vault-namespace", namespace}]
          end

      case HttpClient.request(:get, url, headers, nil, Keyword.get(opts, :timeout_ms, 5_000),
             max_response_bytes: @max_response_bytes
           ) do
        {:ok, 200, reply} when is_binary(reply) -> pick(reply, field)
        {:ok, status, _reply} when status in [403, 404] -> {:error, :not_found}
        _other -> {:error, :unavailable}
      end
    end
  end

  defp split(ref) do
    case String.split(ref, "#", parts: 2) do
      [path, field] when field != "" -> {path, field}
      [path | _] -> {path, nil}
    end
  end

  # A path is `a/b/c`: no empty segment, and none that climbs out of the mount.
  defp segments(path) do
    segments = String.split(path, "/")

    if segments == [""] or Enum.any?(segments, &(&1 in ["", ".", ".."])),
      do: {:error, :not_found},
      else: {:ok, segments}
  end

  defp encode(segments), do: Enum.map(segments, &URI.encode(&1, @encode))

  defp pick(reply, field) do
    with {:ok, %{"data" => %{"data" => %{} = data}}} <- JSON.decode(reply),
         {:ok, value} <- field(data, field) do
      {:ok, value}
    else
      {:error, :not_found} -> {:error, :not_found}
      _other -> {:error, :unavailable}
    end
  end

  defp field(data, nil) do
    case Map.values(data) do
      [value] when is_binary(value) -> {:ok, value}
      _none_or_many -> {:error, :not_found}
    end
  end

  defp field(data, field) do
    case data do
      %{^field => value} when is_binary(value) -> {:ok, value}
      _missing -> {:error, :not_found}
    end
  end
end
