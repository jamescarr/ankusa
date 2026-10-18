defmodule Ankusa.SDK.Receiver do
  @moduledoc """
  A `Plug` that receives `Ankusa.Sink.Http` deliveries and hands each one to
  your handler module.

  Mount it **before** any body parser (`Plug.Parsers`), so the raw bytes can be
  read: the SDK hands the handler exactly what the sink sent. With `:secret`
  set it first verifies the Standard Webhooks signature a sink configured with
  the same `secret` adds (`Ankusa.SDK.Signature`); without it, nothing is
  verified.

  ```elixir
  # Bandit, standalone
  {Bandit, plug: {Ankusa.SDK.Receiver, handler: MyApp.Hooks}, port: 4200}

  # Phoenix, in an endpoint
  plug Ankusa.SDK.Receiver, path: "/deliveries", handler: MyApp.Hooks
  plug Plug.Parsers, parsers: [:json], json_decoder: JSON
  ```

  Responses, matching what `Ankusa.Sink.Http` treats as success:

  | Situation | Response |
  | --- | --- |
  | handler returned `:ok` | `202`, empty body |
  | handler returned `{:error, reason}` | `503` (the dispatcher retries per `dispatch.retry`) |
  | `x-ankusa-id` missing | `400`, `{"error":"missing x-ankusa-id"}` |
  | body unreadable | `400`, `{"error":"body unreadable"}` |
  | body over `:max_body_bytes` | `413`, `{"error":"body too large"}` |
  | signature does not verify (`:secret` set) | `401`, `{"error":"invalid_signature","code":"..."}` |

  Anything else the handler does — a different return value, a raise — is not
  rescued: Plug turns it into a `500`, which the dispatcher retries exactly
  like a `503`.

  The request method is not checked. A source's `dispatch` config may use
  `POST`, `PUT` or `PATCH`; the hook id, not the method, identifies the
  delivery.

  ## Options

  * `:handler` (required) — a module implementing `Ankusa.SDK.Handler`, or
    `{module, arg}`; a bare module means the arg `[]`.
  * `:path` (optional) — when set, requests to any other path are passed
    through untouched, so the plug can sit in a shared endpoint.
  * `:max_body_bytes` (optional) — default `8_000_000`, the same cap core's
    ingest applies to a raw webhook body.
  * `:secret` (optional) — the sink's secret (`whsec_` + base64, or any other
    string used as its own bytes), or a list of them during a rotation.
  * `:tolerance_seconds` (optional) — how far `webhook-timestamp` may be from
    now; default `300`. Only meaningful with `:secret`.
  """

  @behaviour Plug

  import Plug.Conn

  require Logger

  alias Ankusa.SDK.{Hook, Signature, Webhook}

  @default_max_body_bytes 8_000_000
  @read_chunk 1_000_000
  @options [:handler, :path, :max_body_bytes, :secret, :tolerance_seconds]

  @impl Plug
  def init(opts) when is_list(opts) do
    case Keyword.keys(opts) -- @options do
      [] ->
        :ok

      [key | _] ->
        raise ArgumentError,
              "unknown option #{inspect(key)} — expected one of " <>
                "#{Enum.map_join(@options, ", ", &inspect/1)}"
    end

    handler =
      case Keyword.fetch(opts, :handler) do
        {:ok, handler} ->
          normalize_handler!(handler)

        :error ->
          raise ArgumentError,
                "the :handler option is required (a module, or {module, arg})"
      end

    path =
      case Keyword.get(opts, :path) do
        nil -> nil
        path when is_binary(path) -> path
        other -> raise ArgumentError, ":path must be a string, got: #{inspect(other)}"
      end

    max_body_bytes = Keyword.get(opts, :max_body_bytes, @default_max_body_bytes)

    unless is_integer(max_body_bytes) and max_body_bytes > 0 do
      raise ArgumentError,
            ":max_body_bytes must be a positive integer, got: #{inspect(max_body_bytes)}"
    end

    {handler, path, max_body_bytes, verification(opts)}
  end

  def init(other) do
    raise ArgumentError, "options must be a keyword list, got: #{inspect(other)}"
  end

  defp verification(opts) do
    tolerance = Keyword.get(opts, :tolerance_seconds, 300)

    unless is_integer(tolerance) and tolerance >= 0 do
      raise ArgumentError,
            ":tolerance_seconds must be a non-negative integer, got: #{inspect(tolerance)}"
    end

    case Keyword.fetch(opts, :secret) do
      :error ->
        nil

      {:ok, secret} ->
        secrets = List.wrap(secret)

        # Secrets are checked before anything else, so verifying an empty
        # request reports a bad secret here, at boot, instead of on every hook.
        case Signature.verify(%{}, "", secrets) do
          {:error, %{code: "invalid_secret"}} ->
            raise ArgumentError,
                  ":secret must be a non-empty string (or a list of them); a whsec_ secret must be base64"

          {:error, _missing_header} ->
            {secrets, tolerance}
        end
    end
  end

  @impl Plug
  def call(conn, {handler, path, max_body_bytes, verification}) do
    if is_nil(path) or conn.request_path == path do
      receive_hook(conn, handler, max_body_bytes, verification)
    else
      conn
    end
  end

  defp receive_hook(conn, handler, max_body_bytes, verification) do
    case conn.body_params do
      %Plug.Conn.Unfetched{} ->
        handle(conn, handler, max_body_bytes, verification)

      _parsed ->
        raise ArgumentError,
              "Ankusa.SDK.Receiver needs the raw request body; mount it before Plug.Parsers"
    end
  end

  defp handle(conn, handler, max_body_bytes, verification) do
    case Webhook.parse_headers(conn.req_headers) do
      {:error, %Ankusa.SDK.MissingHookIdError{}} ->
        error_response(conn, 400, "missing x-ankusa-id")

      {:ok, headers} ->
        case read_whole_body(conn, max_body_bytes) do
          {:ok, body, conn} ->
            case verify(conn, body, verification) do
              :ok ->
                dispatch(conn, handler, headers, body)

              {:error, code} ->
                conn
                |> put_resp_content_type("application/json")
                |> send_resp(401, JSON.encode!(%{error: "invalid_signature", code: code}))
                |> halt()
            end

          {:too_large, conn} ->
            error_response(conn, 413, "body too large")

          {:error, _reason, conn} ->
            error_response(conn, 400, "body unreadable")
        end
    end
  end

  defp verify(_conn, _body, nil), do: :ok

  defp verify(conn, body, {secrets, tolerance}) do
    case Signature.verify(conn.req_headers, body, secrets, tolerance_seconds: tolerance) do
      {:ok, _verified} -> :ok
      {:error, error} -> {:error, error.code}
    end
  end

  defp dispatch(conn, {module, arg}, headers, body) do
    hook = %Hook{
      id: headers.id,
      source_id: headers.source,
      tenant_id: headers.tenant,
      content_type: headers.content_type,
      body: body,
      received_at: nil,
      size: byte_size(body),
      dedupe_key: headers.dedupe_key,
      replay_id: headers.replay_id,
      idempotency_key: headers.idempotency_key,
      headers: headers.headers
    }

    case module.handle_hook(hook, arg) do
      :ok ->
        conn |> send_resp(202, "") |> halt()

      {:error, reason} ->
        Logger.warning("ankusa hook #{hook.id} not handled: #{inspect(reason)}")
        conn |> send_resp(503, "") |> halt()
    end
  end

  defp error_response(conn, status, error) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, JSON.encode!(%{error: error}))
    |> halt()
  end

  defp read_whole_body(conn, max_body_bytes) do
    read_chunks(conn, max_body_bytes, [], 0)
  end

  defp read_chunks(conn, max, acc, size) do
    # `length` caps the read at one byte past the limit, so an oversized body
    # is detected without buffering it.
    case read_body(conn, read_length: @read_chunk, length: max - size + 1) do
      {:ok, data, conn} ->
        if size + byte_size(data) > max do
          {:too_large, conn}
        else
          {:ok, IO.iodata_to_binary(Enum.reverse([data | acc])), conn}
        end

      {:more, data, conn} ->
        size = size + byte_size(data)

        if size > max do
          {:too_large, conn}
        else
          read_chunks(conn, max, [data | acc], size)
        end

      {:error, reason} ->
        {:error, reason, conn}
    end
  end

  defp normalize_handler!({module, arg}) when is_atom(module) and not is_nil(module),
    do: {module, arg}

  defp normalize_handler!(module) when is_atom(module) and not is_nil(module), do: {module, []}

  defp normalize_handler!(other) do
    raise ArgumentError, ":handler must be a module or {module, arg}, got: #{inspect(other)}"
  end
end
