defmodule Ankusa.SDK.Receiver do
  @moduledoc """
  A `Plug` that receives `Ankusa.Sink.Http` deliveries and hands each one to
  your handler module.

  Mount it **before** any body parser (`Plug.Parsers`), so the raw bytes can be
  read: the SDK hands the handler exactly what the sink sent. `Ankusa.Sink.Http`
  signs every delivery unless it is configured `allow_unsigned: true`, so the
  receiver is closed by default: it needs the sink's `:secret` to verify the
  Standard Webhooks signature (`Ankusa.SDK.Signature`) and refuses to start
  without one, unless you pass `allow_unsigned: true`. Otherwise any host that
  can reach this URL could inject hooks.

  ```elixir
  # Bandit, standalone
  {Bandit,
   plug: {Ankusa.SDK.Receiver, handler: MyApp.Hooks, secret: System.fetch_env!("ANKUSA_WHSEC")},
   port: 4200}

  # Phoenix, in an endpoint
  plug Ankusa.SDK.Receiver,
    path: "/deliveries",
    handler: MyApp.Hooks,
    secret: System.fetch_env!("ANKUSA_WHSEC")

  plug Plug.Parsers, parsers: [:json], json_decoder: JSON
  ```

  Responses, matching what `Ankusa.Sink.Http` treats as success:

  | Situation | Response |
  | --- | --- |
  | handler returned `:ok` | `202`, empty body |
  | handler returned `{:error, reason}` | `503` (the dispatcher retries per `dispatch.retry`) |
  | body unreadable | `400`, `{"error":"body unreadable"}` |
  | body over `:max_body_bytes` | `413`, `{"error":"body too large"}` |
  | signature missing or does not verify | `401`, `{"error":"invalid_signature","code":"..."}` |
  | `x-ankusa-id` missing (after the signature passed) | `400`, `{"error":"missing x-ankusa-id"}` |

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
  * `:secret` (required unless `allow_unsigned: true`) — the sink's secret
    (`whsec_` + base64, or any other string used as its own bytes), or a list
    of them during a rotation.
  * `:allow_unsigned` (optional) — `true` accepts deliveries with no
    signature when no `:secret` is set. Default `false`. A `:secret` is
    never weakened by it: with one, every delivery must verify.
  * `:tolerance_seconds` (optional) — how far `webhook-timestamp` may be from
    now; default `300`. Only meaningful with `:secret`.
  """

  @behaviour Plug

  import Plug.Conn

  require Logger

  alias Ankusa.SDK.{Hook, Signature, Webhook}

  @default_max_body_bytes 8_000_000
  @read_chunk 1_000_000
  @options [:handler, :path, :max_body_bytes, :secret, :allow_unsigned, :tolerance_seconds]

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

  # The options handed to `Webhook.receive_delivery/3`. A receiver with neither a
  # secret nor `allow_unsigned: true` would answer every request `401`, so it
  # fails at boot instead.
  defp verification(opts) do
    tolerance = Keyword.get(opts, :tolerance_seconds, 300)

    unless is_integer(tolerance) and tolerance >= 0 do
      raise ArgumentError,
            ":tolerance_seconds must be a non-negative integer, got: #{inspect(tolerance)}"
    end

    allow_unsigned? = Keyword.get(opts, :allow_unsigned, false)

    unless is_boolean(allow_unsigned?) do
      raise ArgumentError,
            ":allow_unsigned must be true or false, got: #{inspect(allow_unsigned?)}"
    end

    case Keyword.fetch(opts, :secret) do
      :error when allow_unsigned? ->
        [allow_unsigned: true]

      :error ->
        raise ArgumentError,
              "the :secret option is required: Ankusa.Sink.Http signs every delivery, and a " <>
                "receiver that does not verify accepts hooks from any host that can reach it " <>
                "(pass allow_unsigned: true for a sink configured allow_unsigned: true)"

      {:ok, secret} ->
        secrets = List.wrap(secret)

        # Secrets are checked before anything else, so verifying an empty
        # request reports a bad secret here, at boot, instead of on every hook.
        case Signature.verify(%{}, "", secrets) do
          {:error, %{code: "invalid_secret"}} ->
            raise ArgumentError,
                  ":secret must be a non-empty string (or a list of them); a whsec_ secret must be base64"

          {:error, _missing_header} ->
            [secrets: secrets, allow_unsigned: allow_unsigned?, tolerance_seconds: tolerance]
        end
    end
  end

  @impl Plug
  def call(conn, {handler, path, max_body_bytes, receive_opts}) do
    if is_nil(path) or conn.request_path == path do
      receive_hook(conn, handler, max_body_bytes, receive_opts)
    else
      conn
    end
  end

  defp receive_hook(conn, handler, max_body_bytes, receive_opts) do
    case conn.body_params do
      %Plug.Conn.Unfetched{} ->
        handle(conn, handler, max_body_bytes, receive_opts)

      _parsed ->
        raise ArgumentError,
              "Ankusa.SDK.Receiver needs the raw request body; mount it before Plug.Parsers"
    end
  end

  # The body is read before anything is decided: the signature covers it, and it
  # is judged before the hook id (a request that is not a signed delivery is a
  # `401`, whatever else it lacks).
  defp handle(conn, handler, max_body_bytes, receive_opts) do
    case read_whole_body(conn, max_body_bytes) do
      {:ok, body, conn} ->
        case Webhook.receive_delivery(conn.req_headers, body, receive_opts) do
          {:ok, %{headers: headers}} ->
            dispatch(conn, handler, headers, body)

          {:error, %Ankusa.SDK.InvalidSignatureError{code: code}} ->
            conn
            |> put_resp_content_type("application/json")
            |> send_resp(401, JSON.encode!(%{error: "invalid_signature", code: code}))
            |> halt()

          {:error, %Ankusa.SDK.MissingHookIdError{}} ->
            error_response(conn, 400, "missing x-ankusa-id")
        end

      {:too_large, conn} ->
        error_response(conn, 413, "body too large")

      {:error, _reason, conn} ->
        error_response(conn, 400, "body unreadable")
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
      verification: headers.verification,
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
