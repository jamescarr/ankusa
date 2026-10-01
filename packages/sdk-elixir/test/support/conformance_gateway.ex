defmodule Ankusa.SDK.ConformanceGateway do
  @moduledoc false

  # A raw-socket mock gateway for the conformance vectors: it answers every
  # request with the case's `gateway` response and records what it saw. Built on
  # `:gen_tcp` (like the Ruby runner's) because Bypass-style servers cannot
  # express a delayed response that the client abandons mid-flight.

  @timeout 15_000

  alias Ankusa.SDK.Recorder

  @doc """
  Listen on a free port and answer `spec` to every connection.

  Returns the base URL; the listener is closed by an `on_exit` callback
  registered here.
  """
  @spec start(map(), pid()) :: String.t()
  def start(spec, recorder) do
    {:ok, listen} =
      :gen_tcp.listen(0, [
        :binary,
        packet: :http_bin,
        active: false,
        reuseaddr: true,
        ip: {127, 0, 0, 1}
      ])

    {:ok, port} = :inet.port(listen)
    acceptor = spawn(fn -> accept_loop(listen, spec, recorder) end)

    ExUnit.Callbacks.on_exit(fn ->
      :gen_tcp.close(listen)
      Process.exit(acceptor, :kill)
    end)

    "http://127.0.0.1:#{port}"
  end

  @doc "A vector `Body` as bytes; a missing body is zero bytes."
  @spec body_bytes(map() | nil) :: binary()
  def body_bytes(nil), do: ""
  def body_bytes(%{"text" => text}), do: text
  def body_bytes(%{"base64" => base64}), do: Base.decode64!(base64)
  def body_bytes(%{"json" => json}), do: JSON.encode!(json)

  defp accept_loop(listen, spec, recorder) do
    case :gen_tcp.accept(listen) do
      {:ok, socket} ->
        handler =
          spawn(fn ->
            receive do
              {:socket, socket} -> serve(socket, spec, recorder)
            end
          end)

        :ok = :gen_tcp.controlling_process(socket, handler)
        send(handler, {:socket, socket})
        accept_loop(listen, spec, recorder)

      {:error, _reason} ->
        :ok
    end
  end

  defp serve(socket, spec, recorder) do
    case read_request(socket) do
      {:ok, request} ->
        # Recorded before the delay: a client that times out mid-response still
        # sent the request.
        Recorder.record(recorder, request)
        Process.sleep(spec["delay_ms"] || 0)
        respond(socket, spec)

      :error ->
        :ok
    end

    :gen_tcp.close(socket)
  end

  defp read_request(socket) do
    with {:ok, method, path} <- request_line(socket),
         {:ok, headers} <- recv_headers(socket, %{}) do
      # `:http_bin` stops exactly at the end of the headers; the body (and any
      # bytes that arrived with it) is read in raw mode.
      :ok = :inet.setopts(socket, packet: :raw)

      {:ok,
       %{
         "method" => method,
         "path" => path,
         "headers" => headers,
         "body" => Recorder.decode_body(recv_body(socket, headers))
       }}
    end
  end

  defp request_line(socket) do
    case :gen_tcp.recv(socket, 0, @timeout) do
      {:ok, {:http_request, method, {:abs_path, path}, _version}} ->
        {:ok, to_string(method), path}

      _other ->
        :error
    end
  end

  defp recv_headers(socket, acc) do
    case :gen_tcp.recv(socket, 0, @timeout) do
      {:ok, {:http_header, _number, name, _reserved, value}} ->
        recv_headers(socket, Map.put(acc, header_name(name), String.trim(value)))

      {:ok, :http_eoh} ->
        {:ok, acc}

      _other ->
        :error
    end
  end

  defp header_name(name) when is_atom(name), do: name |> Atom.to_string() |> String.downcase()
  defp header_name(name) when is_binary(name), do: String.downcase(name)

  defp recv_body(socket, headers) do
    case headers["content-length"] do
      nil ->
        ""

      value ->
        case Integer.parse(value) do
          {0, _rest} ->
            ""

          {length, _rest} ->
            case :gen_tcp.recv(socket, length, @timeout) do
              {:ok, body} -> body
              {:error, _reason} -> ""
            end

          :error ->
            ""
        end
    end
  end

  defp respond(socket, spec) do
    status = spec["status"]
    headers = spec["headers"] || %{}
    payload = body_bytes(spec["body"])

    response = [
      "HTTP/1.1 #{status} X\r\n",
      Enum.map(headers, fn {name, value} -> [name, ": ", value, "\r\n"] end),
      "content-length: #{byte_size(payload)}\r\n",
      "connection: close\r\n\r\n",
      payload
    ]

    # Ignored: the client may have timed out and gone away, which is a case the
    # vectors exercise on purpose.
    _ = :gen_tcp.send(socket, response)
    :ok
  end
end
