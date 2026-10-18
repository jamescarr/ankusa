defmodule Ankusa.Http do
  @moduledoc """
  Small `Plug.Conn` helpers shared by every Bandit-fronted router in the
  framework (`Ankusa.Edge.Router`, `Ankusa.ClaimCheck.Router`). Kept here
  instead of duplicated so both surfaces enforce request bodies identically.
  """

  # One socket read asks the transport for at most this many bytes. It is also
  # the stride of the deadline check, so a body that arrives slower than
  # `chunk / read_timeout` bytes per second runs into the deadline instead of
  # holding the connection for as long as it keeps trickling.
  @chunk 1_000_000

  @typedoc """
  Limits on how long reading a body may take, both in milliseconds. Absent keys
  leave Bandit's own per-read timeout (15 s) and no overall deadline.

    * `:read_timeout` — one socket read of the body must complete within this
      (`listener.body_read_timeout_ms`)
    * `:deadline` — the whole body must have arrived this long after the read
      started (`listener.request_deadline_ms`)
  """
  @type read_opts :: [read_timeout: pos_integer(), deadline: pos_integer()]

  @doc """
  The `read_body_limited/3` options `config.listener` asks for.
  """
  @spec read_opts(Ankusa.Config.t()) :: read_opts()
  def read_opts(%Ankusa.Config{listener: listener}) do
    [read_timeout: listener.body_read_timeout_ms, deadline: listener.request_deadline_ms]
  end

  @doc """
  Read the body, refusing anything over `max` bytes without buffering it all.

  `{:too_large, conn}` is a body that exceeded the limit; `{:error, reason, conn}`
  is one that could not be read at all (client disconnect, read timeout, or
  `:request_deadline` when the body had not arrived within `:deadline`). Those
  are different failures and are reported as such — folding a dropped connection
  into "payload too large" tells the caller to shrink a body that was never the
  problem.

  A body that arrives in one read (every ordinary webhook) is returned as the
  binary the transport produced, uncopied.
  """
  @spec read_body_limited(Plug.Conn.t(), pos_integer(), read_opts()) ::
          {:ok, binary(), Plug.Conn.t()}
          | {:too_large, Plug.Conn.t()}
          | {:error, term(), Plug.Conn.t()}
  def read_body_limited(conn, max, opts \\ []) do
    deadline =
      case opts[:deadline] do
        nil -> nil
        ms -> System.monotonic_time(:millisecond) + ms
      end

    read_chunks(conn, max, opts[:read_timeout], deadline, [], 0)
  end

  # `length` is what may still arrive plus one byte: a body that fills it is
  # over the limit, and one that ends first is whole.
  defp read_chunks(conn, max, read_timeout, deadline, acc, size) do
    case remaining(read_timeout, deadline) do
      :deadline ->
        {:error, :request_deadline, conn}

      {:ok, timeout} ->
        read_opts =
          [length: min(@chunk, max - size + 1), read_length: @chunk] ++
            if(timeout, do: [read_timeout: timeout], else: [])

        case Plug.Conn.read_body(conn, read_opts) do
          {:ok, data, conn} -> finish(conn, max, acc, size, data)
          {:more, data, conn} -> more(conn, max, read_timeout, deadline, acc, size, data)
          {:error, reason} -> {:error, reason, conn}
        end
    end
  end

  defp finish(conn, max, acc, size, data) do
    cond do
      size + byte_size(data) > max -> {:too_large, conn}
      acc == [] -> {:ok, data, conn}
      true -> {:ok, IO.iodata_to_binary([acc, data]), conn}
    end
  end

  defp more(conn, max, read_timeout, deadline, acc, size, data) do
    size = size + byte_size(data)

    if size > max,
      do: {:too_large, conn},
      else: read_chunks(conn, max, read_timeout, deadline, [acc, data], size)
  end

  # The timeout for the next read: the configured one, cut to what is left of
  # the deadline so the last read cannot run past it.
  defp remaining(read_timeout, nil), do: {:ok, read_timeout}

  defp remaining(read_timeout, deadline) do
    left = deadline - System.monotonic_time(:millisecond)

    cond do
      left <= 0 -> :deadline
      read_timeout == nil -> {:ok, left}
      true -> {:ok, min(read_timeout, left)}
    end
  end

  @doc "Send a JSON-encoded response body with the given status."
  @spec send_json(Plug.Conn.t(), pos_integer(), map()) :: Plug.Conn.t()
  def send_json(conn, status, payload) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, JSON.encode!(payload))
  end
end
