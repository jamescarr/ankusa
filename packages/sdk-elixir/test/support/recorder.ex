defmodule Ankusa.SDK.Recorder do
  @moduledoc false

  # Records the requests a test's clients send, in arrival order. Lives in an
  # Agent because a Req `:plug` transport runs inside the test process while the
  # raw-socket conformance gateway runs in a handler process.

  @spec new() :: pid()
  def new do
    {:ok, recorder} = Agent.start_link(fn -> [] end)
    recorder
  end

  @spec record(pid(), map()) :: :ok
  def record(recorder, request), do: Agent.update(recorder, &[request | &1])

  @spec requests(pid()) :: [map()]
  def requests(recorder), do: Agent.get(recorder, &Enum.reverse/1)

  @doc "A request body as recorded: `nil` when empty, else decoded JSON."
  @spec decode_body(binary()) :: term()
  def decode_body(""), do: nil

  def decode_body(body) do
    case JSON.decode(body) do
      {:ok, decoded} -> decoded
      {:error, _reason} -> body
    end
  end
end
