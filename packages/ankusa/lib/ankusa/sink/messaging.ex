defmodule Ankusa.Sink.Messaging do
  @moduledoc """
  What a queue-style sink shares: `use` it, implement `c:publish/4`, and the
  sink is an `Ankusa.Sink` whose `deliver/3` encodes the hook as
  `Ankusa.Sink.Message` (inline up to `:inline_max_bytes`, a claim ticket
  above it — `c:Ankusa.Sink.inline_max_bytes/1` is implemented for you) and
  hands the payload to `publish/4`. Everything transport-specific — the
  destination, headers (`Ankusa.Sink.Message.headers/2` /
  `Ankusa.Sink.Message.attributes/2`), the size gate
  (`Ankusa.Sink.Message.fits/2`), the connection (`Ankusa.Sink.Connections`)
  and the error classes — is the sink's. `durable?/1` and `describe/2` are
  still the sink's to add.

      defmodule MyApp.Sink.Queue do
        use Ankusa.Sink.Messaging

        @impl true
        def publish(payload, env, _ctx, opts) do
          MyApp.Queue.send(Keyword.fetch!(opts, :queue), payload, message_id: env.id)
        end
      end

  An option that may differ per hook — a routing key, a subject, a record key
  — is a `t:per_hook/1`: one string for every hook, or a function of the
  `Ankusa.Envelope`. `resolve/2` is its value for one hook and `static/1` the
  value a `describe/2` can name (`nil` when a function computes it).
  `endpoints/1` reads a broker list in every shape an adapter accepts: a list
  of `"host:port"` strings or `{host, port}` tuples, or one comma-separated
  string from a YAML loader.

  A sink that needs its own `deliver/3` implements `Ankusa.Sink` directly
  instead, as `Ankusa.Sink.Http` does.
  """

  alias Ankusa.Envelope
  alias Ankusa.Sink.Message

  @doc """
  Publish `payload`, the encoded `Ankusa.Sink.Message` for `env`. `:ok` only
  once the transport confirmed it; otherwise `{:error, reason}` in the classes
  `Ankusa.Sink` documents.
  """
  @callback publish(payload :: binary(), Envelope.t(), Ankusa.Sink.ctx(), opts :: keyword()) ::
              :ok | {:error, Ankusa.Sink.failure()}

  defmacro __using__(_opts) do
    quote do
      @behaviour Ankusa.Sink
      @behaviour Ankusa.Sink.Messaging

      @impl Ankusa.Sink
      def deliver(%Ankusa.Envelope{} = env, ctx, opts),
        do: Ankusa.Sink.Messaging.deliver(__MODULE__, env, ctx, opts)

      @impl Ankusa.Sink
      def inline_max_bytes(opts), do: Ankusa.Sink.Message.inline_max_bytes(opts)
    end
  end

  @doc """
  `c:Ankusa.Sink.deliver/3` for a sink that `use`s this module: encode, then
  `mod.publish/4`. A failed claim check-in is `{:error, {:claim_check,
  reason}}`, and `publish/4` never runs.
  """
  @spec deliver(module(), Envelope.t(), Ankusa.Sink.ctx(), keyword()) :: :ok | {:error, term()}
  def deliver(mod, %Envelope{} = env, ctx, opts) do
    with {:ok, payload} <- Message.encode(env, ctx, Message.inline_max_bytes(opts)),
         do: mod.publish(payload, env, ctx, opts)
  end

  @typedoc "A sink option that is one value for every hook, or a function of the hook."
  @type per_hook(value) :: value | (Envelope.t() -> value)

  @doc "A `t:per_hook/1` option's value for `env`; `nil` stays `nil`."
  @spec resolve(per_hook(String.t()) | nil, Envelope.t()) :: String.t() | nil
  def resolve(nil, _env), do: nil
  def resolve(value, _env) when is_binary(value), do: value
  def resolve(fun, %Envelope{} = env) when is_function(fun, 1), do: fun.(env)

  @doc """
  The one value a `t:per_hook/1` option has for every hook, or `nil` when a
  function computes it, so a description cannot name it.
  """
  @spec static(per_hook(String.t()) | nil) :: String.t() | nil
  def static(value) when is_binary(value), do: value
  def static(_fun_or_nil), do: nil

  @doc """
  Broker endpoints as `{host, port}` pairs with an integer port, from a list
  of `"host:port"` strings and/or `{host, port}` tuples, or one
  comma-separated `"host:port,host:port"` string (what a YAML loader hands
  over). Raises `ArgumentError` on an entry with no port.
  """
  @spec endpoints(String.t() | [String.t() | {String.t() | charlist(), pos_integer()}]) ::
          [{String.t(), pos_integer()}]
  def endpoints(servers) when is_binary(servers) do
    servers
    |> String.split(",")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> endpoints()
  end

  def endpoints(servers) when is_list(servers), do: Enum.map(servers, &endpoint/1)

  defp endpoint({host, port}) when is_integer(port) and port > 0, do: {to_string(host), port}

  defp endpoint(host_port) when is_binary(host_port) do
    case String.split(host_port, ":", parts: 2) do
      [host, port] when host != "" ->
        {host, String.to_integer(port)}

      _ ->
        raise ArgumentError, "expected \"host:port\" or {host, port}, got: #{inspect(host_port)}"
    end
  end

  defp endpoint(other),
    do: raise(ArgumentError, "expected \"host:port\" or {host, port}, got: #{inspect(other)}")
end
