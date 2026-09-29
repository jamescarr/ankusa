defmodule Ankusa.Edge.Publish do
  @moduledoc """
  The `wal: :none` ack path: publish one verified envelope to every sink its
  source declares, in the request process, and answer only once each has
  confirmed.

  Sinks run in declaration order, one at a time. The first failure is the
  answer: `503` with `Retry-After`, and the provider retries. Sinks that already
  confirmed keep their copy, so the retry is a second hook with a second `id` —
  the same at-least-once contract a provider retry after a lost ack has always
  had here, and the reason consumers dedupe on the provider's own event id.

  There is no retry policy and no dead-letter queue in this mode: the provider
  is the retry, and the broker is the durable store.
  """

  require Logger

  alias Ankusa.{Envelope, Sink, Source}
  alias Ankusa.Sink.Message

  @spec publish(atom(), Source.t(), Envelope.t()) ::
          {:ok, Envelope.t()} | {:error, :store_unavailable}
  def publish(_instance, %Source{sinks: []}, %Envelope{} = env) do
    Logger.warning("[ankusa] source #{env.source_id} has no sinks; nothing can ack")
    {:error, :store_unavailable}
  end

  def publish(instance, %Source{sinks: sinks}, %Envelope{} = env) do
    with {:ok, ctx} <- ctx(instance, sinks, env) do
      Enum.reduce_while(sinks, {:ok, env}, fn {mod, opts}, acc ->
        case Sink.safe_deliver(mod, env, ctx, opts) do
          :ok ->
            emit(instance, :ok)
            {:cont, acc}

          {:error, reason} ->
            emit(instance, :error)

            Logger.warning(
              "[ankusa] sink #{inspect(mod)} refused hook #{env.id}: #{inspect(reason)}"
            )

            {:halt, {:error, :store_unavailable}}
        end
      end)
    end
  end

  # The ctx every sink already handles under dispatch (`Pipeline.ctx/3`),
  # including the one claim-check ref every sink and retry shares.
  defp ctx(instance, sinks, env) do
    ctx = %{
      instance: instance,
      source_id: env.source_id,
      tenant_id: env.tenant_id,
      attempt: 1
    }

    if Enum.any?(sinks, fn {mod, opts} -> needs_claim?(mod, opts, env) end) do
      case Message.check_in(instance, env) do
        {:ok, claim} ->
          {:ok, Map.put(ctx, :claim, claim)}

        {:error, reason} ->
          Logger.warning("[ankusa] claim check failed for hook #{env.id}: #{inspect(reason)}")
          {:error, :store_unavailable}
      end
    else
      {:ok, ctx}
    end
  end

  defp needs_claim?(mod, opts, env) do
    case Sink.inline_max_bytes(mod, opts) do
      nil -> false
      max -> env.size > max
    end
  end

  # Same event dispatch emits, so `/metrics` counts this path with no change:
  # one attempt, no retry — the provider is the retry.
  defp emit(instance, result) do
    Ankusa.Telemetry.emit([:dispatch, :stop], %{}, %{
      instance: instance,
      result: result,
      attempts: 1
    })
  end
end
