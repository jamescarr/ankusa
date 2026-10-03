defmodule Ankusa.Edge.Publish do
  @moduledoc """
  The `wal: :none` ack path: publish one verified envelope to every sink its
  source declares, in the request process, and answer only once each has
  confirmed.

  All sinks publish concurrently, and the whole set runs under one deadline,
  `direct_publish_timeout_ms` (default 8 000 ms, under the provider's own
  timeout). A sink that fails, crashes, or misses the deadline is a `503` with
  `Retry-After`, and the provider retries. Sinks that already confirmed keep
  their copy, so the retry is a second hook with a second `id` — the same
  at-least-once contract a provider retry after a lost ack has always had
  here, and the reason consumers dedupe on the provider's own event id.

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

  def publish(instance, %Source{} = source, %Envelope{} = env) do
    with {:ok, ctx} <- ctx(instance, source, env) do
      timeout = Ankusa.config(instance).direct_publish_timeout_ms

      tasks =
        Enum.map(source.sinks, fn {mod, opts} ->
          Task.async(fn -> Sink.safe_deliver(mod, env, ctx, opts) end)
        end)

      results = Task.yield_many(tasks, timeout: timeout, on_timeout: :kill_task)

      outcomes =
        source.sinks
        |> Enum.zip(results)
        |> Enum.map(fn {{mod, _opts}, {_task, result}} ->
          case result do
            {:ok, :ok} ->
              {mod, :ok}

            {:ok, {:error, reason}} ->
              Logger.warning(
                "[ankusa] sink #{inspect(mod)} refused hook #{env.id}: #{inspect(reason)}"
              )

              {mod, :error}

            nil ->
              Logger.warning(
                "[ankusa] sink #{inspect(mod)} did not confirm hook #{env.id} within " <>
                  "#{timeout}ms; answered 503 (the sink may still store it; " <>
                  "consumers dedupe on id/dedupe_key)"
              )

              {mod, :error}

            {:exit, reason} ->
              Logger.warning(
                "[ankusa] sink #{inspect(mod)} crashed on hook #{env.id}: #{inspect(reason)}"
              )

              {mod, :error}
          end
        end)

      Enum.each(outcomes, fn {_mod, outcome} -> emit(instance, outcome) end)

      if Enum.all?(outcomes, &match?({_mod, :ok}, &1)) do
        {:ok, env}
      else
        {:error, :store_unavailable}
      end
    end
  end

  # The ctx every sink already handles under dispatch (`Pipeline.ctx/3`),
  # including the one claim-check ref every sink and retry shares.
  defp ctx(instance, %Source{} = source, env) do
    ctx = %{
      instance: instance,
      source_id: env.source_id,
      tenant_id: env.tenant_id,
      attempt: 1,
      forward_headers: source.forward_headers
    }

    if Enum.any?(source.sinks, fn {mod, opts} -> needs_claim?(mod, opts, env) end) do
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
