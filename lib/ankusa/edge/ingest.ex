defmodule Ankusa.Edge.Ingest do
  @moduledoc """
  Orchestrates one webhook on the hot path: build the envelope, verify inline,
  apply the per-source failure policy, extract the dedup key, then hand off to the
  group-commit batcher and block until it commits.

  The only place a `2xx`-worthy result is produced is *after* the batcher's WAL
  commit returns (`{:committed, _}` / `{:duplicate, _}`), or after a durable
  quarantine write. Everything else is a non-2xx.
  """

  alias Ankusa.{Envelope, Source, Verification}
  alias Ankusa.Edge.{Batcher, BatcherSupervisor, Quarantine}

  @type result ::
          {:ok, Envelope.t()}
          | {:duplicate, Envelope.t()}
          | {:quarantined, term()}
          | {:rejected, term()}
          | {:error, :unknown_source | :overload | :store_unavailable}

  @type request :: %{
          required(:source_id) => String.t(),
          required(:method) => String.t(),
          required(:path) => String.t(),
          required(:headers) => [{String.t(), String.t()}],
          required(:body) => binary(),
          # optional tenant override from a multi-tenant RouteResolver; when
          # absent, the source's own tenant_id is used
          optional(:tenant_id) => String.t()
        }

  @spec ingest(atom(), request()) :: result()
  def ingest(instance, %{source_id: source_id} = req) do
    Ankusa.Telemetry.span([:ingest], %{instance: instance, source_id: source_id}, fn ->
      result =
        case Ankusa.SourceStore.fetch(instance, source_id) do
          {:ok, source} -> do_ingest(instance, source, req)
          :error -> {:error, :unknown_source}
        end

      {result, %{size: byte_size(req.body), outcome: tag(result)}}
    end)
  end

  defp do_ingest(instance, %Source{} = source, req) do
    tenant_id = Map.get(req, :tenant_id) || source.tenant_id
    env = build_envelope(source, tenant_id, req)

    case verify(instance, source, env) do
      {:accept, env} -> commit(instance, env)
      {:quarantine, env, reason} -> quarantine(instance, env, reason)
      {:reject, reason} -> {:rejected, reason}
    end
  end

  # ── verification + policy ─────────────────────────────────────────────────

  defp verify(instance, %Source{verifier: {mod, opts}} = source, env) do
    outcome =
      Ankusa.Telemetry.span([:verify], %{instance: instance, source_id: source.id}, fn ->
        {mod.verify(env, opts), %{provider: mod}}
      end)

    case outcome do
      :ok ->
        {:accept, %{env | verification: %Verification{status: :ok, provider: mod}}}

      {:error, reason} ->
        v = %Verification{status: :failed, provider: mod, reason: reason}

        case source.on_verify_failure do
          :reject -> {:reject, reason}
          :quarantine -> {:quarantine, %{env | verification: v}, reason}
          :accept_flag -> {:accept, %{env | verification: %{v | flagged: true}}}
        end
    end
  end

  # ── dedup + commit ────────────────────────────────────────────────────────

  defp commit(instance, env) do
    env = %{env | dedup_key: dedup_key(instance, env)}
    partition = BatcherSupervisor.partition(instance, env.id)

    try do
      case Batcher.commit(instance, partition, %{envelope: env}) do
        {:committed, committed} -> {:ok, committed}
        {:duplicate, seq} -> {:duplicate, %{env | seq: seq}}
        {:error, :overload} -> {:error, :overload}
      end
    catch
      :exit, _ -> {:error, :store_unavailable}
    end
  end

  defp dedup_key(instance, env) do
    {mod, opts} =
      case Ankusa.SourceStore.fetch(instance, env.source_id) do
        {:ok, %Source{dedup: dedup}} -> dedup
        :error -> {Ankusa.DedupKey.Rules, []}
      end

    case mod.extract(env, opts) do
      {:ok, key} when is_binary(key) -> key
      _ -> nil
    end
  end

  # ── quarantine ────────────────────────────────────────────────────────────

  defp quarantine(instance, env, reason) do
    case Quarantine.put(instance, env, reason) do
      :ok -> {:quarantined, reason}
      :rate_limited -> {:rejected, {:quarantine_rate_limited, reason}}
    end
  end

  # ── envelope construction ─────────────────────────────────────────────────

  defp build_envelope(%Source{} = source, tenant_id, req) do
    %Envelope{
      id: Ankusa.UUIDv7.generate(),
      source_id: source.id,
      tenant_id: tenant_id,
      received_at: System.system_time(:millisecond),
      method: req.method,
      path: req.path,
      headers: req.headers,
      content_type: content_type(req.headers),
      # keep raw bytes verbatim; copy so a small slice can't pin a large binary
      body: :binary.copy(req.body),
      size: byte_size(req.body)
    }
  end

  defp content_type(headers) do
    Enum.find_value(headers, fn {k, v} ->
      if String.downcase(k) == "content-type", do: v
    end)
  end

  defp tag({:ok, _}), do: :committed
  defp tag({:duplicate, _}), do: :duplicate
  defp tag({:quarantined, _}), do: :quarantined
  defp tag({:rejected, _}), do: :rejected
  defp tag({:error, reason}), do: reason
end
