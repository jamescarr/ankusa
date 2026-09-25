defmodule Ankusa.Edge.Ingest do
  @moduledoc """
  Orchestrates one webhook on the hot path: build the envelope, verify inline,
  apply the per-source failure policy, then hand off to the
  group-commit batcher and block until it commits.

  The only place a `2xx`-worthy result is produced is *after* the batcher's WAL
  commit returns `{:committed, _}`, or after a durable quarantine write.
  Everything else is a non-2xx. There is no duplicate result to produce:
  whether a copy is the first one is dispatch's decision, made against what has
  been delivered, not the edge's.
  """

  alias Ankusa.{Envelope, Source, Verification}
  alias Ankusa.Edge.{Batcher, BatcherSupervisor, Quarantine}

  @type result ::
          {:ok, Envelope.t()}
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

    # The tenant names a storage partition and a gateway path segment, so it is
    # checked here, before anything is written: a bad one from any
    # RouteResolver is a 404, like an unknown source, not a dispatch failure
    # after the provider was already acked.
    if Ankusa.ClaimCheck.Ref.valid_tenant?(tenant_id) do
      accept(instance, source, tenant_id, req)
    else
      {:error, :unknown_source}
    end
  end

  defp accept(instance, source, tenant_id, req) do
    env = build_envelope(source, tenant_id, req)

    case verify(instance, source, env) do
      {:accept, env} -> commit(instance, source, env)
      {:quarantine, env, reason} -> quarantine(instance, env, reason)
      {:reject, reason} -> {:rejected, reason}
    end
  end

  # ── verification + policy ─────────────────────────────────────────────────

  defp verify(instance, %Source{verifier: {mod, opts}} = source, env) do
    scheme = verifier_scheme(mod, opts)

    outcome =
      Ankusa.Telemetry.span([:verify], %{instance: instance, source_id: source.id}, fn ->
        result = mod.verify(env, opts)
        {result, %{provider: mod, scheme: scheme, status: verify_status(result)}}
      end)

    case outcome do
      :ok ->
        {:accept,
         %{env | verification: %Verification{status: :ok, provider: mod, scheme: scheme}}}

      {:error, reason} ->
        v = %Verification{status: :failed, provider: mod, scheme: scheme, reason: reason}

        case source.on_verify_failure do
          :reject -> {:reject, reason}
          :quarantine -> {:quarantine, %{env | verification: v}, reason}
          :accept_flag -> {:accept, %{env | verification: %{v | flagged: true}}}
        end
    end
  end

  defp verifier_scheme(mod, opts) do
    # `function_exported?/3` answers false for a module that has not been loaded
    # yet, which would report the verifier's own name as its scheme on the first
    # hook of a process's life — and only then.
    Code.ensure_loaded(mod)

    if function_exported?(mod, :scheme_name, 1), do: mod.scheme_name(opts), else: inspect(mod)
  end

  defp verify_status(:ok), do: :ok
  defp verify_status({:error, _}), do: :failed

  # ── commit ────────────────────────────────────────────────────────────────

  defp commit(instance, _source, env) do
    partition = BatcherSupervisor.partition(instance, env.id)

    try do
      case Batcher.commit(instance, partition, %{envelope: env}) do
        {:committed, committed} -> {:ok, committed}
        {:error, :overload} -> {:error, :overload}
        {:error, :store_unavailable} -> {:error, :store_unavailable}
      end
    catch
      :exit, _ -> {:error, :store_unavailable}
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
    env = %Envelope{
      id: Ankusa.UUIDv7.generate(),
      source_id: source.id,
      tenant_id: tenant_id,
      received_at: System.system_time(:millisecond),
      method: req.method,
      path: req.path,
      headers: req.headers,
      content_type: nil,
      # keep raw bytes verbatim; copy so a small slice can't pin a large binary
      body: :binary.copy(req.body),
      size: byte_size(req.body)
    }

    # Read it back through the envelope so "the content-type header" is defined
    # in exactly one place, `Ankusa.Envelope.header/2`.
    %{env | content_type: Envelope.header(env, "content-type")}
  end

  defp tag({:ok, _}), do: :committed
  defp tag({:quarantined, _}), do: :quarantined
  defp tag({:rejected, _}), do: :rejected
  defp tag({:error, reason}), do: reason
end
