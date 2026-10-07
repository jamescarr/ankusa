defmodule Ankusa.Edge.Ingest do
  @moduledoc """
  Orchestrates one webhook on the hot path: build the envelope, verify inline,
  apply the per-source failure policy, then ack once the hook is durable.

  Which system accepts it is one config key. With `wal.type: disk` (the
  default) the group-commit batcher commits the hook to this node's WAL and
  dispatch reads it back later; with `wal.type: none` the hook is published to
  the source's sinks inside the request and the ack waits for their confirms
  (`Ankusa.Edge.Publish`).

  The only place a `2xx`-worthy result is produced is *after* one of those
  durable accepts returns, or after a durable quarantine write. Everything
  else is a non-2xx.

  ## Lookup precedes the body

  The source is looked up (`lookup/2`) before the request body is read, and
  `ingest/4` only ever runs on a source that exists. A request for a source that
  does not exist is a refusal (`refused/2`), counted on its own bounded counter
  and never on the `[:ankusa, :ingest]` span, so an unauthenticated caller
  cannot mint a metric series per URL it invents. `ingest/2` is the
  transport-less entry point (tests, benches, embedding): lookup, then ingest.
  """

  alias Ankusa.{Envelope, Source, Verification}
  alias Ankusa.Edge.{Batcher, BatcherSupervisor, Publish, Quarantine, RateLimiter}

  @type result ::
          {:ok, Envelope.t()}
          | {:duplicate, Envelope.t()}
          | {:quarantined, term()}
          | {:rejected, term()}
          | {:error,
             :unknown_source
             | :overload
             | :store_unavailable
             | :quarantine_full
             | {:rate_limited, pos_integer()}
             | {:quarantine_rate_limited, pos_integer()}}

  @typedoc """
  Why a request was answered before it reached `ingest/4`. The whole set: every
  refusal goes through `refused/2`, so a metric label built from it is bounded.
  """
  @type refusal :: :unknown_source | :payload_too_large | :body_read_failed

  @type raw_request :: %{
          required(:method) => String.t(),
          required(:path) => String.t(),
          required(:headers) => [{String.t(), String.t()}],
          required(:body) => binary()
        }

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

  @doc """
  Resolve a route to its source and the tenant the hook is stored under.

  The tenant names a storage partition and a gateway path segment, so it is
  checked here, before anything is written: a bad one from any RouteResolver is
  a 404, like an unknown source, not a dispatch failure after the provider was
  already acked. A miss is counted as a `:unknown_source` refusal.
  """
  @spec lookup(atom(), Ankusa.Route.t()) ::
          {:ok, Source.t(), tenant_id :: String.t()} | {:error, :unknown_source}
  def lookup(instance, %Ankusa.Route{} = route) do
    with {:ok, %Source{} = source} <- Ankusa.SourceStore.fetch(instance, route.source_id),
         tenant_id = route.tenant_id || source.tenant_id,
         true <- Ankusa.ClaimCheck.Ref.valid_tenant?(tenant_id) do
      {:ok, source, tenant_id}
    else
      _ ->
        refused(instance, :unknown_source)
        {:error, :unknown_source}
    end
  end

  @doc """
  Count a request answered without being ingested. Emits
  `[:ankusa, :ingest, :refused]` with the `:reason`; the one place a refusal is
  counted.
  """
  @spec refused(atom(), refusal()) :: :ok
  def refused(instance, reason) do
    Ankusa.Telemetry.emit([:ingest, :refused], %{}, %{instance: instance, reason: reason})
  end

  @doc "Look the source up, then `ingest/4`."
  @spec ingest(atom(), request()) :: result()
  def ingest(instance, %{source_id: source_id} = req) do
    route = %Ankusa.Route{source_id: source_id, tenant_id: Map.get(req, :tenant_id)}

    case lookup(instance, route) do
      {:ok, source, tenant_id} -> ingest(instance, source, tenant_id, req)
      {:error, :unknown_source} = error -> error
    end
  end

  @doc "Ingest one hook for a source `lookup/2` returned."
  @spec ingest(atom(), Source.t(), String.t(), raw_request()) :: result()
  def ingest(instance, %Source{} = source, tenant_id, req) do
    Ankusa.Telemetry.span([:ingest], %{instance: instance, source_id: source.id}, fn ->
      result = accept(instance, source, tenant_id, req)
      {result, %{size: byte_size(req.body), outcome: tag(result)}}
    end)
  end

  defp accept(instance, source, tenant_id, req) do
    env = build_envelope(source, tenant_id, req)

    case verify(instance, source, env) do
      {:accept, env} -> admit(instance, source, env)
      {:quarantine, env, reason} -> quarantine(instance, env, reason)
      {:reject, reason} -> {:rejected, reason}
    end
  end

  # The charge comes after verification, and only for hooks verification
  # accepted: a forged flood is free, so it can never lock a tenant out of its
  # own budget. Quarantine has its own per-source bucket and never spends a
  # tenant's.
  defp admit(instance, source, env) do
    # A forged, flag-accepted request must not claim a provider event key and
    # suppress the real event, so no key is extracted when the hook was flagged.
    env =
      if match?(%Verification{flagged: true}, env.verification) do
        env
      else
        %{env | dedupe_key: Ankusa.Dedupe.key(source.dedupe, env)}
      end

    case RateLimiter.hit(instance, env.tenant_id) do
      :ok ->
        commit(instance, source, env)

      {:error, {:rate_limited, _}} = limited ->
        Ankusa.Telemetry.emit([:rate_limit, :rejected], %{}, %{
          instance: instance,
          tenant_id: env.tenant_id,
          source_id: env.source_id
        })

        limited
    end
  end

  # ── verification + policy ─────────────────────────────────────────────────

  defp verify(instance, %Source{verifier: {mod, opts}} = source, env) do
    scheme = Ankusa.Verifier.scheme_name(mod, opts)

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

  defp verify_status(:ok), do: :ok
  defp verify_status({:error, _}), do: :failed

  # ── commit ────────────────────────────────────────────────────────────────

  # `Ankusa.config/1` is a `:persistent_term` read: this branch costs nothing
  # on the hot path.
  defp commit(instance, source, env) do
    case Ankusa.config(instance).wal do
      :none -> Publish.publish(instance, source, env)
      _ -> buffered_commit(instance, source, env)
    end
  end

  # The sinks travel with the hook: the queue writes one delivery row per sink in
  # the same batch as the hook itself.
  defp buffered_commit(instance, source, env) do
    partition = BatcherSupervisor.partition(instance, env.id)

    record = %{
      envelope: env,
      sinks: source.sinks,
      dedupe_ttl_ms: source.dedupe && source.dedupe.ttl_ms
    }

    try do
      case Batcher.commit(instance, partition, record) do
        {:committed, committed} -> {:ok, committed}
        {:duplicate, duplicate} -> {:duplicate, duplicate}
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
      {:rate_limited, retry_after_ms} -> {:error, {:quarantine_rate_limited, retry_after_ms}}
      :full -> {:error, :quarantine_full}
      {:error, :store_unavailable} -> {:error, :store_unavailable}
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
      # keep raw bytes verbatim; a slice of a larger binary is copied so it
      # can't pin its parent (the socket buffer)
      body: standalone(req.body),
      size: byte_size(req.body)
    }

    # Read it back through the envelope so "the content-type header" is defined
    # in exactly one place, `Ankusa.Envelope.header/2`.
    %{env | content_type: Envelope.header(env, "content-type")}
  end

  # A sub-binary of the socket buffer pins that whole buffer for as long as the
  # envelope lives, so it is copied. A body that already owns its bytes (a
  # multi-chunk read produces one) is not copied a second time.
  defp standalone(bin) do
    if :binary.referenced_byte_size(bin) > byte_size(bin), do: :binary.copy(bin), else: bin
  end

  defp tag({:ok, _}), do: :committed
  defp tag({:duplicate, _}), do: :duplicate
  defp tag({:quarantined, _}), do: :quarantined
  defp tag({:rejected, _}), do: :rejected
  defp tag({:error, {:rate_limited, _retry_after_ms}}), do: :rate_limited
  defp tag({:error, {:quarantine_rate_limited, _retry_after_ms}}), do: :quarantine_rate_limited
  defp tag({:error, reason}), do: reason
end
