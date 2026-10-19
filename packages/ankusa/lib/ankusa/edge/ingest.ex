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

  ## Stored headers

  Verification and the dedupe key read the request's complete header list.
  Only then does the stored-header policy (`Ankusa.Edge.StoredHeaders`: the
  instance's `store_headers`, overridden by the source's) remove or hash
  credential-bearing headers, so the hook that is committed, archived,
  claim-checked and published to sinks never carries them. The number removed
  is `:dropped_headers` on the `[:ankusa, :ingest]` span's stop metadata.

  ## Lookup precedes the body

  The source is looked up (`lookup/2`) before the request body is read, and
  `ingest/4` only ever runs on a source that exists. A request for a source that
  does not exist is a refusal (`refused/2`), counted on its own bounded counter
  and never on the `[:ankusa, :ingest]` span, so an unauthenticated caller
  cannot mint a metric series per URL it invents. `ingest/2` is the
  transport-less entry point (tests, benches, embedding): lookup, then ingest.

  ## Admission

  What may be refused without the body — the tenant's `max_body_bytes`, and the
  per-address, per-node and per-source limits — is the HTTP edge's, after the
  lookup and before the read (`Ankusa.Edge.Router`,
  `Ankusa.Edge.RateLimiter.pre_read/4`). What needs the verified hook is here:
  the tenant's hooks and bytes per second (`{:error, {:rate_limited, ms}}`), and
  its share of the batchers, charged by `Ankusa.Edge.Batcher.commit/4`
  (`{:error, {:tenant_overload, ms}}`). Both are `429`s, and neither is the
  node's `503 overload`. With `rate_limits.scope: :fleet` / `dedupe.scope: :fleet`
  the quota and the provider event key are counted fleet-wide
  (`Ankusa.Edge.Fleet`): an event key another node has claimed but not yet made
  durable is `{:error, {:dedupe_pending, ms}}` (`409`, retried by the sender), and
  one it has committed is a `{:duplicate, env}` carrying that node's hook id.
  """

  alias Ankusa.Edge.{
    Batcher,
    BatcherSupervisor,
    Fleet,
    Publish,
    Quarantine,
    StoredHeaders
  }

  alias Ankusa.{Envelope, Source, Verification}

  @type result ::
          {:ok, Envelope.t()}
          | {:duplicate, Envelope.t()}
          | {:quarantined, term()}
          | {:rejected, term()}
          | {:error,
             :unknown_source
             | :overload
             | :tenant_limit
             | :dedupe_quota
             | :dlq_full
             | :store_unavailable
             | :intake_unavailable
             | :intake_pending
             | :quarantine_full
             | {:invalid_header, String.t() | nil}
             | {:rate_limited, pos_integer()}
             | {:tenant_overload, pos_integer()}
             | {:quarantine_rate_limited, pos_integer()}
             | {:dedupe_pending, pos_integer()}}

  @typedoc """
  Why a request was answered before it reached `ingest/4`. The whole set: every
  refusal goes through `refused/2`, so a metric label built from it is bounded.
  The `*_rate_limited` ones are the pre-read admission limits
  (`Ankusa.Edge.RateLimiter.pre_read/4`): per client address, per node, per
  source.
  """
  @type refusal ::
          :unknown_source
          | :payload_too_large
          | :body_read_failed
          | :invalid_header
          | :store_unavailable
          | :ip_rate_limited
          | :node_rate_limited
          | :source_rate_limited

  # Extra keys (`:source_id`, `:tenant_id` from a `request`) ride along: a closed
  # map type here would make every `ingest/2` call "break the contract". The
  # edge adds `:query` (the raw query string), `:route_id` (the route the guard
  # matched, `nil` with routes off; reported as `:route_id` on the
  # `[:ankusa, :ingest]` span) and, for a source verified by
  # `Ankusa.Verifier.ClientCert` only, `:peer_cert` (the DER client certificate
  # the TLS handshake accepted, or `nil`).
  @type raw_request :: %{
          required(:method) => String.t(),
          required(:path) => String.t(),
          required(:headers) => [{String.t(), String.t()}],
          required(:body) => binary(),
          optional(atom()) => term()
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

  A source owned by a tenant (any `tenant_id` but `"default"`, the value a
  source gets when none is given) answers only for that tenant: a route that
  names a different one is the same `404` as a missing source, so the URL's
  tenant learns nothing about another tenant's sources. A `"default"` source
  is shared: it serves whatever tenant the route names (one `stripe` source
  behind `/webhooks/:tenant/stripe` for every customer).

  A source store that cannot answer (`{:error, :unavailable}`) is
  `{:error, :store_unavailable}`, a `503` the provider retries, never a `404`.

  `binding` is the route the guard matched (`Ankusa.Routes.Route`; `nil` with
  routes off, and for `ingest/2`), held against what the resolver said. A
  disagreement is the same `404` as a missing source:

    * a route bound to a `source_id` admits only that source;
    * a route owned by a tenant (any `tenant_id` but `"default"`) admits only
      that tenant: a resolver that names another tenant, or a source owned by
      another, is refused. Where the resolver names no tenant and the source is
      shared, the hook is stored under the route's tenant: the tenant a route
      resolves for a shared source;
    * a route owned by `"default"` binds no tenant, as before.
  """
  @spec lookup(atom(), Ankusa.Route.t(), Ankusa.Routes.Route.t() | nil) ::
          {:ok, Source.t(), tenant_id :: String.t()}
          | {:error, :unknown_source | :store_unavailable}
  def lookup(instance, %Ankusa.Route{} = route, binding \\ nil) do
    with {:ok, route} <- bind(route, binding),
         {:ok, %Source{} = source} <- Ankusa.SourceStore.fetch(instance, route.source_id) do
      tenant_id = route.tenant_id || source.tenant_id

      if bound_elsewhere?(source, route) or not Ankusa.ClaimCheck.Ref.valid_tenant?(tenant_id),
        do: unknown_source(instance),
        else: {:ok, source, tenant_id}
    else
      {:error, :unavailable} ->
        refused(instance, :store_unavailable)
        {:error, :store_unavailable}

      _ ->
        unknown_source(instance)
    end
  end

  # The route the guard matched against the resolver's answer. A route bound to
  # another source never reaches the source store.
  defp bind(route, nil), do: {:ok, route}

  defp bind(%Ankusa.Route{source_id: source_id}, %Ankusa.Routes.Route{source_id: bound})
       when is_binary(bound) and bound != source_id,
       do: :error

  defp bind(%Ankusa.Route{tenant_id: asked} = route, %Ankusa.Routes.Route{tenant_id: owner})
       when owner != "default" do
    if is_nil(asked) or asked == owner,
      do: {:ok, %{route | tenant_id: owner}},
      else: :error
  end

  defp bind(route, %Ankusa.Routes.Route{}), do: {:ok, route}

  defp bound_elsewhere?(%Source{tenant_id: "default"}, _route), do: false
  defp bound_elsewhere?(_source, %Ankusa.Route{tenant_id: nil}), do: false

  defp bound_elsewhere?(%Source{tenant_id: own}, %Ankusa.Route{tenant_id: asked}),
    do: own != asked

  defp unknown_source(instance) do
    refused(instance, :unknown_source)
    {:error, :unknown_source}
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

  @doc "Look the source up, refuse undeliverable header bytes, then `ingest/4`."
  @spec ingest(atom(), request()) :: result()
  def ingest(instance, %{source_id: source_id} = req) do
    route = %Ankusa.Route{source_id: source_id, tenant_id: Map.get(req, :tenant_id)}

    with {:ok, source, tenant_id} <- lookup(instance, route),
         :ok <- check_headers(req.headers) do
      ingest(instance, source, tenant_id, req)
    else
      {:error, reason} = error when reason in [:unknown_source, :store_unavailable] ->
        error

      {:invalid_header, name} ->
        refused(instance, :invalid_header)
        {:error, {:invalid_header, name}}
    end
  end

  @doc """
  Refuse header bytes no sink can carry: a name must be visible ASCII
  (`0x21..0x7E`), a value visible ASCII, space or tab (RFC 9110 field-value
  without `obs-text`). Returns the offending name when it is itself printable.
  """
  @spec check_headers([{String.t(), String.t()}]) :: :ok | {:invalid_header, String.t() | nil}
  def check_headers([]), do: :ok

  def check_headers([{name, value} | rest]) do
    cond do
      not name_bytes?(name) -> {:invalid_header, nil}
      not value_bytes?(value) -> {:invalid_header, name}
      true -> check_headers(rest)
    end
  end

  defp name_bytes?(<<>>), do: true
  defp name_bytes?(<<c, rest::binary>>) when c in 0x21..0x7E, do: name_bytes?(rest)
  defp name_bytes?(_), do: false

  defp value_bytes?(<<>>), do: true

  defp value_bytes?(<<c, rest::binary>>) when c in 0x20..0x7E or c == ?\t,
    do: value_bytes?(rest)

  defp value_bytes?(_), do: false

  @doc """
  Ingest one hook for a source `lookup/2` returned. The caller has already run
  `check_headers/1`.
  """
  @spec ingest(atom(), Source.t(), String.t(), raw_request()) :: result()
  def ingest(instance, %Source{} = source, tenant_id, req) do
    metadata = %{
      instance: instance,
      source_id: source.id,
      tenant_id: tenant_id,
      route_id: Map.get(req, :route_id)
    }

    Ankusa.Telemetry.span([:ingest], metadata, fn ->
      {result, dropped} = accept(instance, source, tenant_id, req)

      {result, %{size: byte_size(req.body), outcome: tag(result), dropped_headers: dropped}}
    end)
  end

  # Returns the result and how many request headers the stored-header policy
  # removed from the hook (0 for a hook that was not stored).
  defp accept(instance, source, tenant_id, req) do
    env = build_envelope(source, tenant_id, req)

    case verify(instance, source, env, req) do
      {:accept, env} ->
        admit(instance, source, env)

      {:quarantine, env, reason} ->
        {env, dropped} = store_headers(instance, source, env)
        {quarantine(instance, env, reason), dropped}

      {:reject, reason} ->
        {{:rejected, reason}, 0}
    end
  end

  # The charge comes after verification, and only for hooks verification
  # accepted cleanly: a forged flood never spends a tenant's budget, so it can
  # never lock a tenant out of it. A hook accepted *flagged* (`accept_flag`)
  # spends its source's quarantine bucket instead, so a forged flood is not
  # free either; quarantined hooks spend the same bucket in `Quarantine.put/3`.
  defp admit(instance, source, env) do
    if match?(%Verification{flagged: true}, env.verification) do
      # A forged, flag-accepted request must not claim a provider event key and
      # suppress the real event, so no key is extracted when the hook was flagged.
      case Quarantine.hit(instance, env.source_id, env.tenant_id) do
        :ok -> commit_stored(instance, source, env, nil)
        {:rate_limited, ms} -> {{:error, {:quarantine_rate_limited, ms}}, 0}
        {:error, :store_unavailable} -> {{:error, :store_unavailable}, 0}
      end
    else
      # The dedupe key is read from the complete header list: a source may
      # dedupe on a header the stored-header policy then removes.
      env = %{env | dedupe_key: Ankusa.Dedupe.key(source.dedupe, env)}

      case Fleet.admit(instance, source, env) do
        {:ok, claim} ->
          commit_stored(instance, source, env, claim)

        {:duplicate, hook_id} ->
          {{:duplicate, %{env | id: hook_id, seq: nil}}, 0}

        {:error, {:rate_limited, _}} = limited ->
          rate_limit_rejected(instance, env)
          {limited, 0}

        {:error, {:dedupe_pending, _}} = pending ->
          {pending, 0}
      end
    end
  end

  defp rate_limit_rejected(instance, env) do
    Ankusa.Telemetry.emit([:rate_limit, :rejected], %{}, %{
      instance: instance,
      tenant_id: env.tenant_id,
      source_id: env.source_id
    })
  end

  # ── stored-header policy ──────────────────────────────────────────────────

  # Verification has read the complete header list; what is stored, archived,
  # claim-checked and forwarded from here on is the list the policy leaves.
  # `Ankusa.config/1` is a `:persistent_term` read, and a header list the policy
  # does not touch is the same list, not a copy.
  defp store_headers(instance, %Source{store_headers: override, verifier: verifier}, env) do
    policy =
      override
      |> StoredHeaders.resolve(Ankusa.config(instance).store_headers)
      |> drop_credentials(verifier)

    {headers, dropped} = StoredHeaders.strip(env.headers, policy)
    {%{env | headers: headers}, dropped}
  end

  # A token source's credential is the secret itself: whatever the instance
  # policy says, it is not stored, archived, claim-checked or forwarded.
  defp drop_credentials(policy, {Ankusa.Verifier.Token, opts}),
    do: %{policy | drop: Ankusa.Verifier.Token.credential_headers(opts) ++ policy.drop}

  defp drop_credentials(policy, _verifier), do: policy

  # The claim (`dedupe.scope: :fleet`) is settled with the commit's outcome. A hook
  # that persisted in only one of the places `wal: :dual` promises
  # (`{:error, {:half, reason, id}}`) is carried to completion by the reconciler,
  # so its claim is neither promoted nor released: the reconciler promotes it when
  # both halves are confirmed, and the provider's retry meets it pending.
  defp commit_stored(instance, source, env, claim) do
    {env, dropped} = store_headers(instance, source, env)
    {settle(instance, claim, commit(instance, source, env)), dropped}
  end

  defp settle(instance, claim, {:error, {:half, reason, id}}) do
    if claim != nil do
      case Ankusa.Intake.Reconciler.hold(instance, id, claim) do
        :held -> :ok
        :completed -> Fleet.promote(instance, claim, id)
        :stuck -> Fleet.release(instance, claim)
      end
    end

    {:error, reason}
  end

  defp settle(instance, claim, result) do
    Fleet.settle(instance, claim, result)
    result
  end

  # ── verification + policy ─────────────────────────────────────────────────

  defp verify(instance, %Source{verifier: {mod, opts}} = source, env, req) do
    scheme = Ankusa.Verifier.scheme_name(mod, opts)
    weak_hash = Ankusa.Verifier.weak_hash?(mod, opts)
    opts = request_opts(mod, opts, req)

    outcome =
      Ankusa.Telemetry.span(
        [:verify],
        %{instance: instance, source_id: source.id, tenant_id: env.tenant_id},
        fn ->
          result = mod.verify(env, opts)

          {result,
           %{provider: mod, scheme: scheme, status: verify_status(result), weak_hash: weak_hash}}
        end
      )

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

  # The request context a verifier reads from `opts` (`Ankusa.Verifier`): the
  # client certificate for `ClientCert`, the query string for `Token`, and nothing
  # for anybody else, so no other verifier pays for it. The edge's value goes
  # first, so it is the one `Keyword.get/2` finds.
  defp request_opts(Ankusa.Verifier.ClientCert, opts, req),
    do: [{:peer_cert, Map.get(req, :peer_cert)} | opts]

  defp request_opts(Ankusa.Verifier.Token, opts, req),
    do: [{:query, Map.get(req, :query)} | opts]

  defp request_opts(_mod, opts, _req), do: opts

  defp verify_status(:ok), do: :ok
  defp verify_status({:error, _}), do: :failed

  # ── commit ────────────────────────────────────────────────────────────────

  # `Ankusa.config/1` is a `:persistent_term` read: this branch costs nothing
  # on the hot path.
  defp commit(instance, source, env) do
    case Ankusa.config(instance).wal do
      :none -> Publish.publish(instance, source, env)
      _ -> buffered_or_full(instance, source, env)
    end
  end

  # A DLQ over `queue.dlq.max_rows`/`max_bytes` with no archived row left to evict
  # refuses new hooks (`503 dlq_full`) rather than discard an acked one
  # (`Ankusa.Queue.DlqReaper`): a `:persistent_term` read.
  defp buffered_or_full(instance, source, env) do
    if Ankusa.Queue.DlqReaper.full?(instance),
      do: {:error, :dlq_full},
      else: buffered_commit(instance, source, env)
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
        {:error, :tenant_limit} -> {:error, :tenant_limit}
        {:error, :dedupe_quota} -> {:error, :dedupe_quota}
        {:error, {:tenant_overload, _ms}} = refused -> tenant_overload(instance, env, refused)
        {:error, :store_unavailable} -> {:error, :store_unavailable}
        # `wal: :dual`
        {:error, :intake_unavailable} -> {:error, :intake_unavailable}
        {:error, :intake_pending} -> {:error, :intake_pending}
        {:error, {:half, _reason, _id}} = half -> half
      end
    catch
      :exit, _ -> {:error, :store_unavailable}
    end
  end

  # The tenant is over its share of this node's batchers: its own `429`, counted
  # with the tenant's other rate-limit denials, never the global `503`.
  defp tenant_overload(instance, env, refused) do
    rate_limit_rejected(instance, env)
    refused
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
  defp tag({:error, {:tenant_overload, _retry_after_ms}}), do: :tenant_overload
  defp tag({:error, {:quarantine_rate_limited, _retry_after_ms}}), do: :quarantine_rate_limited
  defp tag({:error, {:dedupe_pending, _retry_after_ms}}), do: :dedupe_pending
  defp tag({:error, reason}), do: reason
end
