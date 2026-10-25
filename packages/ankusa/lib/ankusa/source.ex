defmodule Ankusa.Source do
  @moduledoc """
  Per-source configuration: how to verify, what to do on a verification
  failure, and where delivered hooks go.

  A `Ankusa.SourceStore` returns one of these for a given `source_id`.
  """

  @enforce_keys [:id]
  defstruct [
    :id,
    # owning tenant; the storage/retention scope. Defaults to "default"
    # for single-tenant static config. A multi-tenant RouteResolver may override
    # per-request via Ankusa.Route.tenant_id.
    tenant_id: "default",
    # {module, opts} implementing Ankusa.Verifier; opts carry the secret
    verifier: {Ankusa.Verifier.None, []},
    # what to do when verification fails: :reject | :quarantine | :accept_flag
    on_verify_failure: :reject,
    # with a resolver that takes the tenant from the request, a shared source
    # with no verifier must set this: it accepts that the request names the
    # tenant, unverified (see `Ankusa.Verifier.validate_shared!/1`)
    trust_url_tenant: false,
    # [{module, opts}] implementing Ankusa.Sink. `ordered` and `ordering_key` are
    # read off each entry by `Ankusa.Ordering` and are not in the opts a sink sees
    sinks: [{Ankusa.Sink.Log, []}],
    # nil (no sink is ordered), or one `Ankusa.Ordering.sink_ordering()` per sink
    # (ADR 0004): `nil` for an unordered sink, `{:ordered, key spec | nil}` else
    ordering: nil,
    # %Ankusa.Dedupe{} or nil; the provider event key collapsed at ingest
    dedupe: nil,
    # :default | [header names]; provider request headers forwarded to sinks
    forward_headers: :default,
    # caps on the forwarded headers per message: at most this many names, and
    # this many bytes of name plus value in all (`Ankusa.Sink.Message.forwarded_headers/2`)
    max_headers: Ankusa.Sink.Message.default_max_headers(),
    forward_headers_max_bytes: Ankusa.Sink.Message.default_forward_headers_max_bytes(),
    # nil (the instance's `store_headers` policy), or %{drop: [..] | nil, hash:
    # [..] | nil}: which request headers the stored hook keeps, per key over the
    # instance's policy (see `Ankusa.Edge.StoredHeaders`)
    store_headers: nil,
    # nil (the instance's `dispatch.retry` as is), or a keyword list of
    # `Ankusa.RetryPolicy` options laid over it: `base_ms`, `max_ms`,
    # `max_attempts`, `jitter` and `max_elapsed_ms` (the horizon: how long after
    # its first attempt a failing delivery is dead-lettered; claim-checked
    # sources are bounded by `claim_check.bucket_expiry_days`)
    retry: nil
  ]

  @type policy :: :reject | :quarantine | :accept_flag
  @type t :: %__MODULE__{
          id: String.t(),
          tenant_id: String.t(),
          verifier: {module(), keyword()},
          on_verify_failure: policy(),
          trust_url_tenant: boolean(),
          sinks: [{module(), keyword()}],
          ordering: [Ankusa.Ordering.sink_ordering()] | nil,
          dedupe: Ankusa.Dedupe.t() | nil,
          forward_headers: :default | [String.t()],
          max_headers: pos_integer(),
          forward_headers_max_bytes: pos_integer(),
          store_headers: Ankusa.Edge.StoredHeaders.override() | nil,
          retry: keyword() | nil
        }

  @doc """
  Build a source from a keyword list / map, applying defaults. Raises if
  `:tenant_id` isn't `[A-Za-z0-9_-]{1,64}`: the tenant names a storage
  partition and a claim-check path segment, so a bad one fails at boot.
  """
  @spec new(String.t(), keyword() | map()) :: t()
  def new(id, opts) do
    opts = Map.new(opts)
    tenant_id = Map.get(opts, :tenant_id, "default")
    trust = Map.get(opts, :trust_url_tenant, false)

    {sinks, ordering} =
      Ankusa.Ordering.normalize_sinks!(id, Map.get(opts, :sinks, [{Ankusa.Sink.Log, []}]))

    unless Ankusa.ClaimCheck.Ref.valid_tenant?(tenant_id) do
      raise ArgumentError,
            "source #{inspect(id)}: tenant_id #{inspect(tenant_id)} must match [A-Za-z0-9_-]{1,64}"
    end

    unless is_boolean(trust) do
      raise ArgumentError,
            "source #{inspect(id)}: trust_url_tenant must be a boolean, got #{inspect(trust)}"
    end

    %__MODULE__{
      id: id,
      tenant_id: tenant_id,
      verifier: Map.get(opts, :verifier, {Ankusa.Verifier.None, []}),
      on_verify_failure: Map.get(opts, :on_verify_failure, :reject),
      trust_url_tenant: trust,
      sinks: sinks,
      ordering: ordering,
      dedupe: Ankusa.Dedupe.new!(Map.get(opts, :dedupe)),
      forward_headers: forward_headers(id, Map.get(opts, :forward_headers, :default)),
      max_headers:
        positive_integer!(
          id,
          :max_headers,
          Map.get(opts, :max_headers, Ankusa.Sink.Message.default_max_headers())
        ),
      forward_headers_max_bytes:
        positive_integer!(
          id,
          :forward_headers_max_bytes,
          Map.get(
            opts,
            :forward_headers_max_bytes,
            Ankusa.Sink.Message.default_forward_headers_max_bytes()
          )
        ),
      store_headers:
        Ankusa.Edge.StoredHeaders.normalize_override!(
          Map.get(opts, :store_headers),
          "source #{inspect(id)}: store_headers"
        ),
      retry: Ankusa.RetryPolicy.normalize_opts!(Map.get(opts, :retry), "source #{inspect(id)}")
    }
  end

  defp positive_integer!(_id, _key, value) when is_integer(value) and value > 0, do: value

  defp positive_integer!(id, key, value) do
    raise ArgumentError,
          "source #{inspect(id)}: #{key} must be a positive integer, got #{inspect(value)}"
  end

  defp forward_headers(_id, :default), do: :default

  defp forward_headers(id, headers) do
    if is_list(headers) and Enum.all?(headers, &(is_binary(&1) and &1 != "")) do
      Enum.map(headers, &String.downcase/1)
    else
      raise ArgumentError,
            "source #{inspect(id)}: forward_headers must be :default or a list of header names"
    end
  end
end
