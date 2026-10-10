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
    # [{module, opts}] implementing Ankusa.Sink
    sinks: [{Ankusa.Sink.Log, []}],
    # %Ankusa.Dedupe{} or nil; the provider event key collapsed at ingest
    dedupe: nil,
    # :default | [header names]; provider request headers forwarded to sinks
    forward_headers: :default
  ]

  @type policy :: :reject | :quarantine | :accept_flag
  @type t :: %__MODULE__{
          id: String.t(),
          tenant_id: String.t(),
          verifier: {module(), keyword()},
          on_verify_failure: policy(),
          trust_url_tenant: boolean(),
          sinks: [{module(), keyword()}],
          dedupe: Ankusa.Dedupe.t() | nil,
          forward_headers: :default | [String.t()]
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
      sinks: Map.get(opts, :sinks, [{Ankusa.Sink.Log, []}]),
      dedupe: Ankusa.Dedupe.new!(Map.get(opts, :dedupe)),
      forward_headers: forward_headers(id, Map.get(opts, :forward_headers, :default))
    }
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
