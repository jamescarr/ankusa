defmodule Ankusa.Source do
  @moduledoc """
  Per-source configuration: how to verify, how to dedup, what to do on a
  verification failure, and where delivered hooks go.

  A `Ankusa.SourceStore` returns one of these for a given `source_id`.
  """

  @enforce_keys [:id]
  defstruct [
    :id,
    # owning tenant; the dedup/storage/retention scope. Defaults to "default"
    # for single-tenant static config. A multi-tenant RouteResolver may override
    # per-request via Ankusa.Route.tenant_id.
    tenant_id: "default",
    # {module, opts} implementing Ankusa.Verifier; opts carry the secret
    verifier: {Ankusa.Verifier.None, []},
    # {module, opts} implementing Ankusa.DedupKey
    dedup: {Ankusa.DedupKey.Rules, []},
    # what to do when verification fails: :reject | :quarantine | :accept_flag
    on_verify_failure: :reject,
    # [{module, opts}] implementing Ankusa.Sink
    sinks: [{Ankusa.Sink.Log, []}]
  ]

  @type policy :: :reject | :quarantine | :accept_flag
  @type t :: %__MODULE__{
          id: String.t(),
          tenant_id: String.t(),
          verifier: {module(), keyword()},
          dedup: {module(), keyword()},
          on_verify_failure: policy(),
          sinks: [{module(), keyword()}]
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

    unless Ankusa.ClaimCheck.Ref.valid_tenant?(tenant_id) do
      raise ArgumentError,
            "source #{inspect(id)}: tenant_id #{inspect(tenant_id)} must match [A-Za-z0-9_-]{1,64}"
    end

    %__MODULE__{
      id: id,
      tenant_id: tenant_id,
      verifier: Map.get(opts, :verifier, {Ankusa.Verifier.None, []}),
      dedup: Map.get(opts, :dedup, {Ankusa.DedupKey.Rules, []}),
      on_verify_failure: Map.get(opts, :on_verify_failure, :reject),
      sinks: Map.get(opts, :sinks, [{Ankusa.Sink.Log, []}])
    }
  end
end
