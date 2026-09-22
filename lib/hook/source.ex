defmodule Hook.Source do
  @moduledoc """
  Per-source configuration: how to verify, how to dedup, what to do on a
  verification failure, and where delivered hooks go.

  A `Hook.SourceStore` returns one of these for a given `source_id`.
  """

  @enforce_keys [:id]
  defstruct [
    :id,
    # owning tenant; the dedup/storage/retention scope. Defaults to "default"
    # for single-tenant static config. A multi-tenant RouteResolver may override
    # per-request via Hook.Route.tenant_id.
    tenant_id: "default",
    # {module, opts} implementing Hook.Verifier; opts carry the secret
    verifier: {Hook.Verifier.None, []},
    # {module, opts} implementing Hook.DedupKey
    dedup: {Hook.DedupKey.Rules, []},
    # what to do when verification fails: :reject | :quarantine | :accept_flag
    on_verify_failure: :reject,
    # [{module, opts}] implementing Hook.Sink
    sinks: [{Hook.Sink.Log, []}]
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

  @doc "Build a source from a keyword list / map, applying defaults."
  @spec new(String.t(), keyword() | map()) :: t()
  def new(id, opts) do
    opts = Map.new(opts)

    %__MODULE__{
      id: id,
      tenant_id: Map.get(opts, :tenant_id, "default"),
      verifier: Map.get(opts, :verifier, {Hook.Verifier.None, []}),
      dedup: Map.get(opts, :dedup, {Hook.DedupKey.Rules, []}),
      on_verify_failure: Map.get(opts, :on_verify_failure, :reject),
      sinks: Map.get(opts, :sinks, [{Hook.Sink.Log, []}])
    }
  end
end
