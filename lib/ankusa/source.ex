defmodule Ankusa.Source do
  @moduledoc """
  Per-source configuration: how to verify, how dispatch dedups, what to do on a
  verification failure, and where delivered hooks go.

  Dedup is a dispatch decision, so it is configured as two things: `dedup`
  (`:auto` to dedup on the key `dedup_key` extracts, `:none` to deliver every
  copy) and `dedup_key`, the `Ankusa.DedupKey` module that pulls the provider's
  event id out of a record. A source with `dedup: :auto` and no `dedup_key` has
  nothing to dedup on, so it delivers every copy and says so at boot.

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
    # what dispatch does with a provider's retry: :auto dedups on the key
    # `dedup_key` extracts, :none delivers every copy
    dedup: :auto,
    # {module, opts} implementing Ankusa.DedupKey, or nil when the source has no
    # provider event id to key on
    dedup_key: nil,
    # what to do when verification fails: :reject | :quarantine | :accept_flag
    on_verify_failure: :reject,
    # [{module, opts}] implementing Ankusa.Sink
    sinks: [{Ankusa.Sink.Log, []}]
  ]

  @type policy :: :reject | :quarantine | :accept_flag
  @type dedup_mode :: :auto | :none
  @type t :: %__MODULE__{
          id: String.t(),
          tenant_id: String.t(),
          verifier: {module(), keyword()},
          dedup: dedup_mode(),
          dedup_key: {module(), keyword()} | nil,
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
      dedup: dedup_mode!(id, Map.get(opts, :dedup, :auto)),
      dedup_key: dedup_key!(id, Map.get(opts, :dedup_key)),
      on_verify_failure: Map.get(opts, :on_verify_failure, :reject),
      sinks: Map.get(opts, :sinks, [{Ankusa.Sink.Log, []}])
    }
  end

  defp dedup_mode!(_id, mode) when mode in [:auto, :none], do: mode

  defp dedup_mode!(id, other) do
    raise ArgumentError,
          "source #{inspect(id)}: dedup must be :auto or :none, got #{inspect(other)}"
  end

  defp dedup_key!(_id, nil), do: nil

  defp dedup_key!(_id, {mod, opts}) when is_atom(mod) and is_list(opts), do: {mod, opts}

  defp dedup_key!(id, other) do
    raise ArgumentError,
          "source #{inspect(id)}: dedup_key must be {module, opts} or nil, got #{inspect(other)}"
  end
end
