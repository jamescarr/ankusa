defmodule Hook.Route do
  @moduledoc """
  The resolved identity of an ingest request: which tenant and source a catch
  URL maps to. Produced by a `Hook.RouteResolver` from the raw `Plug.Conn`, so
  the URL *scheme* (single path segment, tenant-in-path, opaque token, host
  routing) is a pluggable concern the rest of the framework never sees.

    * `source_id` — the key a `Hook.SourceStore` resolves to a `%Hook.Source{}`.
    * `tenant_id` — the owning tenant. `nil` means "take it from the source",
      which is the single-tenant / static-config case. A multi-tenant resolver
      that carries the tenant in the URL sets it explicitly.
    * `params` — any extra captures the resolver wants to pass through
      (e.g. an app id, a token scope). Advisory; the hot path ignores it.
  """

  @enforce_keys [:source_id]
  defstruct [:source_id, :tenant_id, params: %{}]

  @type t :: %__MODULE__{
          source_id: String.t(),
          tenant_id: String.t() | nil,
          params: map()
        }
end
