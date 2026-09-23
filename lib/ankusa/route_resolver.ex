defmodule Ankusa.RouteResolver do
  @moduledoc """
  Maps a raw ingest request (`Plug.Conn`) to a `Ankusa.Route`. This is the seam
  that makes the catch-URL scheme pluggable, so the framework fits a single
  static server, a multi-tenant SaaS handing every customer their own path, or a
  product minting opaque catch URLs of its own shape at runtime —
  without touching the hot path.

  This module is both the **behaviour** every resolver implements and the
  instance-scoped **facade** (resolves `config.route_resolver` and delegates).

  A resolver does URL-scheme work only. It never reads the body, verifies
  signatures, or touches storage: it answers "which endpoint is this?" and hands
  back a `Ankusa.Route`. Policy (verify/dedup/sinks) still comes from the
  `Ankusa.SourceStore` keyed by the returned `source_id`.
  """

  alias Ankusa.{Config, Route}

  @callback resolve(instance :: atom(), conn :: Plug.Conn.t(), opts :: keyword()) ::
              {:ok, Route.t()} | :error

  @spec resolve(atom(), Plug.Conn.t()) :: {:ok, Route.t()} | :error
  def resolve(instance, conn) do
    %Config{route_resolver: {mod, opts}} = Ankusa.config(instance)
    mod.resolve(instance, conn, opts)
  end
end

defmodule Ankusa.RouteResolver.Path do
  @moduledoc """
  Default resolver: a single path segment after a fixed prefix is the source id.
  `POST /hooks/:source_id` (the original scheme). Tenant is left `nil`, so it is
  taken from the resolved `%Ankusa.Source{}` — the single-tenant / static case.

  Opts:

    * `:prefix` — path segments before the id. Default `["hooks"]`.
  """

  @behaviour Ankusa.RouteResolver

  alias Ankusa.Route

  @impl true
  def resolve(_instance, conn, opts) do
    prefix = Keyword.get(opts, :prefix, ["hooks"])
    plen = length(prefix)

    case conn.path_info do
      segments when length(segments) == plen + 1 ->
        {head, [source_id]} = Enum.split(segments, plen)
        if head == prefix, do: {:ok, %Route{source_id: source_id}}, else: :error

      _ ->
        :error
    end
  end
end

defmodule Ankusa.RouteResolver.TenantPath do
  @moduledoc """
  Multi-tenant resolver: the tenant is carried in the URL. `POST
  /hooks/:tenant_id/:source_id` maps to `%Route{tenant_id: t, source_id: s}`, so
  one instance serves many tenants and the tenant is authoritative from the path
  rather than inferred from the source.

  Opts:

    * `:prefix` — path segments before `tenant/source`. Default `["hooks"]`.
  """

  @behaviour Ankusa.RouteResolver

  alias Ankusa.Route

  @impl true
  def resolve(_instance, conn, opts) do
    prefix = Keyword.get(opts, :prefix, ["hooks"])
    plen = length(prefix)

    case conn.path_info do
      segments when length(segments) == plen + 2 ->
        {head, [tenant_id, source_id]} = Enum.split(segments, plen)

        if head == prefix,
          do: {:ok, %Route{source_id: source_id, tenant_id: tenant_id}},
          else: :error

      _ ->
        :error
    end
  end
end
