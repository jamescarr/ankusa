defmodule Ankusa.Lifecycle do
  @moduledoc """
  Lifecycle events: a message when a webhook endpoint (a *source*) or a route is
  created, updated, or deleted.

  Off by default. `config.lifecycle.sinks` (YAML: `lifecycle.sinks`) is a list of
  `Ankusa.Sink`s, shaped like a source's; with at least one, every change made
  through `Ankusa.SourceStore.put/5`, `Ankusa.SourceStore.delete/3`, or
  `Ankusa.Routes` (the admin API and the route-management API are both thin
  wrappers over those) becomes one CloudEvents 1.0 event delivered to those
  sinks.

  ## Delivery bypasses the store

  An event is an `Ankusa.Envelope` of the reserved source id
  `#{inspect("ankusa:lifecycle")}`, but it never touches the store: it is handed to
  `Ankusa.Lifecycle.Publisher`, a supervised in-memory process that delivers it
  to every lifecycle sink independently, retrying a failed sink with
  `dispatch.retry`. The change that caused it does not wait: the call returns
  once the event is queued.

  Delivery is best effort. The publisher's queue is bounded (10,000 pending sink
  deliveries); when it is full, when a sink's retries run out, or when the
  publisher is not running, the event is dropped for that sink, logged, and
  counted (`[:ankusa, :lifecycle, :dropped]`). Pending events are lost on a node
  restart, and there is no ordering. A lifecycle failure is never returned to
  the caller whose change succeeded.

  The reserved id exists only on the event: the edge never resolves it, so
  `POST /webhooks/ankusa:lifecycle` is a `404`. The colon keeps it out of reach
  of every tenant-scoped id, which is `[A-Za-z0-9_-]` joined by a dot.

  ## The event

  A structured-mode CloudEvent, carried as the body of a `Ankusa.Sink.Message`
  with `content_type: "application/cloudevents+json"`:

    * `type` — `io.ankusa.source.{created,updated,deleted}` or
      `io.ankusa.route.{created,updated,deleted}`
    * `source` — `urn:ankusa:instance:<instance>`
    * `subject` — the source id (`<tenant>.<name>`) or the route id
    * `data` — the entity as the admin API shows it, redacted by
      `Ankusa.Admin.Redact`; for a deletion, what is left to say (the last view of
      a source, `%{"id" => id}` for a route)

  Routes belong to no tenant, so a route event's envelope carries the default
  tenant, `"default"`.
  """

  alias Ankusa.{Config, Envelope, Source}
  alias Ankusa.Admin.Redact
  alias Ankusa.Lifecycle.Publisher
  alias Ankusa.Routes.Route

  @source_id "ankusa:lifecycle"
  @content_type "application/cloudevents+json"

  @type action :: :created | :updated | :deleted

  @doc "The reserved source id lifecycle events travel under."
  @spec source_id() :: String.t()
  def source_id, do: @source_id

  @doc """
  The lifecycle pseudo-source for `instance`, or `:error` when lifecycle events
  are off (no sinks).

  Built as a struct literal: `Ankusa.Source.new/2` requires a tenant, and the
  tenant of a lifecycle event varies per event.
  """
  @spec source(atom()) :: {:ok, Source.t()} | :error
  def source(instance) do
    case Ankusa.config(instance).lifecycle.sinks do
      [] -> :error
      sinks -> {:ok, %Source{id: @source_id, tenant_id: nil, sinks: sinks}}
    end
  end

  @doc """
  Reject a lifecycle configuration that cannot work. Raises `ArgumentError`.

  The reserved source id must not be declared as a static source: it would let
  any provider post hooks that consumers read as lifecycle events.
  """
  @spec validate_config!(Config.t()) :: :ok
  def validate_config!(%Config{lifecycle: %{sinks: sinks}} = config) do
    unless is_list(sinks) and Enum.all?(sinks, &sink?/1) do
      raise ArgumentError, "lifecycle.sinks must be a list of {module, opts} sinks"
    end

    if reserved_source_declared?(config) do
      raise ArgumentError, "source #{inspect(@source_id)} is reserved for lifecycle events"
    end

    :ok
  end

  defp sink?({mod, opts}), do: is_atom(mod) and is_list(opts)
  defp sink?(_other), do: false

  defp reserved_source_declared?(%Config{source_store: {_mod, opts}}) do
    case Keyword.get(opts, :sources, %{}) do
      sources when is_map(sources) -> Map.has_key?(sources, @source_id)
      _ -> false
    end
  end

  @doc """
  A source changed. `entry` is the `Ankusa.SourceStore.stored()` map, or for a
  deletion the store could not read back, just `%{tenant: tenant, name: name}`.
  """
  @spec source_changed(atom(), action(), map()) :: :ok
  def source_changed(instance, action, %{tenant: tenant, name: name} = entry) do
    data =
      if Map.has_key?(entry, :spec),
        do: Redact.source_entry(entry),
        else: %{"tenant" => tenant, "name" => name}

    emit(instance, "io.ankusa.source.#{action}", "#{tenant}.#{name}", tenant, data)
  end

  @doc """
  A route changed. `route` is the `Ankusa.Routes.Route` for a creation or an
  update, the route id for a deletion.
  """
  @spec route_changed(atom(), action(), Route.t() | String.t()) :: :ok
  def route_changed(instance, action, %Route{} = route) do
    emit(instance, "io.ankusa.route.#{action}", route.id, "default", Route.to_json(route))
  end

  def route_changed(instance, action, id) when is_binary(id) do
    emit(instance, "io.ankusa.route.#{action}", id, "default", %{"id" => id})
  end

  defp emit(instance, type, subject, tenant, data) do
    case source(instance) do
      :error ->
        :ok

      {:ok, _source} ->
        env = envelope(instance, type, subject, tenant, data)
        Publisher.publish(instance, env, type, subject)
    end
  end

  defp envelope(instance, type, subject, tenant, data) do
    id = Ankusa.UUIDv7.generate()

    body =
      JSON.encode!(%{
        "specversion" => "1.0",
        "id" => id,
        "source" => "urn:ankusa:instance:#{instance}",
        "type" => type,
        "subject" => subject,
        "time" => DateTime.to_iso8601(DateTime.utc_now()),
        "datacontenttype" => "application/json",
        "data" => data
      })

    %Envelope{
      id: id,
      source_id: @source_id,
      tenant_id: tenant,
      received_at: System.system_time(:millisecond),
      method: "POST",
      path: "/_ankusa/lifecycle",
      headers: [{"content-type", @content_type}],
      content_type: @content_type,
      body: body,
      size: byte_size(body)
    }
  end
end
