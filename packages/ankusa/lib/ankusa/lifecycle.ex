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

  ## A durable outbox

  The event is not a message handed to a process; it is a row. The change's
  audit row (`Ankusa.Audit`) is flagged `publish: true` and carries, beside it
  and in the same synced batch, the redacted entity the event reports. So the
  change and its event are committed or lost together: a `kill -9` right after
  `Ankusa.SourceStore.put/5` returns still delivers `source.created` after the
  restart.

  `Ankusa.Lifecycle.Publisher` is a cursor reader over those rows. It keeps the
  last `seq` it delivered to every sink in `m:lifecycle_cursor` (synced on every
  advance), delivers the next flagged row to every sink, and moves on once each
  sink confirmed or gave up. Rows are read in `seq` order, which is commit
  order, so three rapid updates of one source arrive in revision order.
  `Ankusa.Replay` with `kind: :lifecycle` re-emits a window of the trail
  (`since`/`until`/`tenant`), which is how a sink that was down for longer than
  `dispatch.retry` waits is caught up.

  Delivery is at least once. A node killed between a sink's confirmation and
  the cursor write delivers that event again after the restart, and so does a
  replay; both carry the **same event `id`** (see below), so a consumer dedupes
  on it. A sink that keeps failing holds back the events behind it until it
  succeeds or `dispatch.retry` gives up (`[:ankusa, :lifecycle, :dropped]`, reason
  `:gave_up`); the event is then lost *for that sink* and can be replayed.

  Turning lifecycle events on starts the outbox at the end of the trail: changes
  made while they were off are not announced.

  The reserved id exists only on the event: the edge never resolves it, so
  `POST /webhooks/ankusa:lifecycle` is a `404`. The colon keeps it out of reach
  of every tenant-scoped id, which is `[A-Za-z0-9_-]` joined by a dot.

  ## The event

  A structured-mode CloudEvent, carried as the body of a `Ankusa.Sink.Message`
  with `content_type: "application/cloudevents+json"`:

    * `id` — derived from the row, never minted: a keyed hash of the instance
      and the row's `seq` laid out as a UUID. The same row is the same id
      whenever and however often it is emitted.
    * `type` — `io.ankusa.source.{created,updated,deleted}`,
      `io.ankusa.route.{created,updated,deleted}`, or `io.ankusa.tenant.deleted`
      (a tenant erase, `Ankusa.Tenant`, after the `deleted` event of each source and
      route it removed; its `data` is `{"tenant": <id>}` and its `subject` the tenant id)
    * `source` — `urn:ankusa:instance:<instance>`
    * `subject` — the source id (`<tenant>.<name>`) or the route id
    * `time` — when the change committed (the row's time, not the publisher's)
    * `data` — the entity as the admin API shows it, redacted by
      `Ankusa.Admin.Redact`, as the change left it; for a deletion, as it was
      the moment before
    * `revision` — the subject's change counter (see `Ankusa.Audit`): a
      consumer keeps the highest it has seen per `subject` and ignores a lower
      one. Absent for a store that keeps none (the Redis source store)
    * `actor` — the principal id that made the change (`system` for code)
    * `instance` — the instance name
    * `sequence` — the audit `seq`, a decimal string: events of one instance
      are totally ordered by it
    * `replayid` — only on an event a replay job re-emitted

  A route event's envelope carries the route's tenant (`tenant_id`; `"default"`
  for a route that belongs to none), so a replay filtered by `tenant` and a
  tenant-limited reader of `GET /v1/audit` see their own routes' changes.

  ## A change nobody saw finish

  A source in the Redis store or a route is a dual write (see `Ankusa.Audit`),
  so a node killed in the middle leaves a *pending* row. `reconcile/2` settles
  it from the subject's present state instead of guessing: the state hashes to
  the row's `after_hash`, the change was applied (`"ok"`, announced); to its
  `before_hash`, it was not (`"failed"`, not announced); to neither, somebody
  changed it meanwhile, and the row stays `"interrupted"` and announces a
  state-refresh event carrying the current entity and revision.
  """

  alias Ankusa.Admin.Redact
  alias Ankusa.{Audit, Config, Envelope, Routes, Source, SourceStore}
  alias Ankusa.Routes.Route

  @source_id "ankusa:lifecycle"
  @content_type "application/cloudevents+json"

  @typedoc "A `Source` with no tenant: each lifecycle event names its own."
  @type lifecycle_source :: %Source{tenant_id: nil}

  @doc "The reserved source id lifecycle events travel under."
  @spec source_id() :: String.t()
  def source_id, do: @source_id

  @doc """
  The lifecycle pseudo-source for `instance`, or `:error` when lifecycle events
  are off (no sinks).

  Built as a struct literal: `Ankusa.Source.new/2` requires a tenant, and the
  tenant of a lifecycle event varies per event.
  """
  @spec source(atom()) :: {:ok, lifecycle_source()} | :error
  def source(instance) do
    case Ankusa.config(instance).lifecycle.sinks do
      [] -> :error
      sinks -> {:ok, %Source{id: @source_id, tenant_id: nil, sinks: sinks}}
    end
  end

  @doc "Whether `instance` announces changes (it has lifecycle sinks)."
  @spec enabled?(atom()) :: boolean()
  def enabled?(instance), do: Ankusa.config(instance).lifecycle.sinks != []

  @doc """
  The audit attrs that put a change in the outbox: `%{entity: entity}` when
  lifecycle events are on, `%{}` when they are off. `entity` is a function so
  that nothing is redacted for an instance that announces nothing.
  """
  @spec outbox(atom(), (-> map())) :: %{optional(:entity) => map()}
  def outbox(instance, entity) when is_function(entity, 0) do
    if enabled?(instance), do: %{entity: entity.()}, else: %{}
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

  # ── events ────────────────────────────────────────────────────────────────

  @doc """
  The CloudEvents type of an audit row: its action, unless a reconciliation
  settled on another verb (`row.event`).
  """
  @spec type(Audit.row()) :: String.t()
  def type(%{action: action} = row) do
    [kind, verb] = String.split(action, ".", parts: 2)
    "io.ankusa.#{kind}.#{Map.get(row, :event) || past(verb)}"
  end

  defp past("create"), do: "created"
  defp past("update"), do: "updated"
  defp past("delete"), do: "deleted"

  @doc """
  The event of an audit row flagged for publication, as the `Ankusa.Envelope` a
  lifecycle sink delivers. `entity` is what `Ankusa.Audit.entity/2` returned for
  the row; `replay_id` is set only by a replay job.

  Pure in the row: the same row gives the same event `id`, `time` and body
  however often, and wherever on the instance, it is built.
  """
  @spec envelope(atom(), Audit.row(), map(), String.t() | nil) ::
          {:ok, Envelope.t()} | {:error, :no_audit_key}
  def envelope(instance, %{seq: seq} = row, entity, replay_id \\ nil) when is_map(entity) do
    with {:ok, id} <- event_id(instance, seq) do
      body =
        %{
          "specversion" => "1.0",
          "id" => id,
          "source" => "urn:ankusa:instance:#{instance}",
          "type" => type(row),
          "subject" => row.resource,
          "time" => row.at |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601(),
          "datacontenttype" => "application/json",
          "data" => entity,
          "actor" => Map.get(row, :actor),
          "instance" => to_string(instance),
          "sequence" => Integer.to_string(seq)
        }
        |> put_present("revision", Map.get(row, :revision))
        |> put_present("replayid", replay_id)
        |> JSON.encode!()

      {:ok,
       %Envelope{
         id: id,
         source_id: @source_id,
         tenant_id: tenant(row),
         received_at: row.at,
         method: "POST",
         path: "/_ankusa/lifecycle",
         headers: [{"content-type", @content_type}],
         content_type: @content_type,
         dedupe_key: id,
         body: body,
         size: byte_size(body)
       }}
    end
  end

  defp put_present(map, _key, nil), do: map
  defp put_present(map, key, value), do: Map.put(map, key, value)

  @doc """
  The tenant a row's event is delivered under: the row's own (a source's tenant,
  or the tenant that owns a route), the default one for a row that has none.
  """
  @spec tenant(Audit.row()) :: String.t()
  def tenant(row), do: Map.get(row, :tenant) || "default"

  @doc """
  Whether a row's event falls in a replay job's filter (`:since` and `:until`
  bound the row's time, `:tenants` is the list of tenants it may belong to).
  """
  @spec in_window?(Audit.row(), map()) :: boolean()
  def in_window?(row, filter) do
    since = Map.get(filter, :since)
    until = Map.get(filter, :until)
    tenants = Map.get(filter, :tenants)

    (is_nil(since) or row.at >= since) and (is_nil(until) or row.at <= until) and
      (is_nil(tenants) or tenant(row) in tenants)
  end

  # A UUID (version 8, the custom-layout one) from a keyed hash of the instance
  # and the row's `seq`. The key is the store's own (`m:audit_key`), so two
  # nodes that both started at seq 1 never mint the same id for different
  # events, and the same store gives the same id for the same row for good.
  defp event_id(instance, seq) do
    case Audit.digest(instance, {:lifecycle_event, instance, seq}) do
      nil ->
        {:error, :no_audit_key}

      hex ->
        <<a::48, _::4, b::12, _::2, c::62, _::binary>> = Base.decode16!(hex, case: :lower)
        {:ok, uuid(<<a::48, 8::4, b::12, 2::2, c::62>>)}
    end
  end

  defp uuid(
         <<a::binary-size(4), b::binary-size(2), c::binary-size(2), d::binary-size(2), e::binary>>
       ) do
    [a, b, c, d, e]
    |> Enum.map_join("-", &Base.encode16(&1, case: :lower))
  end

  # ── a change nobody saw finish ────────────────────────────────────────────

  @doc """
  Whether `reconcile/2` can settle a pending audit row: the change of a source
  or a route that lives outside this node's store.
  """
  @spec reconcilable?(Audit.row()) :: boolean()
  def reconcilable?(%{action: "source." <> _}), do: true
  def reconcilable?(%{action: "route." <> _}), do: true
  def reconcilable?(_row), do: false

  @doc """
  Settle a pending row from the subject's present state: `{:ok, update}` for
  `Ankusa.Audit`'s finish, `:not_ready` while the subject's store has not
  loaded (ask again), `:skip` for a row that is not a source or route change.
  """
  @spec reconcile(atom(), Audit.row()) :: {:ok, map()} | :not_ready | :skip
  def reconcile(instance, %{action: "route." <> _} = row) do
    case Routes.meta(instance) do
      nil ->
        :not_ready

      _meta ->
        current =
          case Routes.get(instance, row.resource) do
            {:ok, route} -> route
            {:error, :not_found} -> nil
          end

        {:ok,
         verdict(instance, row, %{
           hash: current && Routes.definition_hash(instance, current),
           entity: current && Route.to_json(current),
           revision: current && current.revision,
           gone: %{"id" => row.resource}
         })}
    end
  end

  def reconcile(instance, %{action: "source." <> _, tenant: tenant, resource: resource} = row)
      when is_binary(tenant) do
    if SourceStore.ready?(instance) do
      name = String.replace_prefix(resource, tenant <> ".", "")

      current =
        case SourceStore.get(instance, tenant, name) do
          {:ok, stored} -> stored
          :error -> nil
        end

      {:ok,
       verdict(instance, row, %{
         hash: current && Audit.digest(instance, current.spec),
         entity: current && Redact.source_entry(current),
         revision: current && Map.get(current, :revision),
         gone: %{"tenant" => tenant, "name" => name}
       })}
    else
      :not_ready
    end
  end

  def reconcile(_instance, _row), do: :skip

  defp verdict(instance, row, subject) do
    cond do
      subject.hash == Map.get(row, :after_hash) -> applied(instance, row, subject)
      subject.hash == Map.get(row, :before_hash) -> not_applied(row)
      true -> diverged(instance, row, subject)
    end
  end

  # A deletion has nothing left to read: the entity is the last view of the
  # subject that the row recorded before the change.
  defp applied(instance, row, subject) do
    entity =
      if String.ends_with?(row.action, ".delete"),
        do: Map.get(row, :entity) || subject.gone,
        else: subject.entity

    publish(instance, %{result: "ok", detail: detail(row, "applied")}, entity)
  end

  defp not_applied(row), do: %{result: "failed", detail: detail(row, "not_applied")}

  # Neither the state the change started from nor the one it was heading to:
  # say what is there now, as an update (or a deletion, when nothing is).
  defp diverged(instance, row, subject) do
    update = %{
      result: "interrupted",
      revision: subject.revision,
      event: if(subject.entity, do: "updated", else: "deleted"),
      detail: detail(row, "diverged")
    }

    publish(instance, update, subject.entity || subject.gone)
  end

  defp publish(instance, update, entity) do
    if enabled?(instance), do: Map.merge(update, %{publish: true, entity: entity}), else: update
  end

  defp detail(row, verdict), do: Map.put(Map.get(row, :detail) || %{}, "reconciled", verdict)
end
