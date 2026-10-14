defmodule Ankusa.Audit do
  @moduledoc """
  The audit log: who did what to this instance, durably.

  Off by default (`audit.enabled: false`). On, `Ankusa.Admin.Guard` records

    * every **mutation** on the admin (`admin.port`), route-management
      (`routes.admin.port`) and SCIM (`/scim/v2`) surfaces, and
    * every **denial** (`401`, `403`) on those surfaces.

  Reads are not recorded, and neither are `/health` and `/ready`.

  ## Fail closed

  A mutation is written as a `pending` entry, synced to this node's store,
  *before* the handler runs. If that write fails the request is refused with
  `503 audit_unavailable` and the mutation never happens: an unauditable change
  is not made. When the handler answers, the same entry is rewritten with the
  outcome and the status:

  | outcome | meaning |
  | --- | --- |
  | `success` | a `2xx` answer |
  | `failure` | the handler refused (`4xx`, other `5xx`): nothing, or nothing complete, changed |
  | `error` | the handler crashed (`500`): the change may or may not have happened |
  | `denied` | `401`/`403`: refused before the handler, or by it |
  | `pending` | the answer was never recorded (a crash of the node, or a failed rewrite) |

  A failed rewrite is logged at `error` with the whole entry, emitted as
  `[:ankusa, :audit, :write_failed]`, and leaves the `pending` entry as
  evidence; the response is unaffected. A denial is recorded in one write
  before the response is sent, and a failed write is logged and counted the
  same way: the request is refused regardless.

  ## The entry

  A JSON object with string keys, stored under `a:<at, ms><per-node sequence><id>` in this
  node's store (the log is node-local, like the DLQ and the quarantine pen):

  | key | |
  | --- | --- |
  | `id` | a UUIDv7 |
  | `at`, `time` | when the request arrived, in ms and ISO 8601 |
  | `instance`, `listener` | `"admin"` or `"routes"` (SCIM is on the admin listener) |
  | `action` | the name from `Ankusa.Admin.Actions`, e.g. `source.create` |
  | `outcome`, `status` | see above; `status` is `null` while `pending` |
  | `method`, `path` | the request line, without the query string |
  | `tenant`, `target` | the tenant and entity acted on, when there is one |
  | `actor` | `type` (`user`, `service`, `scim`, `anonymous`), `subject`, `username`, `user_id`, `ip` |
  | `request_id` | the `x-request-id` header, when the caller sent one |

  Request bodies are never recorded: sources carry credentials.

  ## Reading it

  `GET /v1/audit` and `GET /v1/tenants/:tenant/audit` (`list/2`), newest
  first, paged by `cursor`. They need the `admin` role.

  ## Shipping it

  `audit.sinks` is a list of `Ankusa.Sink`s. `Ankusa.Audit.Shipper` delivers
  every entry to each sink at least once, in order, as a CloudEvent
  (`io.ankusa.audit.<action>`, `id` = the entry id, so a consumer can drop
  duplicates), and keeps a cursor per sink in the store so a restart resumes.
  Entries are deleted after `audit.retention_days`, but never ahead of the
  slowest sink's cursor: an entry nobody has received outlives retention.

  The reserved source id `#{inspect("ankusa:audit")}` exists only on those
  events, like `Ankusa.Lifecycle`'s.
  """

  require Logger

  alias Ankusa.{Config, Store, Telemetry}
  alias Ankusa.Auth.Principal
  alias Ankusa.Store.Keys

  @source_id "ankusa:audit"
  @default_limit 100
  @max_limit 1000
  @max_scan 100_000
  @max_ms 0xFFFFFFFFFFFFFFFE

  @type entry :: %{optional(String.t()) => term()}
  @type filters :: %{
          optional(:tenant) => String.t() | nil,
          optional(:action) => String.t() | nil,
          optional(:actor) => String.t() | nil,
          optional(:outcome) => String.t() | nil,
          optional(:since) => non_neg_integer() | nil,
          optional(:until) => non_neg_integer() | nil,
          optional(:limit) => integer() | nil,
          optional(:cursor) => String.t() | nil
        }

  @doc "The reserved source id audit events travel under."
  @spec source_id() :: String.t()
  def source_id, do: @source_id

  @doc """
  Reject an audit configuration that cannot work. Raises `ArgumentError`.

  The reserved source id must not be declared as a static source: it would let
  any provider post hooks that consumers read as audit events.
  """
  @spec validate_config!(Config.t()) :: :ok
  def validate_config!(%Config{audit: audit} = config) do
    retention = Map.get(audit, :retention_days)

    unless is_nil(retention) or (is_integer(retention) and retention >= 1) do
      raise ArgumentError,
            "audit.retention_days must be nil or an integer >= 1, got #{inspect(retention)}"
    end

    sinks = Map.get(audit, :sinks)

    unless is_list(sinks) and Enum.all?(sinks, &sink?/1) do
      raise ArgumentError, "audit.sinks must be a list of {module, opts} sinks"
    end

    if reserved_source_declared?(config) do
      raise ArgumentError, "source #{inspect(@source_id)} is reserved for audit events"
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

  # ── recording ──────────────────────────────────────────────────────────────

  @doc """
  Record the entity a request created, for the audit entry of that request.
  A create answers before its id is known to the guard, so the handler says
  which entity it made; every other action's target comes from its path.
  """
  @spec put_target(Plug.Conn.t(), String.t()) :: Plug.Conn.t()
  def put_target(conn, target) when is_binary(target),
    do: Plug.Conn.put_private(conn, :ankusa_audit_target, target)

  @doc """
  Write the `pending` entry of a mutating request, synced, and remember it in
  the conn for `finish/1`. `{:error, reason}` means the request must not run.
  """
  @spec begin(Plug.Conn.t()) :: {:ok, Plug.Conn.t()} | {:error, term()}
  def begin(%Plug.Conn{} = conn) do
    entry = build(conn, "pending", nil)
    key = entry_key(entry)

    case write(instance(conn), key, entry) do
      :ok ->
        {:ok,
         conn
         |> Plug.Conn.put_private(:ankusa_audit_entry, entry)
         |> Plug.Conn.put_private(:ankusa_audit_key, key)}

      {:error, reason} = error ->
        write_failed(instance(conn), entry, reason)
        error
    end
  end

  @doc """
  Rewrite the entry `begin/1` wrote with the outcome the response implies (see
  the moduledoc) and its status. Never fails the caller.
  """
  @spec finish(Plug.Conn.t()) :: Plug.Conn.t()
  def finish(%Plug.Conn{private: %{ankusa_audit_entry: entry, ankusa_audit_key: key}} = conn) do
    updated =
      Map.merge(entry, %{
        "outcome" => outcome(conn),
        "status" => conn.status,
        "target" => conn.private[:ankusa_audit_target] || entry["target"]
      })

    persist(conn, key, updated)
    conn
  end

  def finish(conn), do: conn

  @doc """
  Write a final entry in one step, for a request that never reaches a handler
  (a denial). `status` defaults to the conn's. Never fails the caller.
  """
  @spec record(Plug.Conn.t(), String.t(), integer() | nil) :: Plug.Conn.t()
  def record(%Plug.Conn{} = conn, outcome, status \\ nil) do
    entry = build(conn, outcome, status || conn.status)
    persist(conn, entry_key(entry), entry)
    conn
  end

  defp persist(conn, key, entry) do
    instance = instance(conn)

    case write(instance, key, entry) do
      :ok ->
        Telemetry.emit([:audit, :recorded], %{}, %{instance: instance, outcome: entry["outcome"]})

      {:error, reason} ->
        write_failed(instance, entry, reason)
    end
  end

  # The id is random within a millisecond, so on its own it would not order two
  # requests that arrive in the same one: a per-node sequence breaks the tie, in
  # the order the entries were made.
  defp entry_key(entry),
    do: Keys.audit(entry["at"], :erlang.unique_integer([:monotonic, :positive]), entry["id"])

  defp write(instance, key, entry) do
    Store.write(instance, [{:put, :default, key, JSON.encode!(entry)}], sync: true)
  end

  defp write_failed(instance, entry, reason) do
    Logger.error(
      "[ankusa] audit entry could not be written (#{inspect(reason)}): #{JSON.encode!(entry)}"
    )

    Telemetry.emit([:audit, :write_failed], %{}, %{instance: instance})
  end

  defp outcome(conn) do
    cond do
      conn.private[:ankusa_audit_error] -> "error"
      conn.status in 200..299 -> "success"
      conn.status in [401, 403] -> "denied"
      true -> "failure"
    end
  end

  defp build(conn, outcome, status) do
    info = conn.private.ankusa_action
    principal = conn.private[:ankusa_principal] || %Principal{ip: ip(conn)}
    at = System.system_time(:millisecond)

    %{
      "id" => Ankusa.UUIDv7.generate(),
      "at" => at,
      "time" => at |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601(),
      "instance" => to_string(instance(conn)),
      "listener" => to_string(conn.private[:ankusa_listener]),
      "action" => info.action,
      "outcome" => outcome,
      "status" => status,
      "method" => conn.method,
      "path" => conn.request_path,
      "tenant" => info.tenant,
      "target" => conn.private[:ankusa_audit_target] || info.target,
      "actor" => Principal.to_audit(principal),
      "request_id" => request_id(conn)
    }
  end

  defp request_id(conn) do
    case Plug.Conn.get_req_header(conn, "x-request-id") do
      [id | _] -> id
      [] -> nil
    end
  end

  defp instance(conn), do: conn.private.ankusa_instance

  @doc "The text form of a request's peer address."
  @spec ip(Plug.Conn.t()) :: String.t() | nil
  def ip(%Plug.Conn{remote_ip: ip}) when is_tuple(ip), do: ip |> :inet.ntoa() |> to_string()
  def ip(_conn), do: nil

  # ── reading ────────────────────────────────────────────────────────────────

  @doc """
  The entries matching `filters`, newest first.

    * `:tenant`, `:action`, `:outcome` match exactly; `:actor` matches the
      actor's `subject` or `username`.
    * `:since`/`:until` bound the entry time in ms, inclusive.
    * `:limit` defaults to #{@default_limit} and is clamped to 1..#{@max_limit}.
    * `:cursor` is a `next_cursor` of an earlier page.

  The scan examines at most #{@max_scan} entries per call. When it stops before
  the end of the log (the limit, or that bound), `next_cursor` is the position
  to resume from; `nil` means the log was read to its start.
  """
  @spec list(atom(), filters()) ::
          {:ok, %{entries: [entry()], next_cursor: String.t() | nil}}
          | {:error, :store_unavailable | :invalid_cursor}
  def list(instance, filters \\ %{}) do
    limit = filters |> Map.get(:limit) |> Kernel.||(@default_limit) |> max(1) |> min(@max_limit)

    with {:ok, range} <- range(filters) do
      scan(instance, range, filters, limit)
    end
  end

  defp scan(instance, range, filters, limit) do
    result =
      Store.fold(
        instance,
        :audit,
        range,
        %{scanned: 0, hits: [], found: 0, last: nil, stopped: false},
        fn key, value, acc -> step(key, value, acc, filters, limit) end,
        reverse: true
      )

    case result do
      {:ok, acc} ->
        {:ok,
         %{
           entries: Enum.reverse(acc.hits),
           next_cursor: if(acc.stopped, do: Base.url_encode64(acc.last, padding: false))
         }}

      {:error, _reason} ->
        {:error, :store_unavailable}
    end
  end

  defp step(_key, _value, %{scanned: @max_scan} = acc, _filters, _limit),
    do: {:halt, %{acc | stopped: acc.last != nil}}

  defp step(key, value, acc, filters, limit) do
    acc = %{acc | scanned: acc.scanned + 1, last: key}

    case decode(value) do
      {:ok, entry} ->
        if matches?(entry, filters), do: take(acc, entry, limit), else: {:cont, acc}

      :error ->
        {:cont, acc}
    end
  end

  defp take(acc, entry, limit) do
    acc = %{acc | hits: [entry | acc.hits], found: acc.found + 1}
    if acc.found >= limit, do: {:halt, %{acc | stopped: true}}, else: {:cont, acc}
  end

  @doc "Decode a stored entry."
  @spec decode(binary()) :: {:ok, entry()} | :error
  def decode(value) do
    case JSON.decode(value) do
      {:ok, %{} = entry} -> {:ok, entry}
      _ -> :error
    end
  end

  defp matches?(entry, filters) do
    field?(entry, "tenant", filters[:tenant]) and
      field?(entry, "action", filters[:action]) and
      field?(entry, "outcome", filters[:outcome]) and
      actor?(entry, filters[:actor])
  end

  defp field?(_entry, _key, nil), do: true
  defp field?(entry, key, wanted), do: entry[key] == wanted

  defp actor?(_entry, nil), do: true

  defp actor?(entry, wanted) do
    actor = entry["actor"] || %{}
    actor["subject"] == wanted or actor["username"] == wanted
  end

  # `{lower, upper}`, upper exclusive: the time bounds, narrowed by the cursor
  # (an entry key, exclusive, so a page resumes strictly older than the last).
  defp range(filters) do
    %{lo: lo, hi: hi} = Keys.family(:audit)
    since = filters |> Map.get(:since) |> Kernel.||(0) |> max(0) |> min(@max_ms)
    lower = <<"a:", since::64>>

    upper =
      case Map.get(filters, :until) do
        nil -> hi
        until when until >= @max_ms -> hi
        until -> <<"a:", max(until + 1, 0)::64>>
      end

    case cursor_key(Map.get(filters, :cursor)) do
      {:ok, nil} -> {:ok, {max(lower, lo), upper}}
      {:ok, key} -> {:ok, {max(lower, lo), min(upper, key)}}
      :error -> {:error, :invalid_cursor}
    end
  end

  defp cursor_key(nil), do: {:ok, nil}

  defp cursor_key(cursor) when is_binary(cursor) do
    case Base.url_decode64(cursor, padding: false) do
      {:ok, <<"a:", _rest::binary>> = key} -> {:ok, key}
      _ -> :error
    end
  end
end
