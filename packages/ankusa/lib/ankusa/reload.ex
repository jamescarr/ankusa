defmodule Ankusa.Reload do
  @moduledoc """
  Changing a running instance's config without a restart: the part of a config
  reload that core owns.

  What a reload *is* (re-reading a file, resolving secrets, deciding which keys
  changed and whether they may) belongs to whoever loads the config; for the
  image that is `AnkusaServer.Reload`. This module is the seam between that and
  the instance:

    * `register/2` / `run/2` — the loader registers a reload function for an
      instance, and `POST /v1/config/reload` (`Ankusa.Admin.Router`) calls it.
      An instance nobody registered one for answers `{:error, :unsupported}`.
    * `apply_config/3` — what the loader calls once it holds a validated
      `%Ankusa.Config{}`: the *reloadable set* is taken from it, everything else
      stays as it is, and the result is applied atomically.

  ## The reloadable set

  | Field | How it takes effect |
  | --- | --- |
  | `rate_limits` (all but `scope`) | read on every request |
  | `dispatch.concurrency`, `dispatch.sink_concurrency`, `dispatch.tenants` | `Ankusa.Dispatch.Pipeline.reconfigure/1`: the next scheduling step |
  | `metrics.detail` | the instance's metric definitions are rebuilt (`Ankusa.Metrics` restarts: its counters start from zero) |
  | `admin.gauge_interval_ms` | the gauge poller restarts with the new period |
  | `routes.seed` | the difference is applied through `Ankusa.Routes`: a changed or new seed route is replaced, a route that left the seed is deleted; routes created through the API are left alone |
  | static `sources` | `Ankusa.SourceStore.Static.with_sources/2`: the next `fetch/2` sees them |

  Every other field of the running config is kept, whatever `next` says; the
  loader refuses a reload that changes one (`{:error, {:not_reloadable, keys}}`).

  ## Atomicity and audit

  The merged config is validated with the checks the instance boots under
  before anything is touched, so a reload that would not boot is refused with
  nothing applied. One audit row (`config.reload`, `detail.keys`) brackets the
  application; its `result` is `ok` or `failed`. A reload the trail cannot record
  is not made (`:audit_backlog`, `:store_unavailable`). The seed routes are
  applied one by one through `Ankusa.Routes` (each its own audited change), so a
  seed route that conflicts with a route made through the API fails the reload
  part-way: the routes already applied stay, the config is not replaced, and the
  same reload can be run again.
  """

  require Logger

  alias Ankusa.Audit
  alias Ankusa.Audit.Context
  alias Ankusa.Config
  alias Ankusa.Dispatch.Pipeline
  alias Ankusa.Instance.Isolated
  alias Ankusa.Routes
  alias Ankusa.SourceStore

  @typedoc "What a reload did: the dotted keys that changed, the source ids whose spec changed, secrets that kept their old value."
  @type summary :: %{
          keys: [String.t()],
          sources: [String.t()],
          kept: [%{path: String.t(), reason: String.t()}]
        }

  @type error ::
          {:not_reloadable, [String.t()]}
          | {:invalid_config, String.t()}
          | :unsupported
          | :audit_backlog
          | :store_unavailable

  @doc """
  Register `fun` as `instance`'s reload: it takes the `Ankusa.Audit.Context` of
  whoever asked and returns `{:ok, summary}` or `{:error, error}`.
  """
  @spec register(atom(), (Context.t() -> {:ok, summary()} | {:error, error()})) :: :ok
  def register(instance, fun) when is_atom(instance) and is_function(fun, 1),
    do: :persistent_term.put({__MODULE__, instance}, fun)

  @doc "Run `instance`'s registered reload on behalf of `context`."
  @spec run(atom(), Context.t()) :: {:ok, summary()} | {:error, error()}
  def run(instance, %Context{} = context) do
    case :persistent_term.get({__MODULE__, instance}, nil) do
      nil -> {:error, :unsupported}
      fun -> fun.(context)
    end
  end

  @doc """
  Apply the reloadable fields of `next` to the running `instance`.

  Options: `:audit` (the `Ankusa.Audit.Context`, default `Context.system/0`) and
  `:keys` (the dotted config keys the loader found changed, recorded in the audit
  row). Returns `{:ok, %{sources: ids}}` with the source ids whose spec changed,
  or `{:error, {:invalid_config, message}}` and nothing applied when the merged
  config would not boot (or a seed route is refused part-way, see above).
  """
  @spec apply_config(atom(), Config.t(), keyword()) ::
          {:ok, %{sources: [String.t()]}} | {:error, error()}
  def apply_config(instance, %Config{} = next, opts \\ []) do
    context = Keyword.get(opts, :audit) || Context.system()
    keys = Keyword.get(opts, :keys, [])
    current = Ankusa.config(instance)

    with {:ok, merged, sources} <- merge(current, next),
         {:ok, seed_ops} <- seed_ops(instance, current, merged) do
      attrs = %{action: "config.reload", resource: "config", detail: %{keys: keys}}

      audited(instance, context, attrs, fn ->
        case apply_seed(instance, seed_ops, context) do
          :ok ->
            :ok = Ankusa.put_config(merged)
            reconfigure(instance, current, merged)
            {{:ok, %{sources: sources}}, %{result: "ok"}}

          {:error, message} ->
            {{:error, {:invalid_config, message}}, %{result: "failed"}}
        end
      end)
    end
  end

  # ── merge and check ─────────────────────────────────────────────────────────

  defp merge(%Config{} = current, %Config{} = next) do
    {source_store, sources} = merge_sources(current.source_store, next.source_store)

    merged = %Config{
      current
      | rate_limits: %{next.rate_limits | scope: current.rate_limits.scope},
        dispatch: merge_dispatch(current.dispatch, next.dispatch),
        metrics: %{current.metrics | detail: next.metrics.detail},
        admin: %{current.admin | gauge_interval_ms: next.admin.gauge_interval_ms},
        routes: %{current.routes | seed: next.routes.seed},
        source_store: source_store
    }

    {merged, sources} = check_sources(merged, sources)
    check(merged)
    {:ok, merged, sources}
  rescue
    error in ArgumentError -> {:error, {:invalid_config, error.message}}
  end

  defp merge_dispatch(current, next) do
    Map.merge(
      current,
      Map.take(next, [:concurrency, :sink_concurrency, :tenants])
    )
  end

  # Static sources are the only ones a reload owns; any other store keeps its
  # sources elsewhere and the file's are seeds.
  defp merge_sources({SourceStore.Static, _} = current, {SourceStore.Static, next}),
    do: {current, {:publish, Keyword.get(next, :sources, %{})}}

  defp merge_sources(current, _next), do: {current, :keep}

  defp check_sources(merged, :keep), do: {merged, []}

  defp check_sources(merged, {:publish, sources}),
    do: SourceStore.Static.with_sources(merged, sources)

  # The checks `Ankusa.Instance` makes at boot for what the reloadable set touches.
  defp check(merged) do
    Pipeline.validate_config!(merged)
    Ankusa.Edge.RateLimiter.validate_config!(merged)
    Routes.validate_config!(merged)
  end

  # ── routes.seed ─────────────────────────────────────────────────────────────

  defp seed_ops(instance, current, merged) do
    old = current.routes.seed
    new = merged.routes.seed

    cond do
      old == new -> {:ok, []}
      Routes.meta(instance) == nil -> {:ok, []}
      true -> diff_seed(old, new)
    end
  end

  defp diff_seed(old, new) do
    with {:ok, old} <- index_seed(old),
         {:ok, new} <- index_seed(new) do
      deletes = for {id, _attrs} <- old, not is_map_key(new, id), do: {:delete, id}

      replaces =
        for {id, attrs} <- new, Map.get(old, id) != attrs, do: {:replace, id, attrs}

      {:ok, Enum.sort_by(deletes, &elem(&1, 1)) ++ Enum.sort_by(replaces, &elem(&1, 1))}
    end
  end

  # A seed route without an id gets a fresh one every time it is read, so there is
  # nothing to compare a changed seed against.
  defp index_seed(seed) do
    seed
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, %{}}, fn {attrs, index}, {:ok, acc} ->
      case attrs do
        %{"id" => id} when is_binary(id) ->
          {:cont, {:ok, Map.put(acc, id, attrs)}}

        _ ->
          {:halt,
           {:error,
            {:invalid_config, "routes.seed[#{index}] needs an id to be reloaded (set `id:`)"}}}
      end
    end)
  end

  defp apply_seed(instance, ops, context) do
    Enum.reduce_while(ops, :ok, fn op, :ok ->
      case apply_seed_op(instance, op, context) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, seed_message(op, reason)}}
      end
    end)
  end

  defp apply_seed_op(instance, {:delete, id}, context) do
    case Routes.delete(instance, id, audit: context) do
      :ok -> :ok
      {:error, :not_found} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp apply_seed_op(instance, {:replace, id, attrs}, context) do
    case Routes.replace(instance, id, attrs, audit: context) do
      {:ok, _route} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp seed_message(op, {:invalid, field, message}),
    do: "routes.seed route #{elem(op, 1)}: #{field} #{message}"

  defp seed_message(op, {:conflict, _other}),
    do: "routes.seed route #{elem(op, 1)} conflicts with another route"

  defp seed_message(op, reason),
    do:
      "routes.seed route #{elem(op, 1)} was not applied (#{inspect(reason)}); run the reload again"

  # ── audit ───────────────────────────────────────────────────────────────────

  # A node with no store has no trail (a `claim_check`-only node with no admin
  # API): the reload is made and logged.
  defp audited(instance, context, attrs, fun) do
    if Ankusa.whereis(instance, :audit) do
      Audit.record(instance, context, attrs, fun)
    else
      {reply, _update} = fun.()
      reply
    end
  end

  # ── taking effect ───────────────────────────────────────────────────────────

  defp reconfigure(instance, current, merged) do
    if Map.take(current.dispatch, [:concurrency, :sink_concurrency, :tenants]) !=
         Map.take(merged.dispatch, [:concurrency, :sink_concurrency, :tenants]) do
      Pipeline.reconfigure(instance)
    end

    if current.metrics.detail != merged.metrics.detail,
      do: restart_metrics_child(instance, {Ankusa.Metrics, instance})

    if current.admin.gauge_interval_ms != merged.admin.gauge_interval_ms,
      do: restart_metrics_child(instance, {Ankusa.Metrics.Gauges, instance})

    :ok
  end

  # Both children read the config when they start. A metrics domain that is down
  # (or never started) picks the new config up when it comes back.
  defp restart_metrics_child(instance, id) do
    with sup when is_pid(sup) <- Isolated.subtree(instance, :metrics),
         :ok <- Supervisor.terminate_child(sup, id),
         {:ok, _pid} <- Supervisor.restart_child(sup, id) do
      :ok
    else
      other ->
        Logger.warning(
          "[ankusa] config reload: #{inspect(elem(id, 0))} was not restarted (#{inspect(other)}); " <>
            "it applies the new config the next time it starts"
        )
    end
  end
end
