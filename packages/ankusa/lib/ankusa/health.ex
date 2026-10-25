defmodule Ankusa.Health do
  @moduledoc """
  Readiness, as `GET /ready` on the ingress, admin and metrics listeners answers
  it.

  `GET /health` is liveness: the listener answers, nothing more. `/ready` asks
  whether this node can ack a hook right now, by running every
  `Ankusa.Health.Check` that applies to its config and listing each one's state
  in the body:

      {"status": "ready", "instance": "default",
       "store": "ok", "backlog": "failing", "reasons": {"backlog": "over_threshold"}}

  A check's state is `"ok"`, `"failing"` or `"draining"`. A check that does not
  apply to the node (no store, no threshold set, another `wal` mode) has no key.
  `reasons` appears only while some check is not `"ok"` and names why, one atom
  string per check from a fixed set (never an error term):

    * `store`: the failure class (`"no_space"`, `"io_error"`, `"read_only"`,
      `"corruption"`, `"busy"`, `"timeout"`, `"not_running"`, `"other"`; see
      `Ankusa.Store.failure_class/1`). A full disk, a store that is closed or
      reopening, or one that does not answer fails it; it passes again on its
      own once writes succeed.
    * `intake` (`wal: :dual`): `"down"` or `"recovering"`.
    * `sinks` (`wal: :none`): `"publish_failed"`, a sink failed its last publish
      within `listener.ready_sink_error_window_ms`.
    * `disk` (`listener.ready_min_free_bytes`): `"below_floor"` or `"unmeasured"`.
    * `backlog` (`queue.max_oldest_due_ms`): `"over_threshold"` or `"unavailable"`.
    * `backup` (`backup.max_age_ms`): `"stale"`.
    * `drain` (only while the node is being stopped): `"shutting_down"`.

  A stop is announced first: `drain/1` adds a `drain` key (`"draining"`, reason
  `"shutting_down"`) to every body, so a probe sees the flip before the
  listener closes (`Ankusa.Health.Check.Drain`). The answer is `503` with
  `status: "draining"` while it lasts, and `503` with `status: "unavailable"`
  when any other check that *gates* is not ok: `store`, `intake`, `sinks` and
  `disk` always, `backup` with `backup.require_fresh`. `backlog` never does: it
  is a label, so a node that is behind stays in the load balancer and the
  alert, not the probe, says so. A node with none of these checks (one that
  runs only the claim check gateway, so no store) is ready while it runs.
  """

  alias Ankusa.Health.Check

  @type state :: String.t()

  @type body :: %{
          required(:status) => String.t(),
          required(:instance) => String.t(),
          optional(:reasons) => %{atom() => String.t()},
          optional(atom()) => state() | String.t() | %{atom() => String.t()}
        }

  @spec ready(atom()) :: {:ok, body()} | {:error, body()}
  def ready(instance), do: evaluate(instance, Check.all())

  @doc """
  Start answering `/ready` with `503` and `status: "draining"`: this node is
  being stopped (`Ankusa.Instance.Drain`), so a load balancer should send it
  nothing new. It stays that way until the instance starts again (its supervisor
  clears it when it initialises).
  """
  @spec drain(atom()) :: :ok
  def drain(instance), do: :persistent_term.put(drain_key(instance), true)

  @doc "Whether `drain/1` has been called for `instance` since it last started."
  @spec draining?(atom()) :: boolean()
  def draining?(instance), do: :persistent_term.get(drain_key(instance), false)

  @doc false
  # Erasing a `:persistent_term` scans every process (a global GC), so the key is
  # only erased when it is there: a node that never drained pays nothing.
  @spec undrain(atom()) :: :ok
  def undrain(instance) do
    if draining?(instance), do: :persistent_term.erase(drain_key(instance))
    :ok
  end

  defp drain_key(instance), do: {__MODULE__, :draining, instance}

  @doc false
  # `ready/1` over an explicit list of checks, so a test can run one that no
  # registered check produces (a `"draining"` state).
  @spec evaluate(atom(), [module()]) :: {:ok, body()} | {:error, body()}
  def evaluate(instance, checks) do
    config = Ankusa.config(instance)

    results =
      for check <- checks, check.applies?(config), do: {check, Check.run(check, config)}

    body =
      Enum.reduce(results, %{status: "ready", instance: to_string(instance)}, fn {check, result},
                                                                                 body ->
        Map.put(body, check.name(), state(result))
      end)

    body =
      case for({check, result} <- results, result != :ok, do: {check.name(), reason(result)}) do
        [] -> body
        reasons -> Map.put(body, :reasons, Map.new(reasons))
      end

    gating = for {check, result} <- results, result != :ok, check.gates?(config), do: result

    cond do
      gating == [] -> {:ok, body}
      Enum.any?(gating, &match?({:draining, _}, &1)) -> {:error, %{body | status: "draining"}}
      true -> {:error, %{body | status: "unavailable"}}
    end
  end

  defp state(:ok), do: "ok"
  defp state({:failing, _reason}), do: "failing"
  defp state({:draining, _reason}), do: "draining"

  defp reason({_state, reason}), do: Atom.to_string(reason)
end
