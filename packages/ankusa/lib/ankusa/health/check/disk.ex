defmodule Ankusa.Health.Check.Disk do
  @moduledoc """
  `disk`, with `listener.ready_min_free_bytes` set (YAML `node.min_free_bytes`): the file
  system holding `data_dir` has at least that many bytes free. Below it the
  reason is `below_floor`, and the node is unready *before* the store hits
  `ENOSPC`, which is what lets the load balancer drain it while the operator
  still has room to act.

  Free space comes from `:disksup` (`:os_mon`), which core does not start and
  the server image does. A floor with no `:disksup` running, an answer that is
  not a disk entry, or one that takes longer than `@timeout_ms` (a hung mount)
  is reason `unmeasured`: a configured floor that cannot be checked fails, it
  is never silently skipped.
  """

  @behaviour Ankusa.Health.Check

  # `:os_mon` is optional: core never starts it, and only calls `:disksup` when
  # its process is running.
  @compile {:no_warn_undefined, :disksup}

  @timeout_ms 2_000

  @impl true
  def name, do: :disk

  @impl true
  def applies?(config), do: config.listener.ready_min_free_bytes != nil

  @impl true
  def gates?(_config), do: true

  @impl true
  def run(config) do
    case free_bytes(config.data_dir) do
      {:ok, free} when free >= config.listener.ready_min_free_bytes -> :ok
      {:ok, _free} -> {:failing, :below_floor}
      :error -> {:failing, :unmeasured}
    end
  end

  # `disksup` runs `df` for the path under its own process: ask from a task so a
  # `df` that never returns costs the probe `@timeout_ms`, not forever.
  @dialyzer {:no_unknown, free_bytes: 1}
  defp free_bytes(data_dir) do
    if is_pid(Process.whereis(:disksup)) do
      path = data_dir |> Path.expand() |> String.to_charlist()
      task = Task.async(fn -> :disksup.get_disk_info(path) end)

      case Task.yield(task, @timeout_ms) || Task.shutdown(task, :brutal_kill) do
        {:ok, [{_id, total_kib, available_kib, _capacity} | _]} when total_kib > 0 ->
          {:ok, available_kib * 1024}

        _other ->
          :error
      end
    else
      :error
    end
  end
end
