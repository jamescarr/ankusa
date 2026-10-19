defmodule Ankusa.Tenant.Gate do
  @moduledoc false

  # The tenants an erase is working on (`Ankusa.Tenant.Eraser`), readable by the
  # processes that must stop feeding them without a message to a singleton:
  #
  #   * `Ankusa.Queue.Writer` refuses a closed tenant's hooks (`tenant_erasing`),
  #     and drops its tenant-ordinal cache whenever the epoch moves, so an
  #     ordinal an erase retired is never used again;
  #   * `Ankusa.Edge.Quarantine` refuses to hold a closed tenant's hooks;
  #   * `Ankusa.Dispatch.Pipeline` does not claim a closed tenant's rows.
  #
  # The state is one `:persistent_term` per instance, written only by the erase
  # job (a handful of times per erase, never on a request path) and read with
  # `get/2` by everyone else. It is rebuilt at boot from the `m:erase:*` rows
  # (`Ankusa.Tenant.Eraser.init/1`), before any listener accepts a request.

  @empty %{epoch: 0, tenants: %{}, ords: MapSet.new()}

  @typedoc "`tenants` maps a closed tenant to its ordinal (`nil`: it has none); `ords` is those ordinals."
  @type state :: %{
          epoch: non_neg_integer(),
          tenants: %{String.t() => pos_integer() | nil},
          ords: MapSet.t(pos_integer())
        }

  @spec state(atom()) :: state()
  def state(instance), do: :persistent_term.get({__MODULE__, instance}, @empty)

  @doc "Whether `tenant` is being erased on this node."
  @spec closed?(atom(), String.t()) :: boolean()
  def closed?(instance, tenant) do
    %{tenants: tenants} = state(instance)
    map_size(tenants) > 0 and is_map_key(tenants, tenant)
  end

  @doc "Close `tenant` (ordinal `ord`, `nil` when it has none yet). Idempotent; a known ordinal is kept."
  @spec close(atom(), String.t(), pos_integer() | nil) :: :ok
  def close(instance, tenant, ord) do
    %{epoch: epoch, tenants: tenants} = state(instance)
    put(instance, epoch + 1, Map.update(tenants, tenant, ord, &(ord || &1)))
  end

  @doc "Open `tenant` again; the epoch moves, so caches drop what they knew."
  @spec open(atom(), String.t()) :: :ok
  def open(instance, tenant) do
    %{epoch: epoch, tenants: tenants} = state(instance)
    put(instance, epoch + 1, Map.delete(tenants, tenant))
  end

  defp put(instance, epoch, tenants) do
    ords = tenants |> Map.values() |> Enum.reject(&is_nil/1) |> MapSet.new()
    :persistent_term.put({__MODULE__, instance}, %{epoch: epoch, tenants: tenants, ords: ords})
  end
end
