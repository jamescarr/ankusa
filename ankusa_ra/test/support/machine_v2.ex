defmodule Ankusa.WAL.Ra.MachineV2 do
  @moduledoc """
  A later machine version, for the machine-upgrade drill: the *only* difference
  is the version it advertises, so `which_module/1` maps the version the cluster
  recorded (`1`, the real machine's) to the real machine and this module's own
  version (`2`) to this module.

  Ra's default `machine_upgrade_strategy` is `all`: a member that does not
  support the newer version will not apply its commands, which is what makes a
  rolling upgrade safe — and what the drill proves.
  """

  @behaviour :ra_machine

  alias Ankusa.WAL.Ra.Machine

  @impl true
  def init(config), do: Machine.init(config)

  # A command only this version understands, for the upgrade drill: it must not
  # be applied until every member supports version 2.
  def apply(_meta, {:v2_ping}, state), do: {state, :pong, []}

  @impl true
  def apply(meta, command, state), do: Machine.apply(meta, command, state)

  @impl true
  def live_indexes(state), do: Machine.live_indexes(state)

  @impl true
  def overview(state), do: Machine.overview(state)

  @impl true
  def init_aux(name), do: Machine.init_aux(name)

  @impl true
  def handle_aux(raft_state, kind, command, aux, internal) do
    Machine.handle_aux(raft_state, kind, command, aux, internal)
  end

  @impl true
  def version, do: 2

  @impl true
  def which_module(2), do: __MODULE__
  def which_module(version) when version in [0, 1], do: Machine
end
