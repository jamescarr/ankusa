defmodule Ankusa.WAL.Ra.MachineV2 do
  @moduledoc """
  A second machine version, for the machine-upgrade drill: the *only* difference
  is the version it advertises, so `which_module/1` must map version 1 to the
  real machine and version 2 to this module.

  Ra's default `machine_upgrade_strategy` is `all`: a member that does not
  support version 2 will not apply a version-2 machine's commands, which is what
  makes a rolling upgrade safe — and what the drill proves.
  """

  @behaviour :ra_machine

  alias Ankusa.WAL.Ra.Machine

  @impl true
  def init(config), do: Machine.init(config)

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
  def which_module(1), do: Machine
  def which_module(_version), do: __MODULE__
end
