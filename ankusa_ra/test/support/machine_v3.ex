defmodule Ankusa.WAL.Ra.MachineV3 do
  @moduledoc """
  A later machine version, for the machine-upgrade drill: the *only* difference
  is the version it advertises, so `which_module/1` must map the version the
  cluster recorded (2, the real machine's) to the real machine and this
  module's own version to this module.

  Ra's default `machine_upgrade_strategy` is `all`: a member that does not
  support the newer version will not apply its commands, which is what makes a
  rolling upgrade safe — and what the drill proves.

  It is one version above `Ankusa.WAL.Ra.Machine`, not a fixed 2: the real
  machine bumps its version when its state changes shape, and a fake "next"
  version that collides with the current one would make the drill assert
  nothing.
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
  def version, do: 3

  @impl true
  def which_module(2), do: Machine
  def which_module(_version), do: __MODULE__
end
