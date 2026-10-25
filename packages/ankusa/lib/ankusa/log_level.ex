defmodule Ankusa.LogLevel do
  @moduledoc """
  The VM's `Logger` level, changed while the node runs.

  `set/1` is what `PUT /v1/log-level` (`Ankusa.Admin.Router`, scope
  `config:write`) calls, and what `ankusa_server` calls at boot with
  `log.level`: one place decides which levels exist. The change is global and
  immediate (`Logger.configure/1` moves the primary level), needs no restart and
  is not persisted: a restarted node logs at the level its config file names.

  Only `debug`, `info`, `warning` and `error` are accepted, the same set
  `log.level` takes.
  """

  @levels [:debug, :info, :warning, :error]
  @by_name Map.new(@levels, &{Atom.to_string(&1), &1})

  @typedoc "A level this module accepts."
  @type level :: :debug | :info | :warning | :error

  @doc "Every accepted level, most verbose first."
  @spec levels() :: [level()]
  def levels, do: @levels

  @doc "The level now in force."
  @spec get() :: Logger.level()
  def get, do: Logger.level()

  @doc """
  Read `level` (an atom, or its name as a string) as an accepted level.
  """
  @spec parse(term()) :: {:ok, level()} | :error
  def parse(level) when level in @levels, do: {:ok, level}

  def parse(level) when is_binary(level) do
    case Map.fetch(@by_name, level) do
      {:ok, _level} = ok -> ok
      :error -> :error
    end
  end

  def parse(_other), do: :error

  @doc """
  Make `level` the VM's log level. Returns the level that was in force, so the
  caller can report (and audit) the change.
  """
  @spec set(term()) :: {:ok, previous :: Logger.level()} | {:error, :invalid_level}
  def set(level) do
    case parse(level) do
      {:ok, level} ->
        previous = Logger.level()
        Logger.configure(level: level)
        {:ok, previous}

      :error ->
        {:error, :invalid_level}
    end
  end
end
