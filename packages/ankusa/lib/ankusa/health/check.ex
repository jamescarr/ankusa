defmodule Ankusa.Health.Check do
  @moduledoc """
  One thing `GET /ready` asks about (`Ankusa.Health`).

  A check says whether it `applies?/1` to this node's config (a node with no
  store has no `store` check, a threshold left `nil` switches its check off),
  whether a failure of it makes the node unready (`gates?/1`), and `run/1`s
  itself. `all/0` is the registry, in the order the body lists them:

    * `Ankusa.Health.Check.Drain`: the node is being stopped
      (`Ankusa.Health.drain/1`). Only present while it is; reports `draining`. Gates.
    * `Ankusa.Health.Check.Store`: the store takes a synced write. Gates.
    * `Ankusa.Health.Check.Intake`: the `wal: :dual` intake log answers. Gates.
    * `Ankusa.Health.Check.Sinks`: under `wal: :none`, no sink failed its last
      publish within `listener.ready_sink_error_window_ms`. Gates.
    * `Ankusa.Health.Check.Disk`: `listener.ready_min_free_bytes` of `data_dir`'s file
      system are free. Gates.
    * `Ankusa.Health.Check.Backlog`: the oldest due row has waited no longer than
      `queue.max_oldest_due_ms`. A label: never makes the node unready.
    * `Ankusa.Health.Check.Backup`: the last backup is younger than
      `backup.max_age_ms`. Gates only with `backup.require_fresh`.

  `run/1` answers `:ok`, `{:failing, reason}` or `{:draining, reason}`, the
  `reason` an atom from a fixed set per check (it reaches the `/ready` body, so
  it never carries an error term). A check must not hold a request longer than a
  readiness probe waits: bound every call it makes.
  """

  require Logger

  alias Ankusa.Config

  @type result :: :ok | {:failing, atom()} | {:draining, atom()}

  @doc "The key the check's state has in the `/ready` body."
  @callback name() :: atom()

  @doc "Whether this node's config has the check run at all."
  @callback applies?(Config.t()) :: boolean()

  @doc "Whether a check that is not `:ok` makes the node unready (`503`)."
  @callback gates?(Config.t()) :: boolean()

  @callback run(Config.t()) :: result()

  @doc "Every check, in body order."
  @spec all() :: [module()]
  def all do
    [
      __MODULE__.Drain,
      __MODULE__.Store,
      __MODULE__.Intake,
      __MODULE__.Sinks,
      __MODULE__.Disk,
      __MODULE__.Backlog,
      __MODULE__.Backup
    ]
  end

  @doc """
  Run `check`. A check that raises, exits or throws is failing with reason
  `:error`: readiness never crashes the probe, and a check that cannot answer
  must not read as healthy.
  """
  @spec run(module(), Config.t()) :: result()
  def run(check, %Config{} = config) do
    check.run(config)
  rescue
    exception ->
      Logger.warning(
        "[ankusa] readiness check #{check.name()} failed: " <>
          Exception.format(:error, exception, __STACKTRACE__)
      )

      {:failing, :error}
  catch
    kind, reason ->
      Logger.warning(
        "[ankusa] readiness check #{check.name()} failed: #{inspect({kind, reason})}"
      )

      {:failing, :error}
  end
end
