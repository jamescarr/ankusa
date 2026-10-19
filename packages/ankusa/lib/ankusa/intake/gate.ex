defmodule Ankusa.Intake.Gate do
  @moduledoc """
  The intake breaker of `wal: :dual` (ADR 0002, section 7). Under dual there is
  no degraded mode: a node that cannot confirm in the log refuses, it never acks
  on local fsync alone.

  `closed` admits every batch. It opens after `intake.breaker_failures`
  consecutive failed publish attempts, or when the adapter's `ready?/1` says no.
  `open` refuses until `intake.probe_ms` has passed, then lets **one** batch
  through (`half_open`): its confirm closes the breaker, its failure reopens it.
  The probe is a real batch, not a probe record.

  The state lives in the pending set's ETS table (`Ankusa.Intake.Pending`), which
  the writer reads inside its own process: no `:persistent_term.put`, no call,
  nothing on the request path but a table read (ground rule 6).
  """

  alias Ankusa.Intake.Pending

  @doc "`:ok` to publish a batch now, `{:error, :open}` to refuse before touching either half."
  @spec admit(Pending.table(), Ankusa.Config.t(), integer()) :: :ok | {:error, :open}
  def admit(t, config, now) do
    case Pending.meta(t, :gate, :closed) do
      :closed ->
        :ok

      :half_open ->
        # one batch at a time: the probe is already out
        {:error, :open}

      :open ->
        opened_at = Pending.meta(t, :gate_opened_at, now)

        if now - opened_at >= config.intake.probe_ms do
          Pending.put_meta(t, :gate, :half_open)
          :ok
        else
          {:error, :open}
        end
    end
  end

  @doc "Records a publish attempt's outcome (`true` = confirmed)."
  @spec record(Pending.table(), Ankusa.Config.t(), boolean(), integer()) :: :ok
  def record(t, _config, true, _now) do
    Pending.put_meta(t, :gate, :closed)
    Pending.put_meta(t, :gate_failures, 0)
    :ok
  end

  def record(t, config, false, now) do
    failures = Pending.meta(t, :gate_failures, 0) + 1
    Pending.put_meta(t, :gate_failures, failures)

    if Pending.meta(t, :gate, :closed) == :half_open or failures >= config.intake.breaker_failures,
      do: open(t, now),
      else: :ok
  end

  @doc "The adapter says it is not ready: open the breaker (the reconciler's tick)."
  @spec not_ready(Pending.table(), integer()) :: :ok
  def not_ready(t, now) do
    if Pending.meta(t, :gate, :closed) == :closed, do: open(t, now), else: :ok
  end

  @doc "The probe batch never published (it turned out to have nothing to publish)."
  @spec abort_probe(Pending.table(), integer()) :: :ok
  def abort_probe(t, now) do
    if Pending.meta(t, :gate, :closed) == :half_open, do: open(t, now - 1_000_000_000), else: :ok
  end

  @doc "`:closed`, `:open` or `:half_open`."
  @spec state(Pending.table()) :: :closed | :open | :half_open
  def state(t), do: Pending.meta(t, :gate, :closed)

  defp open(t, now) do
    Pending.put_meta(t, :gate, :open)
    Pending.put_meta(t, :gate_opened_at, now)
    :ok
  end
end
